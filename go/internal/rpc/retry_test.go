package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// An ambiguous status can follow a committed effect and a lost response: no
// status without the not-dispatched trailer authorizes a replay, key or not.
func TestDispatchedErrorsAreNeverPreDispatch(t *testing.T) {
	for _, code := range []codes.Code{
		codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted, codes.Internal,
		codes.Aborted, codes.Unknown, codes.InvalidArgument, codes.NotFound, codes.PermissionDenied,
		codes.FailedPrecondition, codes.Canceled,
	} {
		if PreDispatch(status.Error(code, "effect may already have committed")) {
			t.Fatalf("unsafe replay for %v", code)
		}
	}
}

func TestSelectionFailureIsPreDispatch(t *testing.T) {
	err := fmt.Errorf("selection: %w", &SelectionError{Reason: ErrNoCandidates})
	if !PreDispatch(err) {
		t.Fatalf("a selection failure is proven pre-dispatch: %v", err)
	}
}

func TestNotDispatchedTrailerIsPreDispatchProof(t *testing.T) {
	trailer := metadata.Pairs(NotDispatchedKey, "1")
	err := markNotDispatched(status.Error(codes.Unavailable, "draining"), trailer, true)
	if !PreDispatch(err) || !directPreDispatch(err) {
		t.Fatalf("a status with the trailer is pre-dispatch proof: %v", err)
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("the status must survive the marker, got %v", status.Code(err))
	}
	viaProxy := markNotDispatched(status.Error(codes.ResourceExhausted, "busy"), trailer, false)
	if !PreDispatch(viaProxy) || directPreDispatch(viaProxy) {
		t.Fatal("a proxied proof must not switch transports")
	}
	if PreDispatch(markNotDispatched(status.Error(codes.Unavailable, "x"), metadata.MD{}, true)) {
		t.Fatal("no trailer, no proof")
	}
	if markNotDispatched(nil, trailer, true) != nil {
		t.Fatal("nil stays nil")
	}
}

func TestHandlerErrorIsNeverPreDispatch(t *testing.T) {
	err := handlerError("VALIDATION", "amount must be positive")
	if PreDispatch(err) {
		t.Fatal("a handler business error must not be retried")
	}
	var he *HandlerError
	if !errors.As(err, &he) || he.Code != "VALIDATION" {
		t.Fatalf("handler error must stay inspectable, got %#v", err)
	}
	if he.Error() != "VALIDATION: amount must be positive" || (&HandlerError{Code: "X"}).Error() != "X" {
		t.Fatalf("handler error text: %q", he.Error())
	}
}

func TestLocalErrorsAreNotPreDispatch(t *testing.T) {
	for _, err := range []error{ErrNoLease, ErrDirectClosed, errors.New("boom"), context.DeadlineExceeded, context.Canceled} {
		if PreDispatch(err) {
			t.Fatalf("PreDispatch(%v) = true", err)
		}
	}
}

func TestBackoffGrowsExponentiallyAndClamps(t *testing.T) {
	// Random pinned at the midpoint cancels the jitter term exactly.
	p := RetryPolicy{
		MaxAttempts: 10, BaseMs: 200, MaxMs: 5000, Multiplier: 2, JitterRatio: 0.3,
		Random: func() float64 { return 0.5 },
	}.normalized()

	want := []int64{200, 400, 800, 1600, 3200, 5000, 5000}
	for attempt, expect := range want {
		if got := p.BackoffMs(attempt); got != expect {
			t.Fatalf("BackoffMs(%d) = %d, want %d", attempt, got, expect)
		}
	}
}

func TestBackoffJitterStaysInsideItsBand(t *testing.T) {
	base := RetryPolicy{MaxAttempts: 3, BaseMs: 1000, MaxMs: 5000, Multiplier: 2, JitterRatio: 0.3}

	low := base
	low.Random = func() float64 { return 0 }
	if got := low.normalized().BackoffMs(0); got != 700 {
		t.Fatalf("lower jitter bound = %d, want 700", got)
	}

	high := base
	high.Random = func() float64 { return 1 }
	if got := high.normalized().BackoffMs(0); got != 1300 {
		t.Fatalf("upper jitter bound = %d, want 1300", got)
	}
}

func TestNormalizedFillsZeroFields(t *testing.T) {
	p := RetryPolicy{}.normalized()

	if p.MaxAttempts != DefaultMaxAttempts || p.BaseMs != DefaultBaseMs || p.MaxMs != DefaultMaxMs {
		t.Fatalf("zero policy did not pick up the defaults: %#v", p)
	}
	if p.Random == nil {
		t.Fatal("zero policy must get a randomness source")
	}
	if got := p.BackoffMs(0); got <= 0 {
		t.Fatalf("normalized policy must never wait zero, got %d", got)
	}
}

func TestSleepMsReturnsAsSoonAsTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := sleepMs(ctx, 10_000)
	if err == nil {
		t.Fatal("sleepMs must fail on a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepMs error must unwrap to the context cause, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("sleepMs waited %v after cancellation", elapsed)
	}
}

func TestSleepMsChecksTheContextEvenAtZeroDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := sleepMs(ctx, 0); err == nil {
		t.Fatal("a zero delay must still observe a cancelled context")
	}
	if err := sleepMs(context.Background(), 0); err != nil {
		t.Fatalf("a zero delay on a live context must not fail: %v", err)
	}
}

func TestNormalizedClampsOutOfRangeFields(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts: -1, BaseMs: -5, MaxMs: 1, Multiplier: 0.1, JitterRatio: 7,
	}.normalized()

	if p.MaxAttempts != DefaultMaxAttempts || p.BaseMs != DefaultBaseMs {
		t.Fatalf("negative fields must fall back to the defaults: %#v", p)
	}
	if p.MaxMs < p.BaseMs {
		t.Fatalf("a ceiling below the floor is not a ceiling: MaxMs=%d BaseMs=%d", p.MaxMs, p.BaseMs)
	}
	if p.Multiplier < 1 {
		t.Fatalf("a multiplier below 1 would shrink the delay each attempt, got %v", p.Multiplier)
	}
	if p.JitterRatio > 1 {
		t.Fatalf("a jitter ratio above 1 could produce a negative delay, got %v", p.JitterRatio)
	}
	if got := p.BackoffMs(0); got < 0 {
		t.Fatalf("BackoffMs must never be negative, got %d", got)
	}
}
