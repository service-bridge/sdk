package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// An ambiguous status can follow a committed effect and a lost response.
func TestDispatchedErrorsNeverRetryEvenWithKey(t *testing.T) {
	for _, code := range []codes.Code{codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted, codes.Internal, codes.Aborted, codes.Unknown} {
		for _, keyed := range []bool{false, true} {
			err := status.Error(code, "effect may already have committed")
			if Classify(err) != RetryNever || Retryable(err, keyed) {
				t.Fatalf("unsafe replay for %v keyed=%v", code, keyed)
			}
		}
	}
}

func TestOnlyLocalSelectionProofRetries(t *testing.T) {
	err := fmt.Errorf("selection: %w", &SelectionError{Reason: status.Error(codes.Unavailable, "no candidates")})
	if Classify(err) != RetryAlways || !Retryable(err, false) {
		t.Fatalf("proven pre-dispatch failure should retry: %v", err)
	}
}

func TestBusinessCodesAreNeverRetried(t *testing.T) {
	business := []codes.Code{
		codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition,
		codes.OutOfRange, codes.Unimplemented, codes.Canceled,
	}
	for _, code := range business {
		t.Run(code.String(), func(t *testing.T) {
			err := status.Error(code, "decided")
			if got := Classify(err); got != RetryNever {
				t.Fatalf("Classify(%v) = %v, want %v", code, got, RetryNever)
			}
			if Retryable(err, true) {
				t.Fatalf("%v must not be retried even with an idempotency key", code)
			}
		})
	}
}

// TestHandlerErrorIsNeverRetried: the handler ran and decided. It carries no
// wire code, and a code-less error must not fall through to the UNKNOWN branch.
func TestHandlerErrorIsNeverRetried(t *testing.T) {
	err := handlerError("VALIDATION", "amount must be positive")

	if got := Classify(err); got != RetryNever {
		t.Fatalf("Classify(handler error) = %v, want %v", got, RetryNever)
	}
	if Retryable(err, true) {
		t.Fatal("a handler business error must not be retried, key or not")
	}
	var he *HandlerError
	if !errors.As(err, &he) || he.Code != "VALIDATION" {
		t.Fatalf("handler error must stay inspectable, got %#v", err)
	}
}

func TestLocalErrorsWithoutAWireCodeAreNeverRetried(t *testing.T) {
	for _, err := range []error{ErrNoLease, ErrDirectClosed, errors.New("boom")} {
		if got := Classify(err); got != RetryNever {
			t.Fatalf("Classify(%v) = %v, want %v", err, got, RetryNever)
		}
	}
}

func TestWrappedStatusKeepsItsClass(t *testing.T) {
	err := fmt.Errorf("rpc: direct unary Ping to 10.0.0.1:14446: %w", status.Error(codes.Unavailable, "conn refused"))
	if got := Classify(err); got != RetryNever {
		t.Fatalf("wrapping cannot create pre-dispatch proof: got %v", got)
	}
}

func TestContextDeadlineIsTreatedAsTheWireDeadline(t *testing.T) {
	if got := Classify(context.DeadlineExceeded); got != RetryNever {
		t.Fatalf("Classify(context.DeadlineExceeded) = %v, want %v", got, RetryNever)
	}
	if got := Classify(context.Canceled); got != RetryNever {
		t.Fatalf("Classify(context.Canceled) = %v, want %v", got, RetryNever)
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

func TestRetryClassNamesItself(t *testing.T) {
	cases := map[RetryClass]string{
		RetryNever:  "never",
		RetryAlways: "always",
	}
	for class, want := range cases {
		if got := class.String(); got != want {
			t.Fatalf("RetryClass(%d).String() = %q, want %q", class, got, want)
		}
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
