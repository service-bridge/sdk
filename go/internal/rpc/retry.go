package rpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Retry policy defaults. They mirror the Node SDK so a mixed-language fleet
// backs off on the same schedule.
const (
	DefaultMaxAttempts = 3
	DefaultBaseMs      = 200
	DefaultMaxMs       = 5000
	DefaultMultiplier  = 2.0
	DefaultJitterRatio = 0.3
)

// NotDispatchedKey is the trailer a callee sets on every rejection it makes
// before the handler runs. A status carrying it is proof the call never
// executed, which is the only thing that makes a retry on another instance safe.
const NotDispatchedKey = "x-sb-not-dispatched"

// ErrPeerUnreachable means the channel to the picked callee did not become
// ready before the request could be written: nothing reached the handler.
var ErrPeerUnreachable = errors.New("rpc: callee channel is not ready")

// NotDispatchedError marks a failure proven to have happened before the
// callee's handler ran: the channel never became ready, or the callee answered
// with the not-dispatched trailer. Direct reports that the proof came from the
// direct path, which is what lets transport auto fall back to the proxy.
type NotDispatchedError struct {
	Err    error
	Direct bool
}

func (e *NotDispatchedError) Error() string { return e.Err.Error() }

func (e *NotDispatchedError) Unwrap() error { return e.Err }

// PreDispatch reports whether err proves the call never reached a handler:
// no candidate was selectable, the callee channel was not ready, or the callee
// rejected the call with the not-dispatched trailer. Everything else — a gRPC
// status without the trailer, a deadline, a handler answer — may follow a
// committed effect, so it is never retried.
func PreDispatch(err error) bool {
	var selection *SelectionError
	if errors.As(err, &selection) {
		return true
	}
	var nd *NotDispatchedError
	return errors.As(err, &nd)
}

// directPreDispatch reports whether err is a pre-dispatch failure of the direct
// path, the one condition under which transport auto switches to the proxy.
func directPreDispatch(err error) bool {
	var nd *NotDispatchedError
	return errors.As(err, &nd) && nd.Direct
}

// callCode extracts the gRPC code an error carries. The second result is false
// when the error carries none, which is what separates the wire code UNKNOWN —
// a real answer from the far side — from a local error that never reached it.
//
// @internal — см. ./README.md
func callCode(err error) (codes.Code, bool) {
	if err == nil {
		return codes.OK, true
	}
	if s, ok := status.FromError(err); ok {
		return s.Code(), true
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded, true
	case errors.Is(err, context.Canceled):
		return codes.Canceled, true
	}
	return codes.Unknown, false
}

// RetryPolicy is the exponential backoff ladder for one call loop.
type RetryPolicy struct {
	// MaxAttempts counts total tries, not extra ones: 3 means one call and two
	// retries.
	MaxAttempts int
	BaseMs      int64
	MaxMs       int64
	Multiplier  float64
	// JitterRatio spreads each rung by ±this fraction, applied after the MaxMs
	// clamp so the ceiling stays a ceiling in expectation.
	JitterRatio float64
	// Random yields a float in [0, 1). Tests pin it.
	Random func() float64
}

// DefaultRetryPolicy returns the ladder used when the caller configures none.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: DefaultMaxAttempts,
		BaseMs:      DefaultBaseMs,
		MaxMs:       DefaultMaxMs,
		Multiplier:  DefaultMultiplier,
		JitterRatio: DefaultJitterRatio,
		Random:      rand.Float64,
	}
}

// normalized fills the zero fields with defaults so a partially configured
// policy cannot spin at zero delay or refuse to attempt anything.
func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}
	if p.BaseMs <= 0 {
		p.BaseMs = DefaultBaseMs
	}
	if p.MaxMs <= 0 {
		p.MaxMs = DefaultMaxMs
	}
	if p.MaxMs < p.BaseMs {
		p.MaxMs = p.BaseMs
	}
	if p.Multiplier < 1 {
		p.Multiplier = DefaultMultiplier
	}
	if p.JitterRatio < 0 {
		p.JitterRatio = 0
	}
	if p.JitterRatio > 1 {
		p.JitterRatio = 1
	}
	if p.Random == nil {
		p.Random = rand.Float64
	}
	return p
}

// BackoffMs is the delay before attempt+1, with attempt counted from zero.
func (p RetryPolicy) BackoffMs(attempt int) int64 {
	if attempt < 0 {
		attempt = 0
	}
	d := float64(p.BaseMs) * math.Pow(p.Multiplier, float64(attempt))
	if d > float64(p.MaxMs) {
		d = float64(p.MaxMs)
	}
	if p.JitterRatio > 0 {
		d *= 1 - p.JitterRatio + p.Random()*2*p.JitterRatio
	}
	if d < 0 {
		return 0
	}
	return int64(math.Round(d))
}

// sleepMs waits ms, or returns as soon as ctx is done. A retry loop must not
// outlive its caller's deadline.
//
// @internal — см. ./README.md
func sleepMs(ctx context.Context, ms int64) error {
	if ms <= 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("rpc: backoff: %w", ctx.Err())
		default:
			return nil
		}
	}
	t := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rpc: backoff: %w", ctx.Err())
	}
}

// markNotDispatched wraps a status error whose trailer carries the
// not-dispatched proof. Without the trailer the error is returned as is: a bare
// status may follow a committed effect.
func markNotDispatched(err error, trailer metadata.MD, direct bool) error {
	if err == nil {
		return nil
	}
	if vals := trailer.Get(NotDispatchedKey); len(vals) > 0 && vals[0] == "1" {
		return &NotDispatchedError{Err: err, Direct: direct}
	}
	return err
}
