package rpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
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

// RetryClass is how far an error may be retried.
type RetryClass uint8

const (
	// RetryNever covers every business outcome: the callee ran, decided, and
	// answered. Repeating it changes nothing.
	RetryNever RetryClass = iota
	// RetryAlways covers codes that prove the request never executed.
	RetryAlways
	// RetryIfIdempotent covers codes that leave the callee's state unknown.
	RetryIfIdempotent
)

func (c RetryClass) String() string {
	switch c {
	case RetryAlways:
		return "always"
	case RetryIfIdempotent:
		return "if-idempotent"
	default:
		return "never"
	}
}

// Classify maps an error onto its retry class.
//
// DeadlineExceeded sits behind the idempotency gate on purpose (ADR-0001 §2).
// The deadline expires on the CALLER side and says nothing about the callee,
// which may have completed the work and answered a moment late. The Node SDK
// retried it unconditionally and turned one Charge into three.
// An error carrying no wire code at all — a handler's business failure, a
// local configuration fault — is never retried: nothing about it says the call
// can be repeated.
func Classify(err error) RetryClass {
	code, ok := callCode(err)
	if !ok {
		return RetryNever
	}
	switch code {
	case codes.Unavailable, codes.ResourceExhausted:
		// Unavailable: the connection was never established. ResourceExhausted:
		// rejected by a limiter before the handler ran. Both prove no side
		// effect happened.
		return RetryAlways
	case codes.DeadlineExceeded, codes.Internal, codes.Aborted, codes.Unknown:
		return RetryIfIdempotent
	default:
		return RetryNever
	}
}

// Retryable answers whether err may be retried given whether the caller
// supplied an idempotency key.
func Retryable(err error, hasIdempotencyKey bool) bool {
	switch Classify(err) {
	case RetryAlways:
		return true
	case RetryIfIdempotent:
		return hasIdempotencyKey
	default:
		return false
	}
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
