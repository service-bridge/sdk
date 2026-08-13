// Package stream owns the lifecycle of every long-lived gRPC stream in the SDK.
package stream

import (
	"math/rand/v2"
	"time"
)

// DefaultJitterRatio spreads each rung by ±this fraction. A herd of clients
// reconnecting on the same rung smears across a window instead of hitting the
// runtime as one synchronized spike.
const DefaultJitterRatio = 0.2

// defaultLadder returns a fresh copy so a caller mutating the slice cannot
// corrupt every other Backoff in the process.
func defaultLadder() []time.Duration {
	return []time.Duration{
		1 * time.Second,
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
	}
}

// Backoff is the shared reconnect delay ladder. Immutable after construction,
// safe for concurrent use.
type Backoff struct {
	ladder      []time.Duration
	jitterRatio float64
	random      func() float64
}

// BackoffOption configures a Backoff at construction time.
type BackoffOption func(*Backoff)

// WithLadder replaces the delay rungs. Panics on an empty ladder: a ladder with
// no rungs has no defined delay for any attempt, and the mistake is a caller bug
// visible at construction, not a runtime condition.
func WithLadder(rungs ...time.Duration) BackoffOption {
	return func(b *Backoff) {
		if len(rungs) == 0 {
			panic("stream: WithLadder: empty ladder")
		}
		b.ladder = append([]time.Duration(nil), rungs...)
	}
}

// WithJitterRatio sets the ±fraction applied to the chosen rung. Zero disables
// jitter and yields the exact rung. Panics on a negative ratio.
func WithJitterRatio(ratio float64) BackoffOption {
	return func(b *Backoff) {
		if ratio < 0 {
			panic("stream: WithJitterRatio: negative ratio")
		}
		b.jitterRatio = ratio
	}
}

// WithRandom injects the randomness source, a function returning a float in
// [0, 1). Tests pin it to make the jitter deterministic.
func WithRandom(fn func() float64) BackoffOption {
	return func(b *Backoff) {
		if fn == nil {
			panic("stream: WithRandom: nil source")
		}
		b.random = fn
	}
}

// NewBackoff builds the ladder. Without options: rungs 1s/5s/15s/30s/60s, ±20%
// jitter, process-global randomness.
func NewBackoff(opts ...BackoffOption) Backoff {
	b := Backoff{
		ladder:      defaultLadder(),
		jitterRatio: DefaultJitterRatio,
		random:      rand.Float64,
	}
	for _, opt := range opts {
		opt(&b)
	}
	return b
}

// Delay returns the wait before the given zero-based attempt. The rung is
// clamped into the ladder bounds — attempts past the end saturate on the last
// rung — and then offset by bounded jitter.
func (b Backoff) Delay(attempt int) time.Duration {
	if len(b.ladder) == 0 {
		panic("stream: Backoff.Delay: zero-value Backoff, build it with NewBackoff")
	}
	idx := attempt
	if idx < 0 {
		idx = 0
	}
	if idx >= len(b.ladder) {
		idx = len(b.ladder) - 1
	}
	base := b.ladder[idx]
	if b.jitterRatio == 0 {
		return base
	}
	// Map [0,1) → [-ratio, +ratio) so the spread is symmetric around the rung.
	offset := (b.random()*2 - 1) * b.jitterRatio
	d := time.Duration(float64(base) * (1 + offset))
	if d < 0 {
		return 0
	}
	return d
}

// Rungs returns a copy of the configured ladder.
func (b Backoff) Rungs() []time.Duration {
	return append([]time.Duration(nil), b.ladder...)
}

// isZero reports whether the Backoff was never built by NewBackoff.
func (b Backoff) isZero() bool { return len(b.ladder) == 0 }
