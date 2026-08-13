package stream_test

import (
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/stream"
)

func TestBackoffRungs(t *testing.T) {
	b := stream.NewBackoff(stream.WithJitterRatio(0))

	want := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
	}
	for attempt, expected := range want {
		if got := b.Delay(attempt); got != expected {
			t.Fatalf("attempt %d: got %s, want %s", attempt, got, expected)
		}
	}
}

func TestBackoffSaturatesOnLastRung(t *testing.T) {
	b := stream.NewBackoff(stream.WithJitterRatio(0))

	for _, attempt := range []int{5, 6, 100, 1_000_000} {
		if got := b.Delay(attempt); got != 60*time.Second {
			t.Fatalf("attempt %d: got %s, want 60s", attempt, got)
		}
	}
}

func TestBackoffClampsNegativeAttempt(t *testing.T) {
	b := stream.NewBackoff(stream.WithJitterRatio(0))

	if got := b.Delay(-7); got != time.Second {
		t.Fatalf("got %s, want 1s", got)
	}
}

func TestBackoffJitterStaysWithinBounds(t *testing.T) {
	// A deterministic sweep across the whole [0,1) range of the randomness
	// source: every produced delay must land inside ±20% of its rung.
	const samples = 1000
	i := 0
	b := stream.NewBackoff(stream.WithRandom(func() float64 {
		v := float64(i%samples) / samples
		i++
		return v
	}))

	rungs := b.Rungs()
	for attempt, rung := range rungs {
		low := time.Duration(float64(rung) * 0.8)
		high := time.Duration(float64(rung) * 1.2)
		for range samples {
			got := b.Delay(attempt)
			if got < low || got > high {
				t.Fatalf("attempt %d: delay %s outside [%s, %s]", attempt, got, low, high)
			}
		}
	}
}

func TestBackoffJitterEndpoints(t *testing.T) {
	lowest := stream.NewBackoff(stream.WithRandom(func() float64 { return 0 }))
	if got := lowest.Delay(0); got != 800*time.Millisecond {
		t.Fatalf("random=0: got %s, want 800ms", got)
	}

	middle := stream.NewBackoff(stream.WithRandom(func() float64 { return 0.5 }))
	if got := middle.Delay(0); got != time.Second {
		t.Fatalf("random=0.5: got %s, want 1s", got)
	}

	// The source never reaches 1.0, so +20% is an open bound.
	highest := stream.NewBackoff(stream.WithRandom(func() float64 { return 0.999999 }))
	if got := highest.Delay(0); got <= time.Second || got > 1200*time.Millisecond {
		t.Fatalf("random→1: got %s, want (1s, 1.2s]", got)
	}
}

func TestBackoffCustomLadder(t *testing.T) {
	b := stream.NewBackoff(
		stream.WithLadder(2*time.Millisecond, 7*time.Millisecond),
		stream.WithJitterRatio(0),
	)

	want := []time.Duration{2, 7, 7, 7}
	for attempt, ms := range want {
		if got := b.Delay(attempt); got != time.Duration(ms)*time.Millisecond {
			t.Fatalf("attempt %d: got %s, want %dms", attempt, got, ms)
		}
	}
}

func TestBackoffLadderIsCopied(t *testing.T) {
	rungs := []time.Duration{3 * time.Millisecond}
	b := stream.NewBackoff(stream.WithLadder(rungs...), stream.WithJitterRatio(0))
	rungs[0] = time.Hour

	if got := b.Delay(0); got != 3*time.Millisecond {
		t.Fatalf("got %s, want 3ms", got)
	}
	if out := b.Rungs(); out[0] != 3*time.Millisecond {
		t.Fatalf("Rungs leaked the caller slice: %s", out[0])
	}
}

func TestBackoffRejectsBadConfig(t *testing.T) {
	cases := map[string]func(){
		"empty ladder":    func() { stream.NewBackoff(stream.WithLadder()) },
		"negative jitter": func() { stream.NewBackoff(stream.WithJitterRatio(-0.1)) },
		"nil random":      func() { stream.NewBackoff(stream.WithRandom(nil)) },
		"zero value":      func() { var b stream.Backoff; b.Delay(0) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
}
