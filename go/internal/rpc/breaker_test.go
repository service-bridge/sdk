package rpc

import (
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stepClock is a hand-cranked unix-ms clock. Nothing in the outbound path
// sleeps on wall time, so every time-dependent branch is testable without one.
type stepClock struct {
	mu sync.Mutex
	ms int64
}

func newStepClock(ms int64) *stepClock { return &stepClock{ms: ms} }

func (c *stepClock) now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ms
}

func (c *stepClock) advance(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ms += ms
}

func breakerUnderTest(clk *stepClock) *Breaker {
	cfg := DefaultBreakerConfig()
	cfg.Now = clk.now
	return NewBreaker(cfg)
}

func reportOnce(t *testing.T, b *Breaker, k BreakerKey, err error) {
	t.Helper()
	ticket, ok := b.Acquire(k)
	if !ok {
		t.Fatalf("Acquire(%v) refused while the breaker should admit calls", k)
	}
	ticket.Report(err)
}

// TestBreakerIgnoresBusinessErrors is the invariant that keeps a caller's own
// validation bug from taking a healthy instance out of the fleet.
func TestBreakerIgnoresBusinessErrors(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	business := []error{
		status.Error(codes.InvalidArgument, "bad field"),
		status.Error(codes.NotFound, "no such row"),
		status.Error(codes.AlreadyExists, "duplicate"),
		handlerError("VALIDATION", "amount must be positive"),
	}
	for i := 0; i < 40; i++ {
		reportOnce(t, b, k, business[i%len(business)])
	}

	if !b.Allows(k) {
		t.Fatal("business errors must never trip the breaker")
	}
	if got := b.State(k); got != "closed" {
		t.Fatalf("state after 40 business errors = %q, want closed", got)
	}
}

func TestBreakerOpensOnTransportFailures(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown} {
		t.Run(code.String(), func(t *testing.T) {
			clk := newStepClock(1_700_000_000_000)
			b := breakerUnderTest(clk)
			k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

			for i := 0; i < DefaultBreakerMinCalls; i++ {
				reportOnce(t, b, k, status.Error(code, "down"))
			}

			if b.Allows(k) {
				t.Fatalf("%v must trip the breaker", code)
			}
			if got := b.State(k); got != "open" {
				t.Fatalf("state = %q, want open", got)
			}
			if _, ok := b.Acquire(k); ok {
				t.Fatal("an open breaker must refuse the call slot")
			}
		})
	}
}

func TestBreakerNeedsTheSampleFloorBeforeTripping(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	for i := 0; i < DefaultBreakerMinCalls-1; i++ {
		reportOnce(t, b, k, status.Error(codes.Unavailable, "down"))
	}
	if !b.Allows(k) {
		t.Fatalf("below %d samples the failure ratio is noise and must not trip", DefaultBreakerMinCalls)
	}

	reportOnce(t, b, k, status.Error(codes.Unavailable, "down"))
	if b.Allows(k) {
		t.Fatal("the sample floor was reached, the breaker must trip")
	}
}

func TestBreakerStaysClosedBelowTheFailureRatio(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	// Five successes then five failures: the sample floor is met and the ratio
	// lands exactly on the threshold, which is exclusive.
	for i := 0; i < 5; i++ {
		reportOnce(t, b, k, nil)
	}
	for i := 0; i < 5; i++ {
		reportOnce(t, b, k, status.Error(codes.Unavailable, "down"))
	}
	if !b.Allows(k) {
		t.Fatal("a failure ratio equal to the threshold must not trip the breaker")
	}

	// One more failure pushes the ratio past it.
	reportOnce(t, b, k, status.Error(codes.Unavailable, "down"))
	if b.Allows(k) {
		t.Fatal("crossing the threshold must trip the breaker")
	}
}

func openBreaker(t *testing.T, b *Breaker, k BreakerKey) {
	t.Helper()
	for i := 0; i < DefaultBreakerMinCalls; i++ {
		reportOnce(t, b, k, status.Error(codes.Unavailable, "down"))
	}
	if b.Allows(k) {
		t.Fatal("setup: the breaker did not open")
	}
}

// TestHalfOpenAdmitsExactlyOneProbe separates the two checks: filtering asks
// Allows and claims nothing, only the dispatched winner calls Acquire.
func TestHalfOpenAdmitsExactlyOneProbe(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	openBreaker(t, b, k)
	clk.advance(DefaultBreakerOpenMs)

	if got := b.State(k); got != "half-open" {
		t.Fatalf("state after the open interval = %q, want half-open", got)
	}
	// Filtering must be able to ask repeatedly without consuming the probe.
	for i := 0; i < 5; i++ {
		if !b.Allows(k) {
			t.Fatal("Allows must not consume the probe slot")
		}
	}

	first, ok := b.Acquire(k)
	if !ok {
		t.Fatal("the first caller must win the probe slot")
	}
	if _, ok := b.Acquire(k); ok {
		t.Fatal("a second concurrent caller must not get a probe slot")
	}
	if b.Allows(k) {
		t.Fatal("Allows must report the probe as taken while it is in flight")
	}

	first.Report(nil)
	if got := b.State(k); got != "closed" {
		t.Fatalf("state after a successful probe = %q, want closed", got)
	}
}

func TestFailedProbeServesAnotherOpenInterval(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	openBreaker(t, b, k)
	clk.advance(DefaultBreakerOpenMs)

	probe, ok := b.Acquire(k)
	if !ok {
		t.Fatal("setup: no probe slot")
	}
	probe.Report(status.Error(codes.Unavailable, "still down"))

	if got := b.State(k); got != "open" {
		t.Fatalf("state after a failed probe = %q, want open", got)
	}
	clk.advance(DefaultBreakerOpenMs - 1)
	if b.Allows(k) {
		t.Fatal("a failed probe must buy a full new open interval, not a shorter one")
	}
	clk.advance(1)
	if !b.Allows(k) {
		t.Fatal("the breaker must go half-open again once the interval elapses")
	}
}

func TestProbeSuccessResetsTheWindow(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	openBreaker(t, b, k)
	clk.advance(DefaultBreakerOpenMs)

	probe, ok := b.Acquire(k)
	if !ok {
		t.Fatal("setup: no probe slot")
	}
	probe.Report(nil)

	// A single later failure must not re-trip: the old window is gone.
	reportOnce(t, b, k, status.Error(codes.Unavailable, "blip"))
	if !b.Allows(k) {
		t.Fatal("a healed instance must not re-open on one failure carried over from the old window")
	}
}

// TestSingleProbeSurvivesConcurrentAcquire is the invariant under -race: two
// callers must never both hold the half-open probe.
func TestSingleProbeSurvivesConcurrentAcquire(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	openBreaker(t, b, k)
	clk.advance(DefaultBreakerOpenMs)

	const racers = 64
	var winners atomic.Int64
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(racers)

	for i := 0; i < racers; i++ {
		go func() {
			defer done.Done()
			start.Wait()
			if _, ok := b.Acquire(k); ok {
				winners.Add(1)
			}
		}()
	}
	start.Done()
	done.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("%d callers claimed the half-open probe, want exactly 1", got)
	}
}

func TestReportIsIdempotent(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	openBreaker(t, b, k)
	clk.advance(DefaultBreakerOpenMs)

	probe, ok := b.Acquire(k)
	if !ok {
		t.Fatal("setup: no probe slot")
	}
	probe.Report(nil)
	probe.Report(status.Error(codes.Unavailable, "late duplicate"))

	if got := b.State(k); got != "closed" {
		t.Fatalf("a second Report must do nothing, state = %q", got)
	}
}

// TestRetainEvictsDepartedInstances is the leak fix: a rolling deploy mints a
// fresh instanceID per pod, so records must leave with their instances.
func TestRetainEvictsDepartedInstances(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)

	live := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-live"}
	for i := 0; i < 30; i++ {
		gone := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-gone-" + string(rune('a'+i%26)) + string(rune('0'+i/26))}
		reportOnce(t, b, gone, status.Error(codes.Unavailable, "down"))
	}
	reportOnce(t, b, live, nil)

	if b.Len() < 2 {
		t.Fatalf("setup: expected many tracked instances, got %d", b.Len())
	}

	b.Retain(map[BreakerKey]struct{}{live: {}})

	if got := b.Len(); got != 1 {
		t.Fatalf("after Retain the breaker tracks %d instances, want 1", got)
	}
	if !b.Allows(live) {
		t.Fatal("Retain must keep the live instance's record")
	}
}

// TestIdleSweepEvictsUntouchedRecords is the backstop for the window between
// two registry frames.
func TestIdleSweepEvictsUntouchedRecords(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)

	stale := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-stale"}
	reportOnce(t, b, stale, nil)
	if b.Len() != 1 {
		t.Fatalf("setup: tracked %d instances, want 1", b.Len())
	}

	clk.advance(DefaultBreakerIdleMs + 1)

	// Any later traffic runs the sweep.
	fresh := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-fresh"}
	reportOnce(t, b, fresh, nil)

	if got := b.Len(); got != 1 {
		t.Fatalf("after the idle sweep the breaker tracks %d instances, want 1 (only the fresh one)", got)
	}
	if !b.Allows(stale) {
		t.Fatal("an evicted record must read as closed")
	}
}

func TestUnknownInstanceIsAllowedWithoutAllocatingARecord(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	b := breakerUnderTest(clk)

	for i := 0; i < 100; i++ {
		if !b.Allows(BreakerKey{ServiceID: "svc-a", InstanceID: "never-called"}) {
			t.Fatal("an unknown instance must be allowed")
		}
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("Allows allocated %d records; a read-only check must allocate none", got)
	}
}

func TestBreakerConfigNormalizesOutOfRangeValues(t *testing.T) {
	b := NewBreaker(BreakerConfig{
		WindowMs: -1, Buckets: 0, MinCalls: -3, Ratio: 4, OpenMs: -1, IdleMs: -1,
	})
	k := BreakerKey{ServiceID: "svc-a", InstanceID: "inst-1"}

	// A nonsense config must still behave like the default one.
	for i := 0; i < DefaultBreakerMinCalls; i++ {
		reportOnce(t, b, k, status.Error(codes.Unavailable, "down"))
	}
	if b.Allows(k) {
		t.Fatal("a normalized config must still trip on the default thresholds")
	}
}
