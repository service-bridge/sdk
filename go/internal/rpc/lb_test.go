package rpc

import (
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func candidate(instanceID, endpoint string) Candidate {
	return Candidate{
		ServiceID:   "svc-a",
		ServiceName: "billing",
		InstanceID:  instanceID,
		Endpoint:    endpoint,
	}
}

func allowAll(Candidate) bool { return true }

// scriptedIntn replays a fixed sequence, repeating the last value once it runs
// out, so a test pins the draw without caring how many times it is consulted.
func scriptedIntn(values ...int) func(int) int {
	i := 0
	return func(n int) int {
		v := values[i]
		if i < len(values)-1 {
			i++
		}
		if v >= n {
			v = n - 1
		}
		return v
	}
}

func TestPickSkipsCandidatesWithoutAnEndpoint(t *testing.T) {
	b := NewBalancer(WithIntn(scriptedIntn(0)))
	cands := []Candidate{
		candidate("inst-1", ""),
		candidate("inst-2", "10.0.0.2:14446"),
	}

	got, stats, ok := b.Pick(cands, allowAll, nil)
	if !ok {
		t.Fatal("one addressed candidate must be pickable")
	}
	if got.InstanceID != "inst-2" {
		t.Fatalf("picked %q, want the only addressed instance", got.InstanceID)
	}
	if stats.Total != 2 || stats.Addressed != 1 || stats.Eligible != 1 {
		t.Fatalf("stats = %+v, want total 2 addressed 1 eligible 1", stats)
	}
}

// TestPickStatsSeparateTheThreeFailureShapes is what lets the call loop tell an
// operator which of three unrelated problems they actually have.
func TestPickStatsSeparateTheThreeFailureShapes(t *testing.T) {
	b := NewBalancer(WithIntn(scriptedIntn(0)))

	t.Run("nothing serves this contract", func(t *testing.T) {
		_, stats, ok := b.Pick(nil, allowAll, nil)
		if ok {
			t.Fatal("an empty candidate set must not yield a pick")
		}
		if stats.Total != 0 {
			t.Fatalf("stats = %+v, want total 0", stats)
		}
	})

	t.Run("nobody advertises an address", func(t *testing.T) {
		cands := []Candidate{candidate("inst-1", ""), candidate("inst-2", "")}
		_, stats, ok := b.Pick(cands, allowAll, nil)
		if ok {
			t.Fatal("candidates without endpoints must not yield a pick")
		}
		if stats.Total != 2 || stats.Addressed != 0 {
			t.Fatalf("stats = %+v, want total 2 addressed 0", stats)
		}
	})

	t.Run("everything is shed", func(t *testing.T) {
		cands := []Candidate{candidate("inst-1", "10.0.0.1:1"), candidate("inst-2", "10.0.0.2:1")}
		_, stats, ok := b.Pick(cands, func(Candidate) bool { return false }, nil)
		if ok {
			t.Fatal("a fully shed fleet must not yield a pick")
		}
		if stats.Total != 2 || stats.Addressed != 2 || stats.Eligible != 0 {
			t.Fatalf("stats = %+v, want total 2 addressed 2 eligible 0", stats)
		}
	})
}

func TestPickPrefersTheLeastLoadedOfTheDrawnPair(t *testing.T) {
	cands := []Candidate{candidate("inst-1", "10.0.0.1:1"), candidate("inst-2", "10.0.0.2:1")}

	for _, coin := range []int{0, 1} {
		b := NewBalancer(WithIntn(scriptedIntn(coin)))
		// Load inst-1 without releasing it.
		if _, _, ok := b.Pick(cands[:1], allowAll, nil); !ok {
			t.Fatal("setup: could not load the first instance")
		}
		if b.InFlight(cands[0]) != 1 {
			t.Fatalf("setup: in-flight = %d, want 1", b.InFlight(cands[0]))
		}

		got, _, ok := b.Pick(cands, allowAll, nil)
		if !ok {
			t.Fatal("a loaded fleet must still be pickable")
		}
		if got.InstanceID != "inst-2" {
			t.Fatalf("coin=%d picked %q, want the idle instance inst-2", coin, got.InstanceID)
		}
	}
}

// TestBothCandidatesCanWinOnAnIdleFleet is the Node bug in test form: the
// reservoir samples the PAIR uniformly but not its ORDER, so without the coin
// flip the second candidate is stuck in slot B and loses every load tie.
func TestBothCandidatesCanWinOnAnIdleFleet(t *testing.T) {
	cands := []Candidate{candidate("inst-1", "10.0.0.1:1"), candidate("inst-2", "10.0.0.2:1")}
	b := NewBalancer()

	won := map[string]int{}
	for i := 0; i < 400; i++ {
		got, _, ok := b.Pick(cands, allowAll, nil)
		if !ok {
			t.Fatal("an idle fleet must always be pickable")
		}
		won[got.InstanceID]++
		b.Release(got)
	}

	if won["inst-1"] == 0 || won["inst-2"] == 0 {
		t.Fatalf("both instances must be reachable on an idle fleet, got %v", won)
	}
}

func TestPickSpreadsAcrossALargerIdleFleet(t *testing.T) {
	var cands []Candidate
	for _, id := range []string{"inst-1", "inst-2", "inst-3", "inst-4", "inst-5"} {
		cands = append(cands, candidate(id, "10.0.0.1:1"))
	}
	b := NewBalancer()

	won := map[string]int{}
	for i := 0; i < 2000; i++ {
		got, _, ok := b.Pick(cands, allowAll, nil)
		if !ok {
			t.Fatal("an idle fleet must always be pickable")
		}
		won[got.InstanceID]++
		b.Release(got)
	}
	for _, c := range cands {
		if won[c.InstanceID] == 0 {
			t.Fatalf("instance %q never won a draw, distribution %v", c.InstanceID, won)
		}
	}
}

func TestReleaseDropsTheKeyAtZero(t *testing.T) {
	b := NewBalancer(WithIntn(scriptedIntn(0)))
	c := candidate("inst-1", "10.0.0.1:1")

	got, _, ok := b.Pick([]Candidate{c}, allowAll, nil)
	if !ok {
		t.Fatal("setup: no pick")
	}
	if b.Tracked() != 1 {
		t.Fatalf("tracked = %d, want 1", b.Tracked())
	}

	b.Release(got)
	if b.InFlight(c) != 0 {
		t.Fatalf("in-flight after release = %d, want 0", b.InFlight(c))
	}
	if b.Tracked() != 0 {
		t.Fatalf("a zeroed key must leave the map, tracked = %d", b.Tracked())
	}
}

func TestHealthHintIsTrustedOnlyForItsTTL(t *testing.T) {
	now := int64(1_700_000_000_000)

	healthy := candidate("inst-1", "10.0.0.1:1")
	if !Healthy(healthy, now, DefaultHealthHintTTLMs) {
		t.Fatal("a zero hint means healthy or unknown")
	}

	fresh := healthy
	fresh.UnhealthySinceMs = now - DefaultHealthHintTTLMs + 1
	if Healthy(fresh, now, DefaultHealthHintTTLMs) {
		t.Fatal("a fresh hint must shed the instance")
	}

	stale := healthy
	stale.UnhealthySinceMs = now - DefaultHealthHintTTLMs
	if !Healthy(stale, now, DefaultHealthHintTTLMs) {
		t.Fatal("past its TTL the hint is stale and must stop shedding: a dead runtime would otherwise black-hole the instance forever")
	}
}

func TestEligibleReportsTheSameCountsAsPick(t *testing.T) {
	cands := []Candidate{
		candidate("inst-1", ""),
		candidate("inst-2", "10.0.0.2:1"),
		candidate("inst-3", "10.0.0.3:1"),
	}
	shedThird := func(c Candidate) bool { return c.InstanceID != "inst-3" }

	eligible, stats := Eligible(cands, shedThird)
	if len(eligible) != 1 || eligible[0].InstanceID != "inst-2" {
		t.Fatalf("eligible = %v, want only inst-2", eligible)
	}
	if stats.Total != 3 || stats.Addressed != 2 || stats.Eligible != 1 {
		t.Fatalf("stats = %+v, want total 3 addressed 2 eligible 1", stats)
	}
}

// TestPickHoldsTheSingleProbeInvariantUnderConcurrentCalls wires the balancer
// to a real half-open breaker: the filter checks without claiming, the winner
// claims, and a loser is excluded and re-drawn rather than leaking a probe.
func TestPickHoldsTheSingleProbeInvariantUnderConcurrentCalls(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	breaker := breakerUnderTest(clk)
	cands := []Candidate{candidate("inst-1", "10.0.0.1:1"), candidate("inst-2", "10.0.0.2:1")}

	// Put both instances in the half-open state.
	for _, c := range cands {
		openBreaker(t, breaker, c.Key())
	}
	clk.advance(DefaultBreakerOpenMs)

	b := NewBalancer()
	allows := func(c Candidate) bool { return breaker.Allows(c.Key()) }
	acquire := func(c Candidate) bool {
		_, ok := breaker.Acquire(c.Key())
		return ok
	}

	const racers = 64
	var picks atomic.Int64
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(racers)

	for i := 0; i < racers; i++ {
		go func() {
			defer done.Done()
			start.Wait()
			if _, _, ok := b.Pick(cands, allows, acquire); ok {
				picks.Add(1)
			}
		}()
	}
	start.Done()
	done.Wait()

	// Two half-open instances means exactly two probes, never more.
	if got := picks.Load(); got != 2 {
		t.Fatalf("%d calls were dispatched against 2 half-open instances, want exactly 2", got)
	}
}

func TestPickRedrawsWhenTheWinnerLosesTheProbeRace(t *testing.T) {
	clk := newStepClock(1_700_000_000_000)
	breaker := breakerUnderTest(clk)
	cands := []Candidate{candidate("inst-1", "10.0.0.1:1"), candidate("inst-2", "10.0.0.2:1")}

	openBreaker(t, breaker, cands[0].Key())
	clk.advance(DefaultBreakerOpenMs)
	// A concurrent call already holds inst-1's only probe.
	if _, ok := breaker.Acquire(cands[0].Key()); !ok {
		t.Fatal("setup: could not take the probe")
	}

	b := NewBalancer(WithIntn(scriptedIntn(0)))
	// allows still admits inst-1 at filter time; only the claim refuses it.
	got, _, ok := b.Pick(cands, func(Candidate) bool { return true }, func(c Candidate) bool {
		_, taken := breaker.Acquire(c.Key())
		return taken
	})
	if !ok {
		t.Fatal("the healthy sibling must still be pickable")
	}
	if got.InstanceID != "inst-2" {
		t.Fatalf("picked %q, want inst-2 after the probe race was lost", got.InstanceID)
	}
	if b.InFlight(cands[0]) != 0 {
		t.Fatal("a refused winner must not keep an in-flight reservation")
	}
}

func TestPickAndReleaseAreRaceFree(t *testing.T) {
	cands := []Candidate{candidate("inst-1", "10.0.0.1:1"), candidate("inst-2", "10.0.0.2:1")}
	b := NewBalancer()
	breaker := NewBreaker(DefaultBreakerConfig())

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				got, _, ok := b.Pick(cands, func(c Candidate) bool { return breaker.Allows(c.Key()) }, func(c Candidate) bool {
					ticket, taken := breaker.Acquire(c.Key())
					if taken {
						ticket.Report(status.Error(codes.InvalidArgument, "business"))
					}
					return taken
				})
				if ok {
					b.Release(got)
				}
			}
		}()
	}
	wg.Wait()

	for _, c := range cands {
		if got := b.InFlight(c); got != 0 {
			t.Fatalf("instance %q leaked %d in-flight slots", c.InstanceID, got)
		}
	}
}
