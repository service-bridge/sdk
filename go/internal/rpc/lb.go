package rpc

import (
	"math/rand/v2"
	"sync"
)

// DefaultHealthHintTTLMs bounds how long the runtime's unhealthy hint is
// trusted. It is twice the runtime's healing window: past that the hint is
// stale — a runtime that died mid-window would otherwise black-hole an instance
// forever — and the local breaker carries the decision alone.
const DefaultHealthHintTTLMs int64 = 60_000

// Candidate is one callee instance the registry offers for a call.
type Candidate struct {
	ServiceID   string
	ServiceName string
	InstanceID  string
	// Endpoint is the address peers dial for direct RPC. Empty means the
	// instance advertises none and is unreachable by either transport — the
	// runtime's resolver requires a non-empty call_endpoint too.
	Endpoint string
	// UnhealthySinceMs is the start of the runtime's current unhealthy window,
	// unix-ms. Zero means healthy or unknown.
	UnhealthySinceMs int64
}

// Key is the breaker and in-flight key for this candidate.
func (c Candidate) Key() BreakerKey {
	return BreakerKey{ServiceID: c.ServiceID, InstanceID: c.InstanceID}
}

// Healthy applies the runtime's hint with its trust window.
func Healthy(c Candidate, nowMs, hintTTLMs int64) bool {
	if c.UnhealthySinceMs == 0 {
		return true
	}
	if hintTTLMs <= 0 {
		hintTTLMs = DefaultHealthHintTTLMs
	}
	return nowMs-c.UnhealthySinceMs >= hintTTLMs
}

// PickStats explains what happened to a candidate set. The three counts are
// what separates the three ways selection fails, which have opposite fixes:
// nothing published this contract, the callee advertises no address, or the
// fleet is shedding.
type PickStats struct {
	// Total is every candidate the registry indexed for this contract.
	Total int
	// Addressed counts those advertising a call endpoint.
	Addressed int
	// Eligible counts those that also passed the health hint and the breaker.
	Eligible int
}

// Eligible filters cands and reports the counts behind the decision. It claims
// nothing and is safe to call for diagnostics.
func Eligible(cands []Candidate, allows func(Candidate) bool) ([]Candidate, PickStats) {
	stats := PickStats{Total: len(cands)}
	var out []Candidate
	for _, c := range cands {
		if c.Endpoint == "" {
			continue
		}
		stats.Addressed++
		if allows != nil && !allows(c) {
			continue
		}
		stats.Eligible++
		out = append(out, c)
	}
	return out, stats
}

// BalancerOption configures a Balancer at construction time.
type BalancerOption func(*Balancer)

// WithIntn injects the randomness source, a function returning a value in
// [0, n). Tests pin it to make the choice deterministic. It is only ever called
// while the balancer's lock is held, so it need not be safe for concurrent use.
func WithIntn(fn func(n int) int) BalancerOption {
	return func(b *Balancer) {
		if fn == nil {
			panic("rpc: WithIntn: nil source")
		}
		b.intn = fn
	}
}

// Balancer spreads calls over the eligible instances by power-of-two-choices on
// in-flight request count.
type Balancer struct {
	mu       sync.Mutex
	inflight map[BreakerKey]int
	intn     func(n int) int
}

// NewBalancer builds a balancer with process-global randomness.
func NewBalancer(opts ...BalancerOption) *Balancer {
	b := &Balancer{
		inflight: make(map[BreakerKey]int),
		intn:     rand.IntN,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Pick selects the instance to dispatch to and reserves an in-flight slot on
// it. The caller must Release the returned candidate once the call settles.
//
// allows is the side-effect-free eligibility check — health hint plus breaker.
// acquire claims the call slot on the winner alone, and may refuse: in the
// half-open state exactly one probe is allowed and a concurrent call may hold
// it. A refused winner is excluded and the draw is repeated, so the invariant
// survives concurrency instead of leaking a second probe.
//
// Selection and the in-flight increment happen under one lock, so two
// concurrent calls cannot both read the same pre-increment load.
func (b *Balancer) Pick(cands []Candidate, allows func(Candidate) bool, acquire func(Candidate) bool) (Candidate, PickStats, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var excluded map[string]struct{}
	for {
		first, second, stats := b.draw(cands, allows, excluded)
		if stats.Eligible == 0 {
			return Candidate{}, stats, false
		}

		winner := first
		if stats.Eligible > 1 {
			// The reservoir samples the PAIR uniformly but not its ORDER: the
			// second slot is always filled second, and a load tie resolves to
			// the first. Without this flip the second candidate never wins on
			// an idle fleet.
			if b.intn(2) == 1 {
				first, second = second, first
			}
			winner = first
			if b.inflight[second.Key()] < b.inflight[first.Key()] {
				winner = second
			}
		}

		if acquire != nil && !acquire(winner) {
			if excluded == nil {
				excluded = make(map[string]struct{}, 1)
			}
			excluded[winner.InstanceID] = struct{}{}
			continue
		}

		b.inflight[winner.Key()]++
		return winner, stats, true
	}
}

// draw runs a size-2 reservoir over the eligible candidates in a single pass —
// no intermediate slice, so the hot path allocates nothing.
func (b *Balancer) draw(cands []Candidate, allows func(Candidate) bool, excluded map[string]struct{}) (Candidate, Candidate, PickStats) {
	var first, second Candidate
	stats := PickStats{Total: len(cands)}

	for _, c := range cands {
		if c.Endpoint == "" {
			continue
		}
		stats.Addressed++
		if _, skip := excluded[c.InstanceID]; skip {
			continue
		}
		if allows != nil && !allows(c) {
			continue
		}
		stats.Eligible++
		switch stats.Eligible {
		case 1:
			first = c
		case 2:
			second = c
		default:
			switch b.intn(stats.Eligible) {
			case 0:
				first = c
			case 1:
				second = c
			}
		}
	}
	return first, second, stats
}

// Release frees the in-flight slot Pick reserved. The key is deleted at zero so
// a fleet churning through instance IDs cannot grow the map without bound.
func (b *Balancer) Release(c Candidate) {
	b.mu.Lock()
	defer b.mu.Unlock()

	k := c.Key()
	if n := b.inflight[k]; n > 1 {
		b.inflight[k] = n - 1
		return
	}
	delete(b.inflight, k)
}

// InFlight reports the reserved slot count for one instance.
func (b *Balancer) InFlight(c Candidate) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inflight[c.Key()]
}

// Tracked reports how many instances hold a reservation.
func (b *Balancer) Tracked() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.inflight)
}
