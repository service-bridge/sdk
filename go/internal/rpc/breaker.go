package rpc

import (
	"sync"
	"time"

	"google.golang.org/grpc/codes"
)

// Circuit breaker defaults. They mirror the Node SDK so both languages shed a
// broken instance on the same schedule.
const (
	DefaultBreakerWindowMs int64 = 10_000
	DefaultBreakerBuckets        = 10
	DefaultBreakerMinCalls       = 10
	DefaultBreakerRatio          = 0.5
	DefaultBreakerOpenMs   int64 = 30_000
	DefaultBreakerIdleMs   int64 = 60_000
)

// BreakerKey identifies one callee instance. The breaker is per
// (serviceID, instanceID) pair: a single sick pod must not shed traffic from
// its healthy siblings.
type BreakerKey struct {
	ServiceID  string
	InstanceID string
}

// BreakerConfig tunes the sliding window and the open interval.
type BreakerConfig struct {
	WindowMs int64
	Buckets  int
	// MinCalls is the sample floor. Below it the failure ratio is noise and the
	// breaker stays closed.
	MinCalls int
	// Ratio is the failure share that trips the breaker, exclusive.
	Ratio  float64
	OpenMs int64
	// IdleMs drops a record nothing has touched for this long. Rolling deploys
	// mint a fresh instanceID per pod, so a map that only ever grows is an
	// unbounded leak — that is exactly what the Node SDK shipped.
	IdleMs int64
	// Now yields unix-ms. Tests pin it.
	Now func() int64
}

// DefaultBreakerConfig returns the tuning used when the caller configures none.
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		WindowMs: DefaultBreakerWindowMs,
		Buckets:  DefaultBreakerBuckets,
		MinCalls: DefaultBreakerMinCalls,
		Ratio:    DefaultBreakerRatio,
		OpenMs:   DefaultBreakerOpenMs,
		IdleMs:   DefaultBreakerIdleMs,
		Now:      nowUnixMs,
	}
}

func (c BreakerConfig) normalized() BreakerConfig {
	if c.WindowMs <= 0 {
		c.WindowMs = DefaultBreakerWindowMs
	}
	if c.Buckets <= 0 {
		c.Buckets = DefaultBreakerBuckets
	}
	if c.MinCalls <= 0 {
		c.MinCalls = DefaultBreakerMinCalls
	}
	if c.Ratio <= 0 || c.Ratio > 1 {
		c.Ratio = DefaultBreakerRatio
	}
	if c.OpenMs <= 0 {
		c.OpenMs = DefaultBreakerOpenMs
	}
	if c.IdleMs <= 0 {
		c.IdleMs = DefaultBreakerIdleMs
	}
	if c.Now == nil {
		c.Now = nowUnixMs
	}
	return c
}

type breakerState uint8

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type breakerBucket struct {
	epoch int64
	ok    int
	fail  int
}

type breakerRecord struct {
	state         breakerState
	openedAtMs    int64
	probeInFlight bool
	touchedAtMs   int64
	buckets       []breakerBucket
}

// Breaker sheds traffic from instances that keep failing.
//
// It never calls back into the balancer, so holding the balancer's lock across
// Allows and Acquire cannot deadlock.
type Breaker struct {
	cfg      BreakerConfig
	bucketMs int64

	mu          sync.Mutex
	records     map[BreakerKey]*breakerRecord
	lastSweepMs int64
}

// NewBreaker builds a breaker. Zero fields in cfg fall back to the defaults.
func NewBreaker(cfg BreakerConfig) *Breaker {
	c := cfg.normalized()
	bucketMs := c.WindowMs / int64(c.Buckets)
	if bucketMs <= 0 {
		bucketMs = 1
	}
	return &Breaker{
		cfg:      c,
		bucketMs: bucketMs,
		records:  make(map[BreakerKey]*breakerRecord),
	}
}

// BreakerFailure reports whether err is the breaker's business.
//
// Only transport and server failures count. A callee rejecting bad input is
// working correctly; counting InvalidArgument would let one caller's validation
// bug take a healthy instance out of the fleet. An error carrying no wire code
// never reached the instance and says nothing about it either.
func BreakerFailure(err error) bool {
	if err == nil {
		return false
	}
	code, ok := callCode(err)
	if !ok {
		return false
	}
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.DataLoss:
		// Unavailable also carries connection resets and mTLS handshake
		// failures — gRPC surfaces both as transport errors.
		return true
	default:
		return false
	}
}

// Allows reports whether the instance may be dialled. It claims nothing, so it
// is the check candidate filtering uses: claiming during filtering leaks the
// half-open probe onto instances that are never called.
//
// It may still retire an expired open interval — that is a pure function of the
// clock, not a claim — but it never creates a record for an unknown key.
func (b *Breaker) Allows(k BreakerKey) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	r, ok := b.records[k]
	if !ok {
		return true
	}
	now := b.cfg.Now()
	b.advance(r, now)
	switch r.state {
	case breakerOpen:
		return false
	case breakerHalfOpen:
		return !r.probeInFlight
	default:
		return true
	}
}

// Acquire claims the right to call the instance and must be invoked only for
// the candidate actually about to be dispatched. In the half-open state exactly
// one caller wins the probe slot; every concurrent caller is refused until the
// probe reports back.
func (b *Breaker) Acquire(k BreakerKey) (*BreakerTicket, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.cfg.Now()
	r := b.record(k, now)
	b.advance(r, now)

	switch r.state {
	case breakerOpen:
		return nil, false
	case breakerHalfOpen:
		if r.probeInFlight {
			return nil, false
		}
		r.probeInFlight = true
		b.maybeSweep(now)
		return &BreakerTicket{breaker: b, key: k, probe: true}, true
	default:
		b.maybeSweep(now)
		return &BreakerTicket{breaker: b, key: k}, true
	}
}

// BreakerTicket is one claimed call slot. Exactly one Report settles it and
// releases the half-open probe; later calls do nothing.
type BreakerTicket struct {
	breaker *Breaker
	key     BreakerKey
	probe   bool
	settled bool
}

// Report closes the ticket with the call's outcome. A business error settles as
// a success: the instance answered, which is all the breaker measures.
func (t *BreakerTicket) Report(err error) {
	if t == nil || t.settled {
		return
	}
	t.settled = true
	t.breaker.settle(t.key, t.probe, BreakerFailure(err))
}

// Retain drops every record whose instance is no longer in the mesh. The
// registry's live set is the authority; the idle sweep is only a backstop for
// the window between two registry frames.
func (b *Breaker) Retain(live map[BreakerKey]struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for k := range b.records {
		if _, ok := live[k]; !ok {
			delete(b.records, k)
		}
	}
}

// Len reports how many instances the breaker currently tracks.
func (b *Breaker) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

// State exposes the current state for tests and diagnostics.
func (b *Breaker) State(k BreakerKey) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	r, ok := b.records[k]
	if !ok {
		return "closed"
	}
	b.advance(r, b.cfg.Now())
	switch r.state {
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

func (b *Breaker) settle(k BreakerKey, probe bool, failure bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.cfg.Now()
	r := b.record(k, now)
	b.advance(r, now)

	if probe && r.state == breakerHalfOpen {
		r.probeInFlight = false
		if failure {
			// The probe proved the instance is still down: serve another full
			// open interval rather than letting a probe per call slip through.
			r.state = breakerOpen
			r.openedAtMs = now
			return
		}
		r.state = breakerClosed
		r.buckets = nil
		return
	}

	// A late report from a call that started before the state moved on still
	// counts toward the window, but never re-opens a breaker on its own.
	bucket := b.bucket(r, now)
	if failure {
		bucket.fail++
	} else {
		bucket.ok++
	}
	if r.state != breakerClosed {
		return
	}
	if !failure {
		return
	}
	ok, fail := b.totals(r, now)
	total := ok + fail
	if total >= b.cfg.MinCalls && float64(fail)/float64(total) > b.cfg.Ratio {
		r.state = breakerOpen
		r.openedAtMs = now
		r.probeInFlight = false
	}
}

// record returns the record for k, creating it, and marks it as touched so the
// idle sweep in the same critical section cannot evict the key in use.
func (b *Breaker) record(k BreakerKey, now int64) *breakerRecord {
	r, ok := b.records[k]
	if !ok {
		r = &breakerRecord{}
		b.records[k] = r
	}
	r.touchedAtMs = now
	return r
}

// advance retires an expired open interval. Half-open is entered lazily on
// read, so no timer goroutine has to exist per instance.
func (b *Breaker) advance(r *breakerRecord, now int64) {
	if r.state == breakerOpen && now-r.openedAtMs >= b.cfg.OpenMs {
		r.state = breakerHalfOpen
		r.probeInFlight = false
	}
}

func (b *Breaker) bucket(r *breakerRecord, now int64) *breakerBucket {
	if r.buckets == nil {
		r.buckets = make([]breakerBucket, b.cfg.Buckets)
	}
	epoch := now / b.bucketMs
	idx := int(epoch % int64(b.cfg.Buckets))
	if idx < 0 {
		idx += b.cfg.Buckets
	}
	slot := &r.buckets[idx]
	if slot.epoch != epoch {
		slot.epoch = epoch
		slot.ok = 0
		slot.fail = 0
	}
	return slot
}

func (b *Breaker) totals(r *breakerRecord, now int64) (ok, fail int) {
	oldest := now/b.bucketMs - int64(b.cfg.Buckets) + 1
	for i := range r.buckets {
		slot := &r.buckets[i]
		if slot.epoch < oldest {
			continue
		}
		ok += slot.ok
		fail += slot.fail
	}
	return ok, fail
}

// maybeSweep drops idle records at most once per IdleMs. Callers hold b.mu.
func (b *Breaker) maybeSweep(now int64) {
	if now-b.lastSweepMs < b.cfg.IdleMs {
		return
	}
	b.lastSweepMs = now
	for k, r := range b.records {
		if now-r.touchedAtMs >= b.cfg.IdleMs {
			delete(b.records, k)
		}
	}
}

// nowUnixMs is the single clock the outbound path reads.
//
// @internal — см. ./README.md
func nowUnixMs() int64 { return time.Now().UnixMilli() }
