package telemetry

import (
	"sync"
	"sync/atomic"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// RingKind names one of the four independent buffers. Each kind gets its own
// byte budget so a burst of one signal cannot starve another.
type RingKind uint8

const (
	RingOps RingKind = iota
	RingLogs
	RingMetrics
	RingPayloads
)

func (k RingKind) String() string {
	switch k {
	case RingOps:
		return "ops"
	case RingLogs:
		return "logs"
	case RingMetrics:
		return "metrics"
	case RingPayloads:
		return "payloads"
	default:
		return "unknown"
	}
}

// Default per-kind byte budgets. Ops holds the dense per-step emission a
// workflow run produces between flush ticks, so it is sized for hundreds of
// frames rather than dozens.
const (
	defaultOpsBudget      = 256 * 1024
	defaultLogsBudget     = 64 * 1024
	defaultMetricsBudget  = 16 * 1024
	defaultPayloadsBudget = 256 * 1024
)

// Budgets overrides the per-kind byte budget. A zero field keeps the default.
type Budgets struct {
	Ops      int
	Logs     int
	Metrics  int
	Payloads int
}

// DefaultBudgets returns the budgets applied when nothing is overridden.
func DefaultBudgets() Budgets {
	return Budgets{
		Ops:      defaultOpsBudget,
		Logs:     defaultLogsBudget,
		Metrics:  defaultMetricsBudget,
		Payloads: defaultPayloadsBudget,
	}
}

func budgetOr(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}

// Item is one buffered message plus the identity Commit acknowledges it by.
type Item[T any] struct {
	ID    uint64
	Msg   T
	Bytes int
}

// Batch is what one Peek yields: up to maxPerKind items from every kind, still
// resident in the ring. Handing the same value back to Commit releases exactly
// those items.
type Batch struct {
	Ops      []Item[*pb.OpReport]
	Logs     []Item[*pb.Log]
	Metrics  []Item[*pb.MetricPoint]
	Payloads []Item[*pb.PayloadAttachment]
}

// Len counts the items across all kinds.
func (b Batch) Len() int {
	return len(b.Ops) + len(b.Logs) + len(b.Metrics) + len(b.Payloads)
}

// Empty reports whether the batch carries nothing.
func (b Batch) Empty() bool { return b.Len() == 0 }

// Ring is the SDK's telemetry buffer: four byte-budgeted rings drained by
// peek-then-commit. An item leaves the ring only once the runtime has
// acknowledged it, which is what makes delivery at-least-once across a stream
// reconnect. Safe for concurrent use.
type Ring struct {
	ids      atomic.Uint64
	ops      *kindRing[*pb.OpReport]
	logs     *kindRing[*pb.Log]
	metrics  *kindRing[*pb.MetricPoint]
	payloads *kindRing[*pb.PayloadAttachment]
}

// NewRing builds the four rings. Zero fields in b keep their defaults.
func NewRing(b Budgets) *Ring {
	r := &Ring{}
	r.ops = newKindRing(budgetOr(b.Ops, defaultOpsBudget), &r.ids, sizeOfOp)
	r.logs = newKindRing(budgetOr(b.Logs, defaultLogsBudget), &r.ids, sizeOfLog)
	r.metrics = newKindRing(budgetOr(b.Metrics, defaultMetricsBudget), &r.ids, sizeOfMetric)
	r.payloads = newKindRing(budgetOr(b.Payloads, defaultPayloadsBudget), &r.ids, sizeOfPayload)
	return r
}

func (r *Ring) PushOp(m *pb.OpReport)               { r.ops.push(m) }
func (r *Ring) PushLog(m *pb.Log)                   { r.logs.push(m) }
func (r *Ring) PushMetric(m *pb.MetricPoint)        { r.metrics.push(m) }
func (r *Ring) PushPayload(m *pb.PayloadAttachment) { r.payloads.push(m) }

// Peek returns up to maxPerKind oldest items of every kind without removing
// them. Unacknowledged items are returned again by the next Peek.
func (r *Ring) Peek(maxPerKind int) Batch {
	return Batch{
		Ops:      r.ops.peek(maxPerKind),
		Logs:     r.logs.peek(maxPerKind),
		Metrics:  r.metrics.peek(maxPerKind),
		Payloads: r.payloads.peek(maxPerKind),
	}
}

// Commit releases the items the runtime acknowledged, freeing their bytes.
// Items absent from b stay buffered.
func (r *Ring) Commit(b Batch) {
	r.ops.commit(b.Ops)
	r.logs.commit(b.Logs)
	r.metrics.commit(b.Metrics)
	r.payloads.commit(b.Payloads)
}

// Len reports the buffered item count of one kind.
func (r *Ring) Len(k RingKind) int {
	switch k {
	case RingOps:
		return r.ops.count()
	case RingLogs:
		return r.logs.count()
	case RingMetrics:
		return r.metrics.count()
	case RingPayloads:
		return r.payloads.count()
	default:
		return 0
	}
}

// Bytes reports the estimated bytes one kind currently holds.
func (r *Ring) Bytes(k RingKind) int {
	switch k {
	case RingOps:
		return r.ops.byteSize()
	case RingLogs:
		return r.logs.byteSize()
	case RingMetrics:
		return r.metrics.byteSize()
	case RingPayloads:
		return r.payloads.byteSize()
	default:
		return 0
	}
}

// Dropped reports how many messages of one kind were evicted or refused.
func (r *Ring) Dropped(k RingKind) uint64 {
	switch k {
	case RingOps:
		return r.ops.dropCount()
	case RingLogs:
		return r.logs.dropCount()
	case RingMetrics:
		return r.metrics.dropCount()
	case RingPayloads:
		return r.payloads.dropCount()
	default:
		return 0
	}
}

// TotalDropped sums the loss counters of all kinds.
func (r *Ring) TotalDropped() uint64 {
	return r.ops.dropCount() + r.logs.dropCount() +
		r.metrics.dropCount() + r.payloads.dropCount()
}

// moveCount exposes how many slots the ring has physically relocated. Only the
// amortization test reads it.
// @internal — см. ./README.md
func (r *Ring) moveCount(k RingKind) uint64 {
	switch k {
	case RingOps:
		return r.ops.relocations()
	case RingLogs:
		return r.logs.relocations()
	case RingMetrics:
		return r.metrics.relocations()
	case RingPayloads:
		return r.payloads.relocations()
	default:
		return 0
	}
}

// compactSlack is the number of dead slots tolerated before a repack. It keeps
// a small ring from repacking on every acknowledgement.
const compactSlack = 64

// initialCapacity is the slot count a ring starts with; it doubles from there.
const initialCapacity = 16

type slot[T any] struct {
	id    uint64
	msg   T
	bytes int
	live  bool
}

// kindRing is a circular buffer over [head, head+span). A slot goes dead in
// place on eviction or acknowledgement, so neither operation shifts the
// buffer — that shift is what turns a saturated ring into a quadratic hot
// path. The dead head prefix is trimmed immediately, and holes left by an
// out-of-order acknowledgement are reclaimed by an amortized repack.
type kindRing[T any] struct {
	mu      sync.Mutex
	buf     []slot[T]
	head    int
	span    int // slots in use from head, live and dead alike
	live    int
	used    int
	dropped uint64
	moved   uint64

	budget int
	ids    *atomic.Uint64
	sizeOf func(T) int
}

func newKindRing[T any](budget int, ids *atomic.Uint64, sizeOf func(T) int) *kindRing[T] {
	return &kindRing[T]{budget: budget, ids: ids, sizeOf: sizeOf}
}

func (r *kindRing[T]) push(msg T) {
	n := r.sizeOf(msg)

	r.mu.Lock()
	defer r.mu.Unlock()

	if n > r.budget {
		r.dropped++
		return
	}
	for r.used+n > r.budget && r.live > 0 {
		r.evictOldest()
	}
	r.grow()
	idx := (r.head + r.span) % len(r.buf)
	r.buf[idx] = slot[T]{id: r.ids.Add(1), msg: msg, bytes: n, live: true}
	r.span++
	r.live++
	r.used += n
}

func (r *kindRing[T]) peek(max int) []Item[T] {
	if max <= 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.live == 0 {
		return nil
	}
	out := make([]Item[T], 0, min(max, r.live))
	for i := 0; i < r.span && len(out) < max; i++ {
		s := &r.buf[(r.head+i)%len(r.buf)]
		if !s.live {
			continue
		}
		out = append(out, Item[T]{ID: s.id, Msg: s.msg, Bytes: s.bytes})
	}
	return out
}

func (r *kindRing[T]) commit(items []Item[T]) {
	if len(items) == 0 {
		return
	}
	acked := make(map[uint64]struct{}, len(items))
	for _, it := range items {
		acked[it.ID] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// peek hands out oldest-first, so an acknowledgement almost always covers a
	// head prefix; stopping once every acked id is accounted for keeps the
	// common case proportional to the batch, not to the ring depth.
	removed := 0
	for i := 0; i < r.span && removed < len(acked); i++ {
		idx := (r.head + i) % len(r.buf)
		s := &r.buf[idx]
		if !s.live {
			continue
		}
		if _, ok := acked[s.id]; !ok {
			continue
		}
		r.kill(idx)
		removed++
	}
	r.trimHead()
	r.compact()
}

func (r *kindRing[T]) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.live
}

func (r *kindRing[T]) byteSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.used
}

func (r *kindRing[T]) dropCount() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

func (r *kindRing[T]) relocations() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.moved
}

// evictOldest drops the oldest live item to make room, counting the loss. Its
// caller checks live > 0, so trimHead always leaves a live slot at head.
func (r *kindRing[T]) evictOldest() {
	r.trimHead()
	r.kill(r.head)
	r.dropped++
	r.trimHead()
}

// kill releases one slot's bytes and clears its message so the buffer does not
// pin a dead payload.
func (r *kindRing[T]) kill(idx int) {
	r.used -= r.buf[idx].bytes
	r.live--
	r.buf[idx] = slot[T]{}
}

func (r *kindRing[T]) trimHead() {
	for r.span > 0 && !r.buf[r.head].live {
		r.buf[r.head] = slot[T]{}
		r.head = (r.head + 1) % len(r.buf)
		r.span--
	}
}

// grow guarantees a free slot, doubling and re-linearizing when the ring is full.
func (r *kindRing[T]) grow() {
	if len(r.buf) == 0 {
		r.buf = make([]slot[T], initialCapacity)
		return
	}
	if r.span < len(r.buf) {
		return
	}
	next := make([]slot[T], len(r.buf)*2)
	for i := 0; i < r.span; i++ {
		next[i] = r.buf[(r.head+i)%len(r.buf)]
	}
	r.moved += uint64(r.span)
	r.buf = next
	r.head = 0
}

// compact repacks live slots to the front once dead ones outnumber them. Each
// repack needs at least live-many further removals to become due again, so
// removal stays amortized constant.
func (r *kindRing[T]) compact() {
	dead := r.span - r.live
	if dead <= compactSlack || dead <= r.live {
		return
	}
	next := make([]slot[T], len(r.buf))
	n := 0
	for i := 0; i < r.span; i++ {
		s := r.buf[(r.head+i)%len(r.buf)]
		if !s.live {
			continue
		}
		next[n] = s
		n++
	}
	r.moved += uint64(r.span)
	r.buf = next
	r.head = 0
	r.span = n
}

// Size estimation is deliberately approximate: it sums the variable-length
// fields that dominate memory and adds a flat base for the rest. Encoding each
// message to learn its exact wire size would cost more on the hot path than the
// few percent of budget the estimate misses — the number only has to bound
// buffer growth. Every field that actually retains memory is counted; a metric
// whose labels went uncounted is how a ring silently outgrows its budget.
const sizeBase = 64

func sizeOfOp(m *pb.OpReport) int {
	return sizeBase +
		len(m.GetTraceId()) +
		len(m.GetOpId()) +
		len(m.GetParentOpId()) +
		len(m.GetSubject()) +
		len(m.GetPeerServiceId()) +
		len(m.GetBusinessKey()) +
		len(m.GetStatusMessage()) +
		len(m.GetMetaJson()) +
		len(m.GetAttrsJson())
}

func sizeOfLog(m *pb.Log) int {
	return sizeBase +
		len(m.GetMessage()) +
		len(m.GetFieldsJson()) +
		len(m.GetTraceId()) +
		len(m.GetOpId()) +
		len(m.GetInstanceId()) +
		len(m.GetSource())
}

func sizeOfMetric(m *pb.MetricPoint) int {
	n := sizeBase +
		len(m.GetName()) +
		len(m.GetUnit()) +
		len(m.GetInstanceId()) +
		len(m.GetBucketsJson())
	for k, v := range m.GetLabels() {
		n += len(k) + len(v)
	}
	return n
}

func sizeOfPayload(m *pb.PayloadAttachment) int {
	return sizeBase +
		len(m.GetBytes()) +
		len(m.GetContractHash()) +
		len(m.GetTraceId()) +
		len(m.GetOpId())
}
