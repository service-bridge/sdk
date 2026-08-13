package telemetry

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

func opWithSubject(subject string) *pb.OpReport {
	return &pb.OpReport{Subject: subject}
}

func subjects(items []Item[*pb.OpReport]) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.Msg.GetSubject())
	}
	return out
}

func TestRingPeekIsFIFO(t *testing.T) {
	r := NewRing(Budgets{})

	for _, s := range []string{"first", "second", "third"} {
		r.PushOp(opWithSubject(s))
	}

	got := subjects(r.Peek(10).Ops)
	want := []string{"first", "second", "third"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("peek order = %v, want %v", got, want)
	}
	if r.Len(RingOps) != 3 {
		t.Fatalf("len = %d, want 3", r.Len(RingOps))
	}
}

func TestRingDefaultBudgetsApplyToZeroFields(t *testing.T) {
	r := NewRing(Budgets{Ops: 1024})

	if got := r.ops.budget; got != 1024 {
		t.Fatalf("ops budget = %d, want 1024", got)
	}
	if got := r.logs.budget; got != defaultLogsBudget {
		t.Fatalf("logs budget = %d, want %d", got, defaultLogsBudget)
	}
	if got := r.metrics.budget; got != defaultMetricsBudget {
		t.Fatalf("metrics budget = %d, want %d", got, defaultMetricsBudget)
	}
	if got := r.payloads.budget; got != defaultPayloadsBudget {
		t.Fatalf("payloads budget = %d, want %d", got, defaultPayloadsBudget)
	}
}

func TestRingEvictsOldestAndCountsTheLoss(t *testing.T) {
	item := sizeOfOp(opWithSubject("a"))
	r := NewRing(Budgets{Ops: item * 3})

	for _, s := range []string{"a", "b", "c", "d"} {
		r.PushOp(opWithSubject(s))
	}

	got := subjects(r.Peek(10).Ops)
	if strings.Join(got, ",") != "b,c,d" {
		t.Fatalf("after eviction = %v, want [b c d]", got)
	}
	if r.Dropped(RingOps) != 1 {
		t.Fatalf("dropped = %d, want 1", r.Dropped(RingOps))
	}
	if r.TotalDropped() != 1 {
		t.Fatalf("total dropped = %d, want 1", r.TotalDropped())
	}
	if r.Bytes(RingOps) != item*3 {
		t.Fatalf("bytes = %d, want %d", r.Bytes(RingOps), item*3)
	}
}

func TestRingRefusesItemLargerThanBudget(t *testing.T) {
	r := NewRing(Budgets{Ops: 32})

	r.PushOp(opWithSubject(strings.Repeat("x", 128)))

	if r.Len(RingOps) != 0 {
		t.Fatalf("len = %d, want 0", r.Len(RingOps))
	}
	if r.Dropped(RingOps) != 1 {
		t.Fatalf("dropped = %d, want 1", r.Dropped(RingOps))
	}
}

func TestRingPeekDoesNotRemove(t *testing.T) {
	r := NewRing(Budgets{})
	r.PushOp(opWithSubject("a"))
	r.PushOp(opWithSubject("b"))

	first := r.Peek(10)
	second := r.Peek(10)

	if len(first.Ops) != 2 || len(second.Ops) != 2 {
		t.Fatalf("peek sizes = %d and %d, want 2 and 2", len(first.Ops), len(second.Ops))
	}
	if first.Ops[0].ID != second.Ops[0].ID || first.Ops[1].ID != second.Ops[1].ID {
		t.Fatal("consecutive peeks returned different ids")
	}
	if r.Len(RingOps) != 2 {
		t.Fatalf("len after peeks = %d, want 2", r.Len(RingOps))
	}
}

func TestRingPeekRespectsMaxPerKind(t *testing.T) {
	r := NewRing(Budgets{})
	for i := range 5 {
		r.PushOp(opWithSubject(fmt.Sprintf("op-%d", i)))
	}

	if got := len(r.Peek(2).Ops); got != 2 {
		t.Fatalf("peek(2) = %d items, want 2", got)
	}
	if got := r.Peek(0); !got.Empty() {
		t.Fatalf("peek(0) = %d items, want none", got.Len())
	}
}

// Uncommitted items are the at-least-once contract: only what the runtime
// acknowledged leaves the ring, and the rest comes back on the next peek.
func TestRingCommitKeepsUnacknowledgedItems(t *testing.T) {
	r := NewRing(Budgets{})
	for _, s := range []string{"a", "b", "c"} {
		r.PushOp(opWithSubject(s))
	}

	batch := r.Peek(10)
	// Acknowledge the head and the tail, leaving a hole in the middle.
	r.Commit(Batch{Ops: []Item[*pb.OpReport]{batch.Ops[0], batch.Ops[2]}})

	left := r.Peek(10)
	if got := subjects(left.Ops); strings.Join(got, ",") != "b" {
		t.Fatalf("after partial commit = %v, want [b]", got)
	}
	if left.Ops[0].ID != batch.Ops[1].ID {
		t.Fatalf("surviving id = %d, want %d", left.Ops[0].ID, batch.Ops[1].ID)
	}
	if r.Bytes(RingOps) != sizeOfOp(opWithSubject("b")) {
		t.Fatalf("bytes = %d, want the single surviving item", r.Bytes(RingOps))
	}

	// It survives repeated peeks until it is acknowledged in its turn.
	again := r.Peek(10)
	if len(again.Ops) != 1 || again.Ops[0].ID != left.Ops[0].ID {
		t.Fatal("unacknowledged item did not survive a second peek")
	}

	r.Commit(again)
	if r.Len(RingOps) != 0 || r.Bytes(RingOps) != 0 {
		t.Fatalf("after full commit len=%d bytes=%d, want 0 and 0", r.Len(RingOps), r.Bytes(RingOps))
	}
}

func TestRingCommitIgnoresUnknownIDs(t *testing.T) {
	r := NewRing(Budgets{})
	r.PushOp(opWithSubject("a"))

	r.Commit(Batch{Ops: []Item[*pb.OpReport]{{ID: 9999}}})

	if r.Len(RingOps) != 1 {
		t.Fatalf("len = %d, want 1", r.Len(RingOps))
	}
}

func TestRingKindsAreIndependent(t *testing.T) {
	r := NewRing(Budgets{})
	r.PushOp(opWithSubject("op"))
	r.PushLog(&pb.Log{Message: "log"})
	r.PushMetric(&pb.MetricPoint{Name: "metric"})
	r.PushPayload(&pb.PayloadAttachment{Bytes: []byte("payload")})

	batch := r.Peek(10)
	if batch.Len() != 4 {
		t.Fatalf("batch len = %d, want 4", batch.Len())
	}

	r.Commit(Batch{Logs: batch.Logs})

	if r.Len(RingLogs) != 0 {
		t.Fatalf("logs len = %d, want 0", r.Len(RingLogs))
	}
	if r.Len(RingOps) != 1 || r.Len(RingMetrics) != 1 || r.Len(RingPayloads) != 1 {
		t.Fatal("committing one kind disturbed another")
	}
}

// The metric estimate must count labels: a metric whose labels went uncounted
// is how a ring silently outgrows its declared budget.
func TestMetricSizeCountsLabels(t *testing.T) {
	bare := &pb.MetricPoint{Name: "rpc_calls"}
	labelled := &pb.MetricPoint{
		Name:   "rpc_calls",
		Labels: map[string]string{"service": strings.Repeat("s", 100), "method": strings.Repeat("m", 100)},
	}

	if got, want := sizeOfMetric(labelled), sizeOfMetric(bare)+213; got != want {
		t.Fatalf("labelled metric size = %d, want %d", got, want)
	}
}

func TestSizeEstimatesCountVariableFields(t *testing.T) {
	op := sizeOfOp(&pb.OpReport{Subject: "abc", MetaJson: []byte("{}"), AttrsJson: []byte("{}")})
	if op != sizeBase+3+2+2 {
		t.Fatalf("op size = %d, want %d", op, sizeBase+7)
	}
	log := sizeOfLog(&pb.Log{Message: "hi", FieldsJson: []byte("{}"), Source: "sdk"})
	if log != sizeBase+2+2+3 {
		t.Fatalf("log size = %d, want %d", log, sizeBase+7)
	}
	payload := sizeOfPayload(&pb.PayloadAttachment{Bytes: []byte("12345"), ContractHash: "ab"})
	if payload != sizeBase+5+2 {
		t.Fatalf("payload size = %d, want %d", payload, sizeBase+7)
	}
}

func TestRingCountersCoverEveryKind(t *testing.T) {
	r := NewRing(Budgets{Ops: 1, Logs: 1, Metrics: 1, Payloads: 1})

	// Every message is over its one-byte budget, so each kind refuses one.
	r.PushOp(opWithSubject("op"))
	r.PushLog(&pb.Log{Message: "log"})
	r.PushMetric(&pb.MetricPoint{Name: "metric"})
	r.PushPayload(&pb.PayloadAttachment{Bytes: []byte("payload")})

	for _, k := range []RingKind{RingOps, RingLogs, RingMetrics, RingPayloads} {
		if got := r.Dropped(k); got != 1 {
			t.Fatalf("%s dropped = %d, want 1", k, got)
		}
		if got := r.Len(k); got != 0 {
			t.Fatalf("%s len = %d, want 0", k, got)
		}
		if got := r.Bytes(k); got != 0 {
			t.Fatalf("%s bytes = %d, want 0", k, got)
		}
		if got := r.moveCount(k); got != 0 {
			t.Fatalf("%s moves = %d, want 0", k, got)
		}
	}
	if r.TotalDropped() != 4 {
		t.Fatalf("total dropped = %d, want 4", r.TotalDropped())
	}

	unknown := RingKind(200)
	if r.Len(unknown) != 0 || r.Bytes(unknown) != 0 || r.Dropped(unknown) != 0 || r.moveCount(unknown) != 0 {
		t.Fatal("an unknown kind reported non-zero counters")
	}
	if unknown.String() != "unknown" {
		t.Fatalf("RingKind(200) = %q", unknown.String())
	}
	names := map[RingKind]string{RingOps: "ops", RingLogs: "logs", RingMetrics: "metrics", RingPayloads: "payloads"}
	for k, want := range names {
		if k.String() != want {
			t.Fatalf("%d = %q, want %q", k, k.String(), want)
		}
	}
}

func TestDefaultBudgets(t *testing.T) {
	b := DefaultBudgets()
	if b.Ops != defaultOpsBudget || b.Logs != defaultLogsBudget ||
		b.Metrics != defaultMetricsBudget || b.Payloads != defaultPayloadsBudget {
		t.Fatalf("DefaultBudgets = %+v", b)
	}
}

// Holes left by out-of-order acknowledgements are reclaimed by a repack; the
// surviving items keep their identity and their order across it.
func TestRingRepacksAfterScatteredCommits(t *testing.T) {
	const pushed = 1000
	r := NewRing(Budgets{})
	for i := range pushed {
		r.PushOp(opWithSubject(fmt.Sprintf("op-%04d", i)))
	}

	batch := r.Peek(pushed)
	acked := make([]Item[*pb.OpReport], 0, 700)
	survivors := make([]Item[*pb.OpReport], 0, 300)
	for i, it := range batch.Ops {
		if i%10 < 7 {
			acked = append(acked, it)
			continue
		}
		survivors = append(survivors, it)
	}
	r.Commit(Batch{Ops: acked})

	if r.moveCount(RingOps) == 0 {
		t.Fatal("scattered commits left the buffer unpacked")
	}
	left := r.Peek(pushed)
	if len(left.Ops) != len(survivors) {
		t.Fatalf("left = %d items, want %d", len(left.Ops), len(survivors))
	}
	for i, it := range left.Ops {
		if it.ID != survivors[i].ID || it.Msg.GetSubject() != survivors[i].Msg.GetSubject() {
			t.Fatalf("item %d = %d/%q, want %d/%q", i,
				it.ID, it.Msg.GetSubject(), survivors[i].ID, survivors[i].Msg.GetSubject())
		}
	}
}

// Eviction and acknowledgement must not shift the buffer: an array-shifting
// ring degrades linearly exactly when it is full, which is when it matters.
func TestRingStaysAmortizedUnderSaturation(t *testing.T) {
	const total = 50_000
	item := sizeOfOp(opWithSubject("saturation"))
	r := NewRing(Budgets{Ops: item * 200})

	for i := range total {
		r.PushOp(opWithSubject("saturation"))
		if i%10 != 9 {
			continue
		}
		// Acknowledge every other item so holes form mid-ring, the shape that
		// forces a repack.
		batch := r.Peek(20)
		acked := make([]Item[*pb.OpReport], 0, len(batch.Ops)/2)
		for j := 0; j < len(batch.Ops); j += 2 {
			acked = append(acked, batch.Ops[j])
		}
		r.Commit(Batch{Ops: acked})
	}

	if moves := r.moveCount(RingOps); moves > 4*total {
		t.Fatalf("relocated %d slots for %d pushes, want amortized constant work", moves, total)
	}
	if r.Bytes(RingOps) > item*200 {
		t.Fatalf("bytes = %d, over the %d budget", r.Bytes(RingOps), item*200)
	}
}

func TestRingIsSafeForConcurrentUse(t *testing.T) {
	const writers = 8
	const perWriter = 500
	// A budget wide enough for every push keeps the invariant exact: with
	// eviction in play a peeked item can be evicted before its commit lands,
	// and no count could tell the two apart.
	item := sizeOfOp(opWithSubject("concurrent"))
	r := NewRing(Budgets{Ops: item * writers * perWriter})

	stop := make(chan struct{})
	drained := make(chan int, 1)
	go func() {
		committed := 0
		for {
			batch := r.Peek(16)
			r.Commit(batch)
			committed += len(batch.Ops)
			select {
			case <-stop:
				last := r.Peek(writers * perWriter)
				r.Commit(last)
				drained <- committed + len(last.Ops)
				return
			default:
				runtime.Gosched()
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(writers)
	for range writers {
		go func() {
			defer wg.Done()
			for range perWriter {
				r.PushOp(opWithSubject("concurrent"))
			}
		}()
	}
	wg.Wait()
	close(stop)
	committed := <-drained

	dropped := int(r.Dropped(RingOps))
	remaining := r.Len(RingOps)
	if got := committed + dropped + remaining; got != writers*perWriter {
		t.Fatalf("committed %d + dropped %d + left %d = %d, want %d",
			committed, dropped, remaining, got, writers*perWriter)
	}
	if remaining != 0 {
		t.Fatalf("final drain left %d items", remaining)
	}
	if r.Bytes(RingOps) != 0 {
		t.Fatalf("bytes after full drain = %d, want 0", r.Bytes(RingOps))
	}
}
