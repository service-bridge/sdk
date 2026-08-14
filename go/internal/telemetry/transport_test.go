package telemetry

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
)

// fakeStream records what the transport wrote and replays scripted
// acknowledgements.
type fakeStream struct {
	ctx  context.Context
	acks chan *pb.TelemetryAck
	fail chan error

	mu        sync.Mutex
	batches   []*pb.TelemetryBatch
	closeSend int
	sendErr   error
}

func newFakeStream(ctx context.Context) *fakeStream {
	return &fakeStream{
		ctx:  ctx,
		acks: make(chan *pb.TelemetryAck, 8),
		fail: make(chan error, 1),
	}
}

func (f *fakeStream) Send(b *pb.TelemetryBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	f.batches = append(f.batches, b)
	return nil
}

func (f *fakeStream) Recv() (*pb.TelemetryAck, error) {
	select {
	case ack := <-f.acks:
		return ack, nil
	case err := <-f.fail:
		return nil, err
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeStream) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeSend++
	return nil
}

func (f *fakeStream) closeSendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeSend
}

func (f *fakeStream) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

// opSubjects returns every op subject the stream received, in order.
func (f *fakeStream) opSubjects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, b := range f.batches {
		for _, item := range b.GetOps().GetItems() {
			out = append(out, item.GetSubject())
		}
	}
	return out
}

func (f *fakeStream) metricPoints() []*pb.MetricPoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*pb.MetricPoint
	for _, b := range f.batches {
		out = append(out, b.GetMetrics().GetItems()...)
	}
	return out
}

// streamFactory hands out one fake stream per open, so a test can watch a
// reconnect land on a fresh one.
type streamFactory struct {
	mu      sync.Mutex
	opened  []*fakeStream
	waiters []chan struct{}
}

func (sf *streamFactory) open(ctx context.Context) (ReportStream, error) {
	st := newFakeStream(ctx)
	sf.mu.Lock()
	sf.opened = append(sf.opened, st)
	for _, w := range sf.waiters {
		close(w)
	}
	sf.waiters = nil
	sf.mu.Unlock()
	return st, nil
}

func (sf *streamFactory) count() int {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return len(sf.opened)
}

func (sf *streamFactory) at(i int) *fakeStream {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.opened[i]
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// startTransport wires a transport over the factory and waits until the first
// stream is live. The flush interval is long enough that only the explicit
// Flush calls and the ack path drive the wire.
func startTransport(t *testing.T, cfg TransportConfig) (*Transport, *streamFactory) {
	t.Helper()

	sf := &streamFactory{}
	cfg.Open = sf.open
	if cfg.Ring == nil {
		cfg.Ring = NewRing(Budgets{})
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NewMetrics(cfg.Ring)
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = time.Hour
	}
	cfg.Backoff = stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0))

	tr, err := NewTransport(cfg)
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}
	if err := tr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(tr.Stop)

	waitFor(t, "first stream", func() bool {
		_, ok := tr.sup.Current()
		return ok
	})
	return tr, sf
}

func pushOps(ring *Ring, n int) {
	for i := 0; i < n; i++ {
		ring.PushOp(&pb.OpReport{Subject: "op-" + strconv.Itoa(i)})
	}
}

// The runtime acknowledges on a fixed two-second ticker. One batch per
// acknowledgement would make that ticker the throughput ceiling, so a single
// flush cycle must drain everything the buffer holds.
func TestTransportThroughputIsNotBoundByAckInterval(t *testing.T) {
	ring := NewRing(Budgets{Ops: 1 << 20})
	pushOps(ring, 300)

	tr, sf := startTransport(t, TransportConfig{Ring: ring, MaxBatchItems: 10})
	tr.Flush()

	st := sf.at(0)
	if got := st.batchCount(); got != 30 {
		t.Fatalf("batches written without a single ack = %d, want 30", got)
	}
	if got := len(st.opSubjects()); got != 300 {
		t.Fatalf("ops written = %d, want 300", got)
	}
}

// An acknowledgement names no batch, so it may only release what the runtime
// provably had. Items written after the ack was emitted must survive it.
func TestTransportAckDoesNotReleaseUnseenItems(t *testing.T) {
	ring := NewRing(Budgets{})
	ring.PushOp(&pb.OpReport{Subject: "first"})

	tr, sf := startTransport(t, TransportConfig{Ring: ring})
	tr.Flush()
	st := sf.at(0)

	// "second" is written by the flush the acknowledgement itself triggers, so
	// it races that acknowledgement in exactly the way the epoch lag exists for.
	ring.PushOp(&pb.OpReport{Subject: "second"})
	st.acks <- &pb.TelemetryAck{}
	waitFor(t, "second op on the wire", func() bool { return len(st.opSubjects()) == 2 })

	if got := ring.Len(RingOps); got != 2 {
		t.Fatalf("buffered ops after the first ack = %d, want 2 — neither item is proven received", got)
	}

	st.acks <- &pb.TelemetryAck{}
	waitFor(t, "first op released", func() bool { return ring.Len(RingOps) == 1 })

	// The second item was written during the epoch the second ack closed, so it
	// is still unproven and must stay buffered.
	if got := ring.Len(RingOps); got != 1 {
		t.Fatalf("buffered ops after the second ack = %d, want 1", got)
	}

	st.acks <- &pb.TelemetryAck{}
	waitFor(t, "second op released", func() bool { return ring.Len(RingOps) == 0 })
}

// Backpressure is advisory. Pausing the flusher while producers keep writing
// evicts the oldest buffered frames — the START frames the runtime opens rows
// from — so a congested runtime must not stop the wire.
func TestTransportKeepsFlushingUnderBackpressure(t *testing.T) {
	ring := NewRing(Budgets{})
	tr, sf := startTransport(t, TransportConfig{Ring: ring})
	st := sf.at(0)

	st.acks <- &pb.TelemetryAck{BackpressureLevel: 9}
	waitFor(t, "backpressure observed", func() bool { return tr.BackpressureLevel() == 9 })

	pushOps(ring, 5)
	tr.Flush()

	if got := len(st.opSubjects()); got != 5 {
		t.Fatalf("ops written while congested = %d, want 5", got)
	}
}

func TestTransportDrainFlushesAndClosesSend(t *testing.T) {
	ring := NewRing(Budgets{})
	drained := make(chan string, 1)

	_, sf := startTransport(t, TransportConfig{
		Ring:    ring,
		OnDrain: func(reason string) { drained <- reason },
	})
	st := sf.at(0)

	pushOps(ring, 3)
	st.acks <- &pb.TelemetryAck{DrainReason: "runtime shutting down"}

	select {
	case reason := <-drained:
		if reason != "runtime shutting down" {
			t.Fatalf("drain reason = %q", reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the drain callback")
	}

	if got := len(st.opSubjects()); got != 3 {
		t.Fatalf("ops written on drain = %d, want 3 — the final flush must precede the close", got)
	}
	if got := st.closeSendCount(); got != 1 {
		t.Fatalf("CloseSend calls = %d, want 1", got)
	}
}

func TestTransportResendsUnacknowledgedAfterReconnect(t *testing.T) {
	ring := NewRing(Budgets{})
	pushOps(ring, 4)

	tr, sf := startTransport(t, TransportConfig{Ring: ring})
	tr.Flush()

	first := sf.at(0)
	if got := len(first.opSubjects()); got != 4 {
		t.Fatalf("ops on the first stream = %d, want 4", got)
	}

	first.fail <- errors.New("stream broke")
	waitFor(t, "reconnect", func() bool { return sf.count() == 2 })
	waitFor(t, "second stream live", func() bool {
		_, ok := tr.sup.Current()
		return ok
	})

	if got := ring.Len(RingOps); got != 4 {
		t.Fatalf("buffered ops after the break = %d, want 4 — nothing was acknowledged", got)
	}

	tr.Flush()
	second := sf.at(1)
	if got := second.opSubjects(); len(got) != 4 {
		t.Fatalf("ops resent on the second stream = %d, want 4", len(got))
	}
}

// The aggregator is materialised once per cycle, so a burst of increments
// reaches the wire as one point.
func TestTransportWritesAggregatedMetrics(t *testing.T) {
	ring := NewRing(Budgets{})
	metrics := NewMetrics(ring)

	tr, sf := startTransport(t, TransportConfig{Ring: ring, Metrics: metrics})

	counter, err := metrics.Counter("inst-1", "requests", nil)
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	for i := 0; i < 1000; i++ {
		counter.Inc()
	}
	tr.Flush()

	points := sf.at(0).metricPoints()
	if len(points) != 1 {
		t.Fatalf("metric points on the wire = %d, want 1", len(points))
	}
	if points[0].GetValue() != 1000 {
		t.Fatalf("counter value = %v, want 1000", points[0].GetValue())
	}
}

// Every kind travels on its own batch: TelemetryBatch carries exactly one.
func TestTransportWritesEveryKind(t *testing.T) {
	ring := NewRing(Budgets{})
	ring.PushOp(&pb.OpReport{Subject: "op"})
	ring.PushLog(&pb.Log{Message: "log"})
	ring.PushMetric(&pb.MetricPoint{Name: "metric"})
	ring.PushPayload(&pb.PayloadAttachment{OpId: "op"})

	tr, sf := startTransport(t, TransportConfig{Ring: ring})
	tr.Flush()

	st := sf.at(0)
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.batches) != 4 {
		t.Fatalf("batches = %d, want one per kind", len(st.batches))
	}
	kinds := map[string]bool{}
	for _, b := range st.batches {
		switch b.GetKind().(type) {
		case *pb.TelemetryBatch_Ops:
			kinds["ops"] = true
		case *pb.TelemetryBatch_Logs:
			kinds["logs"] = true
		case *pb.TelemetryBatch_Metrics:
			kinds["metrics"] = true
		case *pb.TelemetryBatch_Payloads:
			kinds["payloads"] = true
		}
	}
	if len(kinds) != 4 {
		t.Fatalf("kinds on the wire = %v", kinds)
	}
}

// A failed write leaves the items where they were: this stream is finished and
// the next one re-reads them from the head.
func TestTransportReportsASendFailureAndKeepsTheItems(t *testing.T) {
	ring := NewRing(Budgets{})
	failures := make(chan error, 4)

	tr, sf := startTransport(t, TransportConfig{
		Ring:    ring,
		OnError: func(err error) { failures <- err },
	})
	st := sf.at(0)
	st.mu.Lock()
	st.sendErr = errors.New("write failed")
	st.mu.Unlock()

	pushOps(ring, 2)
	tr.Flush()

	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("nil error reported")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the send failure went unreported")
	}
	if got := ring.Len(RingOps); got != 2 {
		t.Fatalf("buffered ops = %d, want 2", got)
	}
}

func TestTransportWithoutAStreamKeepsItemsBuffered(t *testing.T) {
	ring := NewRing(Budgets{})
	pushOps(ring, 3)

	tr, err := NewTransport(TransportConfig{
		Open:    (&streamFactory{}).open,
		Ring:    ring,
		Metrics: NewMetrics(ring),
	})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}
	tr.Flush()

	if got := ring.Len(RingOps); got != 3 {
		t.Fatalf("buffered ops = %d, want 3", got)
	}
}

func TestTransportReportsRisingDropCounts(t *testing.T) {
	ring := NewRing(Budgets{})
	drops := make(chan DropInfo, 4)

	_, sf := startTransport(t, TransportConfig{
		Ring:   ring,
		OnDrop: func(info DropInfo) { drops <- info },
	})
	st := sf.at(0)

	st.acks <- &pb.TelemetryAck{DropCountServerSide: 7, BackpressureLevel: 2}
	select {
	case info := <-drops:
		if info.ServerDropped != 7 || info.BackpressureLevel != 2 {
			t.Fatalf("drop info = %+v", info)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the drop report")
	}

	// An unchanged counter is not news and must not fire again.
	st.acks <- &pb.TelemetryAck{DropCountServerSide: 7}
	st.acks <- &pb.TelemetryAck{DropCountServerSide: 9}
	select {
	case info := <-drops:
		if info.ServerDropped != 9 {
			t.Fatalf("drop info = %+v, want only the rise to 9", info)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second drop report")
	}
}

func TestTransportStopReleasesItsGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	ring := NewRing(Budgets{})
	sf := &streamFactory{}
	tr, err := NewTransport(TransportConfig{
		Open:          sf.open,
		Ring:          ring,
		Metrics:       NewMetrics(ring),
		FlushInterval: time.Millisecond,
		Backoff:       stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0)),
	})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}
	if err := tr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, "stream", func() bool { return sf.count() > 0 })
	tr.Stop()

	waitFor(t, "goroutines to settle", func() bool { return runtime.NumGoroutine() <= before+1 })
}

func TestNewTransportRejectsMissingDependencies(t *testing.T) {
	ring := NewRing(Budgets{})
	cases := map[string]TransportConfig{
		"open":    {Ring: ring, Metrics: NewMetrics(ring)},
		"ring":    {Open: (&streamFactory{}).open, Metrics: NewMetrics(ring)},
		"metrics": {Open: (&streamFactory{}).open, Ring: ring},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewTransport(cfg); !errors.Is(err, ErrTransportConfig) {
				t.Fatalf("err = %v, want ErrTransportConfig", err)
			}
		})
	}
}

func TestTransportStartsOnlyOnce(t *testing.T) {
	tr, _ := startTransport(t, TransportConfig{})
	if err := tr.Start(context.Background()); !errors.Is(err, ErrTransportStarted) {
		t.Fatalf("err = %v, want ErrTransportStarted", err)
	}
}
