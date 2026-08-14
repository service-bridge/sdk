package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
)

// ErrTransportConfig is returned by NewTransport when a required dependency is
// missing.
var ErrTransportConfig = errors.New("invalid telemetry transport config")

// ErrTransportStarted is returned by Start on a transport that already runs.
var ErrTransportStarted = errors.New("telemetry transport already started")

// ReportStream is the client side of Telemetry.Report. The generated
// grpc.BidiStreamingClient[TelemetryBatch, TelemetryAck] satisfies it.
type ReportStream interface {
	Send(*pb.TelemetryBatch) error
	Recv() (*pb.TelemetryAck, error)
	CloseSend() error
}

// session is one opened stream, identified by its own address. The supervisor
// is generic over the stream type and hands it back on every frame, so wrapping
// it here gives the transport an identity it can compare without depending on
// the concrete stream being comparable at all.
type session struct {
	stream ReportStream
}

func (s *session) Recv() (*pb.TelemetryAck, error) { return s.stream.Recv() }

// DefaultFlushInterval is how often the transport drains the buffer on its own.
const DefaultFlushInterval = 250 * time.Millisecond

// DefaultMaxBatchItems caps one batch per kind. It is not a throughput ceiling:
// one flush cycle writes as many batches as the buffer holds.
const DefaultMaxBatchItems = 256

// DropInfo reports telemetry that was lost, either by the runtime shedding load
// or by the local buffer overflowing.
type DropInfo struct {
	ServerDropped     uint64
	BufferDropped     uint64
	BackpressureLevel uint32
}

// TransportConfig wires the Telemetry.Report client. See ./README.md.
type TransportConfig struct {
	// Open builds one Report stream bound to ctx. Cancelling ctx must unblock
	// Recv. Evaluated on every reconnect, so a rotated identity is picked up.
	Open func(ctx context.Context) (ReportStream, error)
	// Ring is the buffer drained onto the stream.
	Ring *Ring
	// Metrics is materialised into the buffer once per flush cycle.
	Metrics *Metrics
	// FlushInterval defaults to DefaultFlushInterval.
	FlushInterval time.Duration
	// MaxBatchItems defaults to DefaultMaxBatchItems.
	MaxBatchItems int
	// OnDrop fires when a drop counter rises on either side.
	OnDrop func(DropInfo)
	// OnDrain fires once per stream when the runtime asks the SDK to close. The
	// final flush has already been written when it runs.
	OnDrain func(reason string)
	// OnError reports stream failures. The transport reconnects regardless.
	OnError func(err error)
	// Backoff pins the reconnect ladder. Zero value falls back to the default.
	Backoff stream.Backoff
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Transport keeps the Telemetry.Report bidi stream alive and drains the buffer
// onto it. Items leave the buffer only once an acknowledgement proves the
// runtime saw them, which is what makes delivery at-least-once across a
// reconnect.
type Transport struct {
	cfg TransportConfig
	sup *stream.Supervisor[*pb.TelemetryAck, *session]

	wg sync.WaitGroup

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	// cur is the session the in-flight bookkeeping below belongs to. A cycle
	// that picked up a stream just before it was replaced would otherwise mark
	// items in flight against the new session's ledger and see them released
	// two acknowledgements later, having never left the process.
	cur      *session
	inflight inflightSet
	// ackEpoch counts the acknowledgements seen on the current stream. See
	// inflightSet.release for what it buys.
	ackEpoch      uint64
	draining      bool
	backpressure  uint32
	serverDropped uint64
	bufferDropped uint64
}

// NewTransport validates the config and builds an idle transport.
func NewTransport(cfg TransportConfig) (*Transport, error) {
	if cfg.Open == nil {
		return nil, fmt.Errorf("telemetry: new transport: missing Open: %w", ErrTransportConfig)
	}
	if cfg.Ring == nil {
		return nil, fmt.Errorf("telemetry: new transport: missing Ring: %w", ErrTransportConfig)
	}
	if cfg.Metrics == nil {
		return nil, fmt.Errorf("telemetry: new transport: missing Metrics: %w", ErrTransportConfig)
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.MaxBatchItems <= 0 {
		cfg.MaxBatchItems = DefaultMaxBatchItems
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	t := &Transport{cfg: cfg}
	t.inflight.reset()

	sup, err := stream.NewSupervisor(stream.Config[*pb.TelemetryAck, *session]{
		Name:    "telemetry.report",
		Open:    t.open,
		OnData:  t.onData,
		OnError: t.reportError,
		Backoff: cfg.Backoff,
		Logger:  cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("telemetry: new transport: %w", err)
	}
	t.sup = sup
	return t, nil
}

// Start opens the stream and runs the flush loop until ctx is cancelled or Stop
// runs.
func (t *Transport) Start(ctx context.Context) error {
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return fmt.Errorf("telemetry: transport: start: %w", ErrTransportStarted)
	}
	runCtx, cancel := context.WithCancel(ctx)
	t.started = true
	t.cancel = cancel
	t.mu.Unlock()

	if err := t.sup.Start(runCtx); err != nil {
		cancel()
		return fmt.Errorf("telemetry: transport: start: %w", err)
	}

	t.wg.Add(1)
	go t.flushLoop(runCtx)
	return nil
}

// Stop writes one last cycle, then tears the stream and the flush loop down.
// Terminal: a stopped transport cannot be started again.
func (t *Transport) Stop() {
	t.Flush()

	t.mu.Lock()
	cancel := t.cancel
	t.cancel = nil
	t.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	t.wg.Wait()
	t.sup.Stop()
}

// Flush runs one cycle right now: it materialises the metric aggregator into
// the buffer and writes batches until nothing unsent is left.
func (t *Transport) Flush() {
	sess, ok := t.sup.Current()
	if !ok {
		// No stream: the items stay buffered and go out on the next one.
		return
	}

	// One aggregation window per cycle. Draining here rather than per written
	// batch keeps a cycle's metric points together instead of splintering them
	// across the batches the loop below emits.
	t.cfg.Metrics.Flush(nowUnixMs())

	t.mu.Lock()
	var err error
	if t.cur == sess {
		for {
			var wrote bool
			wrote, err = t.writeOnceLocked(sess.stream)
			if err != nil || !wrote {
				break
			}
		}
	}
	t.mu.Unlock()

	if err != nil {
		t.reportError(err)
	}
}

// BackpressureLevel reports the level the runtime last advertised.
func (t *Transport) BackpressureLevel() uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.backpressure
}

func (t *Transport) flushLoop(ctx context.Context) {
	defer t.wg.Done()

	ticker := time.NewTicker(t.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.Flush()
		}
	}
}

// open builds the stream and resets everything the previous one carried. Items
// that were in flight when it died were never acknowledged, so they stay in the
// buffer and are written again here — at-least-once.
func (t *Transport) open(ctx context.Context) (*session, error) {
	st, err := t.cfg.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("telemetry: transport: open report stream: %w", err)
	}
	sess := &session{stream: st}

	t.mu.Lock()
	t.cur = sess
	t.inflight.reset()
	t.ackEpoch = 0
	t.draining = false
	t.mu.Unlock()

	return sess, nil
}

func (t *Transport) onData(_ context.Context, ack *pb.TelemetryAck, sess *session) {
	t.mu.Lock()
	t.ackEpoch++
	confirmed := t.inflight.release(t.ackEpoch)
	if !confirmed.Empty() {
		t.cfg.Ring.Commit(confirmed)
	}
	t.backpressure = ack.GetBackpressureLevel()
	drop, rose := t.dropInfoLocked(ack)
	drainReason := ""
	if reason := ack.GetDrainReason(); reason != "" && !t.draining {
		t.draining = true
		drainReason = reason
	}
	t.mu.Unlock()

	if rose && t.cfg.OnDrop != nil {
		t.cfg.OnDrop(drop)
	}

	// An acknowledgement is the cheapest proof the runtime is keeping up, so
	// refill the wire now instead of idling until the next tick. The runtime's
	// backpressure level is deliberately not consulted: it is advisory, and
	// pausing the flusher while producers keep writing makes the buffer evict
	// its oldest items — which are the START frames the runtime needs to open a
	// row. Losing the tail of a batch beats losing its head.
	t.Flush()

	if drainReason == "" {
		return
	}
	if err := sess.stream.CloseSend(); err != nil {
		t.reportError(fmt.Errorf("telemetry: transport: close send on drain: %w", err))
	}
	if t.cfg.OnDrain != nil {
		t.cfg.OnDrain(drainReason)
	}
}

// dropInfoLocked reports the current loss counters and whether either of them
// rose since the last report.
func (t *Transport) dropInfoLocked(ack *pb.TelemetryAck) (DropInfo, bool) {
	serverDropped := ack.GetDropCountServerSide()
	bufferDropped := t.cfg.Ring.TotalDropped()
	rose := serverDropped > t.serverDropped || bufferDropped > t.bufferDropped
	t.serverDropped = serverDropped
	t.bufferDropped = bufferDropped
	return DropInfo{
		ServerDropped:     serverDropped,
		BufferDropped:     bufferDropped,
		BackpressureLevel: ack.GetBackpressureLevel(),
	}, rose
}

// writeOnceLocked writes the next unsent slice of the buffer and records it as
// in flight. It reports whether anything went out.
func (t *Transport) writeOnceLocked(st ReportStream) (bool, error) {
	batch := t.selectLocked()
	if batch.Empty() {
		return false, nil
	}
	if err := sendBatch(st, batch); err != nil {
		// The items keep their place in the buffer: this stream is finished, and
		// the next one re-reads them from the head. Anything the peer did take
		// before the failure arrives twice, which the at-least-once contract
		// allows.
		return false, err
	}
	t.inflight.add(batch, t.ackEpoch)
	return true, nil
}

// selectLocked returns up to MaxBatchItems per kind that are not already on the
// wire. It reads past the in-flight prefix: Peek hands out the oldest items
// first and in-flight items sit at that head until they are committed, so a
// Peek capped at MaxBatchItems would return in-flight items only and the
// transport would write nothing at all between acknowledgements — with the
// runtime acknowledging on a fixed two-second ticker, that is where throughput
// silently collapses.
func (t *Transport) selectLocked() Batch {
	max := t.cfg.MaxBatchItems
	peeked := t.cfg.Ring.Peek(max + t.inflight.deepestKind())
	return Batch{
		Ops:      selectUnsent(peeked.Ops, t.inflight.ids, max),
		Logs:     selectUnsent(peeked.Logs, t.inflight.ids, max),
		Metrics:  selectUnsent(peeked.Metrics, t.inflight.ids, max),
		Payloads: selectUnsent(peeked.Payloads, t.inflight.ids, max),
	}
}

func (t *Transport) reportError(err error) {
	t.cfg.Logger.Warn("telemetry: transport", "err", err)
	if t.cfg.OnError != nil {
		t.cfg.OnError(err)
	}
}

func sendBatch(st ReportStream, b Batch) error {
	if len(b.Ops) > 0 {
		items := make([]*pb.OpReport, len(b.Ops))
		for i, it := range b.Ops {
			items[i] = it.Msg
		}
		msg := &pb.TelemetryBatch{Kind: &pb.TelemetryBatch_Ops{Ops: &pb.OpBatch{Items: items}}}
		if err := st.Send(msg); err != nil {
			return fmt.Errorf("telemetry: transport: send ops: %w", err)
		}
	}
	if len(b.Logs) > 0 {
		items := make([]*pb.Log, len(b.Logs))
		for i, it := range b.Logs {
			items[i] = it.Msg
		}
		msg := &pb.TelemetryBatch{Kind: &pb.TelemetryBatch_Logs{Logs: &pb.LogBatch{Items: items}}}
		if err := st.Send(msg); err != nil {
			return fmt.Errorf("telemetry: transport: send logs: %w", err)
		}
	}
	if len(b.Metrics) > 0 {
		items := make([]*pb.MetricPoint, len(b.Metrics))
		for i, it := range b.Metrics {
			items[i] = it.Msg
		}
		msg := &pb.TelemetryBatch{Kind: &pb.TelemetryBatch_Metrics{Metrics: &pb.MetricBatch{Items: items}}}
		if err := st.Send(msg); err != nil {
			return fmt.Errorf("telemetry: transport: send metrics: %w", err)
		}
	}
	if len(b.Payloads) > 0 {
		items := make([]*pb.PayloadAttachment, len(b.Payloads))
		for i, it := range b.Payloads {
			items[i] = it.Msg
		}
		msg := &pb.TelemetryBatch{Kind: &pb.TelemetryBatch_Payloads{Payloads: &pb.PayloadBatch{Items: items}}}
		if err := st.Send(msg); err != nil {
			return fmt.Errorf("telemetry: transport: send payloads: %w", err)
		}
	}
	return nil
}

func selectUnsent[T any](items []Item[T], sent map[uint64]struct{}, max int) []Item[T] {
	var out []Item[T]
	for _, it := range items {
		if len(out) >= max {
			break
		}
		if _, ok := sent[it.ID]; ok {
			continue
		}
		out = append(out, it)
	}
	return out
}

// inflightEntry is one written item tagged with the acknowledgement epoch it
// was written in.
type inflightEntry[T any] struct {
	epoch uint64
	item  Item[T]
}

// inflightSet holds everything written on the current stream and not yet
// released.
type inflightSet struct {
	ids      map[uint64]struct{}
	ops      []inflightEntry[*pb.OpReport]
	logs     []inflightEntry[*pb.Log]
	metrics  []inflightEntry[*pb.MetricPoint]
	payloads []inflightEntry[*pb.PayloadAttachment]
}

func (s *inflightSet) reset() {
	s.ids = make(map[uint64]struct{})
	s.ops = nil
	s.logs = nil
	s.metrics = nil
	s.payloads = nil
}

func (s *inflightSet) add(b Batch, epoch uint64) {
	s.ops = addInflight(s.ops, b.Ops, epoch, s.ids)
	s.logs = addInflight(s.logs, b.Logs, epoch, s.ids)
	s.metrics = addInflight(s.metrics, b.Metrics, epoch, s.ids)
	s.payloads = addInflight(s.payloads, b.Payloads, epoch, s.ids)
}

// release returns everything the acknowledgement that raised the epoch to
// ackEpoch actually proves the runtime received.
//
// TelemetryAck carries no batch identifier and the runtime emits it on a fixed
// ticker (runtime/internal/telemetry/server.go), so no acknowledgement can name
// what it confirms; exact confirmation would need a protocol change. What an
// acknowledgement does prove is that the runtime's receive loop had consumed
// everything that reached it before the ack was emitted. An item written during
// epoch E left this process before ack E arrived here, so the runtime held it
// at most one network delay after ack E was emitted — comfortably before ack
// E+1 goes out a full ack interval later. Epoch E is therefore released by the
// acknowledgement that raises the epoch to E+2: one ack of lag, deliberately.
// Releasing the current epoch as well — what committing the whole in-flight set
// does — would drop items written by a cycle that raced the ack in transit:
// they would vanish from the buffer having never arrived. That is the honesty
// boundary: at-least-once holds, and the cost is one extra ack interval of
// buffer occupancy plus a duplicate delivery window of the same size.
func (s *inflightSet) release(ackEpoch uint64) Batch {
	var confirmed Batch
	confirmed.Ops, s.ops = releaseInflight(s.ops, ackEpoch, s.ids)
	confirmed.Logs, s.logs = releaseInflight(s.logs, ackEpoch, s.ids)
	confirmed.Metrics, s.metrics = releaseInflight(s.metrics, ackEpoch, s.ids)
	confirmed.Payloads, s.payloads = releaseInflight(s.payloads, ackEpoch, s.ids)
	return confirmed
}

// deepestKind reports the largest per-kind in-flight count, which is how far
// past the buffer head a Peek must reach to find unsent items of every kind.
func (s *inflightSet) deepestKind() int {
	return max(max(len(s.ops), len(s.logs)), max(len(s.metrics), len(s.payloads)))
}

func addInflight[T any](dst []inflightEntry[T], items []Item[T], epoch uint64, ids map[uint64]struct{}) []inflightEntry[T] {
	for _, it := range items {
		ids[it.ID] = struct{}{}
		dst = append(dst, inflightEntry[T]{epoch: epoch, item: it})
	}
	return dst
}

func releaseInflight[T any](entries []inflightEntry[T], ackEpoch uint64, ids map[uint64]struct{}) (confirmed []Item[T], held []inflightEntry[T]) {
	for _, e := range entries {
		if e.epoch+1 < ackEpoch {
			confirmed = append(confirmed, e.item)
			delete(ids, e.item.ID)
			continue
		}
		held = append(held, e)
	}
	return confirmed, held
}
