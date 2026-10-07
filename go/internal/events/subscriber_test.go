package events

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// fakeStream is one generation of Events.Subscribe. Recv is counted so a test
// can watch the subscriber stop reading.
type fakeStream struct {
	ctx      context.Context
	incoming chan *pb.SubscribeServerMessage
	recvN    atomic.Int64

	mu   sync.Mutex
	sent []*pb.SubscribeClientMessage

	sentCh chan *pb.SubscribeClientMessage
}

func newFakeStream(ctx context.Context, capacity int) *fakeStream {
	return &fakeStream{
		ctx:      ctx,
		incoming: make(chan *pb.SubscribeServerMessage, capacity),
		sentCh:   make(chan *pb.SubscribeClientMessage, capacity+8),
	}
}

func (f *fakeStream) Recv() (*pb.SubscribeServerMessage, error) {
	f.recvN.Add(1)
	select {
	case msg := <-f.incoming:
		return msg, nil
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeStream) Send(msg *pb.SubscribeClientMessage) error {
	f.mu.Lock()
	f.sent = append(f.sent, msg)
	f.mu.Unlock()
	select {
	case f.sentCh <- msg:
	default:
	}
	return nil
}

func (f *fakeStream) CloseSend() error { return nil }

func (f *fakeStream) frames() []*pb.SubscribeClientMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.SubscribeClientMessage(nil), f.sent...)
}

// nextFrame waits for one client frame past the init.
func (f *fakeStream) nextFrame(t *testing.T) *pb.SubscribeClientMessage {
	t.Helper()
	select {
	case msg := <-f.sentCh:
		return msg
	case <-time.After(3 * time.Second):
		t.Fatal("no client frame arrived")
		return nil
	}
}

func (f *fakeStream) push(t *testing.T, d *pb.EventDelivery) {
	t.Helper()
	select {
	case f.incoming <- &pb.SubscribeServerMessage{
		Kind: &pb.SubscribeServerMessage_Delivery{Delivery: d},
	}:
	case <-time.After(3 * time.Second):
		t.Fatal("the stream would not accept a delivery")
	}
}

func delivery(id, name, partitionKey string, payload []byte) *pb.EventDelivery {
	return &pb.EventDelivery{
		DeliveryId: id,
		LeaseToken: "lease-" + id,
		Attempt:    3,
		Envelope: &pb.EventEnvelope{
			Id:           "evt-" + id,
			Name:         name,
			Payload:      payload,
			PartitionKey: partitionKey,
		},
		MatchedPatterns: []string{name},
	}
}

func encodePayload(t *testing.T, v any) []byte {
	t.Helper()
	enc, err := (testCodec{}).Encode("order.created", v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return enc.Proto
}

// subHarness hands out a fresh fakeStream per open and remembers them all.
type subHarness struct {
	mu       sync.Mutex
	streams  []*fakeStream
	capacity int
	identity func() Identity
	opens    atomic.Int64
}

func (h *subHarness) open(ctx context.Context) (SubscribeStream, error) {
	h.opens.Add(1)
	st := newFakeStream(ctx, h.capacity)
	h.mu.Lock()
	h.streams = append(h.streams, st)
	h.mu.Unlock()
	return st, nil
}

func (h *subHarness) stream(t *testing.T, i int) *fakeStream {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		if len(h.streams) > i {
			st := h.streams[i]
			h.mu.Unlock()
			return st
		}
		h.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("stream %d was never opened", i)
	return nil
}

func newTestSubscriber(t *testing.T, h *subHarness, opts ...func(*SubscriberConfig)) *Subscriber {
	t.Helper()
	if h.capacity == 0 {
		h.capacity = 16
	}
	ident := h.identity
	if ident == nil {
		ident = testIdentity
	}
	cfg := SubscriberConfig{
		Open:     h.open,
		Codec:    testCodec{},
		Identity: ident,
		Logger:   discardLogger(),
		Backoff:  stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0)),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	s, err := NewSubscriber(cfg)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	return s
}

func TestSubscribeInitCarriesTheLiveIdentity(t *testing.T) {
	var instance atomic.Int64
	h := &subHarness{identity: func() Identity {
		// instance_id changes on every certificate rotation, so it must be read
		// per open rather than captured once.
		return Identity{ServiceID: "svc-1", InstanceID: fmt.Sprintf("inst-%d", instance.Add(1))}
	}}
	s := newTestSubscriber(t, h, func(c *SubscriberConfig) { c.MaxInFlight = 4 })
	if err := Subscribe(s, "order.created", "", func(context.Context, order) error { return nil }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	first := h.stream(t, 0)
	init := first.nextFrame(t).GetInit()
	if init == nil {
		t.Fatal("the first frame is not an init")
	}
	if init.GetSubscriberInstanceId() != "inst-1" || init.GetSubscriberServiceId() != "svc-1" {
		t.Fatalf("init = %+v", init)
	}
	if init.GetMaxInFlight() != 4 {
		t.Fatalf("max_in_flight = %d, want 4", init.GetMaxInFlight())
	}

	// Drop the stream: the supervisor reopens and the init must carry the new
	// identity, not the one captured at construction.
	s.sup.Restart()
	second := h.stream(t, 1)
	reinit := second.nextFrame(t).GetInit()
	if reinit == nil {
		t.Fatal("the reopened stream did not start with an init")
	}
	if reinit.GetSubscriberInstanceId() != "inst-2" {
		t.Fatalf("instance after reconnect = %q, want inst-2", reinit.GetSubscriberInstanceId())
	}
}

func TestDeliveryReachesTheHandlerAndIsAcked(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)

	got := make(chan order, 1)
	traces := make(chan telemetry.TraceContext, 1)
	err := Subscribe(s, "order.created", "", func(ctx context.Context, ev order) error {
		if tc, ok := telemetry.FromContext(ctx); ok {
			traces <- tc
		}
		got <- ev
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	st := h.stream(t, 0)
	st.nextFrame(t) // init

	tc, err := telemetry.NewRootContext()
	if err != nil {
		t.Fatalf("NewRootContext: %v", err)
	}
	d := delivery("d-1", "order.created", "", encodePayload(t, order{ID: "o-1", Total: 5}))
	d.Envelope.XSbTrace = telemetry.FormatHeader(tc)
	st.push(t, d)

	select {
	case ev := <-got:
		if ev.ID != "o-1" || ev.Total != 5 {
			t.Fatalf("handler saw %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the handler never ran")
	}

	select {
	case handlerTrace := <-traces:
		if handlerTrace.TraceID != tc.TraceID {
			t.Fatalf("handler trace = %v, want the publisher's %v", handlerTrace.TraceID, tc.TraceID)
		}
	case <-time.After(time.Second):
		t.Fatal("the handler ran without a trace context")
	}

	ack := st.nextFrame(t).GetAck()
	if ack == nil {
		t.Fatal("the delivery was not acked")
	}
	if ack.GetDeliveryId() != "d-1" || string(ack.GetEventId()) != "evt-d-1" || ack.GetLeaseToken() != "lease-d-1" {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestDeliveryWithoutAHandlerIsRejected(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	st := h.stream(t, 0)
	st.nextFrame(t)
	// An unsupported local route must remain retryable; it cannot be silently lost.
	st.push(t, delivery("d-1", "unknown.event", "", []byte("proto:{}")))

	frame := st.nextFrame(t)
	if frame.GetNack() == nil {
		t.Fatalf("frame = %+v, want a rejection", frame)
	}
}

func TestDecodeFailureAndPanicRejectTheDelivery(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)

	if err := Subscribe(s, "order.created", "", func(context.Context, order) error { return nil }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := Subscribe(s, "order.exploded", "", func(context.Context, order) error { panic("boom") }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := Subscribe(s, "order.failed", "", func(context.Context, order) error {
		return errors.New("handler said no")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	st := h.stream(t, 0)
	st.nextFrame(t)

	cases := []struct {
		deliveryID string
		name       string
		payload    []byte
		want       string
	}{
		{"d-1", "order.created", []byte("not-the-canonical-form"), "decode"},
		{"d-2", "order.exploded", encodePayload(t, order{}), "panic"},
		{"d-3", "order.failed", encodePayload(t, order{}), "handler said no"},
	}
	for _, tc := range cases {
		st.push(t, delivery(tc.deliveryID, tc.name, "", tc.payload))
		frame := st.nextFrame(t)
		nack := frame.GetNack()
		if nack == nil {
			t.Fatalf("%s: frame = %+v, want a nack", tc.deliveryID, frame)
		}
		if nack.GetDeliveryId() != tc.deliveryID {
			t.Fatalf("nack for %s = %+v", tc.deliveryID, nack)
		}
		if nack.GetErrorMessage() == "" {
			t.Fatalf("%s: nack carries no reason", tc.deliveryID)
		}
		if !contains(nack.GetErrorMessage(), tc.want) {
			t.Fatalf("%s: nack reason %q does not mention %q", tc.deliveryID, nack.GetErrorMessage(), tc.want)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestDeliveryWithoutAnEnvelopeIsRejected(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	st := h.stream(t, 0)
	st.nextFrame(t)
	st.push(t, &pb.EventDelivery{DeliveryId: "d-1"})

	if nack := st.nextFrame(t).GetNack(); nack == nil {
		t.Fatal("an envelope-less delivery was not rejected")
	}
}

func TestSharedPartitionKeyIsHandledInOrder(t *testing.T) {
	const events = 20
	h := &subHarness{capacity: events + 4}
	s := newTestSubscriber(t, h, func(c *SubscriberConfig) { c.MaxInFlight = events })

	var (
		mu        sync.Mutex
		seen      []string
		active    atomic.Int32
		maxActive atomic.Int32
	)
	err := Subscribe(s, "order.created", "", func(_ context.Context, ev order) error {
		n := active.Add(1)
		for {
			peak := maxActive.Load()
			if n <= peak || maxActive.CompareAndSwap(peak, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		mu.Lock()
		seen = append(seen, ev.ID)
		mu.Unlock()
		active.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	st := h.stream(t, 0)
	st.nextFrame(t)
	for i := range events {
		st.push(t, delivery(fmt.Sprintf("d-%02d", i), "order.created", "cust-1",
			encodePayload(t, order{ID: fmt.Sprintf("%02d", i)})))
	}
	for range events {
		if st.nextFrame(t).GetAck() == nil {
			t.Fatal("a keyed delivery was rejected")
		}
	}

	if maxActive.Load() != 1 {
		t.Fatalf("%d handlers for one partition key ran at once, want 1", maxActive.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for i, id := range seen {
		if id != fmt.Sprintf("%02d", i) {
			t.Fatalf("handled out of order at %d: %v", i, seen)
		}
	}
}

func TestEmptyPartitionKeyRunsInParallel(t *testing.T) {
	const events = 8
	h := &subHarness{capacity: events + 4}
	s := newTestSubscriber(t, h, func(c *SubscriberConfig) { c.MaxInFlight = events })

	arrived := make(chan struct{}, events)
	release := make(chan struct{})
	err := Subscribe(s, "order.created", "", func(context.Context, order) error {
		arrived <- struct{}{}
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		close(release)
		s.Stop()
	}()

	st := h.stream(t, 0)
	st.nextFrame(t)
	for i := range events {
		st.push(t, delivery(fmt.Sprintf("d-%d", i), "order.created", "",
			encodePayload(t, order{ID: fmt.Sprint(i)})))
	}

	// All of them must be inside a handler at once; a serialized dispatch would
	// never get past the first.
	for i := range events {
		select {
		case <-arrived:
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d unkeyed handlers ran concurrently", i, events)
		}
	}
}

func TestMaxInFlightRejectsExcessWithoutBlockingStream(t *testing.T) {
	const maxInFlight = 2
	const queued = 100

	h := &subHarness{capacity: queued + 4}
	s := newTestSubscriber(t, h, func(c *SubscriberConfig) { c.MaxInFlight = maxInFlight })

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var handled atomic.Int64
	err := Subscribe(s, "order.created", "", func(context.Context, order) error {
		<-release
		handled.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		unblock()
		s.Stop()
	}()

	st := h.stream(t, 0)
	st.nextFrame(t)
	for i := range queued {
		st.push(t, delivery(fmt.Sprintf("d-%03d", i), "order.created", "",
			encodePayload(t, order{ID: fmt.Sprint(i)})))
	}

	// Runtime credits prevent this in production. An over-delivering peer gets
	// explicit rejections without blocking stream cancellation or allocating a queue.
	rejected := 0
	for rejected < queued-maxInFlight {
		if frame := st.nextFrame(t); frame.GetNack() != nil {
			rejected++
		} else {
			t.Fatalf("unexpected frame before handlers released: %v", frame)
		}
	}
	if handled.Load() != 0 {
		t.Fatal("blocked handlers unexpectedly completed")
	}
	unblock()
	for range maxInFlight {
		if st.nextFrame(t).GetAck() == nil {
			t.Fatal("accepted delivery was not acknowledged")
		}
	}
	if handled.Load() != maxInFlight {
		t.Fatalf("handled %d, admitted limit %d", handled.Load(), maxInFlight)
	}
}

func TestStopWaitsForHandlersAndLeaksNothing(t *testing.T) {
	before := runtime.NumGoroutine()

	h := &subHarness{capacity: 8}
	s := newTestSubscriber(t, h, func(c *SubscriberConfig) { c.MaxInFlight = 4 })

	running := make(chan struct{}, 1)
	finished := make(chan struct{})
	var done atomic.Bool
	err := Subscribe(s, "order.created", "", func(ctx context.Context, _ order) error {
		running <- struct{}{}
		<-ctx.Done()
		done.Store(true)
		close(finished)
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	st := h.stream(t, 0)
	st.nextFrame(t)
	st.push(t, delivery("d-1", "order.created", "cust-1", encodePayload(t, order{})))

	select {
	case <-running:
	case <-time.After(3 * time.Second):
		t.Fatal("the handler never started")
	}

	s.Stop()
	select {
	case <-finished:
	default:
		t.Fatal("Stop returned before the in-flight handler finished")
	}
	if !done.Load() {
		t.Fatal("Stop did not wait for the handler")
	}
	cancel()

	assertGoroutinesSettle(t, before)
}

func TestSubscriberDrainsChainsWhenTheStreamDies(t *testing.T) {
	before := runtime.NumGoroutine()

	h := &subHarness{capacity: 8}
	s := newTestSubscriber(t, h, func(c *SubscriberConfig) { c.MaxInFlight = 1 })

	blocked := make(chan struct{})
	entered := make(chan struct{}, 1)
	err := Subscribe(s, "order.created", "", func(context.Context, order) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-blocked
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	st := h.stream(t, 0)
	st.nextFrame(t)
	// Two deliveries on one key: the second is parked behind the first.
	st.push(t, delivery("d-1", "order.created", "cust-1", encodePayload(t, order{})))
	st.push(t, delivery("d-2", "order.created", "cust-1", encodePayload(t, order{})))

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the first handler never started")
	}

	cancel()
	close(blocked)
	s.Stop()

	assertGoroutinesSettle(t, before)
}

func assertGoroutinesSettle(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines leaked: %d before, %d after", before, runtime.NumGoroutine())
}

func TestSubscribeValidatesItsArguments(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)

	if err := Subscribe[order](s, "Order.Created", "", func(context.Context, order) error { return nil }); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("Subscribe with a malformed name = %v, want ErrInvalidName", err)
	}
	if err := Subscribe[order](s, "order.created", "", nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Subscribe with a nil handler = %v, want ErrInvalidConfig", err)
	}
	if err := Subscribe[order](nil, "order.created", "", func(context.Context, order) error { return nil }); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Subscribe on a nil subscriber = %v, want ErrInvalidConfig", err)
	}
	if err := Subscribe(s, "order.created", `{"$.total":3}`, func(context.Context, order) error { return nil }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := Subscribe(s, "order.created", "", func(context.Context, order) error { return nil }); !errors.Is(err, ErrDuplicatePattern) {
		t.Fatalf("a second handler for one pattern = %v, want ErrDuplicatePattern", err)
	}
	if err := Subscribe(s, "order.*", "", func(context.Context, order) error { return nil }); err != nil {
		t.Fatalf("Subscribe wildcard: %v", err)
	}
	subs := s.Subscriptions()
	if len(subs) != 2 || subs[0] != (Subscription{Pattern: "order.*"}) ||
		subs[1] != (Subscription{Pattern: "order.created", Filter: `{"$.total":3}`}) {
		t.Fatalf("Subscriptions() = %+v", subs)
	}
}

// Routing is the runtime's: the delivery names the patterns it matched and
// exactly those handlers run — every one of them, each once.
func TestDeliveryRunsTheHandlersOfItsMatchedPatterns(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)

	var mu sync.Mutex
	var ran []string
	record := func(name string) Handler[order] {
		return func(context.Context, order) error {
			mu.Lock()
			ran = append(ran, name)
			mu.Unlock()
			return nil
		}
	}
	for _, pattern := range []string{"order.created", "order.*", "#", "billing.#"} {
		if err := Subscribe(s, pattern, "", record(pattern)); err != nil {
			t.Fatalf("Subscribe %s: %v", pattern, err)
		}
	}

	d := delivery("d-1", "order.created", "", encodePayload(t, order{}))
	d.MatchedPatterns = []string{"order.*", "order.created", "order.*", "unknown.pattern"}
	acked, reason := s.Handle(context.Background(), d)
	if !acked {
		t.Fatalf("not acked: %s", reason)
	}
	sort.Strings(ran)
	if fmt.Sprint(ran) != "[order.* order.created]" {
		t.Fatalf("ran %v, want exactly the matched patterns this process has", ran)
	}

	d.MatchedPatterns = []string{"unknown.pattern"}
	if acked, reason := s.Handle(context.Background(), d); acked || reason != NoHandlerReason {
		t.Fatalf("no matched handler: acked=%v reason=%q", acked, reason)
	}
	d.MatchedPatterns = nil
	if acked, _ := s.Handle(context.Background(), d); acked {
		t.Fatal("a delivery naming no pattern must be rejected, never matched locally")
	}
}

func TestOneFailingHandlerRejectsTheDelivery(t *testing.T) {
	s := newTestSubscriber(t, &subHarness{})
	if err := Subscribe(s, "order.*", "", func(context.Context, order) error { return errors.New("boom") }); err != nil {
		t.Fatal(err)
	}
	if err := Subscribe(s, "order.created", "", func(context.Context, order) error { return nil }); err != nil {
		t.Fatal(err)
	}
	d := delivery("d-1", "order.created", "", encodePayload(t, order{}))
	d.MatchedPatterns = []string{"order.created", "order.*"}
	if acked, reason := s.Handle(context.Background(), d); acked || !contains(reason, "boom") {
		t.Fatalf("acked=%v reason=%q", acked, reason)
	}
}

func TestDrainLeavesNewDeliveriesUntouched(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)
	var calls atomic.Int64
	if err := Subscribe(s, "order.created", "", func(context.Context, order) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	st := h.stream(t, 0)
	st.nextFrame(t)

	s.Drain()
	st.push(t, delivery("d-1", "order.created", "", encodePayload(t, order{})))
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 || len(st.frames()) != 1 {
		t.Fatalf("a draining subscriber took new work: %d calls, %d frames", calls.Load(), len(st.frames()))
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := s.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestNewSubscriberDemandsItsDependencies(t *testing.T) {
	h := &subHarness{}
	base := SubscriberConfig{Open: h.open, Codec: testCodec{}, Identity: testIdentity}
	cases := map[string]func(*SubscriberConfig){
		"open":     func(c *SubscriberConfig) { c.Open = nil },
		"codec":    func(c *SubscriberConfig) { c.Codec = nil },
		"identity": func(c *SubscriberConfig) { c.Identity = nil },
	}
	for name, mutate := range cases {
		cfg := base
		mutate(&cfg)
		if _, err := NewSubscriber(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewSubscriber without %s = %v, want ErrInvalidConfig", name, err)
		}
	}
	s, err := NewSubscriber(base)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if cap(s.slots) != DefaultMaxInFlight {
		t.Fatalf("default MaxInFlight = %d, want %d", cap(s.slots), DefaultMaxInFlight)
	}
}

func TestInitFrameIsTheFirstThingOnTheStream(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	st := h.stream(t, 0)
	st.nextFrame(t)
	frames := st.frames()
	if len(frames) == 0 || frames[0].GetInit() == nil {
		t.Fatalf("first frame = %+v, want an init", frames)
	}
}

func TestSubscriberReportsOpenFailures(t *testing.T) {
	failures := make(chan error, 4)
	var opens atomic.Int64
	cfg := SubscriberConfig{
		Open: func(context.Context) (SubscribeStream, error) {
			opens.Add(1)
			return nil, errors.New("no identity yet")
		},
		Codec:    testCodec{},
		Identity: testIdentity,
		Logger:   discardLogger(),
		Backoff:  stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0)),
		OnError: func(err error) {
			select {
			case failures <- err:
			default:
			}
		},
	}
	s, err := NewSubscriber(cfg)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	select {
	case <-failures:
	case <-time.After(3 * time.Second):
		t.Fatal("an open failure was never reported")
	}
	if err := s.Start(ctx); !errors.Is(err, stream.ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}
}

// unwritableStream fails every Send after the init frame, which is what a
// half-dead connection looks like from the ack path.
type unwritableStream struct {
	*fakeStream
	sends atomic.Int64
}

func (u *unwritableStream) Send(msg *pb.SubscribeClientMessage) error {
	if u.sends.Add(1) == 1 {
		return u.fakeStream.Send(msg)
	}
	return errors.New("stream is gone")
}

func TestAckWriteFailureDoesNotStopTheSubscriber(t *testing.T) {
	var current atomic.Pointer[unwritableStream]
	handled := make(chan struct{}, 4)

	cfg := SubscriberConfig{
		Open: func(ctx context.Context) (SubscribeStream, error) {
			st := &unwritableStream{fakeStream: newFakeStream(ctx, 8)}
			current.Store(st)
			return st, nil
		},
		Codec:    testCodec{},
		Identity: testIdentity,
		Logger:   discardLogger(),
		Backoff:  stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0)),
	}
	s, err := NewSubscriber(cfg)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	err = Subscribe(s, "order.created", "", func(context.Context, order) error {
		handled <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for current.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	st := current.Load()
	if st == nil {
		t.Fatal("the stream was never opened")
	}
	st.push(t, delivery("d-1", "order.created", "", encodePayload(t, order{})))

	select {
	case <-handled:
	case <-time.After(3 * time.Second):
		t.Fatal("the handler never ran")
	}
}

func TestDeliveryMetadataAndRejectionEchoLeaseGeneration(t *testing.T) {
	h := &subHarness{}
	s := newTestSubscriber(t, h)
	seen := make(chan DeliveryInfo, 1)
	if err := Subscribe(s, "order.*", "", func(ctx context.Context, _ order) error {
		info, ok := DeliveryFromContext(ctx)
		if !ok {
			return errors.New("missing attempt metadata")
		}
		seen <- info
		return errors.New("retry business handler")
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	st := h.stream(t, 0)
	st.nextFrame(t)
	d := delivery("d", "order.created", "pk", encodePayload(t, order{ID: "o"}))
	d.MatchedPatterns = []string{"order.*"}
	d.Envelope.Headers = map[string]string{"h": "v"}
	d.Envelope.OccurredAtUnixMs = 1234
	st.push(t, d)
	frame := st.nextFrame(t)
	if frame.GetNack() == nil || frame.GetNack().GetLeaseToken() != "lease-d" {
		t.Fatalf("NACK lost exact lease generation: %v", frame)
	}
	select {
	case info := <-seen:
		if info.Attempt != 3 || info.DeliveryID != "d" || info.EventName != "order.created" || info.LeaseToken != "lease-d" ||
			info.EventID != "evt-d" || info.PartitionKey != "pk" || info.Headers["h"] != "v" || info.OccurredAtMs != 1234 {
			t.Fatalf("metadata lost: %v", info)
		}
	case <-time.After(time.Second):
		t.Fatal("metadata not observed")
	}
}
