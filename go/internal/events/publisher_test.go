package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/outbox"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// order is the payload every test publishes.
type order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

// testCodec keeps the two wire forms visibly different so a test can tell which
// one a component carried.
type testCodec struct {
	encodeErr error
	decodeErr error
}

func (c testCodec) Encode(name string, payload any) (Encoded, error) {
	if c.encodeErr != nil {
		return Encoded{}, c.encodeErr
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return Encoded{}, fmt.Errorf("marshal: %w", err)
	}
	return Encoded{
		Proto:        append([]byte("proto:"), b...),
		JSON:         b,
		ContractHash: "hash-" + name,
	}, nil
}

func (c testCodec) Decode(name string, payload []byte, out any) error {
	if c.decodeErr != nil {
		return c.decodeErr
	}
	const prefix = "proto:"
	if len(payload) < len(prefix) || string(payload[:len(prefix)]) != prefix {
		return fmt.Errorf("payload of %q is not in the canonical form", name)
	}
	return json.Unmarshal(payload[len(prefix):], out)
}

func testIdentity() Identity {
	return Identity{ServiceID: "svc-1", InstanceID: "inst-1"}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// clock is a settable unix-ms source.
type clock struct{ ms atomic.Int64 }

func newClock(startMs int64) *clock {
	c := &clock{}
	c.ms.Store(startMs)
	return c
}

func (c *clock) now() int64          { return c.ms.Load() }
func (c *clock) advance(delta int64) { c.ms.Add(delta) }

// recordingPublish captures every PublishRequest and answers with whatever the
// current responder returns.
type recordingPublish struct {
	mu       sync.Mutex
	requests []*pb.PublishRequest
	respond  func(*pb.PublishRequest) (*pb.PublishResponse, error)
	calls    atomic.Int64
}

func (r *recordingPublish) publish(_ context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	r.calls.Add(1)
	r.mu.Lock()
	r.requests = append(r.requests, req)
	respond := r.respond
	r.mu.Unlock()
	if respond == nil {
		return acceptAll(req), nil
	}
	return respond(req)
}

func (r *recordingPublish) setResponder(fn func(*pb.PublishRequest) (*pb.PublishResponse, error)) {
	r.mu.Lock()
	r.respond = fn
	r.mu.Unlock()
}

func (r *recordingPublish) lastRequest() *pb.PublishRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.requests) == 0 {
		return nil
	}
	return r.requests[len(r.requests)-1]
}

func acceptAll(req *pb.PublishRequest) *pb.PublishResponse {
	resp := &pb.PublishResponse{}
	for _, e := range req.GetEvents() {
		resp.Results = append(resp.Results, &pb.PublishStatusEntry{
			EventId: e.GetId(),
			Status:  pb.PublishStatus_PUBLISH_STATUS_ACCEPTED,
		})
	}
	return resp
}

func openStorage(t *testing.T, dir string) *outbox.Storage {
	t.Helper()
	st, err := outbox.Open(context.Background(), outbox.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func newTestPublisher(t *testing.T, st *outbox.Storage, pub PublishFunc, opts ...func(*PublisherConfig)) *Publisher {
	t.Helper()
	cfg := PublisherConfig{
		Storage:  st,
		Codec:    testCodec{},
		Publish:  pub,
		Identity: testIdentity,
		Logger:   discardLogger(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	p, err := NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return p
}

func TestPublishReturnsFast(t *testing.T) {
	st := openStorage(t, t.TempDir())
	// A transport that never answers: a publish that waited on it would hang.
	blocked := func(ctx context.Context, _ *pb.PublishRequest) (*pb.PublishResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	p := newTestPublisher(t, st, blocked)

	start := time.Now()
	for i := range 50 {
		if _, err := p.Publish(context.Background(), "order.created", order{ID: fmt.Sprint(i)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("50 publishes took %v, want under 500ms — publish must be a local insert", elapsed)
	}
	if st.Rows() != 50 {
		t.Fatalf("outbox holds %d rows, want 50", st.Rows())
	}
}

func TestPublishedEventSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	st, err := outbox.Open(ctx, outbox.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	p := newTestPublisher(t, st, (&recordingPublish{}).publish)
	id, err := p.Publish(ctx, "order.created", order{ID: "o-1", Total: 42},
		WithPartitionKey("cust-1"), WithHeaders(map[string]string{"tenant": "acme"}))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := outbox.Open(ctx, outbox.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen outbox: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	rec, status, err := reopened.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if status != outbox.StatusPending {
		t.Fatalf("status = %q after restart, want pending", status)
	}
	if rec.PartitionKey != "cust-1" || rec.Headers["tenant"] != "acme" {
		t.Fatalf("record lost fields across the restart: %+v", rec)
	}
	var got order
	if err := (testCodec{}).Decode("order.created", rec.Payload, &got); err != nil {
		t.Fatalf("decode restored payload: %v", err)
	}
	if got.ID != "o-1" || got.Total != 42 {
		t.Fatalf("restored payload = %+v", got)
	}
}

func TestPublishStoresBothWireForms(t *testing.T) {
	st := openStorage(t, t.TempDir())
	p := newTestPublisher(t, st, (&recordingPublish{}).publish)
	ctx := context.Background()

	id, err := p.Publish(ctx, "order.created", order{ID: "o-1", Total: 7})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rec, _, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// The runtime's ingest hook fires for every accepted event and reads the
	// JSON mirror to evaluate workflow wait_event filters, so it can never be
	// skipped.
	if string(rec.PayloadJSON) != `{"id":"o-1","total":7}` {
		t.Fatalf("payload_json = %q, want the JSON mirror of the payload", rec.PayloadJSON)
	}
	if string(rec.Payload) != `proto:{"id":"o-1","total":7}` {
		t.Fatalf("payload = %q, want the canonical form", rec.Payload)
	}
	if rec.ContractHash != "hash-order.created" {
		t.Fatalf("contract_hash = %q", rec.ContractHash)
	}
}

func TestPublishRejectsMalformedName(t *testing.T) {
	st := openStorage(t, t.TempDir())
	p := newTestPublisher(t, st, (&recordingPublish{}).publish)

	for _, name := range []string{"", "Order.Created", "order..created", ".order", "order.", "order created", "order/created"} {
		_, err := p.Publish(context.Background(), name, order{})
		if !errors.Is(err, ErrInvalidName) {
			t.Fatalf("Publish(%q) error = %v, want ErrInvalidName", name, err)
		}
	}
	for _, name := range []string{"order", "order.created", "order-v2.created_at", "a.b.c.d"} {
		if !ValidEventName(name) {
			t.Fatalf("ValidEventName(%q) = false", name)
		}
	}
	if st.Rows() != 0 {
		t.Fatalf("a rejected name reached the outbox: %d rows", st.Rows())
	}
}

func TestPublishRefusesWhenOutboxIsFull(t *testing.T) {
	st := openStorage(t, t.TempDir())
	p := newTestPublisher(t, st, (&recordingPublish{}).publish, func(c *PublisherConfig) {
		c.MaxOutboxRows = 2
	})
	ctx := context.Background()

	for i := range 2 {
		if _, err := p.Publish(ctx, "order.created", order{ID: fmt.Sprint(i)}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	_, err := p.Publish(ctx, "order.created", order{ID: "overflow"})
	if !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("Publish past the cap error = %v, want ErrOutboxFull", err)
	}
	if !errors.Is(err, outbox.ErrFull) {
		t.Fatalf("ErrOutboxFull must match outbox.ErrFull, got %v", err)
	}
}

func TestPublishCarriesTheTraceFromContext(t *testing.T) {
	st := openStorage(t, t.TempDir())
	p := newTestPublisher(t, st, (&recordingPublish{}).publish)

	tc, err := telemetry.NewRootContext()
	if err != nil {
		t.Fatalf("NewRootContext: %v", err)
	}
	ctx := telemetry.WithTraceContext(context.Background(), tc)

	id, err := p.Publish(ctx, "order.created", order{ID: "o-1"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rec, _, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if rec.Trace != telemetry.FormatHeader(tc) {
		t.Fatalf("x_sb_trace = %q, want %q", rec.Trace, telemetry.FormatHeader(tc))
	}

	// Without a trace in ctx the field stays empty and the runtime mints a root.
	id2, err := p.Publish(context.Background(), "order.created", order{ID: "o-2"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rec2, _, err := st.Load(context.Background(), id2)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if rec2.Trace != "" {
		t.Fatalf("x_sb_trace = %q without a trace in ctx, want empty", rec2.Trace)
	}
}

func TestFireAndForgetBypassesTheOutbox(t *testing.T) {
	st := openStorage(t, t.TempDir())
	transport := &recordingPublish{}
	p := newTestPublisher(t, st, transport.publish)

	id, err := p.Publish(context.Background(), "order.created", order{ID: "o-1"}, WithFireAndForget())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if st.Rows() != 0 {
		t.Fatalf("the no-wait path buffered %d rows, want 0", st.Rows())
	}
	req := transport.lastRequest()
	if req == nil {
		t.Fatal("the no-wait path sent nothing")
	}
	if req.GetPublisherServiceId() != "svc-1" || req.GetPublisherInstanceId() != "inst-1" {
		t.Fatalf("identity on the wire = %q/%q", req.GetPublisherServiceId(), req.GetPublisherInstanceId())
	}
	env := req.GetEvents()[0]
	if env.GetId() != id || !env.GetFireAndForget() {
		t.Fatalf("envelope = %+v", env)
	}
	if len(env.GetPayloadJson()) == 0 {
		t.Fatal("the no-wait path dropped the JSON mirror; the runtime hook needs it for every accepted event")
	}
}

func TestFireAndForgetSurfacesTransportAndPolicyFailures(t *testing.T) {
	st := openStorage(t, t.TempDir())
	transport := &recordingPublish{}
	p := newTestPublisher(t, st, transport.publish)
	ctx := context.Background()

	transport.setResponder(func(*pb.PublishRequest) (*pb.PublishResponse, error) {
		return nil, errors.New("runtime unreachable")
	})
	if _, err := p.Publish(ctx, "order.created", order{}, WithFireAndForget()); err == nil {
		t.Fatal("the no-wait path swallowed a transport failure")
	}

	transport.setResponder(func(req *pb.PublishRequest) (*pb.PublishResponse, error) {
		return &pb.PublishResponse{Results: []*pb.PublishStatusEntry{{
			EventId: req.GetEvents()[0].GetId(),
			Status:  pb.PublishStatus_PUBLISH_STATUS_REJECTED_FORBIDDEN,
			Message: "event.publish denied",
		}}}, nil
	})
	_, err := p.Publish(ctx, "order.created", order{}, WithFireAndForget())
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("policy denial error = %v, want ErrRejected", err)
	}
}

func TestPublishKicksTheDrain(t *testing.T) {
	st := openStorage(t, t.TempDir())
	var kicks atomic.Int64
	p := newTestPublisher(t, st, (&recordingPublish{}).publish, func(c *PublisherConfig) {
		c.Kick = func() { kicks.Add(1) }
	})

	if _, err := p.Publish(context.Background(), "order.created", order{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if kicks.Load() != 1 {
		t.Fatalf("kicks = %d, want 1", kicks.Load())
	}
	if _, err := p.Publish(context.Background(), "order.created", order{}, WithFireAndForget()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if kicks.Load() != 1 {
		t.Fatal("the no-wait path kicked the drain although it buffered nothing")
	}
}

func TestPublishUsesTheSuppliedOccurredAt(t *testing.T) {
	st := openStorage(t, t.TempDir())
	c := newClock(1_700_000_000_000)
	p := newTestPublisher(t, st, (&recordingPublish{}).publish, func(cfg *PublisherConfig) {
		cfg.Now = c.now
	})
	ctx := context.Background()

	id, err := p.Publish(ctx, "order.created", order{}, WithOccurredAt(1_600_000_000_000))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rec, _, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if rec.OccurredAtMs != 1_600_000_000_000 {
		t.Fatalf("occurred_at_ms = %d, want the supplied value", rec.OccurredAtMs)
	}
	if rec.EnqueuedAtMs != 1_700_000_000_000 {
		t.Fatalf("enqueued_at_ms = %d, want the clock value", rec.EnqueuedAtMs)
	}
}

func TestPublishReportsEncodeFailure(t *testing.T) {
	st := openStorage(t, t.TempDir())
	boom := errors.New("no schema registered")
	p := newTestPublisher(t, st, (&recordingPublish{}).publish, func(c *PublisherConfig) {
		c.Codec = testCodec{encodeErr: boom}
	})
	_, err := p.Publish(context.Background(), "order.created", order{})
	if !errors.Is(err, boom) {
		t.Fatalf("Publish error = %v, want the codec failure", err)
	}
	if st.Rows() != 0 {
		t.Fatalf("an unencodable payload reached the outbox: %d rows", st.Rows())
	}
}

func TestNewPublisherDemandsItsDependencies(t *testing.T) {
	st := openStorage(t, t.TempDir())
	base := PublisherConfig{
		Storage:  st,
		Codec:    testCodec{},
		Publish:  (&recordingPublish{}).publish,
		Identity: testIdentity,
	}
	cases := map[string]func(*PublisherConfig){
		"storage":  func(c *PublisherConfig) { c.Storage = nil },
		"codec":    func(c *PublisherConfig) { c.Codec = nil },
		"publish":  func(c *PublisherConfig) { c.Publish = nil },
		"identity": func(c *PublisherConfig) { c.Identity = nil },
		"max rows": func(c *PublisherConfig) { c.MaxOutboxRows = -1 },
	}
	for name, mutate := range cases {
		cfg := base
		mutate(&cfg)
		if _, err := NewPublisher(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewPublisher without %s = %v, want ErrInvalidConfig", name, err)
		}
	}
	if _, err := NewPublisher(base); err != nil {
		t.Fatalf("NewPublisher with a complete config: %v", err)
	}
}

func TestPublishOptionsReachTheStoredRecord(t *testing.T) {
	st := openStorage(t, t.TempDir())
	p := newTestPublisher(t, st, (&recordingPublish{}).publish)
	ctx := context.Background()

	id, err := p.Publish(ctx, "order.created", order{ID: "o-1"},
		WithIdempotencyKey("idem-1"),
		WithPartitionKey("cust-1"),
		WithHeaders(map[string]string{"tenant": "acme"}),
		WithOccurredAt(1_600_000_000_000))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rec, _, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if rec.IdempotencyKey != "idem-1" || rec.PartitionKey != "cust-1" ||
		rec.Headers["tenant"] != "acme" || rec.OccurredAtMs != 1_600_000_000_000 {
		t.Fatalf("options did not reach the record: %+v", rec)
	}
	if rec.FireAndForget {
		t.Fatal("a buffered row must never carry the no-wait flag")
	}
}

func TestPublishReportsAnIdentifierFailure(t *testing.T) {
	st := openStorage(t, t.TempDir())
	boom := errors.New("entropy source broken")
	p := newTestPublisher(t, st, (&recordingPublish{}).publish, func(c *PublisherConfig) {
		c.NewID = func() (string, error) { return "", boom }
	})
	if _, err := p.Publish(context.Background(), "order.created", order{}); !errors.Is(err, boom) {
		t.Fatalf("Publish error = %v, want the id failure", err)
	}
}
