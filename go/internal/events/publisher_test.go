package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

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

// fakeRuntime answers Publish. verdict decides each envelope; nil accepts.
type fakeRuntime struct {
	mu       sync.Mutex
	requests [][]*pb.EventEnvelope
	verdict  func(call int, env *pb.EventEnvelope) (*pb.PublishStatusEntry, error)
	gate     chan struct{} // when set, every Publish waits for it
	calls    chan int
}

func newFakeRuntime() *fakeRuntime { return &fakeRuntime{calls: make(chan int, 1024)} }

func (f *fakeRuntime) publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req.GetEvents())
	n := len(f.requests)
	verdict, gate := f.verdict, f.gate
	f.mu.Unlock()
	f.calls <- n

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	resp := &pb.PublishResponse{}
	for _, env := range req.GetEvents() {
		entry := &pb.PublishStatusEntry{EventId: env.GetId(), Status: pb.PublishStatus_PUBLISH_STATUS_ACCEPTED}
		if verdict != nil {
			v, err := verdict(n, env)
			if err != nil {
				return nil, err
			}
			if v != nil {
				entry = v
			}
		}
		resp.Results = append(resp.Results, entry)
	}
	return resp, nil
}

func (f *fakeRuntime) sent() [][]*pb.EventEnvelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]*pb.EventEnvelope(nil), f.requests...)
}

func newTestPublisher(t *testing.T, rt *fakeRuntime, tweak func(*PublisherConfig)) *Publisher {
	t.Helper()
	cfg := PublisherConfig{
		Codec:       testCodec{},
		Publish:     rt.publish,
		RetryLadder: []time.Duration{time.Millisecond},
		Logger:      discardLogger(),
	}
	if tweak != nil {
		tweak(&cfg)
	}
	p, err := NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return p
}

func startPublisher(t *testing.T, p *Publisher) {
	t.Helper()
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		p.Close(ctx)
	})
}

func TestPublishResolvesOnTheRuntimeAcknowledgement(t *testing.T) {
	rt := newFakeRuntime()
	p := newTestPublisher(t, rt, nil)
	startPublisher(t, p)

	ctx := telemetry.WithTraceContext(context.Background(), telemetry.TraceContext{})
	id, err := p.Publish(ctx, "order.created", order{ID: "o1", Total: 3},
		WithPartitionKey("o1"), WithIdempotencyKey("k1"), WithHeaders(map[string]string{"h": "v"}), WithOccurredAt(42))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	reqs := rt.sent()
	if len(reqs) != 1 || len(reqs[0]) != 1 {
		t.Fatalf("requests %v", reqs)
	}
	env := reqs[0][0]
	if env.GetId() != id || env.GetName() != "order.created" || env.GetPartitionKey() != "o1" ||
		env.GetIdempotencyKey() != "k1" || env.GetHeaders()["h"] != "v" || env.GetOccurredAtUnixMs() != 42 ||
		env.GetContractHash() != "hash-order.created" {
		t.Fatalf("envelope %+v", env)
	}
	if string(env.GetPayloadJson()) != `{"id":"o1","total":3}` {
		t.Fatalf("payload_json %q: it must always be filled", env.GetPayloadJson())
	}
	if p.Pending() != 0 {
		t.Fatalf("pending %d after the acknowledgement", p.Pending())
	}
}

func TestEventIDsRiseInPublishOrder(t *testing.T) {
	rt := newFakeRuntime()
	p := newTestPublisher(t, rt, nil)
	var ids []string
	for i := range 50 {
		id, err := p.Publish(context.Background(), "order.created", order{Total: i}, WithFireAndForget())
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("ids are not monotonic: %v", ids)
	}
}

func TestFullQueueFailsAtOnce(t *testing.T) {
	rt := newFakeRuntime()
	p := newTestPublisher(t, rt, func(c *PublisherConfig) { c.MaxPending = 2 })
	for range 2 {
		if _, err := p.Publish(context.Background(), "order.created", order{}, WithFireAndForget()); err != nil {
			t.Fatal(err)
		}
	}
	_, err := p.Publish(context.Background(), "order.created", order{}, WithFireAndForget())
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
}

func TestTimeoutSaysWhetherTheEventWasSent(t *testing.T) {
	t.Run("never sent", func(t *testing.T) {
		rt := newFakeRuntime()
		p := newTestPublisher(t, rt, func(c *PublisherConfig) { c.Timeout = 20 * time.Millisecond })
		_, err := p.Publish(context.Background(), "order.created", order{})
		if !errors.Is(err, ErrNotSent) {
			t.Fatalf("got %v, want ErrNotSent", err)
		}
		if p.Pending() != 0 {
			t.Fatal("an abandoned unsent event must leave the queue")
		}
	})
	t.Run("sent, never settled", func(t *testing.T) {
		rt := newFakeRuntime()
		rt.verdict = func(int, *pb.EventEnvelope) (*pb.PublishStatusEntry, error) {
			return nil, errors.New("transport down")
		}
		p := newTestPublisher(t, rt, func(c *PublisherConfig) { c.Timeout = 50 * time.Millisecond })
		startPublisher(t, p)
		_, err := p.Publish(context.Background(), "order.created", order{})
		if !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("got %v, want ErrOutcomeUnknown", err)
		}
		waitFor(t, func() bool { return p.Pending() == 0 })
	})
}

func TestCallerCancellationAbandonsTheEvent(t *testing.T) {
	p := newTestPublisher(t, newFakeRuntime(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Publish(ctx, "order.created", order{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if p.Pending() != 0 {
		t.Fatal("a cancelled unsent event must leave the queue")
	}
}

// One batch in flight, at most BatchSize events, at most one per partition
// key — and every key keeps its publish order.
func TestBatchesKeepOneEventPerKeyAndPerKeyOrder(t *testing.T) {
	rt := newFakeRuntime()
	p := newTestPublisher(t, rt, func(c *PublisherConfig) { c.BatchSize = 3 })

	var published []string
	keys := []string{"a", "a", "b", "", "", "a", "b", "c"}
	for i, key := range keys {
		id, err := p.Publish(context.Background(), "order.created", order{Total: i}, WithPartitionKey(key), WithFireAndForget())
		if err != nil {
			t.Fatal(err)
		}
		published = append(published, id)
	}
	startPublisher(t, p)
	waitFor(t, func() bool { return p.Pending() == 0 })

	order := map[string][]string{}
	for _, batch := range rt.sent() {
		if len(batch) > 3 {
			t.Fatalf("batch of %d over the cap", len(batch))
		}
		seen := map[string]bool{}
		for _, env := range batch {
			key := env.GetPartitionKey()
			if key != "" && seen[key] {
				t.Fatalf("key %q twice in one batch", key)
			}
			seen[key] = true
			order[key] = append(order[key], env.GetId())
		}
	}
	for key, ids := range order {
		if key != "" && !sort.StringsAreSorted(ids) {
			t.Fatalf("key %q went out of order: %v", key, ids)
		}
	}
	if total := len(order["a"]) + len(order["b"]) + len(order["c"]) + len(order[""]); total != len(published) {
		t.Fatalf("sent %d events, want %d", total, len(published))
	}
}

func TestVerdictsMapToResults(t *testing.T) {
	rt := newFakeRuntime()
	rt.verdict = func(_ int, env *pb.EventEnvelope) (*pb.PublishStatusEntry, error) {
		switch env.GetName() {
		case "dup.event":
			return &pb.PublishStatusEntry{EventId: "original-id", Status: pb.PublishStatus_PUBLISH_STATUS_REJECTED_DUPLICATE}, nil
		case "conflict.event":
			return &pb.PublishStatusEntry{Status: pb.PublishStatus_PUBLISH_STATUS_REJECTED_CONFLICT}, nil
		case "bad.event":
			return &pb.PublishStatusEntry{Status: pb.PublishStatus_PUBLISH_STATUS_REJECTED_INVALID_NAME}, nil
		case "secret.event":
			return &pb.PublishStatusEntry{Status: pb.PublishStatus_PUBLISH_STATUS_REJECTED_FORBIDDEN, Message: "no rule"}, nil
		}
		return nil, nil
	}
	var violations []PolicyViolation
	var mu sync.Mutex
	p := newTestPublisher(t, rt, func(c *PublisherConfig) {
		c.OnPolicyViolation = func(v PolicyViolation) {
			mu.Lock()
			violations = append(violations, v)
			mu.Unlock()
		}
	})
	startPublisher(t, p)

	if id, err := p.Publish(context.Background(), "dup.event", order{}); err != nil || id != "original-id" {
		t.Fatalf("duplicate: id %q err %v, want the original id", id, err)
	}
	if _, err := p.Publish(context.Background(), "conflict.event", order{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if _, err := p.Publish(context.Background(), "bad.event", order{}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("invalid name: %v", err)
	}
	if _, err := p.Publish(context.Background(), "secret.event", order{}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("forbidden: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(violations) != 1 || violations[0].EventName != "secret.event" || violations[0].Reason != "no rule" {
		t.Fatalf("violations %+v", violations)
	}
}

// UNSPECIFIED and transport failures retry the same envelope — same id — so
// the runtime deduplicates the retry.
func TestUnsettledEventsAreRetriedWithTheSameID(t *testing.T) {
	rt := newFakeRuntime()
	rt.verdict = func(call int, _ *pb.EventEnvelope) (*pb.PublishStatusEntry, error) {
		switch call {
		case 1:
			return nil, errors.New("transport down")
		case 2:
			return &pb.PublishStatusEntry{Status: pb.PublishStatus_PUBLISH_STATUS_UNSPECIFIED, Message: "publish rate limit exceeded"}, nil
		}
		return nil, nil
	}
	p := newTestPublisher(t, rt, nil)
	startPublisher(t, p)

	id, err := p.Publish(context.Background(), "order.created", order{})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	reqs := rt.sent()
	if len(reqs) != 3 {
		t.Fatalf("%d requests, want 3", len(reqs))
	}
	for _, r := range reqs {
		if r[0].GetId() != id {
			t.Fatalf("a retry minted a new id: %s vs %s", r[0].GetId(), id)
		}
	}
}

// A sender waiting out a long rung retries at once on Kick — the client kicks
// it on every reconnect.
func TestKickCutsTheRetryWaitShort(t *testing.T) {
	rt := newFakeRuntime()
	rt.verdict = func(call int, _ *pb.EventEnvelope) (*pb.PublishStatusEntry, error) {
		if call == 1 {
			return nil, errors.New("transport down")
		}
		return nil, nil
	}
	p := newTestPublisher(t, rt, func(c *PublisherConfig) { c.RetryLadder = []time.Duration{time.Hour} })
	startPublisher(t, p)

	done := make(chan error, 1)
	go func() {
		_, err := p.Publish(context.Background(), "order.created", order{})
		done <- err
	}()
	<-rt.calls
	time.Sleep(10 * time.Millisecond)
	p.Kick()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Kick did not cut the retry wait")
	}
}

func TestFireAndForgetReturnsBeforeTheAcknowledgement(t *testing.T) {
	rt := newFakeRuntime()
	rt.gate = make(chan struct{})
	p := newTestPublisher(t, rt, nil)
	startPublisher(t, p)

	id, err := p.Publish(context.Background(), "order.created", order{}, WithFireAndForget())
	if err != nil || id == "" {
		t.Fatalf("fire and forget: %q %v", id, err)
	}
	close(rt.gate)
	waitFor(t, func() bool { return p.Pending() == 0 })
}

func TestCloseFlushesThenFailsTheRest(t *testing.T) {
	t.Run("flushes within the deadline", func(t *testing.T) {
		rt := newFakeRuntime()
		p := newTestPublisher(t, rt, nil)
		for range 3 {
			if _, err := p.Publish(context.Background(), "order.created", order{}, WithFireAndForget()); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p.Close(ctx)
		if p.Pending() != 0 || len(rt.sent()) == 0 {
			t.Fatalf("pending %d after a flush, %d requests", p.Pending(), len(rt.sent()))
		}
		if _, err := p.Publish(context.Background(), "order.created", order{}); !errors.Is(err, ErrStopped) {
			t.Fatalf("publish after close: %v", err)
		}
	})
	t.Run("rejects leftovers", func(t *testing.T) {
		p := newTestPublisher(t, newFakeRuntime(), nil)
		done := make(chan error, 1)
		go func() {
			_, err := p.Publish(context.Background(), "order.created", order{})
			done <- err
		}()
		waitFor(t, func() bool { return p.Pending() == 1 })
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		p.Close(ctx)
		if err := <-done; !errors.Is(err, ErrStopped) {
			t.Fatalf("leftover: %v, want ErrStopped", err)
		}
	})
}

func TestPublishRefusesBadInput(t *testing.T) {
	p := newTestPublisher(t, newFakeRuntime(), nil)
	if _, err := p.Publish(context.Background(), "Bad Name", order{}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("invalid name: %v", err)
	}
	bad := newTestPublisher(t, newFakeRuntime(), func(c *PublisherConfig) { c.Codec = testCodec{encodeErr: errors.New("nope")} })
	if _, err := bad.Publish(context.Background(), "order.created", order{}); err == nil {
		t.Fatal("encode failure must surface")
	}
	if _, err := NewPublisher(PublisherConfig{Publish: newFakeRuntime().publish}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing codec: %v", err)
	}
	if _, err := NewPublisher(PublisherConfig{Codec: testCodec{}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing publish: %v", err)
	}
	if _, err := NewPublisher(PublisherConfig{Codec: testCodec{}, Publish: newFakeRuntime().publish, MaxPending: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative bound: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second start: %v", err)
	}
	p.Close(context.Background())
}

func TestValidEventNameAndPattern(t *testing.T) {
	if !ValidEventName("order.created") || ValidEventName("order.*") || ValidEventName("") {
		t.Fatal("event name grammar")
	}
	if !ValidEventPattern("order.*") || !ValidEventPattern("#") || ValidEventPattern("Order") {
		t.Fatal("pattern grammar")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
