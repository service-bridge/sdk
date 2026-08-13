package events

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/outbox"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
)

func newTestDrainer(t *testing.T, st *outbox.Storage, pub PublishFunc, opts ...func(*DrainerConfig)) *Drainer {
	t.Helper()
	cfg := DrainerConfig{
		Storage:  st,
		Publish:  pub,
		Identity: testIdentity,
		Logger:   discardLogger(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	d, err := NewDrainer(cfg)
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	return d
}

func seed(t *testing.T, st *outbox.Storage, n int, enqueuedAt int64) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range n {
		id := fmt.Sprintf("evt-%03d", i)
		ids[i] = id
		rec := outbox.Record{
			ID:           id,
			Name:         "order.created",
			Payload:      []byte("proto:{}"),
			PayloadJSON:  []byte("{}"),
			ContractHash: "hash-order.created",
			OccurredAtMs: enqueuedAt,
			EnqueuedAtMs: enqueuedAt + int64(i),
		}
		if err := st.Enqueue(context.Background(), rec, 0); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return ids
}

// TestRuntimeDowntimeNeverBuriesBufferedEvents is the reason the outbox exists:
// the publish already returned success and promised durability, so no amount of
// unreachable runtime may mark an event failed.
func TestRuntimeDowntimeNeverBuriesBufferedEvents(t *testing.T) {
	ctx := context.Background()
	st := openStorage(t, t.TempDir())
	ids := seed(t, st, 3, 0)

	transport := &recordingPublish{}
	transport.setResponder(func(*pb.PublishRequest) (*pb.PublishResponse, error) {
		return nil, errors.New("connection refused")
	})
	c := newClock(0)
	d := newTestDrainer(t, st, transport.publish, func(cfg *DrainerConfig) {
		cfg.Now = c.now
	})

	// Thirteen minutes per iteration clears even the saturated ladder rung, so
	// every pass really re-attempts the batch. Six passes is over an hour of
	// downtime — far past the five minutes a bounded ladder would survive.
	const iterations = 6
	const advanceMs = 13 * 60 * 1000
	for i := range iterations {
		n, err := d.drainOnce(ctx)
		if err != nil {
			t.Fatalf("drainOnce %d: %v", i, err)
		}
		if n != len(ids) {
			t.Fatalf("iteration %d claimed %d rows, want %d", i, n, len(ids))
		}
		failed, err := st.CountByStatus(ctx, outbox.StatusFailed)
		if err != nil {
			t.Fatalf("CountByStatus: %v", err)
		}
		if failed != 0 {
			t.Fatalf("iteration %d marked %d event(s) failed; downtime must not consume a retry budget", i, failed)
		}
		c.advance(advanceMs)
	}

	pending, err := st.CountByStatus(ctx, outbox.StatusPending)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if pending != len(ids) {
		t.Fatalf("pending = %d after the outage, want %d", pending, len(ids))
	}
	for _, id := range ids {
		rec, status, err := st.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load %s: %v", id, err)
		}
		if status != outbox.StatusPending {
			t.Fatalf("%s is %q after the outage, want pending", id, status)
		}
		if rec.Attempts != iterations {
			t.Fatalf("%s attempts = %d, want %d", id, rec.Attempts, iterations)
		}
		// The ladder saturates: the wait never grows past the last rung.
		maxWait := int64(10*time.Minute/time.Millisecond) * 5 / 4
		if wait := rec.NextAttemptAtMs - c.now(); wait > maxWait {
			t.Fatalf("%s waits %dms, want at most the saturated rung %dms", id, wait, maxWait)
		}
	}

	// The runtime comes back.
	transport.setResponder(nil)
	n, err := d.drainOnce(ctx)
	if err != nil {
		t.Fatalf("drainOnce after recovery: %v", err)
	}
	if n != len(ids) {
		t.Fatalf("recovery drain claimed %d rows, want %d", n, len(ids))
	}
	if st.Rows() != 0 {
		t.Fatalf("outbox holds %d rows after recovery, want 0", st.Rows())
	}
}

func TestDrainDeletesAcceptedAndDuplicateEvents(t *testing.T) {
	ctx := context.Background()
	st := openStorage(t, t.TempDir())
	ids := seed(t, st, 2, 0)

	transport := &recordingPublish{}
	transport.setResponder(func(req *pb.PublishRequest) (*pb.PublishResponse, error) {
		return &pb.PublishResponse{Results: []*pb.PublishStatusEntry{
			{EventId: ids[0], Status: pb.PublishStatus_PUBLISH_STATUS_ACCEPTED},
			{EventId: ids[1], Status: pb.PublishStatus_PUBLISH_STATUS_REJECTED_DUPLICATE},
		}}, nil
	})
	d := newTestDrainer(t, st, transport.publish, func(cfg *DrainerConfig) { cfg.Now = newClock(0).now })

	if _, err := d.drainOnce(ctx); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if st.Rows() != 0 {
		t.Fatalf("outbox holds %d rows, want 0 — a duplicate is already durable", st.Rows())
	}
}

func TestDrainParksOnlyMeaningfulRejections(t *testing.T) {
	ctx := context.Background()
	st := openStorage(t, t.TempDir())
	ids := seed(t, st, 3, 0)

	transport := &recordingPublish{}
	transport.setResponder(func(*pb.PublishRequest) (*pb.PublishResponse, error) {
		return &pb.PublishResponse{Results: []*pb.PublishStatusEntry{
			{EventId: ids[0], Status: pb.PublishStatus_PUBLISH_STATUS_REJECTED_INVALID_NAME, Message: "bad name"},
			{EventId: ids[1], Status: pb.PublishStatus_PUBLISH_STATUS_REJECTED_FORBIDDEN, Message: "capability disabled"},
			{EventId: ids[2], Status: pb.PublishStatus_PUBLISH_STATUS_UNSPECIFIED},
		}}, nil
	})

	var seen []PolicyViolation
	d := newTestDrainer(t, st, transport.publish, func(cfg *DrainerConfig) {
		cfg.Now = newClock(0).now
		cfg.OnPolicyViolation = func(v PolicyViolation) { seen = append(seen, v) }
	})

	if _, err := d.drainOnce(ctx); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}

	for _, id := range ids[:2] {
		_, status, err := st.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load %s: %v", id, err)
		}
		if status != outbox.StatusFailed {
			t.Fatalf("%s is %q, want failed", id, status)
		}
	}
	// No verdict is the transport case: the runtime said nothing about the
	// event, so it is retried rather than buried.
	_, status, err := st.Load(ctx, ids[2])
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if status != outbox.StatusPending {
		t.Fatalf("an event without a verdict is %q, want pending", status)
	}

	if len(seen) != 1 {
		t.Fatalf("policy violations reported = %d, want 1", len(seen))
	}
	if seen[0].EventID != ids[1] || seen[0].Reason != "capability disabled" {
		t.Fatalf("policy violation = %+v", seen[0])
	}
}

func TestDrainSendsTheWholeEnvelope(t *testing.T) {
	ctx := context.Background()
	st := openStorage(t, t.TempDir())

	rec := outbox.Record{
		ID:             "evt-1",
		Name:           "order.created",
		Payload:        []byte("proto:{}"),
		PayloadJSON:    []byte(`{"a":1}`),
		ContractHash:   "hash-1",
		PartitionKey:   "cust-1",
		IdempotencyKey: "idem-1",
		Headers:        map[string]string{"tenant": "acme"},
		OccurredAtMs:   1_700_000_000_000,
		EnqueuedAtMs:   1_700_000_000_001,
		Trace:          "trace-header",
	}
	if err := st.Enqueue(ctx, rec, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	transport := &recordingPublish{}
	d := newTestDrainer(t, st, transport.publish, func(cfg *DrainerConfig) {
		cfg.Now = newClock(1_700_000_000_100).now
	})
	if _, err := d.drainOnce(ctx); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}

	req := transport.lastRequest()
	if req == nil {
		t.Fatal("nothing was sent")
	}
	if req.GetPublisherServiceId() != "svc-1" || req.GetPublisherInstanceId() != "inst-1" {
		t.Fatalf("identity = %q/%q", req.GetPublisherServiceId(), req.GetPublisherInstanceId())
	}
	env := req.GetEvents()[0]
	if env.GetPartitionKey() != "cust-1" || env.GetIdempotencyKey() != "idem-1" ||
		env.GetHeaders()["tenant"] != "acme" || env.GetXSbTrace() != "trace-header" ||
		string(env.GetPayloadJson()) != `{"a":1}` || env.GetOccurredAtUnixMs() != 1_700_000_000_000 {
		t.Fatalf("envelope lost fields on the way out: %+v", env)
	}
}

func TestDrainBatchesOneRequestPerIteration(t *testing.T) {
	ctx := context.Background()
	st := openStorage(t, t.TempDir())
	seed(t, st, 250, 0)

	transport := &recordingPublish{}
	d := newTestDrainer(t, st, transport.publish, func(cfg *DrainerConfig) {
		cfg.Now = newClock(1_000).now
		cfg.BatchSize = 100
	})

	n, err := d.drainOnce(ctx)
	if err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if n != 100 {
		t.Fatalf("claimed %d rows, want the batch size 100", n)
	}
	if transport.calls.Load() != 1 {
		t.Fatalf("one batch cost %d requests, want 1", transport.calls.Load())
	}
	if got := len(transport.lastRequest().GetEvents()); got != 100 {
		t.Fatalf("request carried %d envelopes, want 100", got)
	}
}

func TestDrainLoopDeliversAfterAKick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := openStorage(t, t.TempDir())
	transport := &recordingPublish{}
	delivered := make(chan string, 4)
	transport.setResponder(func(req *pb.PublishRequest) (*pb.PublishResponse, error) {
		for _, e := range req.GetEvents() {
			delivered <- e.GetId()
		}
		return acceptAll(req), nil
	})

	d := newTestDrainer(t, st, transport.publish)
	p := newTestPublisher(t, st, transport.publish, func(c *PublisherConfig) { c.Kick = d.Kick })

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()
	if err := d.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}

	id, err := p.Publish(ctx, "order.created", order{ID: "o-1"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-delivered:
		if got != id {
			t.Fatalf("delivered %q, want %q", got, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the drain never woke on the publish kick")
	}
}

func TestDrainLoopKeepsAKickRaisedMidIteration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := openStorage(t, t.TempDir())
	seed(t, st, 1, 0)

	transport := &recordingPublish{}
	release := make(chan struct{})
	inFlight := make(chan struct{})
	first := true
	second := make(chan struct{})
	transport.setResponder(func(req *pb.PublishRequest) (*pb.PublishResponse, error) {
		if first {
			first = false
			close(inFlight)
			<-release
		} else {
			close(second)
		}
		return acceptAll(req), nil
	})

	d := newTestDrainer(t, st, transport.publish)
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	select {
	case <-inFlight:
	case <-time.After(3 * time.Second):
		t.Fatal("the first batch never reached the transport")
	}

	// The kick lands while the first batch is still on the wire. A wake that
	// only fires on an idle loop would be lost here.
	seedRec := outbox.Record{
		ID: "late", Name: "order.created", Payload: []byte("proto:{}"),
		PayloadJSON: []byte("{}"), OccurredAtMs: 1, EnqueuedAtMs: 1,
	}
	if err := st.Enqueue(ctx, seedRec, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	d.Kick()
	close(release)

	select {
	case <-second:
	case <-time.After(3 * time.Second):
		t.Fatal("the kick raised during an iteration was lost")
	}
}

func TestDrainLoopStopsCleanly(t *testing.T) {
	st := openStorage(t, t.TempDir())
	d := newTestDrainer(t, st, (&recordingPublish{}).publish)

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	d.Stop()
	d.Stop()
}

func TestRetryLadderSaturatesAndNeverExpires(t *testing.T) {
	ladder := RetryLadder()
	rungs := ladder.Rungs()
	if len(rungs) == 0 {
		t.Fatal("empty ladder")
	}
	last := rungs[len(rungs)-1]
	for _, attempt := range []int{len(rungs), len(rungs) + 100, 10_000} {
		d := ladder.Delay(attempt)
		if d <= 0 {
			t.Fatalf("Delay(%d) = %v, want a positive wait", attempt, d)
		}
		if d > time.Duration(float64(last)*1.25)+time.Millisecond {
			t.Fatalf("Delay(%d) = %v, want at most the saturated rung %v", attempt, d, last)
		}
	}
}

func TestNewDrainerDemandsItsDependencies(t *testing.T) {
	st := openStorage(t, t.TempDir())
	base := DrainerConfig{Storage: st, Publish: (&recordingPublish{}).publish, Identity: testIdentity}
	cases := map[string]func(*DrainerConfig){
		"storage":  func(c *DrainerConfig) { c.Storage = nil },
		"publish":  func(c *DrainerConfig) { c.Publish = nil },
		"identity": func(c *DrainerConfig) { c.Identity = nil },
	}
	for name, mutate := range cases {
		cfg := base
		mutate(&cfg)
		if _, err := NewDrainer(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewDrainer without %s = %v, want ErrInvalidConfig", name, err)
		}
	}
	d, err := NewDrainer(base)
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	if len(d.cfg.Backoff.Rungs()) != len(RetryLadder().Rungs()) {
		t.Fatal("a zero Backoff did not fall back to the retry ladder")
	}
	if d.cfg.BatchSize != DefaultBatchSize || d.cfg.PublishTimeout != DefaultPublishTimeout {
		t.Fatalf("defaults not applied: batch %d timeout %v", d.cfg.BatchSize, d.cfg.PublishTimeout)
	}
}

func TestDrainReportsStorageFailure(t *testing.T) {
	st, err := outbox.Open(context.Background(), outbox.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d := newTestDrainer(t, st, (&recordingPublish{}).publish, func(cfg *DrainerConfig) {
		cfg.Backoff = stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0))
	})
	if _, err := d.drainOnce(context.Background()); !errors.Is(err, outbox.ErrClosed) {
		t.Fatalf("drainOnce on a closed storage = %v, want ErrClosed", err)
	}
}

func TestDrainLoopKeepsRunningThroughStorageFailures(t *testing.T) {
	st, err := outbox.Open(context.Background(), outbox.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	failures := make(chan error, 8)
	d := newTestDrainer(t, st, (&recordingPublish{}).publish, func(cfg *DrainerConfig) {
		cfg.OnError = func(err error) {
			select {
			case failures <- err:
			default:
			}
		}
		cfg.Sleep = func(ctx context.Context, _ time.Duration) {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
			}
		}
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer d.Stop()

	select {
	case err := <-failures:
		if !errors.Is(err, outbox.ErrClosed) {
			t.Fatalf("reported %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a storage failure was never reported")
	}
}

func TestWaitForWorkReturnsOnKickDeadlineAndCancel(t *testing.T) {
	ctx := context.Background()
	st := openStorage(t, t.TempDir())
	c := newClock(1_000)
	d := newTestDrainer(t, st, (&recordingPublish{}).publish, func(cfg *DrainerConfig) {
		cfg.Now = c.now
	})

	// Nothing pending: the loop arms no timer and parks on the kick alone.
	d.Kick()
	assertReturnsFast(t, "kick raised before the wait", func() { d.waitForWork(ctx) })

	go func() {
		time.Sleep(20 * time.Millisecond)
		d.Kick()
	}()
	assertReturnsFast(t, "kick raised during the wait", func() { d.waitForWork(ctx) })

	// A deferred row already due returns without waiting at all.
	rec := outbox.Record{
		ID: "evt-1", Name: "order.created", Payload: []byte("proto:{}"),
		PayloadJSON: []byte("{}"), OccurredAtMs: 1, EnqueuedAtMs: 1, NextAttemptAtMs: 500,
	}
	if err := st.Enqueue(ctx, rec, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	assertReturnsFast(t, "a due row", func() { d.waitForWork(ctx) })

	// A row far in the future parks until the deadline; cancelling ends it.
	if err := st.Complete(ctx, outbox.Result{Retry: []outbox.Retry{{
		ID: "evt-1", Attempts: 1, LastError: "x", NextAttemptAtMs: c.now() + 1_000_000,
	}}}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	assertReturnsFast(t, "cancelled context", func() { d.waitForWork(cancelled) })
}

func assertReturnsFast(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("waitForWork did not return on %s", what)
	}
}

func TestSleepCtxHonoursItsDeadlineAndCancellation(t *testing.T) {
	start := time.Now()
	sleepCtx(context.Background(), 0)
	sleepCtx(context.Background(), 5*time.Millisecond)
	if time.Since(start) > time.Second {
		t.Fatal("sleepCtx overshot a 5ms wait")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertReturnsFast(t, "a cancelled sleep", func() { sleepCtx(ctx, time.Hour) })
}

func TestWaitForWorkParksUntilADeferredRowIsDue(t *testing.T) {
	ctx := context.Background()
	st := openStorage(t, t.TempDir())
	c := newClock(1_000)
	d := newTestDrainer(t, st, (&recordingPublish{}).publish, func(cfg *DrainerConfig) {
		cfg.Now = c.now
	})

	rec := outbox.Record{
		ID: "evt-1", Name: "order.created", Payload: []byte("proto:{}"),
		PayloadJSON: []byte("{}"), OccurredAtMs: 1, EnqueuedAtMs: 1,
		NextAttemptAtMs: c.now() + 30,
	}
	if err := st.Enqueue(ctx, rec, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// The wait is armed from the row's own deadline, not a fixed poll interval.
	assertReturnsFast(t, "a deferred row coming due", func() { d.waitForWork(ctx) })
}
