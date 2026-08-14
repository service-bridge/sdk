package job_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/google/uuid"

	"github.com/service-bridge/sdk/go/internal/job"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// ---------------------------------------------------------------------------
// harness: a real Jobs server over bufconn
// ---------------------------------------------------------------------------

// scriptedJobs is a real Jobs server. The subscriber reaches it through the
// generated stub over bufconn, so the lifecycle under test is the production one
// and not a hand-rolled stand-in.
type scriptedJobs struct {
	pb.UnimplementedJobsServer

	push       chan *pb.JobExecution
	subscribes chan *pb.JobsSubscribeRequest
	heartbeats chan *pb.JobsHeartbeatRequest
	results    chan *pb.JobResultRequest

	subscribeCalls atomic.Int64
	heartbeatFails atomic.Bool
	resultFails    atomic.Bool
	// endStreamAfter, when positive, makes the Nth Subscribe return immediately
	// so the subscriber has to reopen.
	endStreamAfter atomic.Int64
}

func newScriptedJobs() *scriptedJobs {
	return &scriptedJobs{
		push:       make(chan *pb.JobExecution, 32),
		subscribes: make(chan *pb.JobsSubscribeRequest, 32),
		heartbeats: make(chan *pb.JobsHeartbeatRequest, 64),
		results:    make(chan *pb.JobResultRequest, 32),
	}
}

func (s *scriptedJobs) Subscribe(req *pb.JobsSubscribeRequest, srv pb.Jobs_SubscribeServer) error {
	n := s.subscribeCalls.Add(1)
	select {
	case s.subscribes <- req:
	default:
	}
	if end := s.endStreamAfter.Load(); end > 0 && n <= end {
		return nil
	}
	for {
		select {
		case <-srv.Context().Done():
			return nil
		case msg := <-s.push:
			if err := srv.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (s *scriptedJobs) Heartbeat(_ context.Context, req *pb.JobsHeartbeatRequest) (*pb.JobsHeartbeatResponse, error) {
	select {
	case s.heartbeats <- req:
	default:
	}
	if s.heartbeatFails.Load() {
		return nil, status.Error(codes.Unavailable, "heartbeat refused")
	}
	return &pb.JobsHeartbeatResponse{}, nil
}

func (s *scriptedJobs) JobResult(_ context.Context, req *pb.JobResultRequest) (*pb.JobResultResponse, error) {
	select {
	case s.results <- req:
	default:
	}
	if s.resultFails.Load() {
		return nil, status.Error(codes.Unavailable, "result refused")
	}
	return &pb.JobResultResponse{Accepted: true}, nil
}

func startJobs(t *testing.T) (*scriptedJobs, pb.JobsClient) {
	t.Helper()

	srv := newScriptedJobs()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterJobsServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})
	return srv, pb.NewJobsClient(conn)
}

type staticClients struct {
	client pb.JobsClient
	calls  atomic.Int64
}

func (c *staticClients) JobsClient(context.Context) (pb.JobsClient, error) {
	c.calls.Add(1)
	return c.client, nil
}

// rotatingIdentity mints a new instance id on every read, the way a certificate
// rotation does, and counts how often it was asked.
type rotatingIdentity struct {
	reads atomic.Int64
	fixed bool
}

func (r *rotatingIdentity) get() job.Identity {
	n := r.reads.Add(1)
	if r.fixed {
		n = 1
	}
	return job.Identity{ServiceID: "svc", InstanceID: fmt.Sprintf("instance-%d", n)}
}

// logSink captures what the subscriber logs.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (h *logSink) Enabled(context.Context, slog.Level) bool { return true }

func (h *logSink) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Level.String())
	b.WriteString(" ")
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" ")
		b.WriteString(a.Key)
		b.WriteString("=")
		fmt.Fprintf(&b, "%v", a.Value.Any())
		return true
	})
	h.mu.Lock()
	h.lines = append(h.lines, b.String())
	h.mu.Unlock()
	return nil
}

func (h *logSink) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logSink) WithGroup(string) slog.Handler      { return h }

func (h *logSink) contains(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, line := range h.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

func (h *logSink) dump() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.lines, "\n")
}

// fastBackoff keeps reconnects observable in milliseconds.
func fastBackoff() stream.Backoff {
	return stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0))
}

func declare(t *testing.T, decls *job.Declarations, name string, spec job.Spec, h job.Handler) {
	t.Helper()
	if _, err := decls.Add(name, spec, h); err != nil {
		t.Fatalf("declare %s: %v", name, err)
	}
}

func cronSpec(t *testing.T) job.Spec {
	t.Helper()
	return job.Spec{Trigger: mustCron(t, "* * * * *", "UTC")}
}

func execution(name, id string) *pb.JobExecution {
	return &pb.JobExecution{
		ExecutionId:            id,
		JobName:                name,
		ScheduledAtUnixMs:      1767225600000,
		LocalScheduledAtUnixMs: 1767225600000,
		Attempt:                1,
		LeaseEpoch:             7,
		IdempotencyKey:         name + "-" + id,
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func recvResult(t *testing.T, srv *scriptedJobs) *pb.JobResultRequest {
	t.Helper()
	select {
	case r := <-srv.results:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("no job result arrived")
		return nil
	}
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

func TestNewSubscriberRejectsAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	_, client := startJobs(t)
	full := job.SubscriberConfig{
		Clients:  &staticClients{client: client},
		Identity: (&rotatingIdentity{fixed: true}).get,
		Jobs:     job.NewDeclarations(),
	}

	missing := []struct {
		name string
		mut  func(*job.SubscriberConfig)
	}{
		{"clients", func(c *job.SubscriberConfig) { c.Clients = nil }},
		{"identity", func(c *job.SubscriberConfig) { c.Identity = nil }},
		{"declarations", func(c *job.SubscriberConfig) { c.Jobs = nil }},
	}
	for _, tc := range missing {
		cfg := full
		tc.mut(&cfg)
		if _, err := job.NewSubscriber(cfg); !errors.Is(err, job.ErrInvalidConfig) {
			t.Fatalf("missing %s: got %v, want %v", tc.name, err, job.ErrInvalidConfig)
		}
	}
}

// ---------------------------------------------------------------------------
// identity
// ---------------------------------------------------------------------------

// TestSubscribeReadsTheIdentityOnEveryOpen pins the rule that broke the Node
// workflow subscriber: a captured instance id keeps talking for an instance the
// rotation already replaced, so its leases expire and its executions run twice.
func TestSubscribeReadsTheIdentityOnEveryOpen(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	srv.endStreamAfter.Store(1) // the first stream closes at once, forcing a reopen

	ident := &rotatingIdentity{}
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          ident.get,
		Jobs:              job.NewDeclarations(),
		HeartbeatInterval: time.Hour,
		Backoff:           fastBackoff(),
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()

	first := <-srv.subscribes
	second := <-srv.subscribes
	if first.GetInstanceId() == second.GetInstanceId() {
		t.Fatalf("both opens used the same instance id %q — identity was captured, not read",
			first.GetInstanceId())
	}
	if first.GetServiceId() != "svc" || second.GetServiceId() != "svc" {
		t.Fatalf("service id lost: %q / %q", first.GetServiceId(), second.GetServiceId())
	}
}

func TestHeartbeatReadsTheIdentityOnEveryBeat(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	ident := &rotatingIdentity{}
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          ident.get,
		Jobs:              job.NewDeclarations(),
		HeartbeatInterval: 5 * time.Millisecond,
		Backoff:           fastBackoff(),
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()

	first := <-srv.heartbeats
	second := <-srv.heartbeats
	if first.GetInstanceId() == second.GetInstanceId() {
		t.Fatalf("both beats used the same instance id %q — identity was captured, not read",
			first.GetInstanceId())
	}
}

// ---------------------------------------------------------------------------
// heartbeat failure
// ---------------------------------------------------------------------------

// TestHeartbeatFailureIsLoudAndReopensTheStream: a silently swallowed heartbeat
// failure leaves the runtime reclaiming this instance on timeout and handing its
// executions to someone else, with nothing in the logs to explain the double
// run.
func TestHeartbeatFailureIsLoudAndReopensTheStream(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	srv.heartbeatFails.Store(true)

	sink := &logSink{}
	var reported atomic.Int64
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:            &staticClients{client: client},
		Identity:           (&rotatingIdentity{fixed: true}).get,
		Jobs:               job.NewDeclarations(),
		HeartbeatInterval:  5 * time.Millisecond,
		HeartbeatThreshold: 3,
		Backoff:            fastBackoff(),
		OnError:            func(error) { reported.Add(1) },
		Logger:             slog.New(sink),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()

	waitFor(t, "heartbeat failures to be logged", func() bool { return sink.contains("job: heartbeat failed") })
	waitFor(t, "the threshold to reopen the stream", func() bool {
		return sink.contains("threshold reached") && srv.subscribeCalls.Load() >= 2
	})
	if reported.Load() == 0 {
		t.Fatalf("heartbeat failures never reached OnError; log was:\n%s", sink.dump())
	}
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

func TestConcurrencyIsCappedPerJob(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()

	release := make(chan struct{})
	var live, peak atomic.Int64
	var done atomic.Int64
	capped := cronSpec(t)
	capped.MaxConcurrent = 2
	declare(t, decls, "capped", capped, func(ctx context.Context, _ job.Execution) error {
		cur := live.Add(1)
		for {
			was := peak.Load()
			if cur <= was || peak.CompareAndSwap(was, cur) {
				break
			}
		}
		<-release
		live.Add(-1)
		done.Add(1)
		return nil
	})

	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          (&rotatingIdentity{fixed: true}).get,
		Jobs:              decls,
		HeartbeatInterval: time.Hour,
		Backoff:           fastBackoff(),
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()

	const total = 6
	for i := 0; i < total; i++ {
		srv.push <- execution("capped", fmt.Sprintf("exec-%d", i))
	}

	waitFor(t, "the cap to fill", func() bool { return live.Load() == 2 })
	time.Sleep(30 * time.Millisecond)
	if got := live.Load(); got != 2 {
		t.Fatalf("%d handlers ran at once, cap is 2", got)
	}
	if got := peak.Load(); got > 2 {
		t.Fatalf("peak concurrency %d exceeded the cap of 2", got)
	}

	close(release)
	// The queue is unbounded on purpose: every execution the runtime handed out
	// eventually runs instead of being shed.
	waitFor(t, "every queued execution to run", func() bool { return done.Load() == total })
	if got := peak.Load(); got > 2 {
		t.Fatalf("peak concurrency %d exceeded the cap of 2", got)
	}
}

func TestUnlimitedJobsRunConcurrently(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	release := make(chan struct{})
	var live atomic.Int64
	declare(t, decls, "free", cronSpec(t), func(context.Context, job.Execution) error {
		live.Add(1)
		<-release
		return nil
	})

	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          (&rotatingIdentity{fixed: true}).get,
		Jobs:              decls,
		HeartbeatInterval: time.Hour,
		Backoff:           fastBackoff(),
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { close(release); sub.Stop() }()

	for i := 0; i < 4; i++ {
		srv.push <- execution("free", fmt.Sprintf("exec-%d", i))
	}
	waitFor(t, "all four handlers to run at once", func() bool { return live.Load() == 4 })
}

func TestExecutionContextReachesTheHandler(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	got := make(chan job.Execution, 1)
	declare(t, decls, "report", cronSpec(t), func(_ context.Context, exec job.Execution) error {
		got <- exec
		return nil
	})

	sub := startSubscriber(t, client, decls, nil)
	defer sub.Stop()

	msg := execution("report", "exec-1")
	srv.push <- msg

	select {
	case exec := <-got:
		if exec.Name != "report" || exec.ID != "exec-1" {
			t.Fatalf("identity of the execution lost: %+v", exec)
		}
		if exec.ScheduledAtUnixMs != msg.GetScheduledAtUnixMs() ||
			exec.LocalScheduledAtUnixMs != msg.GetLocalScheduledAtUnixMs() {
			t.Fatalf("schedule times lost: %+v", exec)
		}
		if exec.Attempt != 1 || exec.IdempotencyKey != msg.GetIdempotencyKey() {
			t.Fatalf("attempt or idempotency key lost: %+v", exec)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler never ran")
	}
}

func TestUnknownJobIsDroppedWithAWarning(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	sink := &logSink{}
	sub := startSubscriber(t, client, job.NewDeclarations(), sink)
	defer sub.Stop()

	srv.push <- execution("never-declared", "exec-1")
	waitFor(t, "the drop to be logged", func() bool { return sink.contains("no handler declared") })

	select {
	case r := <-srv.results:
		t.Fatalf("an undeclared job reported a result: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
}

// ---------------------------------------------------------------------------
// results
// ---------------------------------------------------------------------------

func TestResultCarriesTheLeaseEpochAndTheCurrentInstance(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	declare(t, decls, "ok", cronSpec(t), func(context.Context, job.Execution) error { return nil })

	ident := &rotatingIdentity{}
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          ident.get,
		Jobs:              decls,
		HeartbeatInterval: time.Hour,
		Backoff:           fastBackoff(),
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()
	<-srv.subscribes

	srv.push <- execution("ok", "exec-1")
	res := recvResult(t, srv)

	if res.GetLeaseEpoch() != 7 {
		// Without the epoch the runtime cannot fence a result from a lease it has
		// already reassigned.
		t.Fatalf("lease epoch = %d, want 7", res.GetLeaseEpoch())
	}
	if res.GetExecutionId() != "exec-1" {
		t.Fatalf("execution id = %q", res.GetExecutionId())
	}
	if res.GetSuccess() == nil {
		t.Fatalf("outcome is not success: %+v", res.GetOutcome())
	}
	// The identity was read again when the result was sent, not captured when the
	// execution arrived.
	if res.GetInstanceId() == "instance-1" {
		t.Fatal("result reused the instance id of the subscribe call")
	}
}

func TestFailureIsRetryableUnlessPermanent(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	declare(t, decls, "boom", cronSpec(t), func(context.Context, job.Execution) error {
		return errors.New("transient trouble")
	})
	declare(t, decls, "poison", cronSpec(t), func(context.Context, job.Execution) error {
		return fmt.Errorf("bad payload: %w", job.ErrPermanent)
	})

	sub := startSubscriber(t, client, decls, nil)
	defer sub.Stop()

	srv.push <- execution("boom", "exec-1")
	res := recvResult(t, srv)
	if res.GetFailure() == nil || !res.GetFailure().GetRetryable() {
		t.Fatalf("plain error must stay retryable: %+v", res.GetOutcome())
	}
	if !strings.Contains(res.GetFailure().GetErrorMessage(), "transient trouble") {
		t.Fatalf("error message lost: %q", res.GetFailure().GetErrorMessage())
	}

	srv.push <- execution("poison", "exec-2")
	res = recvResult(t, srv)
	if res.GetFailure() == nil || res.GetFailure().GetRetryable() {
		t.Fatalf("ErrPermanent must switch retryable off: %+v", res.GetOutcome())
	}
}

// ---------------------------------------------------------------------------
// trace context
// ---------------------------------------------------------------------------

func TestTraceHeaderIsAdoptedAndBrokenOnesStartANewRoot(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	seen := make(chan telemetry.TraceContext, 2)
	declare(t, decls, "traced", cronSpec(t), func(ctx context.Context, _ job.Execution) error {
		tc, ok := telemetry.FromContext(ctx)
		if !ok {
			t.Errorf("handler ran without a trace context")
			return nil
		}
		seen <- tc
		return nil
	})

	sub := startSubscriber(t, client, decls, nil)
	defer sub.Stop()

	traceID := uuid.Must(uuid.NewV7())
	parentID := uuid.Must(uuid.NewV7())
	good := execution("traced", "exec-1")
	good.XSbTrace = telemetry.FormatHeader(telemetry.TraceContext{TraceID: traceID, ParentOpID: parentID})
	srv.push <- good

	select {
	case tc := <-seen:
		if tc.TraceID != traceID || tc.ParentOpID != parentID {
			t.Fatalf("trace context not adopted from the execution: %+v", tc)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler never ran")
	}

	broken := execution("traced", "exec-2")
	broken.XSbTrace = "not-a-trace-header"
	srv.push <- broken

	select {
	case tc := <-seen:
		if tc.TraceID == uuid.Nil {
			t.Fatal("a broken header left the handler without a trace")
		}
		if tc.TraceID == traceID {
			t.Fatal("a broken header reused the previous trace")
		}
		if !tc.Root() {
			// A broken header means the caller sent no usable trace, so the work
			// starts a tree of its own instead of hanging off a made-up parent.
			t.Fatalf("a broken header produced a child context: %+v", tc)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler never ran for the broken header")
	}
}

// ---------------------------------------------------------------------------
// shutdown
// ---------------------------------------------------------------------------

func TestStopCancelsARunningHandler(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	running := make(chan struct{})
	cancelled := make(chan struct{})
	declare(t, decls, "long", cronSpec(t), func(ctx context.Context, _ job.Execution) error {
		close(running)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	})

	sub := startSubscriber(t, client, decls, nil)
	srv.push <- execution("long", "exec-1")
	<-running

	stopped := make(chan struct{})
	go func() { sub.Stop(); close(stopped) }()

	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not cancel the running handler")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return")
	}

	// The handler ran, so its outcome still reaches the runtime: the result send
	// is detached from the cancelled context on purpose.
	res := recvResult(t, srv)
	if res.GetExecutionId() != "exec-1" {
		t.Fatalf("result for the wrong execution: %+v", res)
	}
}

func TestStopIsQuietAndLeavesNoGoroutines(t *testing.T) {
	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	declare(t, decls, "ok", cronSpec(t), func(context.Context, job.Execution) error { return nil })

	// Warm the channel first: the goroutines gRPC keeps for a live connection
	// belong to the harness, not to the subscriber under test.
	if _, err := client.Heartbeat(context.Background(), &pb.JobsHeartbeatRequest{
		ServiceId: "svc", InstanceId: "instance-1",
	}); err != nil {
		t.Fatalf("warm-up heartbeat: %v", err)
	}
	<-srv.heartbeats
	before := runtime.NumGoroutine()

	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          (&rotatingIdentity{fixed: true}).get,
		Jobs:              decls,
		HeartbeatInterval: 5 * time.Millisecond,
		Backoff:           fastBackoff(),
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-srv.subscribes
	srv.push <- execution("ok", "exec-1")
	recvResult(t, srv)
	sub.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines leaked after Stop: %d before, %d after", before, runtime.NumGoroutine())
}

func TestStartTwiceIsRejected(t *testing.T) {
	t.Parallel()

	_, client := startJobs(t)
	sub := startSubscriber(t, client, job.NewDeclarations(), nil)
	defer sub.Stop()

	if err := sub.Start(context.Background()); !errors.Is(err, stream.ErrAlreadyStarted) {
		t.Fatalf("second start: got %v, want %v", err, stream.ErrAlreadyStarted)
	}
}

// ---------------------------------------------------------------------------
// failures on the way out
// ---------------------------------------------------------------------------

// togglingClients models the channel going away underneath the subscriber.
type togglingClients struct {
	client pb.JobsClient
	broken atomic.Bool
}

func (c *togglingClients) JobsClient(context.Context) (pb.JobsClient, error) {
	if c.broken.Load() {
		return nil, errors.New("channel is down")
	}
	return c.client, nil
}

type errorSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errorSink) add(err error) {
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
}

func (s *errorSink) matching(substr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, err := range s.errs {
		if strings.Contains(err.Error(), substr) {
			return true
		}
	}
	return false
}

func (s *errorSink) is(target error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, err := range s.errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// TestWithoutIdentityNothingIsSentAndTheReasonIsVisible: before the first
// session there is no instance to subscribe or heartbeat as, and that must not
// look like silence.
func TestWithoutIdentityNothingIsSentAndTheReasonIsVisible(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	sink := &logSink{}
	errs := &errorSink{}
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          func() job.Identity { return job.Identity{} },
		Jobs:              job.NewDeclarations(),
		HeartbeatInterval: 5 * time.Millisecond,
		Backoff:           fastBackoff(),
		OnError:           errs.add,
		Logger:            slog.New(sink),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()

	waitFor(t, "the missing identity to surface", func() bool { return errs.is(job.ErrNoIdentity) })
	waitFor(t, "the heartbeat to complain", func() bool { return sink.contains("job: heartbeat failed") })
	if srv.subscribeCalls.Load() != 0 {
		t.Fatal("subscribed without an identity")
	}
}

func TestAChannelThatDisappearsIsReportedOnEveryPath(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	clients := &togglingClients{client: client}
	decls := job.NewDeclarations()
	declare(t, decls, "ok", cronSpec(t), func(context.Context, job.Execution) error {
		// The channel dies between the handler and its result.
		clients.broken.Store(true)
		return nil
	})

	errs := &errorSink{}
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           clients,
		Identity:          (&rotatingIdentity{fixed: true}).get,
		Jobs:              decls,
		HeartbeatInterval: 5 * time.Millisecond,
		ResultTimeout:     2 * time.Second,
		Backoff:           fastBackoff(),
		OnError:           errs.add,
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()
	<-srv.subscribes

	srv.push <- execution("ok", "exec-1")
	waitFor(t, "the failed result send to be reported", func() bool {
		return errs.matching("result: jobs client")
	})
	waitFor(t, "the failed heartbeat to be reported", func() bool {
		return errs.matching("heartbeat: channel is down")
	})
	waitFor(t, "the failed reopen to be reported", func() bool {
		return errs.matching("jobs client: channel is down")
	})
}

func TestARefusedResultIsReported(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	srv.resultFails.Store(true)

	decls := job.NewDeclarations()
	declare(t, decls, "ok", cronSpec(t), func(context.Context, job.Execution) error { return nil })

	errs := &errorSink{}
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          (&rotatingIdentity{fixed: true}).get,
		Jobs:              decls,
		HeartbeatInterval: time.Hour,
		ResultTimeout:     2 * time.Second,
		Backoff:           fastBackoff(),
		OnError:           errs.add,
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sub.Stop()
	<-srv.subscribes

	srv.push <- execution("ok", "exec-1")
	recvResult(t, srv)
	waitFor(t, "the refused result to be reported", func() bool {
		return errs.matching("result for execution exec-1")
	})
}

// TestQueuedExecutionsAreDroppedOnStop: a waiter that got its slot after
// shutdown would run a handler on a subscriber that is already gone. Dropping it
// is safe — the lease expires and the runtime hands the execution to someone
// else.
func TestQueuedExecutionsAreDroppedOnStop(t *testing.T) {
	t.Parallel()

	srv, client := startJobs(t)
	decls := job.NewDeclarations()
	spec := cronSpec(t)
	spec.MaxConcurrent = 1

	running := make(chan struct{}, 1)
	var started atomic.Int64
	declare(t, decls, "serial", spec, func(ctx context.Context, _ job.Execution) error {
		started.Add(1)
		running <- struct{}{}
		<-ctx.Done()
		return nil
	})

	sink := &logSink{}
	sub := startSubscriber(t, client, decls, sink)
	<-srv.subscribes

	srv.push <- execution("serial", "exec-1")
	<-running
	srv.push <- execution("serial", "exec-2")
	waitFor(t, "the second execution to queue", func() bool { return srv.subscribeCalls.Load() > 0 })
	time.Sleep(20 * time.Millisecond)

	sub.Stop()

	if got := started.Load(); got != 1 {
		t.Fatalf("%d handlers started, want 1 — the queued one ran after Stop", got)
	}
	if !sink.contains("stopped while queued") {
		t.Fatalf("the dropped execution was not logged:\n%s", sink.dump())
	}
}

func startSubscriber(t *testing.T, client pb.JobsClient, decls *job.Declarations, sink *logSink) *job.Subscriber {
	t.Helper()
	if sink == nil {
		sink = &logSink{}
	}
	sub, err := job.NewSubscriber(job.SubscriberConfig{
		Clients:           &staticClients{client: client},
		Identity:          (&rotatingIdentity{fixed: true}).get,
		Jobs:              decls,
		HeartbeatInterval: time.Hour,
		ResultTimeout:     2 * time.Second,
		Backoff:           fastBackoff(),
		Logger:            slog.New(sink),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	return sub
}
