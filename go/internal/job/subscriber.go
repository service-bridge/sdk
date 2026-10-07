package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// Defaults of the subscriber. The heartbeat period is the runtime's answer;
// the default applies only until the first one. The threshold gives the stream
// two missed beats before it is reopened.
const (
	DefaultHeartbeatInterval  = 5 * time.Second
	DefaultHeartbeatThreshold = 3
	DefaultResultTimeout      = 10 * time.Second

	// minHeartbeatInterval floors the runtime's hint so a misconfigured
	// runtime cannot put the subscriber into a hot loop.
	minHeartbeatInterval = 100 * time.Millisecond
)

// ErrInvalidConfig is returned by NewSubscriber when a required dependency is
// absent.
var ErrInvalidConfig = errors.New("invalid subscriber config")

// ErrNoIdentity reports that the session has no identity yet, so there is
// nothing to subscribe or heartbeat as.
var ErrNoIdentity = errors.New("no session identity")

// ClientSource yields the Jobs stub to open the stream with. The connection
// layer owns the channel and replaces it on rotation, so the stub is asked for
// on every open instead of captured once.
type ClientSource interface {
	JobsClient(ctx context.Context) (pb.JobsClient, error)
}

// Identity names the live session.
type Identity struct {
	ServiceID  string
	InstanceID string
}

// SubscriberConfig wires the subscriber. See ./README.md.
type SubscriberConfig struct {
	// Clients yields the Jobs stub per open.
	Clients ClientSource
	// Identity reports the live session's identity. It is called per subscribe,
	// per heartbeat and per result, never cached: a certificate rotation mints a
	// fresh InstanceID, and a stale one heartbeats for an instance the runtime
	// has already torn down.
	Identity func() Identity
	// Jobs resolves an execution to its handler and its concurrency limit.
	Jobs *Declarations
	// HeartbeatInterval defaults to DefaultHeartbeatInterval.
	HeartbeatInterval time.Duration
	// HeartbeatThreshold is how many consecutive failures reopen the stream.
	// Defaults to DefaultHeartbeatThreshold.
	HeartbeatThreshold int
	// ResultTimeout bounds one JobResult call. Defaults to DefaultResultTimeout.
	ResultTimeout time.Duration
	// Backoff pins the reconnect ladder. Zero value falls back to the default.
	Backoff stream.Backoff
	// OnError reports stream, heartbeat and result failures. The subscriber
	// recovers on its own regardless.
	OnError func(err error)
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Subscriber owns the Jobs execution stream, the heartbeat and every goroutine
// running a handler. The heartbeat runs for as long as the subscriber does: each
// beat extends the lease of every execution this instance holds, so a long
// handler keeps its lease without any per-execution renewal.
type Subscriber struct {
	cfg    SubscriberConfig
	logger *slog.Logger
	sup    *stream.Supervisor[*pb.JobExecution, pb.Jobs_SubscribeClient]

	mu       sync.Mutex
	started  bool
	stopped  bool
	runCtx   context.Context
	cancel   context.CancelFunc
	slots    map[string]chan struct{}
	pending  chan struct{}
	active   map[string]activeExecution
	draining bool

	wg sync.WaitGroup

	// Touched only by the heartbeat goroutine.
	hbFailures int
}

// NewSubscriber validates the config and builds an idle subscriber.
func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Clients == nil {
		return nil, fmt.Errorf("job: new subscriber: missing client source: %w", ErrInvalidConfig)
	}
	if cfg.Identity == nil {
		return nil, fmt.Errorf("job: new subscriber: missing identity source: %w", ErrInvalidConfig)
	}
	if cfg.Jobs == nil {
		return nil, fmt.Errorf("job: new subscriber: missing declarations: %w", ErrInvalidConfig)
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.HeartbeatThreshold <= 0 {
		cfg.HeartbeatThreshold = DefaultHeartbeatThreshold
	}
	if cfg.ResultTimeout <= 0 {
		cfg.ResultTimeout = DefaultResultTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	s := &Subscriber{
		cfg:     cfg,
		logger:  cfg.Logger,
		slots:   make(map[string]chan struct{}),
		pending: make(chan struct{}, 1024), active: make(map[string]activeExecution),
	}
	sup, err := stream.NewSupervisor(stream.Config[*pb.JobExecution, pb.Jobs_SubscribeClient]{
		Name:    "jobs.subscribe",
		Open:    s.open,
		OnData:  s.onData,
		OnError: s.reportError,
		Backoff: cfg.Backoff,
		Logger:  cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("job: new subscriber: %w", err)
	}
	s.sup = sup
	return s, nil
}

// Start opens the execution stream and starts the heartbeat. Cancelling ctx
// stops both exactly like Stop, minus the wait for the handlers.
func (s *Subscriber) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("job: subscriber: start: %w", stream.ErrAlreadyStarted)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.started = true
	s.runCtx = runCtx
	s.cancel = cancel
	s.mu.Unlock()

	if err := s.sup.Start(runCtx); err != nil {
		cancel()
		return fmt.Errorf("job: subscriber: start: %w", err)
	}

	s.wg.Add(1)
	go s.heartbeat(runCtx)
	return nil
}

// Stop cancels every running handler, closes the stream and waits for the
// goroutines it owns. Terminal: a stopped subscriber cannot be started again.
func (s *Subscriber) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.stopped = true
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.sup.Stop()
	s.wg.Wait()
}

// Drain stops taking new executions. Running handlers finish.
func (s *Subscriber) Drain() {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
}

// Wait blocks until no execution is running or ctx ends.
func (s *Subscriber) Wait(ctx context.Context) error {
	for {
		s.mu.Lock()
		n := len(s.active)
		s.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("job: wait for executions: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Subscriber) open(ctx context.Context) (pb.Jobs_SubscribeClient, error) {
	id := s.cfg.Identity()
	if id.ServiceID == "" || id.InstanceID == "" {
		return nil, fmt.Errorf("job: subscriber: open: %w", ErrNoIdentity)
	}
	client, err := s.cfg.Clients.JobsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("job: subscriber: jobs client: %w", err)
	}
	st, err := client.Subscribe(ctx, &pb.JobsSubscribeRequest{
		ServiceId:  id.ServiceID,
		InstanceId: id.InstanceID,
	})
	if err != nil {
		return nil, fmt.Errorf("job: subscriber: subscribe: %w", err)
	}
	return st, nil
}

// onData runs on the supervisor goroutine, so it must never block: every
// execution gets its own goroutine, and the wait for a concurrency slot happens
// there.
func (s *Subscriber) onData(streamCtx context.Context, msg *pb.JobExecution, _ pb.Jobs_SubscribeClient) {
	select {
	case s.pending <- struct{}{}:
	default:
		s.reportError(fmt.Errorf("job: local assignment queue full; lease will be reclaimed"))
		return
	}
	decl, ok := s.cfg.Jobs.Lookup(msg.GetJobName(), msg.GetFingerprint())
	if !ok {
		go func() {
			defer func() { <-s.pending }()
			s.sendResult(streamCtx, msg, fmt.Errorf("%w: unsupported_version: %s/%s", ErrPermanent, msg.GetJobName(), msg.GetFingerprint()))
		}()
		return
	}

	s.mu.Lock()
	if s.stopped || s.draining || s.runCtx == nil {
		// A draining instance takes no new work; the runtime reclaims the
		// lease once the stream is gone and hands the execution to another.
		s.mu.Unlock()
		<-s.pending
		return
	}
	key, epoch := msg.GetExecutionId(), msg.GetLeaseEpoch()
	if previous, ok := s.active[key]; ok {
		if previous.epoch >= epoch {
			s.mu.Unlock()
			<-s.pending
			return
		}
		previous.cancel()
	}
	executionCtx, cancel := context.WithCancel(streamCtx)
	s.active[key] = activeExecution{epoch: epoch, cancel: cancel}
	s.wg.Add(1)
	s.mu.Unlock()
	go s.dispatch(executionCtx, decl, msg)
}

func (s *Subscriber) dispatch(ctx context.Context, decl Declaration, msg *pb.JobExecution) {
	defer s.wg.Done()
	defer func() { <-s.pending }()
	defer s.retireExecution(msg.GetExecutionId(), msg.GetLeaseEpoch())

	release, ok := s.acquire(ctx, decl)
	if !ok {
		s.logger.Warn("job: stopped while queued, dropping execution",
			"job", decl.Name, "execution_id", msg.GetExecutionId())
		return
	}
	defer release()

	s.run(ctx, decl, msg)
}

// acquire waits for a per-job concurrency slot. The wait queue is deliberately
// unbounded, unlike the inbound RPC path which sheds load on a full queue: an
// execution that got here already carries a runtime-issued lease and the runtime
// counts this instance as its owner. Shedding it would not reject a request, it
// would abandon work the runtime believes is being done.
func (s *Subscriber) acquire(ctx context.Context, decl Declaration) (func(), bool) {
	limit := 32
	if decl.Spec.MaxConcurrent != nil && *decl.Spec.MaxConcurrent > 0 {
		limit = *decl.Spec.MaxConcurrent
	}
	if decl.Spec.Overlap == "" || decl.Spec.Overlap == OverlapSkip {
		limit = 1
	}

	s.mu.Lock()
	slot := s.slots[decl.Name+":"+decl.ContractHash]
	if slot == nil {
		slot = make(chan struct{}, limit)
		s.slots[decl.Name+":"+decl.ContractHash] = slot
	}
	s.mu.Unlock()

	select {
	case slot <- struct{}{}:
		return func() { <-slot }, true
	case <-ctx.Done():
		return nil, false
	}
}

func (s *Subscriber) run(ctx context.Context, decl Declaration, msg *pb.JobExecution) {
	// The JOB.EXEC operation belongs to the runtime (ADR-0001 ownership matrix).
	// The SDK only carries the trace context forward so nested calls hang under
	// the execution instead of starting their own tree.
	ctx = s.withTrace(ctx, msg)

	exec := Execution{
		Name:                   msg.GetJobName(),
		ID:                     msg.GetExecutionId(),
		ScheduledAtUnixMs:      msg.GetScheduledAtUnixMs(),
		LocalScheduledAtUnixMs: msg.GetLocalScheduledAtUnixMs(),
		Attempt:                int(msg.GetAttempt()),
		IdempotencyKey:         msg.GetIdempotencyKey(),
	}

	s.sendResult(ctx, msg, decl.Handler(ctx, exec))
}

func (s *Subscriber) withTrace(ctx context.Context, msg *pb.JobExecution) context.Context {
	tc, err := telemetry.ParseHeader(msg.GetXSbTrace())
	if err != nil {
		s.logger.Warn("job: mint trace context",
			"execution_id", msg.GetExecutionId(), "err", err)
		return ctx
	}
	return telemetry.WithTraceContext(ctx, tc)
}

func (s *Subscriber) sendResult(ctx context.Context, msg *pb.JobExecution, runErr error) {
	// The handler already ran and the runtime is waiting for the outcome, so the
	// send outlives a cancelled subscriber and is bounded by its own timeout.
	if ctx.Err() != nil {
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, s.cfg.ResultTimeout)
	defer cancel()

	// Identity read now, not when the execution arrived: after a rotation the
	// runtime authenticates the call against the current instance, and the lease
	// epoch — not the instance id — is what fences a result from a dead lease.
	id := s.cfg.Identity()
	req := &pb.JobResultRequest{
		ExecutionId: msg.GetExecutionId(),
		InstanceId:  id.InstanceID,
		LeaseEpoch:  msg.GetLeaseEpoch(),
	}
	if runErr == nil {
		req.Outcome = &pb.JobResultRequest_Success{Success: &pb.JobSuccess{}}
	} else {
		req.Outcome = &pb.JobResultRequest_Failure{Failure: &pb.JobFailure{
			ErrorMessage: runErr.Error(),
			Retryable:    !errors.Is(runErr, ErrPermanent),
		}}
	}

	client, err := s.cfg.Clients.JobsClient(sendCtx)
	if err != nil {
		s.reportError(fmt.Errorf("job: subscriber: result: jobs client: %w", err))
		return
	}
	if _, err := client.JobResult(sendCtx, req); err != nil {
		s.reportError(fmt.Errorf("job: subscriber: result for execution %s: %w", msg.GetExecutionId(), err))
	}
}

// heartbeat keeps this instance alive for the runtime and extends the lease of
// every execution it holds. Owned by Start, stopped by the context Stop
// cancels.
func (s *Subscriber) heartbeat(ctx context.Context) {
	defer s.wg.Done()

	interval := s.beat(ctx, s.cfg.HeartbeatInterval)
	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			interval = s.beat(ctx, interval)
			t.Reset(interval)
		}
	}
}

func (s *Subscriber) beat(ctx context.Context, interval time.Duration) time.Duration {
	id := s.cfg.Identity()
	if id.ServiceID == "" || id.InstanceID == "" {
		s.onHeartbeatFailure(ErrNoIdentity)
		return interval
	}
	client, err := s.cfg.Clients.JobsClient(ctx)
	if err != nil {
		s.onHeartbeatFailure(err)
		return interval
	}

	callCtx, cancel := context.WithTimeout(ctx, interval)
	defer cancel()
	resp, err := client.Heartbeat(callCtx, &pb.JobsHeartbeatRequest{
		ServiceId: id.ServiceID, InstanceId: id.InstanceID,
	})
	if err != nil {
		s.onHeartbeatFailure(err)
		return interval
	}
	s.hbFailures = 0
	// The runtime derives the cadence from its lease and instance timeouts; it
	// is the authority in both directions.
	if hint := resp.GetHeartbeatIntervalMs(); hint > 0 {
		return max(time.Duration(hint)*time.Millisecond, minHeartbeatInterval)
	}
	return interval
}

// onHeartbeatFailure is loud on purpose. A swallowed heartbeat failure means the
// runtime reclaims this instance on timeout and reassigns its executions, with
// nothing in the logs to explain why the same job keeps running twice.
func (s *Subscriber) onHeartbeatFailure(err error) {
	s.hbFailures++
	s.logger.Warn("job: heartbeat failed",
		"failures", s.hbFailures, "threshold", s.cfg.HeartbeatThreshold, "err", err)
	s.reportError(fmt.Errorf("job: subscriber: heartbeat: %w", err))

	if s.hbFailures < s.cfg.HeartbeatThreshold {
		return
	}
	s.hbFailures = 0
	s.logger.Warn("job: heartbeat threshold reached, reopening subscription")
	s.sup.Restart()
}

func (s *Subscriber) reportError(err error) {
	if s.cfg.OnError != nil {
		s.cfg.OnError(err)
	}
}

func (s *Subscriber) retireExecution(id string, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active, ok := s.active[id]; ok && active.epoch == epoch {
		active.cancel()
		delete(s.active, id)
	}
}

type activeExecution struct {
	epoch  uint64
	cancel context.CancelFunc
}
