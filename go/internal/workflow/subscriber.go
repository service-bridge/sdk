package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// DefaultHeartbeatInterval matches the runtime's workflow.heartbeat_interval_ms.
// One sweep drives every lease: a timer per run turns five hundred concurrent
// runs into five hundred timers doing the same thing on the same period.
const DefaultHeartbeatInterval = 10 * time.Second

// DefaultCompleteTimeout bounds the last checkpoint of a finished run, which
// runs outside the subscriber's lifetime.
const DefaultCompleteTimeout = 10 * time.Second

// The terminal statuses a finished assignment reports. Which one applies is
// read off the assignment: a forward run that ran out of steps succeeded, a
// compensation that finished undoes either a failure or a cancellation.
const (
	statusSuccess           = "success"
	statusFailedCompensated = "failed_compensated"
	statusCancelled         = "cancelled"

	// reasonStepFailure is the cancel_reason the runtime sets when a step, and
	// not a caller, is what stopped the run.
	reasonStepFailure = "step_failure"
)

// GraphSource resolves a workflow name to the graph declared in this process.
//
// It is not optional and has no wire fallback: a Local step carries a Go
// closure, which no frozen plan can hold, and the graph's step types are a
// closed set that cannot be rebuilt from JSON. The declaration in this process
// is the only executable form of the workflow.
type GraphSource interface {
	Steps(workflowName string) ([]wf.Step, bool)
}

// SubscriberConfig wires the owner side. See ./README.md.
type SubscriberConfig struct {
	Clients ClientSource
	// Identity reports the live session's identity. Called per subscribe and per
	// heartbeat, never cached: a certificate rotation mints a fresh InstanceID,
	// and a stale one heartbeats for an instance the runtime has torn down.
	Identity func() Identity
	Graphs   GraphSource
	Runner   *Runner
	Ops      Ops
	// HeartbeatInterval defaults to DefaultHeartbeatInterval.
	HeartbeatInterval time.Duration
	// CompleteTimeout bounds the final CompleteRun. Defaults to
	// DefaultCompleteTimeout.
	CompleteTimeout time.Duration
	// Backoff pins the reconnect ladder. Zero value falls back to the default.
	Backoff stream.Backoff
	// OnError reports stream and checkpoint failures. The subscriber recovers on
	// its own regardless.
	OnError func(err error)
	Logger  *slog.Logger
}

// Subscriber owns the assignment stream, the heartbeat that keeps this
// instance's leases alive, and every goroutine executing a run.
type Subscriber struct {
	cfg    SubscriberConfig
	logger *slog.Logger
	sup    *stream.Supervisor[*pb.RunAssignment, pb.Workflows_SubscribeClient]

	mu      sync.Mutex
	started bool
	stopped bool
	runCtx  context.Context
	cancel  context.CancelFunc
	// leases maps a run to the epoch of the assignment being executed for it. A
	// re-assignment bumps the epoch, and only the execution holding the current
	// one may retire the lease.
	leases map[string]uint64
	// openedInstanceID is the identity the live stream was opened with. A
	// rotation mints a new one, and a stream still carrying the retired id keeps
	// the runtime assigning runs to an instance it has already torn down.
	openedInstanceID string

	wg sync.WaitGroup
}

// NewSubscriber validates the config and builds an idle subscriber.
func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Clients == nil {
		return nil, fmt.Errorf("workflow: new subscriber: missing client source: %w", ErrInvalidConfig)
	}
	if cfg.Identity == nil {
		return nil, fmt.Errorf("workflow: new subscriber: missing identity source: %w", ErrInvalidConfig)
	}
	if cfg.Graphs == nil {
		return nil, fmt.Errorf("workflow: new subscriber: missing graph source: %w", ErrInvalidConfig)
	}
	if cfg.Runner == nil {
		return nil, fmt.Errorf("workflow: new subscriber: missing runner: %w", ErrInvalidConfig)
	}
	if cfg.Ops == nil {
		return nil, fmt.Errorf("workflow: new subscriber: missing ops: %w", ErrInvalidConfig)
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.CompleteTimeout <= 0 {
		cfg.CompleteTimeout = DefaultCompleteTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	s := &Subscriber{cfg: cfg, logger: cfg.Logger, leases: make(map[string]uint64)}
	sup, err := stream.NewSupervisor(stream.Config[*pb.RunAssignment, pb.Workflows_SubscribeClient]{
		Name:    "workflows.subscribe",
		Open:    s.open,
		OnData:  s.onData,
		OnError: s.reportError,
		Backoff: cfg.Backoff,
		Logger:  cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("workflow: new subscriber: %w", err)
	}
	s.sup = sup
	return s, nil
}

// Start opens the assignment stream and starts the heartbeat sweep. Cancelling
// ctx stops both exactly like Stop, minus the wait for the runs.
func (s *Subscriber) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("workflow: subscriber: start: %w", stream.ErrAlreadyStarted)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.started = true
	s.runCtx = runCtx
	s.cancel = cancel
	s.mu.Unlock()

	if err := s.sup.Start(runCtx); err != nil {
		cancel()
		return fmt.Errorf("workflow: subscriber: start: %w", err)
	}

	s.wg.Add(1)
	go s.sweep(runCtx)
	return nil
}

// Stop cancels every running assignment, closes the stream and waits for the
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

	s.mu.Lock()
	s.leases = make(map[string]uint64)
	s.mu.Unlock()
}

func (s *Subscriber) open(ctx context.Context) (pb.Workflows_SubscribeClient, error) {
	id := s.cfg.Identity()
	if id.ServiceID == "" || id.InstanceID == "" {
		return nil, fmt.Errorf("workflow: subscriber: open: %w", ErrNoIdentity)
	}
	client, err := s.cfg.Clients.WorkflowsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("workflow: subscriber: workflows client: %w", err)
	}
	st, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		ServiceId:  id.ServiceID,
		InstanceId: id.InstanceID,
	})
	if err != nil {
		return nil, fmt.Errorf("workflow: subscriber: subscribe: %w", err)
	}

	s.mu.Lock()
	s.openedInstanceID = id.InstanceID
	s.mu.Unlock()
	return st, nil
}

// onData runs on the supervisor goroutine, so it must never block: every
// assignment gets a goroutine of its own.
func (s *Subscriber) onData(_ context.Context, msg *pb.RunAssignment, _ pb.Workflows_SubscribeClient) {
	steps, ok := s.cfg.Graphs.Steps(msg.GetWorkflowName())
	if !ok {
		s.logger.Warn("workflow: no graph declared, dropping assignment",
			"workflow", msg.GetWorkflowName(), "run_id", msg.GetRunId())
		return
	}

	s.mu.Lock()
	runCtx, stopped := s.runCtx, s.stopped
	if !stopped && runCtx != nil {
		s.wg.Add(1)
	}
	s.mu.Unlock()
	if stopped || runCtx == nil {
		return
	}

	// The run context is the subscriber's, not the stream's: a broken stream
	// makes the runtime reclaim the lease, but abandoning a half-executed run
	// does not undo the effects already committed. The stale epoch is what
	// fences the checkpoints of a superseded holder.
	go s.execute(runCtx, msg, steps)
}

func (s *Subscriber) execute(ctx context.Context, msg *pb.RunAssignment, steps []wf.Step) {
	defer s.wg.Done()

	runID, epoch := msg.GetRunId(), msg.GetLeaseEpoch()
	state, err := runStateOf(msg)
	if err != nil {
		s.logger.Error("workflow: assignment carries unreadable state",
			"run_id", runID, "workflow", msg.GetWorkflowName(), "err", err)
		s.reportError(err)
		return
	}

	s.holdLease(runID, epoch)
	defer s.releaseLease(runID, epoch)

	// The runtime owns the WORKFLOW.RUN operation (ADR 0003). The SDK only
	// carries its trace context forward, so the step sub-operations and the
	// calls they make hang under the run instead of starting their own tree.
	ctx = s.withTrace(ctx, msg)

	outcome, err := s.cfg.Runner.Run(ctx, steps, RunContext{
		RunID:          runID,
		LeaseEpoch:     epoch,
		State:          state,
		Compensating:   msg.GetCompensating(),
		CancelReason:   msg.GetCancelReason(),
		MaxParallelism: int(msg.GetMaxParallelism()),
	})
	if err != nil {
		s.logger.Warn("workflow: run handed back to the runtime",
			"run_id", runID, "workflow", msg.GetWorkflowName(),
			"compensating", msg.GetCompensating(), "err", err)
		s.reportError(err)
		return
	}
	if outcome.Parked {
		s.logger.Debug("workflow: run parked",
			"run_id", runID, "step", outcome.ParkedStepID)
		return
	}

	// The run is over and the runtime is waiting to hear it, so the last
	// checkpoint outlives a stopping subscriber and is bounded by its own
	// timeout instead.
	completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.CompleteTimeout)
	defer cancel()
	if err := s.cfg.Ops.CompleteRun(completeCtx, CompleteRunArgs{
		RunID:          runID,
		FinalState:     outcome.State,
		LeaseEpoch:     epoch,
		TerminalStatus: terminalStatus(msg),
	}); err != nil {
		s.logger.Warn("workflow: complete run failed", "run_id", runID, "err", err)
		s.reportError(err)
	}
}

// terminalStatus reads what the run ended as off the assignment that produced
// it: a forward run succeeded, and a finished compensation either undid a
// failure or undid a cancellation.
func terminalStatus(msg *pb.RunAssignment) string {
	if !msg.GetCompensating() {
		return statusSuccess
	}
	if msg.GetCancelReason() == reasonStepFailure {
		return statusFailedCompensated
	}
	return statusCancelled
}

func (s *Subscriber) withTrace(ctx context.Context, msg *pb.RunAssignment) context.Context {
	tc, err := telemetry.ParseHeader(msg.GetXSbTrace())
	if err != nil {
		s.logger.Warn("workflow: mint trace context", "run_id", msg.GetRunId(), "err", err)
		return ctx
	}
	return telemetry.WithTraceContext(ctx, tc)
}

// runStateOf rebuilds the state the run resumes from: the outputs already
// checkpointed, plus the run input under its own key.
func runStateOf(msg *pb.RunAssignment) (map[string]any, error) {
	state := map[string]any{}
	if blob := msg.GetState(); len(blob) > 0 {
		if err := json.Unmarshal(blob, &state); err != nil {
			return nil, fmt.Errorf("workflow: run %s: decode state: %w", msg.GetRunId(), err)
		}
	}
	input, err := decodeJSON(msg.GetInput())
	if err != nil {
		return nil, fmt.Errorf("workflow: run %s: decode input: %w", msg.GetRunId(), err)
	}
	state[stateInputKey] = input
	return state, nil
}

func (s *Subscriber) holdLease(runID string, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.leases[runID] = epoch
}

// releaseLease retires a lease only if this execution still holds it. A run
// re-assigned while the previous execution is still unwinding arrives at a
// higher epoch; without the check the old execution's exit kills the new
// assignment's heartbeat, the lease expires, the run is re-assigned again —
// livelock on the same run.
func (s *Subscriber) releaseLease(runID string, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leases[runID] == epoch {
		delete(s.leases, runID)
	}
}

// sweep renews every live lease and reopens the stream after a rotation. Owned
// by Start, stopped by the context Stop cancels.
func (s *Subscriber) sweep(ctx context.Context) {
	defer s.wg.Done()

	t := time.NewTicker(s.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx)
		}
	}
}

func (s *Subscriber) tick(ctx context.Context) {
	id := s.cfg.Identity()
	if id.InstanceID == "" {
		s.logger.Warn("workflow: heartbeat skipped", "err", ErrNoIdentity)
		return
	}

	s.mu.Lock()
	opened := s.openedInstanceID
	leases := make(map[string]uint64, len(s.leases))
	for runID, epoch := range s.leases {
		leases[runID] = epoch
	}
	s.mu.Unlock()

	if opened != "" && opened != id.InstanceID {
		s.sup.Restart()
	}

	for runID, epoch := range leases {
		s.beat(ctx, runID, epoch)
	}
}

func (s *Subscriber) beat(ctx context.Context, runID string, epoch uint64) {
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.HeartbeatInterval)
	defer cancel()

	err := s.cfg.Ops.Heartbeat(callCtx, HeartbeatArgs{RunID: runID, LeaseEpoch: epoch})
	if err == nil {
		return
	}
	if errors.Is(err, ErrLeaseLost) {
		// The runtime refuses the renewal: either a newer holder owns the run,
		// or the run holds no lease at all because it was never parked. Either
		// way this instance has nothing to renew — and saying so beats a warning
		// every interval for the rest of the run's life.
		s.logger.Warn("workflow: lease not renewable, heartbeat retired",
			"run_id", runID, "lease_epoch", epoch, "err", err)
		s.releaseLease(runID, epoch)
		return
	}
	s.logger.Warn("workflow: heartbeat failed", "run_id", runID, "lease_epoch", epoch, "err", err)
	s.reportError(err)
}

func (s *Subscriber) reportError(err error) {
	if s.cfg.OnError != nil {
		s.cfg.OnError(err)
	}
}
