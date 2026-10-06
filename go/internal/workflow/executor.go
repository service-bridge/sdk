package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// beatTick is how often the executor checks which leases are due a heartbeat;
// each task's own interval comes from the runtime.
const beatTick = 250 * time.Millisecond

// reportBackoff spaces the retries of a result report over a transient channel
// failure; after them the lease runs out and the runtime re-leases the step.
var reportBackoff = []time.Duration{200 * time.Millisecond, time.Second, 3 * time.Second}

// errLeaseLost cancels an execution whose lease the runtime no longer knows.
var errLeaseLost = errors.New("workflow: task lease lost")

// CallSpec is a resolved call task.
type CallSpec struct {
	Service        string
	Method         string
	Input          any
	Timeout        time.Duration
	Transport      string
	IdempotencyKey string
	RequestID      string
	Retry          *wf.RetryPolicy
}

// PublishSpec is a resolved publish task.
type PublishSpec struct {
	Event          string
	Payload        any
	IdempotencyKey string
	PartitionKey   string
	Headers        map[string]string
}

// Effects performs call and publish tasks through the client.
type Effects interface {
	Call(ctx context.Context, spec CallSpec) (any, error)
	Publish(ctx context.Context, spec PublishSpec) (any, error)
}

// Definitions finds the local function a local task runs.
type Definitions interface {
	Local(workflow, version, stepID string) (wf.LocalFunc, bool)
}

// Span describes the user sub-operation opened around a local step or a
// compensation. Call and publish steps get none: their own RPC.CALL or
// EVENT.PUBLISH operation hangs under the run root.
type Span struct {
	RunID             string
	StepID            string
	Workflow          string
	IsCompensation    bool
	CompensatesStepID string
}

// ExecutorConfig wires the owner side.
type ExecutorConfig struct {
	Clients     ClientSource
	Identity    func() Identity
	Definitions Definitions
	Effects     Effects
	// WrapSpan opens the user sub-operation; nil runs unwrapped.
	WrapSpan func(ctx context.Context, span Span, fn func(context.Context) (any, error)) (any, error)
	// ErrorCode reports the code a failed task carries; "" means "ERROR".
	ErrorCode func(error) string
	Backoff   stream.Backoff
	OnError   func(err error)
	Logger    *slog.Logger
}

// Executor consumes the task stream, executes each leased step, keeps the
// leases alive and reports results by task token. It holds no DAG logic.
type Executor struct {
	cfg ExecutorConfig
	log *slog.Logger
	sup *stream.Supervisor[*pb.StepTask, pb.Workflows_SubscribeClient]

	mu      sync.Mutex
	runCtx  context.Context
	cancel  context.CancelFunc
	stopped bool
	running map[string]*running
	wg      sync.WaitGroup
}

type running struct {
	cancel   context.CancelCauseFunc
	interval time.Duration
	lastBeat time.Time
	lost     bool
}

// NewExecutor validates the config and builds an idle executor.
func NewExecutor(cfg ExecutorConfig) (*Executor, error) {
	switch {
	case cfg.Clients == nil:
		return nil, fmt.Errorf("workflow: new executor: missing client source: %w", ErrInvalidConfig)
	case cfg.Identity == nil:
		return nil, fmt.Errorf("workflow: new executor: missing identity: %w", ErrInvalidConfig)
	case cfg.Definitions == nil:
		return nil, fmt.Errorf("workflow: new executor: missing definitions: %w", ErrInvalidConfig)
	case cfg.Effects == nil:
		return nil, fmt.Errorf("workflow: new executor: missing effects: %w", ErrInvalidConfig)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	e := &Executor{cfg: cfg, log: cfg.Logger, running: map[string]*running{}}
	sup, err := stream.NewSupervisor(stream.Config[*pb.StepTask, pb.Workflows_SubscribeClient]{
		Name:    "workflows.subscribe",
		Open:    e.open,
		OnData:  e.onTask,
		OnError: e.report,
		Backoff: cfg.Backoff,
		Logger:  cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("workflow: new executor: %w", err)
	}
	e.sup = sup
	return e, nil
}

// Start opens the task stream and the heartbeat loop.
func (e *Executor) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.runCtx != nil {
		e.mu.Unlock()
		return fmt.Errorf("workflow: executor: start: %w", stream.ErrAlreadyStarted)
	}
	e.runCtx, e.cancel = context.WithCancel(ctx)
	runCtx := e.runCtx
	e.mu.Unlock()
	if err := e.sup.Start(runCtx); err != nil {
		e.cancel()
		return fmt.Errorf("workflow: executor: start: %w", err)
	}
	e.wg.Add(1)
	go e.beat(runCtx)
	return nil
}

// Stop cancels every execution, closes the stream and waits for the
// goroutines; the leases of unfinished tasks expire and the runtime re-leases
// those steps.
func (e *Executor) Stop() {
	e.mu.Lock()
	e.stopped = true
	cancel := e.cancel
	for _, r := range e.running {
		r.lost = true
		r.cancel(errors.New("workflow: executor stopped"))
	}
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.sup.Stop()
	e.wg.Wait()
}

// InFlight reports how many tasks are executing.
func (e *Executor) InFlight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.running)
}

func (e *Executor) open(ctx context.Context) (pb.Workflows_SubscribeClient, error) {
	if id := e.cfg.Identity(); id.InstanceID == "" {
		return nil, fmt.Errorf("workflow: executor: open: %w", ErrNoIdentity)
	}
	client, err := e.cfg.Clients.WorkflowsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("workflow: executor: workflows client: %w", err)
	}
	st, err := client.Subscribe(ctx, &pb.SubscribeRequest{})
	if err != nil {
		return nil, fmt.Errorf("workflow: executor: subscribe: %w", err)
	}
	return st, nil
}

// onTask runs on the supervisor goroutine: every task gets its own goroutine.
// A task outlives the stream that delivered it; its lease is renewed by unary
// heartbeats.
func (e *Executor) onTask(_ context.Context, task *pb.StepTask, _ pb.Workflows_SubscribeClient) {
	e.mu.Lock()
	if e.stopped || e.runCtx == nil {
		e.mu.Unlock()
		return
	}
	if _, dup := e.running[task.GetTaskToken()]; dup {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancelCause(e.runCtx)
	if d := task.GetDeadlineUnixMs(); d > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithDeadline(ctx, time.UnixMilli(d))
		prev := cancel
		cancel = func(err error) { stop(); prev(err) }
	}
	interval := msDuration(task.GetHeartbeatIntervalMs())
	if interval < beatTick {
		interval = beatTick
	}
	r := &running{cancel: cancel, interval: interval, lastBeat: time.Now()}
	e.running[task.GetTaskToken()] = r
	e.wg.Add(1)
	e.mu.Unlock()
	go e.execute(ctx, task, r)
}

func (e *Executor) execute(ctx context.Context, task *pb.StepTask, r *running) {
	defer e.wg.Done()
	if tc, err := telemetry.ParseHeader(task.GetXSbTrace()); err == nil {
		ctx = telemetry.WithTraceContext(ctx, tc)
	}
	output, err := e.perform(ctx, task)

	e.mu.Lock()
	delete(e.running, task.GetTaskToken())
	lost := r.lost
	e.mu.Unlock()
	r.cancel(nil)
	if lost {
		return
	}
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err != nil {
		e.fail(reportCtx, task, err)
		return
	}
	e.complete(reportCtx, task, output)
}

// errUnsupportedVersion marks a task this process has no code for.
var errUnsupportedVersion = errors.New("UNSUPPORTED_VERSION")

func (e *Executor) perform(ctx context.Context, task *pb.StepTask) (any, error) {
	input, err := decodeJSON(task.GetInput())
	if err != nil {
		return nil, fmt.Errorf("workflow: task input: %w", err)
	}
	switch task.GetKind() {
	case pb.TaskKind_TASK_KIND_LOCAL:
		fn, ok := e.cfg.Definitions.Local(task.GetWorkflow(), task.GetVersion(), task.GetTemplateStepId())
		if !ok {
			return nil, fmt.Errorf("%w: %s@%s step %s", errUnsupportedVersion, task.GetWorkflow(), task.GetVersion(), task.GetTemplateStepId())
		}
		state, err := decodeObject(task.GetState())
		if err != nil {
			return nil, fmt.Errorf("workflow: task state: %w", err)
		}
		ctx = wf.WithTask(ctx, wf.Task{RunID: task.GetRunId(), StepID: task.GetStepId(), Attempt: int(task.GetAttempt())})
		return e.wrap(ctx, task, func(ctx context.Context) (any, error) { return fn(ctx, state) })
	case pb.TaskKind_TASK_KIND_CALL:
		o := task.GetCallOpts()
		spec := CallSpec{
			Service: task.GetService(), Method: task.GetMethod(), Input: input,
			Timeout: msDuration(o.GetTimeoutMs()), Transport: o.GetTransport(),
			IdempotencyKey: o.GetIdempotencyKey(), RequestID: o.GetRequestId(),
		}
		if r := o.GetRetry(); r != nil {
			spec.Retry = &wf.RetryPolicy{MaxAttempts: int(r.GetMaxAttempts()), BaseDelay: msDuration(r.GetBaseDelayMs()),
				Factor: r.GetFactor(), MaxDelay: msDuration(r.GetMaxDelayMs()), Jitter: r.GetJitter()}
		}
		return e.wrap(ctx, task, func(ctx context.Context) (any, error) { return e.cfg.Effects.Call(ctx, spec) })
	case pb.TaskKind_TASK_KIND_PUBLISH:
		o := task.GetPublishOpts()
		spec := PublishSpec{Event: task.GetEvent(), Payload: input, IdempotencyKey: o.GetIdempotencyKey(),
			PartitionKey: o.GetPartitionKey(), Headers: o.GetHeaders()}
		return e.wrap(ctx, task, func(ctx context.Context) (any, error) { return e.cfg.Effects.Publish(ctx, spec) })
	}
	return nil, fmt.Errorf("%w: unknown task kind %v", errUnsupportedVersion, task.GetKind())
}

func (e *Executor) wrap(ctx context.Context, task *pb.StepTask, fn func(context.Context) (any, error)) (any, error) {
	span := task.GetKind() == pb.TaskKind_TASK_KIND_LOCAL || task.GetIsCompensation()
	if !span || e.cfg.WrapSpan == nil {
		return fn(ctx)
	}
	return e.cfg.WrapSpan(ctx, Span{
		RunID: task.GetRunId(), StepID: task.GetStepId(), Workflow: task.GetWorkflow(),
		IsCompensation: task.GetIsCompensation(), CompensatesStepID: task.GetCompensatesStepId(),
	}, fn)
}

// beat renews due leases in one batch and aborts the executions whose lease
// the runtime reports lost.
func (e *Executor) beat(ctx context.Context) {
	defer e.wg.Done()
	t := time.NewTicker(beatTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		var due []string
		e.mu.Lock()
		for token, r := range e.running {
			if now.Sub(r.lastBeat) >= r.interval {
				r.lastBeat = now
				due = append(due, token)
			}
		}
		e.mu.Unlock()
		if len(due) == 0 {
			continue
		}
		lost, err := e.heartbeat(ctx, due)
		if err != nil {
			e.log.Warn("workflow: heartbeat failed", "err", err)
			continue
		}
		e.mu.Lock()
		for _, token := range lost {
			if r, ok := e.running[token]; ok {
				r.lost = true
				r.cancel(errLeaseLost)
			}
		}
		e.mu.Unlock()
	}
}

func (e *Executor) heartbeat(ctx context.Context, tokens []string) ([]string, error) {
	client, err := e.cfg.Clients.WorkflowsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("workflow: heartbeat: workflows client: %w", err)
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := client.Heartbeat(callCtx, &pb.HeartbeatRequest{TaskTokens: tokens})
	if err != nil {
		return nil, fmt.Errorf("workflow: heartbeat: %w", err)
	}
	return resp.GetLostTokens(), nil
}

func (e *Executor) complete(ctx context.Context, task *pb.StepTask, output any) {
	blob, err := encodeJSON(output)
	if err != nil {
		e.fail(ctx, task, fmt.Errorf("workflow: encode output: %w", err))
		return
	}
	e.send(ctx, task, "complete", func(client pb.WorkflowsClient) error {
		_, err := client.CompleteTask(ctx, &pb.CompleteTaskRequest{TaskToken: task.GetTaskToken(), Output: blob})
		return err
	})
}

func (e *Executor) fail(ctx context.Context, task *pb.StepTask, cause error) {
	code := "ERROR"
	if e.cfg.ErrorCode != nil {
		if c := e.cfg.ErrorCode(cause); c != "" {
			code = c
		}
	}
	permanent := errors.Is(cause, errUnsupportedVersion)
	if permanent {
		code = errUnsupportedVersion.Error()
	}
	e.send(ctx, task, "fail", func(client pb.WorkflowsClient) error {
		_, err := client.FailTask(ctx, &pb.FailTaskRequest{
			TaskToken: task.GetTaskToken(), ErrorCode: code, ErrorMessage: cause.Error(), NonRetriable: permanent,
		})
		return err
	})
}

// send reports a result, retrying over a transient channel failure. A lost
// lease (Aborted) means another attempt owns the step: nothing to report.
func (e *Executor) send(ctx context.Context, task *pb.StepTask, what string, call func(pb.WorkflowsClient) error) {
	for attempt := 0; ; attempt++ {
		client, err := e.cfg.Clients.WorkflowsClient(ctx)
		if err == nil {
			err = call(client)
		}
		if err == nil {
			return
		}
		switch status.Code(err) {
		case codes.Aborted:
			return
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			if attempt < len(reportBackoff) {
				select {
				case <-ctx.Done():
				case <-time.After(reportBackoff[attempt]):
					continue
				}
			}
		}
		e.report(fmt.Errorf("workflow: task %s/%s: %s not recorded: %w", task.GetRunId(), task.GetStepId(), what, err))
		return
	}
}

func (e *Executor) report(err error) {
	e.log.Warn("workflow executor", "err", err)
	if e.cfg.OnError != nil {
		e.cfg.OnError(err)
	}
}
