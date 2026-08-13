package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	wf "github.com/service-bridge/sdk/go/workflow"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/status"
)

// stateInputKey holds the run input in run state. It is not a step id, so the
// scheduler must not read it as a completed step.
const stateInputKey = "input"

// compensationSuffix names the synthetic step a compensation checkpoints
// against. The runtime keys it by this id, so it is contract.
const compensationSuffix = ".compensate"

// StepRole tells the telemetry hook what kind of unit it is wrapping, so the
// trace renders group → branch → step instead of a flat list.
type StepRole string

const (
	// RoleStep is one ordinary step.
	RoleStep StepRole = "step"
	// RoleGroup is a parallel or sequence container.
	RoleGroup StepRole = "group"
	// RoleBranch is one iteration of a forEach fanout.
	RoleBranch StepRole = "branch"
	// RoleCompensation is the reverse action of an already completed step.
	RoleCompensation StepRole = "compensation"
)

// StepSpan describes the unit the runner is about to execute.
type StepSpan struct {
	RunID  string
	StepID string
	Name   string
	Role   StepRole
	// CompensatesStepID names the step being undone; empty outside compensation.
	CompensatesStepID string
}

// WrapFunc opens one user sub-operation around a unit of execution and runs fn
// inside it. The runtime owns the WORKFLOW.RUN operation itself; the SDK emits
// only the sub-operations of the steps it executes, so the nested call and
// publish operations hang under their step instead of under the run root.
type WrapFunc func(ctx context.Context, span StepSpan, fn func(context.Context) (any, error)) (any, error)

// CallSpec is one resolved call step.
type CallSpec struct {
	Service        string
	Method         string
	Input          any
	Timeout        time.Duration
	Transport      string
	IdempotencyKey string
	RequestID      string
}

// PublishSpec is one resolved publish step.
type PublishSpec struct {
	Event          string
	Payload        any
	IdempotencyKey string
	PartitionKey   string
	FireAndForget  bool
	Headers        map[string]string
	OccurredAtMs   int64
}

// StartSpec is one resolved nested workflow step.
type StartSpec struct {
	Workflow       string
	Input          any
	IdempotencyKey string
	TimeoutSec     int
	// ParentRunID is the run the step belongs to. The runtime walks the ancestor
	// chain through it to refuse a workflow that would start itself.
	ParentRunID string
}

// Executor performs what a step declares. Declared here, at the consumer, so
// the runner depends on the three operations it needs and not on the whole
// client.
//
// It carries no retry, no backoff and no timeout of its own for the runner's
// sake: the runtime owns those decisions for workflow steps (ADR 0003).
type Executor interface {
	Call(ctx context.Context, spec CallSpec) (any, error)
	Publish(ctx context.Context, spec PublishSpec) (any, error)
	// StartRun starts a nested run and returns its id. The runner then parks:
	// waiting in-process would hold a lease for the whole child run.
	StartRun(ctx context.Context, spec StartSpec) (string, error)
}

// RunContext is the assignment being executed.
type RunContext struct {
	RunID      string
	LeaseEpoch uint64
	// State is the checkpointed output of every step already done, plus the run
	// input under "input".
	State        map[string]any
	Compensating bool
	// CancelReason mirrors the assignment: 'user_cancel' or 'step_failure'.
	CancelReason string
	// MaxParallelism caps how many steps of one level run at once. Zero is
	// unlimited.
	MaxParallelism int
}

// Outcome is what one assignment produced.
type Outcome struct {
	State map[string]any
	// Parked means the run surrendered to the runtime and must not be completed:
	// a durable timer, an event, a signal or a child run will bring it back.
	Parked       bool
	ParkedStepID string
}

// StepError reports a step the runtime decided not to retry. Action carries
// that decision so a caller can tell a compensating run from a failed one
// without parsing the message.
type StepError struct {
	RunID  string
	StepID string
	Action NextAction
	Err    error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("workflow: run %s: step %q failed, runtime answered %q: %s",
		e.RunID, e.StepID, e.Action, e.Err)
}

func (e *StepError) Unwrap() error { return e.Err }

// RunnerConfig wires the runner. See ./README.md.
type RunnerConfig struct {
	Ops      Ops
	Executor Executor
	// WrapStep opens a telemetry sub-operation around every executed unit.
	// Optional: without it the steps run unwrapped.
	WrapStep WrapFunc
	// Sleep waits out a retry delay the runtime dictated. Defaults to a
	// context-aware timer; tests replace it to keep the clock out of the run.
	Sleep  func(ctx context.Context, d time.Duration) error
	Logger *slog.Logger
}

// Runner executes a frozen plan on the owner instance. It is deliberately thin:
// it schedules by readiness, checkpoints every step and executes the runtime's
// decisions. Retry budgets, backoff, circuit breaking and step timeouts belong
// to the runtime and to the operations the executor reaches (ADR 0003).
type Runner struct {
	ops    Ops
	exec   Executor
	wrap   WrapFunc
	sleep  func(context.Context, time.Duration) error
	logger *slog.Logger
}

// NewRunner validates the config and fills its defaults.
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	if cfg.Ops == nil {
		return nil, fmt.Errorf("workflow: new runner: missing ops: %w", ErrInvalidConfig)
	}
	if cfg.Executor == nil {
		return nil, fmt.Errorf("workflow: new runner: missing executor: %w", ErrInvalidConfig)
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Runner{
		ops:    cfg.Ops,
		exec:   cfg.Executor,
		wrap:   cfg.WrapStep,
		sleep:  cfg.Sleep,
		logger: cfg.Logger,
	}, nil
}

// Run executes one assignment to its end, to a park, or to the failure that
// hands the run back to the runtime. A compensating assignment walks the graph
// backwards instead.
func (r *Runner) Run(ctx context.Context, steps []wf.Step, rc RunContext) (Outcome, error) {
	if rc.RunID == "" {
		return Outcome{}, fmt.Errorf("workflow: run: %w: empty run id", ErrInvalidConfig)
	}

	ex := &execution{runner: r, rc: rc}
	state := newRunState(rc.State)

	if rc.Compensating {
		if err := ex.compensate(ctx, steps, state); err != nil {
			return Outcome{State: state.snapshot()}, err
		}
		return Outcome{State: state.snapshot()}, nil
	}

	if err := ex.runLevels(ctx, steps, state, ""); err != nil {
		return Outcome{State: state.snapshot()}, err
	}
	parked, stepID := ex.parkedAt()
	return Outcome{State: state.snapshot(), Parked: parked, ParkedStepID: stepID}, nil
}

// execution is the state one assignment accumulates across every goroutine it
// fans out to.
type execution struct {
	runner *Runner
	rc     RunContext

	mu           sync.Mutex
	parked       bool
	parkedStepID string
}

func (e *execution) markParked(stepID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.parked {
		e.parked = true
		e.parkedStepID = stepID
	}
}

func (e *execution) parkedAt() (bool, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.parked, e.parkedStepID
}

func (e *execution) isParked() bool {
	parked, _ := e.parkedAt()
	return parked
}

// runLevels schedules by readiness: a step runs once every step it waits for is
// done. The same scheduler drives the top level and every parallel group, so a
// waitFor inside a group means what it means outside one.
func (e *execution) runLevels(ctx context.Context, steps []wf.Step, st *runState, parent string) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow: run %s: %w", e.rc.RunID, err)
		}
		// A park surrenders the run: scheduling another level would checkpoint
		// against a run the runtime has already taken back.
		if e.isParked() {
			return nil
		}

		ready := readySteps(steps, st)
		if len(ready) == 0 {
			return nil
		}
		if e.rc.MaxParallelism > 0 && len(ready) > e.rc.MaxParallelism {
			ready = ready[:e.rc.MaxParallelism]
		}
		if err := e.runBatch(ctx, ready, st, parent); err != nil {
			return err
		}
	}
}

// runBatch executes one readiness level. The group context is cancelled the
// moment a step returns an error, so a sibling that has not dispatched yet
// never reaches the outside world: the Node runner used Promise.all and let the
// siblings keep calling and checkpointing against a run the runtime was already
// compensating.
func (e *execution) runBatch(ctx context.Context, batch []wf.Step, st *runState, parent string) error {
	if len(batch) == 1 {
		return e.executeStep(ctx, batch[0], st, parent)
	}

	g, gctx := errgroup.WithContext(ctx)
	if e.rc.MaxParallelism > 0 {
		g.SetLimit(e.rc.MaxParallelism)
	}
	for _, step := range batch {
		g.Go(func() error { return e.executeStep(gctx, step, st, parent) })
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// runSequential executes a sequence group's children one after another.
func (e *execution) runSequential(ctx context.Context, steps []wf.Step, st *runState, parent string) error {
	for _, step := range steps {
		if e.isParked() {
			return nil
		}
		if err := e.executeStep(ctx, step, st, parent); err != nil {
			return err
		}
	}
	return nil
}

func readySteps(steps []wf.Step, st *runState) []wf.Step {
	var ready []wf.Step
	for _, step := range steps {
		common := step.Common()
		if st.isDone(common.ID) {
			continue
		}
		waiting := false
		for _, dep := range common.WaitFor {
			if !st.isDone(dep) {
				waiting = true
				break
			}
		}
		if !waiting {
			ready = append(ready, step)
		}
	}
	return ready
}

// executeStep drives one step through claim → execute → checkpoint, and repeats
// it for exactly as long as the runtime keeps answering "retry".
func (e *execution) executeStep(ctx context.Context, step wf.Step, st *runState, parent string) error {
	common := step.Common()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow: run %s: step %q: %w", e.rc.RunID, common.ID, err)
	}

	if common.When != nil {
		holds, err := EvalPredicate(common.When, st.snapshot())
		if err != nil {
			return fmt.Errorf("workflow: run %s: step %q: when: %w", e.rc.RunID, common.ID, err)
		}
		if !holds {
			st.set(common.ID, nil)
			return nil
		}
	}

	for {
		input, err := snapshotStepInput(step, st.snapshot())
		if err != nil {
			return fmt.Errorf("workflow: run %s: step %q: input snapshot: %w", e.rc.RunID, common.ID, err)
		}

		begin, err := e.runner.ops.BeginStep(ctx, BeginStepArgs{
			RunID:        e.rc.RunID,
			StepID:       common.ID,
			ParentStepID: parent,
			Kind:         step.Kind(),
			Input:        input,
			LeaseEpoch:   e.rc.LeaseEpoch,
		})
		if err != nil {
			return err
		}
		// The runtime already has an output for this step: the run is being
		// replayed and executing again would repeat the effect.
		if begin.AlreadyDone {
			st.set(common.ID, begin.Output)
			return nil
		}

		// Checked here and not only on entry: a sibling may have failed while
		// this step was claiming, and its operation must not go out any more.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow: run %s: step %q: %w", e.rc.RunID, common.ID, err)
		}

		output, err := e.dispatch(ctx, step, st)
		if err == nil {
			if output == parkedSentinel {
				e.markParked(common.ID)
				return nil
			}
			if err := e.runner.ops.CompleteStep(ctx, CompleteStepArgs{
				RunID:      e.rc.RunID,
				StepID:     common.ID,
				Output:     output,
				LeaseEpoch: e.rc.LeaseEpoch,
			}); err != nil {
				return err
			}
			st.set(common.ID, output)
			return nil
		}

		code, message := unpackError(err)
		decision, failErr := e.runner.ops.FailStep(ctx, FailStepArgs{
			RunID:        e.rc.RunID,
			StepID:       common.ID,
			ErrorCode:    code,
			ErrorMessage: message,
			LeaseEpoch:   e.rc.LeaseEpoch,
			Retriable:    retriable(common),
		})
		if failErr != nil {
			// The checkpoint itself did not land — a fenced lease or a dead
			// channel. The run is not this instance's to drive any more.
			return failErr
		}

		switch decision.Action {
		case ActionRetry:
			if err := e.runner.sleep(ctx, time.Duration(decision.RetryDelaySec)*time.Second); err != nil {
				return fmt.Errorf("workflow: run %s: step %q: retry delay: %w", e.rc.RunID, common.ID, err)
			}
			continue
		case ActionCompensate, ActionFailRun:
			return &StepError{RunID: e.rc.RunID, StepID: common.ID, Action: decision.Action, Err: err}
		default:
			return fmt.Errorf("workflow: run %s: step %q: %w: %q",
				e.rc.RunID, common.ID, ErrUnknownAction, decision.Action)
		}
	}
}

// parkedSentinel is what a park-kind step returns instead of an output. It is a
// value and not an error so a park never competes with a real failure for the
// group's first error.
var parkedSentinel = &struct{ name string }{name: "parked"}

func (e *execution) dispatch(ctx context.Context, step wf.Step, st *runState) (any, error) {
	role := RoleStep
	switch step.(type) {
	case wf.Parallel, wf.Sequence:
		role = RoleGroup
	}
	span := StepSpan{RunID: e.rc.RunID, StepID: step.Common().ID, Name: step.Common().ID, Role: role}
	return e.wrapped(ctx, span, func(ctx context.Context) (any, error) {
		return e.dispatchKind(ctx, step, st)
	})
}

func (e *execution) wrapped(ctx context.Context, span StepSpan, fn func(context.Context) (any, error)) (any, error) {
	if e.runner.wrap == nil {
		return fn(ctx)
	}
	return e.runner.wrap(ctx, span, fn)
}

func (e *execution) dispatchKind(ctx context.Context, step wf.Step, st *runState) (any, error) {
	switch s := step.(type) {
	case wf.Call:
		return e.dispatchCall(ctx, s, st)
	case wf.Publish:
		return e.dispatchPublish(ctx, s, st)
	case wf.SubWorkflow:
		return e.dispatchSubWorkflow(ctx, s, st)
	case wf.Sleep:
		return e.dispatchSleep(ctx, s)
	case wf.WaitEvent:
		return e.dispatchWaitEvent(ctx, s, st)
	case wf.WaitSignal:
		return e.dispatchWaitSignal(ctx, s)
	case wf.Parallel:
		return e.dispatchGroup(ctx, s.Control, s.Steps, s.ForEach, true, st)
	case wf.Sequence:
		return e.dispatchGroup(ctx, s.Control, s.Steps, s.ForEach, false, st)
	case wf.Local:
		return e.dispatchLocal(ctx, s, st)
	default:
		return nil, fmt.Errorf("workflow: run %s: %w: %T", e.rc.RunID, ErrUnknownStepKind, step)
	}
}

func (e *execution) dispatchCall(ctx context.Context, step wf.Call, st *runState) (any, error) {
	state := st.snapshot()
	service, err := EvalTarget(step.Service, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: service: %w", step.ID, err)
	}
	method, err := EvalTarget(step.Method, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: method: %w", step.ID, err)
	}
	input, err := EvalValue(step.Input, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: input: %w", step.ID, err)
	}

	spec := CallSpec{Service: service, Method: method, Input: input}
	if step.Opts != nil {
		spec.Timeout = step.Opts.Timeout
		spec.Transport = string(step.Opts.Transport)
		if spec.IdempotencyKey, err = optString(step.Opts.IdempotencyKey, state); err != nil {
			return nil, fmt.Errorf("workflow: step %q: idempotencyKey: %w", step.ID, err)
		}
		if spec.RequestID, err = optString(step.Opts.RequestID, state); err != nil {
			return nil, fmt.Errorf("workflow: step %q: requestId: %w", step.ID, err)
		}
	}
	return e.runner.exec.Call(ctx, spec)
}

func (e *execution) dispatchPublish(ctx context.Context, step wf.Publish, st *runState) (any, error) {
	state := st.snapshot()
	event, err := EvalTarget(step.Event, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: event: %w", step.ID, err)
	}
	payload, err := EvalValue(step.Input, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: input: %w", step.ID, err)
	}

	spec := PublishSpec{Event: event, Payload: payload}
	if step.Opts != nil {
		spec.FireAndForget = step.Opts.FireAndForget
		spec.OccurredAtMs = step.Opts.OccurredAtMs
		if spec.IdempotencyKey, err = optString(step.Opts.IdempotencyKey, state); err != nil {
			return nil, fmt.Errorf("workflow: step %q: idempotencyKey: %w", step.ID, err)
		}
		if spec.PartitionKey, err = optString(step.Opts.PartitionKey, state); err != nil {
			return nil, fmt.Errorf("workflow: step %q: partitionKey: %w", step.ID, err)
		}
		if len(step.Opts.Headers) > 0 {
			spec.Headers = make(map[string]string, len(step.Opts.Headers))
			for key, value := range step.Opts.Headers {
				resolved, err := optString(value, state)
				if err != nil {
					return nil, fmt.Errorf("workflow: step %q: header %q: %w", step.ID, key, err)
				}
				spec.Headers[key] = resolved
			}
		}
	}
	return e.runner.exec.Publish(ctx, spec)
}

// dispatchSubWorkflow starts the child and parks on it. Waiting for the child
// in process would hold this run's lease for the child's whole lifetime and
// lose the wait to any restart; the runtime resumes the parent when the child
// finishes.
func (e *execution) dispatchSubWorkflow(ctx context.Context, step wf.SubWorkflow, st *runState) (any, error) {
	state := st.snapshot()
	name, err := EvalTarget(step.Workflow, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: workflow: %w", step.ID, err)
	}
	input, err := EvalValue(step.Input, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: input: %w", step.ID, err)
	}

	spec := StartSpec{Workflow: name, Input: input, ParentRunID: e.rc.RunID}
	if step.Opts != nil {
		spec.TimeoutSec = step.Opts.TimeoutSec
		if spec.IdempotencyKey, err = optString(step.Opts.IdempotencyKey, state); err != nil {
			return nil, fmt.Errorf("workflow: step %q: idempotencyKey: %w", step.ID, err)
		}
	}

	childRunID, err := e.runner.exec.StartRun(ctx, spec)
	if err != nil {
		return nil, err
	}
	if err := e.runner.ops.Park(ctx, ParkArgs{
		RunID:      e.rc.RunID,
		StepID:     step.ID,
		LeaseEpoch: e.rc.LeaseEpoch,
		Nested:     &NestedWait{ChildRunID: childRunID},
	}); err != nil {
		return nil, err
	}
	return parkedSentinel, nil
}

func (e *execution) dispatchSleep(ctx context.Context, step wf.Sleep) (any, error) {
	if err := e.runner.ops.Park(ctx, ParkArgs{
		RunID:      e.rc.RunID,
		StepID:     step.ID,
		LeaseEpoch: e.rc.LeaseEpoch,
		Sleep:      &SleepWait{DurationSec: uint32(step.DurationSec)},
	}); err != nil {
		return nil, err
	}
	return parkedSentinel, nil
}

func (e *execution) dispatchWaitEvent(ctx context.Context, step wf.WaitEvent, st *runState) (any, error) {
	state := st.snapshot()
	event, err := EvalTarget(step.Event, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: event: %w", step.ID, err)
	}

	filterJSON := ""
	if len(step.Filter) > 0 {
		resolved := make(map[string]any, len(step.Filter))
		for key, value := range step.Filter {
			got, err := EvalValue(value, state)
			if err != nil {
				return nil, fmt.Errorf("workflow: step %q: filter %q: %w", step.ID, key, err)
			}
			resolved[key] = got
		}
		blob, err := json.Marshal(resolved)
		if err != nil {
			return nil, fmt.Errorf("workflow: step %q: encode filter: %w", step.ID, err)
		}
		filterJSON = string(blob)
	}

	if err := e.runner.ops.Park(ctx, ParkArgs{
		RunID:      e.rc.RunID,
		StepID:     step.ID,
		LeaseEpoch: e.rc.LeaseEpoch,
		Event: &EventWait{
			Event:      event,
			FilterJSON: filterJSON,
			TimeoutSec: uint32(step.TimeoutSec),
		},
	}); err != nil {
		return nil, err
	}
	return parkedSentinel, nil
}

func (e *execution) dispatchWaitSignal(ctx context.Context, step wf.WaitSignal) (any, error) {
	if err := e.runner.ops.Park(ctx, ParkArgs{
		RunID:      e.rc.RunID,
		StepID:     step.ID,
		LeaseEpoch: e.rc.LeaseEpoch,
		Signal:     &SignalWait{Signal: step.Signal, TimeoutSec: uint32(step.TimeoutSec)},
	}); err != nil {
		return nil, err
	}
	return parkedSentinel, nil
}

func (e *execution) dispatchLocal(ctx context.Context, step wf.Local, st *runState) (any, error) {
	if step.Fn == nil {
		return nil, fmt.Errorf("workflow: step %q: %w", step.ID, ErrMissingFunc)
	}
	return step.Fn(ctx, st.snapshot())
}

// iteration is one pass over a group's children: the children themselves, the
// bindings the fanout element adds to state, and the suffix that keeps the
// iterations' step ids apart.
type iteration struct {
	steps    []wf.Step
	bindings map[string]any
	suffix   string
}

func (e *execution) dispatchGroup(
	ctx context.Context,
	common wf.Control,
	steps []wf.Step,
	each *wf.ForEach,
	parallel bool,
	st *runState,
) (any, error) {
	iterations, err := e.iterations(common, steps, each, st)
	if err != nil {
		return nil, err
	}

	output := make(map[string]any, len(iterations)*len(steps))
	var outputMu sync.Mutex

	runOne := func(ctx context.Context, it iteration) error {
		sub := newRunState(st.snapshot())
		for name, value := range it.bindings {
			sub.set(name, value)
		}

		body := func(ctx context.Context) (any, error) {
			if parallel {
				return nil, e.runLevels(ctx, it.steps, sub, common.ID)
			}
			return nil, e.runSequential(ctx, it.steps, sub, common.ID)
		}

		// A fanout branch gets a span of its own so the trace shows which
		// element failed. The single implicit iteration of a plain group needs
		// no extra level: its steps already nest under the group's span.
		if it.suffix != "" {
			span := StepSpan{
				RunID:  e.rc.RunID,
				StepID: common.ID + ":" + it.suffix,
				Name:   common.ID + "[" + it.suffix + "]",
				Role:   RoleBranch,
			}
			if _, err := e.wrapped(ctx, span, body); err != nil {
				return err
			}
		} else if _, err := body(ctx); err != nil {
			return err
		}

		outputMu.Lock()
		defer outputMu.Unlock()
		for _, child := range it.steps {
			value, _ := sub.get(child.Common().ID)
			output[child.Common().ID] = value
		}
		return nil
	}

	if !parallel || len(iterations) == 1 {
		for _, it := range iterations {
			if err := runOne(ctx, it); err != nil {
				return nil, err
			}
		}
		return e.groupOutput(output), nil
	}

	g, gctx := errgroup.WithContext(ctx)
	if e.rc.MaxParallelism > 0 {
		g.SetLimit(e.rc.MaxParallelism)
	}
	for _, it := range iterations {
		g.Go(func() error { return runOne(gctx, it) })
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return e.groupOutput(output), nil
}

// groupOutput withholds a group's output once one of its children parked: the
// group is not finished, and checkpointing it would mark the whole subtree done
// while the runtime is still holding the wait.
func (e *execution) groupOutput(output map[string]any) any {
	if e.isParked() {
		return parkedSentinel
	}
	return output
}

func (e *execution) iterations(common wf.Control, steps []wf.Step, each *wf.ForEach, st *runState) ([]iteration, error) {
	if each == nil {
		return []iteration{{steps: steps}}, nil
	}

	from, err := EvalPath(each.From, st.snapshot())
	if err != nil {
		return nil, fmt.Errorf("workflow: step %q: forEach.from: %w", common.ID, err)
	}
	items, ok := from.([]any)
	if !ok {
		return nil, fmt.Errorf("workflow: step %q: forEach.from: %w: got %T",
			common.ID, ErrPathType, from)
	}

	out := make([]iteration, 0, len(items))
	for i, item := range items {
		suffix := strconv.Itoa(i)
		children := make([]wf.Step, 0, len(steps))
		for _, child := range steps {
			children = append(children, rewriteStepIDs(child, suffix))
		}
		out = append(out, iteration{
			steps:    children,
			bindings: map[string]any{each.As: item},
			suffix:   suffix,
		})
	}
	return out, nil
}

// rewriteStepIDs gives one fanout iteration its own step ids, so the runtime
// keys each element's checkpoint separately.
func rewriteStepIDs(step wf.Step, suffix string) wf.Step {
	switch s := step.(type) {
	case wf.Call:
		s.ID += ":" + suffix
		return s
	case wf.Publish:
		s.ID += ":" + suffix
		return s
	case wf.Sleep:
		s.ID += ":" + suffix
		return s
	case wf.WaitEvent:
		s.ID += ":" + suffix
		return s
	case wf.WaitSignal:
		s.ID += ":" + suffix
		return s
	case wf.SubWorkflow:
		s.ID += ":" + suffix
		return s
	case wf.Local:
		s.ID += ":" + suffix
		return s
	case wf.Parallel:
		s.ID += ":" + suffix
		s.Steps = rewriteChildIDs(s.Steps, suffix)
		return s
	case wf.Sequence:
		s.ID += ":" + suffix
		s.Steps = rewriteChildIDs(s.Steps, suffix)
		return s
	default:
		return step
	}
}

func rewriteChildIDs(steps []wf.Step, suffix string) []wf.Step {
	out := make([]wf.Step, 0, len(steps))
	for _, child := range steps {
		out = append(out, rewriteStepIDs(child, suffix))
	}
	return out
}

// compensate walks the completed steps backwards and runs the reverse action of
// every one that declared it.
func (e *execution) compensate(ctx context.Context, steps []wf.Step, st *runState) error {
	flat := flattenSteps(steps, nil)
	var firstFailure error

	for i := len(flat) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow: compensate run %s: %w", e.rc.RunID, err)
		}

		step := flat[i]
		common := step.Common()
		if common.Compensate == nil {
			continue
		}
		// Presence in state is what says the step completed: the runtime builds a
		// compensating run's state out of its successful steps only, skipped
		// ones included nowhere. Reading the value instead would leave every
		// step that produced nothing — a publish, most of all — uncompensated.
		if _, done := st.get(common.ID); !done {
			continue
		}

		if err := e.compensateStep(ctx, step, st); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				// Another holder is compensating this run. Continuing would
				// duplicate every reverse action it has left to do.
				return err
			}
			if firstFailure == nil {
				firstFailure = err
			}
		}
	}

	// Raised after the whole walk, so the caller does not complete the run: the
	// runtime re-assigns it, and the checkpoints below make the second pass skip
	// whatever did succeed.
	if firstFailure != nil {
		return firstFailure
	}
	return nil
}

func (e *execution) compensateStep(ctx context.Context, step wf.Step, st *runState) error {
	common := step.Common()
	comp := common.Compensate
	state := st.snapshot()

	input, err := EvalValue(comp.Input, state)
	if err != nil {
		return fmt.Errorf("workflow: compensate %q: input: %w", common.ID, err)
	}

	compStepID := common.ID + compensationSuffix
	kind := string(comp.Kind)
	if kind == "" {
		kind = step.Kind()
	}

	begin, err := e.runner.ops.BeginStep(ctx, BeginStepArgs{
		RunID:        e.rc.RunID,
		StepID:       compStepID,
		ParentStepID: common.ID,
		Kind:         kind,
		Input:        input,
		LeaseEpoch:   e.rc.LeaseEpoch,
	})
	if err != nil {
		return err
	}
	// The backward walk restarts from the end every time the runtime re-assigns
	// a compensating run. Without this check every reverse action already taken
	// is taken again — one more refund per reassignment.
	if begin.AlreadyDone {
		return nil
	}

	span := StepSpan{
		RunID:             e.rc.RunID,
		StepID:            compStepID,
		Name:              "compensate " + common.ID,
		Role:              RoleCompensation,
		CompensatesStepID: common.ID,
	}
	output, execErr := e.wrapped(ctx, span, func(ctx context.Context) (any, error) {
		return e.runCompensation(ctx, step, comp, kind, input, state)
	})
	if execErr == nil {
		return e.runner.ops.CompleteStep(ctx, CompleteStepArgs{
			RunID:      e.rc.RunID,
			StepID:     compStepID,
			Output:     output,
			LeaseEpoch: e.rc.LeaseEpoch,
		})
	}

	// A failed reverse action must not strand the steps before it, so the walk
	// goes on — but the runtime is told, so the compensation step lands terminal
	// instead of hanging in flight. retriable is false because the reverse
	// action's own budget lives in its retry policy, which the operation it
	// reaches has already spent; the runtime's answer carries no new decision
	// while the run is already compensating.
	code, message := unpackError(execErr)
	if _, err := e.runner.ops.FailStep(ctx, FailStepArgs{
		RunID:        e.rc.RunID,
		StepID:       compStepID,
		ErrorCode:    code,
		ErrorMessage: message,
		LeaseEpoch:   e.rc.LeaseEpoch,
		Retriable:    false,
	}); err != nil {
		return err
	}
	return fmt.Errorf("workflow: compensate %q: %w", common.ID, execErr)
}

func (e *execution) runCompensation(
	ctx context.Context,
	step wf.Step,
	comp *wf.Compensation,
	kind string,
	input any,
	state map[string]any,
) (any, error) {
	idempotencyKey, err := optString(comp.IdempotencyKey, state)
	if err != nil {
		return nil, fmt.Errorf("workflow: compensate %q: idempotencyKey: %w", step.Common().ID, err)
	}

	switch kind {
	case wf.KindCall:
		service, method, err := compensationCall(step, comp, state)
		if err != nil {
			return nil, err
		}
		return e.runner.exec.Call(ctx, CallSpec{
			Service:        service,
			Method:         method,
			Input:          input,
			IdempotencyKey: idempotencyKey,
		})
	case wf.KindPublish:
		event, err := compensationEvent(step, comp, state)
		if err != nil {
			return nil, err
		}
		return e.runner.exec.Publish(ctx, PublishSpec{
			Event:          event,
			Payload:        input,
			IdempotencyKey: idempotencyKey,
		})
	default:
		return nil, fmt.Errorf("workflow: compensate %q: %w: %s",
			step.Common().ID, ErrCompensationUnsupported, kind)
	}
}

// compensationCall resolves who the reverse call goes to. A compensation that
// names neither service nor method undoes the step through the same method it
// called, which is the common shape for an idempotent undo endpoint.
func compensationCall(step wf.Step, comp *wf.Compensation, state map[string]any) (string, string, error) {
	call, isCall := step.(wf.Call)

	service := comp.Service
	if service == nil && isCall {
		service = call.Service
	}
	method := comp.Method
	if method == nil && isCall {
		method = call.Method
	}

	resolvedService, err := EvalTarget(service, state)
	if err != nil {
		return "", "", fmt.Errorf("workflow: compensate %q: service: %w", step.Common().ID, err)
	}
	resolvedMethod, err := EvalTarget(method, state)
	if err != nil {
		return "", "", fmt.Errorf("workflow: compensate %q: method: %w", step.Common().ID, err)
	}
	return resolvedService, resolvedMethod, nil
}

func compensationEvent(step wf.Step, comp *wf.Compensation, state map[string]any) (string, error) {
	event := comp.Event
	if event == nil {
		if publish, ok := step.(wf.Publish); ok {
			event = publish.Event
		}
	}
	resolved, err := EvalTarget(event, state)
	if err != nil {
		return "", fmt.Errorf("workflow: compensate %q: event: %w", step.Common().ID, err)
	}
	return resolved, nil
}

func flattenSteps(steps []wf.Step, out []wf.Step) []wf.Step {
	for _, step := range steps {
		out = append(out, step)
		switch s := step.(type) {
		case wf.Parallel:
			out = flattenSteps(s.Steps, out)
		case wf.Sequence:
			out = flattenSteps(s.Steps, out)
		}
	}
	return out
}

// snapshotStepInput renders what the step is about to do, for the runtime to
// store next to the checkpoint. It is the record of the attempt, not its input
// argument: a park-kind step has no argument and records what it waits for.
func snapshotStepInput(step wf.Step, state map[string]any) (any, error) {
	switch s := step.(type) {
	case wf.Call:
		return EvalValue(s.Input, state)
	case wf.Publish:
		return EvalValue(s.Input, state)
	case wf.SubWorkflow:
		return EvalValue(s.Input, state)
	case wf.Sleep:
		return map[string]any{"durationSec": s.DurationSec}, nil
	case wf.WaitEvent:
		event, err := EvalTarget(s.Event, state)
		if err != nil {
			return nil, err
		}
		return map[string]any{"event": event, "filter": s.Filter}, nil
	case wf.WaitSignal:
		return map[string]any{"signal": s.Signal}, nil
	default:
		return nil, nil
	}
}

// retriable reports whether the step declared a budget for more than one
// attempt. It mirrors what the runtime seeded the step's attempt budget from, so
// the flag never claims a budget that does not exist.
func retriable(common wf.Control) bool {
	return common.Retry != nil && common.Retry.MaxAttempts > 1
}

// unpackError renders a failure the way the runtime stores it. A gRPC status
// carries the code the callee chose; anything else is unclassified.
func unpackError(err error) (string, string) {
	if st, ok := status.FromError(err); ok {
		return st.Code().String(), err.Error()
	}
	return "UNKNOWN", err.Error()
}

// optString resolves a declared option down to the string the wire wants. An
// absent option stays absent rather than becoming the empty string by accident.
func optString(v any, state map[string]any) (string, error) {
	if v == nil {
		return "", nil
	}
	got, err := EvalValue(v, state)
	if err != nil {
		return "", err
	}
	if got == nil {
		return "", nil
	}
	s, ok := got.(string)
	if !ok {
		return "", fmt.Errorf("%w: got %T", ErrTargetShape, got)
	}
	return s, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// runState is the state of one run, or of one fanout branch. Steps of a level
// run at once and every one of them writes what it produced, so the map is
// behind a mutex and readers work off a copy.
type runState struct {
	mu     sync.Mutex
	values map[string]any
	done   map[string]struct{}
}

func newRunState(initial map[string]any) *runState {
	values := make(map[string]any, len(initial)+1)
	completed := make(map[string]struct{}, len(initial))
	for key, value := range initial {
		values[key] = value
		if key == stateInputKey {
			continue
		}
		completed[key] = struct{}{}
	}
	return &runState{values: values, done: completed}
}

func (s *runState) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.values))
	for key, value := range s.values {
		out[key] = value
	}
	return out
}

func (s *runState) set(id string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[id] = value
	s.done[id] = struct{}{}
}

func (s *runState) get(id string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[id]
	return value, ok
}

func (s *runState) isDone(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.done[id]
	return ok
}
