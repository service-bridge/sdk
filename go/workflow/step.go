// Package workflow declares the shape of a workflow graph: what its steps are,
// what each of them waits for and which values it reads out of run state. The
// package is description only: the runtime validates, freezes and interprets
// the graph, and the client executes the task steps it is handed.
package workflow

import (
	"context"
	"time"
)

// The discriminator each kind carries in the frozen graph. The runtime reads it
// out of `type` to walk the graph, so these strings are contract, not labels.
const (
	KindCall       = "call"
	KindPublish    = "publish"
	KindSleep      = "sleep"
	KindWaitEvent  = "wait_event"
	KindWaitSignal = "wait_signal"
	KindWorkflow   = "workflow"
	KindParallel   = "parallel"
	KindSequence   = "sequence"
	KindLocal      = "local"
)

// Step is one node of the graph. The set of kinds is closed: the marker method
// is unexported, so no type outside this package is a Step and a graph can
// never carry a kind the runtime does not know.
type Step interface {
	// Kind is the discriminator the frozen graph carries in `type`.
	Kind() string
	// Common returns the fields every kind carries.
	Common() Control
	isStep()
}

// Control is what every kind of step carries regardless of what it does.
//
// Timeout governs the step itself: when it expires the step fails and the run
// stops. It is not the timeout of the underlying call, which lives in
// CallOpts.Timeout.
type Control struct {
	// ID names the step inside its workflow. Lowercase letters, digits and
	// underscores; unique across the whole graph, nesting included.
	ID string
	// WaitFor holds the IDs this step waits for. Order is not meaningful.
	WaitFor []string
	// When gates the step: a false predicate skips it and every value it would
	// have produced resolves to nothing.
	When Predicate
	// Compensate is the reverse action run when a later step fails. Only call
	// and publish steps accept one.
	Compensate *Compensation
	// Timeout caps how long the step may stay incomplete.
	Timeout time.Duration
	// Retry replaces the workflow-level policy for a task step (call, publish,
	// local).
	Retry *RetryPolicy
}

// Common satisfies the Step accessor for every kind that embeds Control.
func (c Control) Common() Control { return c }

// Call invokes a method on another service.
type Call struct {
	Control
	Service Target
	Method  Target
	// Input is the request body: any JSON value, with Path anywhere inside it
	// resolved against run state.
	Input any
	Opts  *CallOpts
}

// Kind implements Step.
func (Call) Kind() string { return KindCall }
func (Call) isStep()      {}

// Publish emits a durable event.
type Publish struct {
	Control
	Event Target
	Input any
	Opts  *PublishOpts
}

// Kind implements Step.
func (Publish) Kind() string { return KindPublish }
func (Publish) isStep()      {}

// Sleep parks the step on a durable timer. The runtime, not the SDK, holds the
// timer, so the run survives a restart of every instance.
type Sleep struct {
	Control
	Duration time.Duration
}

// Kind implements Step.
func (Sleep) Kind() string { return KindSleep }
func (Sleep) isStep()      {}

// WaitEvent parks the step until a matching event is accepted.
type WaitEvent struct {
	Control
	Event string
	// Filter narrows which event resumes the step: each key is a payload path
	// ("$.order.id"), each value the expected value — a literal or a Path
	// resolved when the step parks.
	Filter map[string]any
}

// Kind implements Step.
func (WaitEvent) Kind() string { return KindWaitEvent }
func (WaitEvent) isStep()      {}

// WaitSignal parks the run until an external caller signals it by name.
type WaitSignal struct {
	Control
	Signal string
}

// Kind implements Step.
func (WaitSignal) Kind() string { return KindWaitSignal }
func (WaitSignal) isStep()      {}

// SubWorkflow starts another workflow and waits for it to finish. The runtime
// starts the child itself, so the wait survives any instance.
type SubWorkflow struct {
	Control
	// Service owns the child workflow; nil means this service.
	Service  Target
	Workflow Target
	Input    any
	// IdempotencyKey is a string or a Path.
	IdempotencyKey any
	// Timeout caps the child run; zero keeps the child definition's.
	Timeout time.Duration
}

// Kind implements Step.
func (SubWorkflow) Kind() string { return KindWorkflow }
func (SubWorkflow) isStep()      {}

// Parallel starts every step it holds at once and completes when all of them do.
type Parallel struct {
	Control
	Steps []Step
	// ForEach fans the group out over a list resolved from run state. It is a
	// property of the group, not a step of its own.
	ForEach *ForEach
}

// Kind implements Step.
func (Parallel) Kind() string { return KindParallel }
func (Parallel) isStep()      {}

// Sequence runs the steps it holds one after another.
type Sequence struct {
	Control
	Steps   []Step
	ForEach *ForEach
}

// Kind implements Step.
func (Sequence) Kind() string { return KindSequence }
func (Sequence) isStep()      {}

// LocalFunc runs inside the declaring process. ctx is cancelled when the task
// lease is lost, the step deadline passes or the client stops; TaskOf(ctx)
// describes the attempt.
type LocalFunc func(ctx context.Context, state map[string]any) (any, error)

// Task describes the attempt a LocalFunc executes.
type Task struct {
	RunID   string
	StepID  string
	Attempt int
}

type taskKey struct{}

// WithTask returns ctx carrying t. The client calls it before a LocalFunc.
func WithTask(ctx context.Context, t Task) context.Context {
	return context.WithValue(ctx, taskKey{}, t)
}

// TaskOf reports the attempt a LocalFunc executes.
func TaskOf(ctx context.Context) (Task, bool) {
	t, ok := ctx.Value(taskKey{}).(Task)
	return t, ok
}

// Local runs a Go function in the declaring process.
//
// Fn does not travel: the definition sent to the runtime carries the step's ID,
// and when the runtime leases the step the locally declared graph supplies the
// function. Bump Definition.Version when Fn changes.
type Local struct {
	Control
	Fn LocalFunc
}

// Kind implements Step.
func (Local) Kind() string { return KindLocal }
func (Local) isStep()      {}
