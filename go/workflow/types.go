package workflow

import "time"

// Definition is a whole workflow: the steps, the policies that apply to all of
// them and the schema of the input a run starts with. Every zero field is left
// out of the frozen graph, so the runtime keeps its own defaults instead of
// receiving a copy of them that drifts.
type Definition struct {
	// Input is the JSON Schema of the run input. It travels inside the frozen
	// graph and is what the runtime validates a start against.
	Input map[string]any
	Steps []Step
	// Retry applies to every operation step that declares none of its own. It is
	// pushed down onto those steps when the graph is frozen, because the runtime
	// reads a retry budget off the step and never off the graph.
	Retry *RetryPolicy
	// MaxParallelism caps how many steps of one run execute at once.
	MaxParallelism int
	// TimeoutSec caps the whole run.
	TimeoutSec int
}

// RetryPolicy is the exponential backoff applied to one operation.
type RetryPolicy struct {
	// MaxAttempts counts the first try, not only the retries. It is the one
	// field the runtime reads: it seeds the step's attempt budget at run start.
	MaxAttempts int
	BaseDelay   time.Duration
	Factor      float64
	MaxDelay    time.Duration
	// Jitter is the fraction of the delay that is randomised, in [0, 1].
	Jitter float64
}

// CompensationKind selects what a compensation does. Left empty, it mirrors the
// step it is attached to.
type CompensationKind string

const (
	// CompensateCall undoes the step with an RPC.
	CompensateCall CompensationKind = "call"
	// CompensatePublish undoes the step by emitting an event.
	CompensatePublish CompensationKind = "publish"
)

// Compensation is the reverse action of a step, run when a later step fails.
// Only call and publish steps accept one: nothing else has an effect to undo.
type Compensation struct {
	Kind    CompensationKind
	Service Target
	Method  Target
	Event   Target
	Input   any
	Retry   *RetryPolicy
	// IdempotencyKey is a string or a Path; compensation runs at most once per
	// key.
	IdempotencyKey any
}

// ForEach fans a group out over a list. Each element runs the group's steps
// once, with the element bound to As in run state.
type ForEach struct {
	// From is the list to fan out over. It is a Path because the list is only
	// known at run time — a list known at declaration time is written as steps.
	From Path
	// As names the element inside the group, in the same alphabet as a step ID.
	As string
}

// Transport selects how a call reaches the callee.
type Transport string

const (
	// TransportAuto goes direct when the callee's endpoint is known.
	TransportAuto Transport = "auto"
	// TransportDirect requires a known endpoint and fails without one.
	TransportDirect Transport = "direct"
	// TransportProxy always routes through the runtime.
	TransportProxy Transport = "proxy"
)

// CallOpts configures the RPC a call step makes. Timeout bounds that RPC; the
// step's own deadline is Control.TimeoutSec.
type CallOpts struct {
	Timeout   time.Duration
	Transport Transport
	// IdempotencyKey is a string or a Path. Set, it opts the call into
	// runtime-side deduplication.
	IdempotencyKey any
	// RequestID is a string or a Path. Left empty, one is minted per attempt.
	RequestID any
	Retry     *RetryPolicy
}

// PublishOpts configures the event a publish step emits.
type PublishOpts struct {
	// IdempotencyKey is a string or a Path.
	IdempotencyKey any
	// PartitionKey is a string or a Path; events sharing one are delivered in
	// order.
	PartitionKey any
	// FireAndForget returns as soon as the event is queued locally.
	FireAndForget bool
	// Headers values are strings or Paths.
	Headers map[string]any
	// OccurredAtMs is the event time in unix milliseconds. Zero means "now".
	OccurredAtMs int64
}

// StartOpts configures the run a workflow step starts. The parent run is set by
// the runner, not declared here.
type StartOpts struct {
	// IdempotencyKey is a string or a Path.
	IdempotencyKey any
	TimeoutSec     int
}
