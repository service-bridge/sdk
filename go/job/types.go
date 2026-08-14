package job

import (
	"time"

	internaljob "github.com/service-bridge/sdk/go/internal/job"
)

// Spec is everything a job declares besides its name and its handler. Build it
// with NewSpec; every option left out is decided by the runtime.
type Spec = internaljob.Spec

// Execution describes one attempt the handler is called for.
//
// The handler must be idempotent by IdempotencyKey, not by Attempt: the key is
// the same across every attempt of one scheduled fire, while Attempt changes on
// each retry. Keying on Attempt makes every retry look like new work.
type Execution = internaljob.Execution

// Handler runs one execution. Its context is cancelled when the client stops.
// Jobs carry no input and no output — the only outcome is an error or nil.
type Handler = internaljob.Handler

// ErrPermanent marks a failure the runtime must not retry. Wrap it to report a
// poisoned input instead of burning every remaining attempt on it.
var ErrPermanent = internaljob.ErrPermanent

// The reasons a declaration is refused. Every one of them is checked before the
// job reaches the runtime, so they surface at the declaration, not at the first
// fire. Match them with errors.Is.
var (
	ErrNoTrigger      = internaljob.ErrNoTrigger
	ErrCronFieldCount = internaljob.ErrCronFieldCount
	ErrCronExpr       = internaljob.ErrCronExpr
	ErrCronTZ         = internaljob.ErrCronTZ
	ErrInterval       = internaljob.ErrInterval
	ErrRunAt          = internaljob.ErrRunAt
	ErrCatchupPolicy  = internaljob.ErrCatchupPolicy
	ErrOverlapPolicy  = internaljob.ErrOverlapPolicy
	ErrDepKind        = internaljob.ErrDepKind
	ErrDepTarget      = internaljob.ErrDepTarget
	ErrRetryInitial   = internaljob.ErrRetryInitial
	ErrNegativeLimit  = internaljob.ErrNegativeLimit
	ErrEmptyName      = internaljob.ErrEmptyName
	ErrNoHandler      = internaljob.ErrNoHandler
	ErrDuplicateName  = internaljob.ErrDuplicateName
)

// CatchupPolicy decides what happens to ticks missed while the runtime was down.
type CatchupPolicy = internaljob.CatchupPolicy

const (
	// CatchupSkip forgets missed ticks and schedules the next one.
	CatchupSkip = internaljob.CatchupSkip
	// CatchupFireOnce fires once for the whole gap.
	CatchupFireOnce = internaljob.CatchupFireOnce
	// CatchupFireAll fires every missed tick, up to the runtime's budget.
	CatchupFireAll = internaljob.CatchupFireAll
)

// OverlapPolicy decides what happens when a job fires while the previous run is
// still executing.
type OverlapPolicy = internaljob.OverlapPolicy

const (
	// OverlapSkip drops the new fire.
	OverlapSkip = internaljob.OverlapSkip
	// OverlapAllow runs it, capped by MaxConcurrent.
	OverlapAllow = internaljob.OverlapAllow
	// OverlapBufferOne queues exactly one fire behind the running one.
	OverlapBufferOne = internaljob.OverlapBufferOne
)

// Dep is a downstream call the job declares up front. The runtime draws it on
// the service map and checks it against the access policy.
type Dep = internaljob.Dep

// RPC declares a call to "service.Method".
func RPC(target string) Dep { return internaljob.RPCDep(target) }

// Event declares a publish of an event by name.
func Event(name string) Dep { return internaljob.EventDep(name) }

// Workflow declares a workflow start by name.
func Workflow(name string) Dep { return internaljob.WorkflowDep(name) }

// RetryPolicy is the per-job exponential backoff.
type RetryPolicy = internaljob.RetryPolicy

// Option sets one job option. Options that are never applied stay absent from
// the specification, and the runtime fills them in — the SDK holds no copy of
// the defaults to drift from.
type Option func(*Spec)

// NewSpec builds the specification of a job firing on t.
func NewSpec(t Trigger, opts ...Option) Spec {
	spec := Spec{Trigger: t}
	for _, opt := range opts {
		opt(&spec)
	}
	return spec
}

// WithCatchup sets the catchup policy.
func WithCatchup(p CatchupPolicy) Option {
	return func(s *Spec) { s.Catchup = p }
}

// WithOverlap sets the overlap policy.
func WithOverlap(p OverlapPolicy) Option {
	return func(s *Spec) { s.Overlap = p }
}

// WithDeps declares the downstream calls the job makes. Repeated use appends.
func WithDeps(deps ...Dep) Option {
	return func(s *Spec) { s.Deps = append(s.Deps, deps...) }
}

// WithMaxAttempts caps how many times one fire is attempted.
func WithMaxAttempts(n int) Option {
	return func(s *Spec) { s.MaxAttempts = n }
}

// WithLeaseTTL sets how long the runtime waits on a silent instance before it
// reassigns the execution.
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Spec) { s.LeaseTTLMs = d.Milliseconds() }
}

// WithMaxConcurrent caps how many executions of this job run at once. It bounds
// both the runtime's dispatch under OverlapAllow and the SDK's own handler
// concurrency.
func WithMaxConcurrent(n int) Option {
	return func(s *Spec) { s.MaxConcurrent = n }
}

// WithRetry replaces the runtime's default backoff.
func WithRetry(p RetryPolicy) Option {
	return func(s *Spec) { s.Retry = &p }
}
