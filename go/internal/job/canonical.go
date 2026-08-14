// Package job builds the canonical specification a scheduled job is registered
// by and runs the declared handlers against the runtime's execution stream.
package job

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// Rules the runtime enforces on a job declaration. Breaking one of them costs a
// round trip and an InvalidArgument at start, so they are checked here, where
// the job is written.
var (
	ErrNoTrigger      = errors.New("job needs exactly one trigger")
	ErrCronFieldCount = errors.New("cron expression must have five fields, seconds are not one of them")
	ErrCronExpr       = errors.New("cron expression is not parseable")
	ErrCronTZ         = errors.New("cron timezone is not a known IANA location")
	ErrInterval       = errors.New("interval must be at least one millisecond")
	ErrRunAt          = errors.New("delayed run time must not be zero")
	ErrCatchupPolicy  = errors.New("catchup policy must be skip, fire_once or fire_all")
	ErrOverlapPolicy  = errors.New("overlap policy must be skip, allow or buffer_one")
	ErrDepKind        = errors.New("declared dependency kind must be rpc, event or workflow")
	ErrDepTarget      = errors.New("declared dependency needs a target")
	ErrRetryInitial   = errors.New("retry policy needs a positive initial delay")
	ErrNegativeLimit  = errors.New("job limits must not be negative")
	ErrEmptyName      = errors.New("job name must not be empty")
	ErrNoHandler      = errors.New("job needs a handler")
	ErrDuplicateName  = errors.New("job name is already declared")
)

// cronParser mirrors runtime/internal/jobs/register.go: five fields, no seconds
// and no @descriptors. A second parser configuration would accept expressions
// the runtime rejects at registration.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// CatchupPolicy decides what the scheduler does with ticks missed while the
// runtime was down.
type CatchupPolicy string

const (
	CatchupSkip     CatchupPolicy = "skip"
	CatchupFireOnce CatchupPolicy = "fire_once"
	CatchupFireAll  CatchupPolicy = "fire_all"
)

// OverlapPolicy decides what happens when a job fires while the previous run is
// still executing.
type OverlapPolicy string

const (
	OverlapSkip      OverlapPolicy = "skip"
	OverlapAllow     OverlapPolicy = "allow"
	OverlapBufferOne OverlapPolicy = "buffer_one"
)

// Dep is a downstream call the job declares up front. The runtime draws it on
// the service map and evaluates it against the access policy.
type Dep struct {
	Kind   string
	Target string
}

// RPCDep declares a call to "service.Method".
func RPCDep(target string) Dep { return Dep{Kind: "rpc", Target: target} }

// EventDep declares a publish of an event by name.
func EventDep(name string) Dep { return Dep{Kind: "event", Target: name} }

// WorkflowDep declares a workflow start by name.
func WorkflowDep(name string) Dep { return Dep{Kind: "workflow", Target: name} }

// RetryPolicy is the per-job exponential backoff. The keys are snake_case
// because the runtime decodes them into jobs.RetryPolicy, whose tags come from
// the persisted job_definitions.retry_policy column — the surrounding spec is
// camelCase and both halves must be reproduced as they are.
type RetryPolicy struct {
	InitialMs  int64   `json:"initial_ms"`
	MaxMs      int64   `json:"max_ms"`
	Multiplier float64 `json:"multiplier"`
	Jitter     float64 `json:"jitter"`
}

type triggerKind uint8

const (
	triggerNone triggerKind = iota
	triggerCron
	triggerDelayed
	triggerInterval
)

// Trigger says when a job fires. It carries exactly one kind by construction:
// the fields are unexported and the only values that exist come out of the three
// constructors, so no spec can declare two triggers or none.
type Trigger struct {
	kind        triggerKind
	cronExpr    string
	cronTZ      string
	runAtUnixMs int64
	everyMs     int64
}

// NewCronTrigger validates a five-field cron expression and an optional IANA
// timezone. Validation happens here so a typo fails where the job is declared
// rather than silently never firing.
func NewCronTrigger(expr, tz string) (Trigger, error) {
	if fields := strings.Fields(expr); len(fields) != 5 {
		return Trigger{}, fmt.Errorf("job: cron trigger %q: %w: got %d", expr, ErrCronFieldCount, len(fields))
	}
	if _, err := cronParser.Parse(expr); err != nil {
		return Trigger{}, fmt.Errorf("job: cron trigger %q: %w: %v", expr, ErrCronExpr, err)
	}
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return Trigger{}, fmt.Errorf("job: cron trigger %q: %w: %v", tz, ErrCronTZ, err)
		}
	}
	return Trigger{kind: triggerCron, cronExpr: expr, cronTZ: tz}, nil
}

// NewIntervalTrigger fires every d. The runtime holds the minimum interval; the
// only thing checked here is that d survives the conversion to whole
// milliseconds, which is the unit on the wire.
func NewIntervalTrigger(d time.Duration) (Trigger, error) {
	ms := d.Milliseconds()
	if ms <= 0 {
		return Trigger{}, fmt.Errorf("job: interval trigger: %w: got %s", ErrInterval, d)
	}
	return Trigger{kind: triggerInterval, everyMs: ms}, nil
}

// NewAtTrigger fires once at t.
func NewAtTrigger(t time.Time) (Trigger, error) {
	if t.IsZero() {
		return Trigger{}, fmt.Errorf("job: delayed trigger: %w", ErrRunAt)
	}
	return Trigger{kind: triggerDelayed, runAtUnixMs: t.UnixMilli()}, nil
}

// Spec is everything a job declares. Every zero field is left out of the
// canonical JSON: the runtime owns the defaults, and repeating them here would
// make the SDK a second source of truth that drifts on the first settings
// change.
type Spec struct {
	Trigger       Trigger
	Catchup       CatchupPolicy
	Overlap       OverlapPolicy
	Deps          []Dep
	MaxAttempts   int
	LeaseTTLMs    int64
	MaxConcurrent int
	Retry         *RetryPolicy
}

// canonicalSpec is the exact shape the runtime decodes out of
// IncomingMethod.input_schema_json (runtime/internal/jobs/canonical.go,
// CanonicalJobSpec). Field order here is key order in the encoded JSON, so this
// struct mirrors the runtime declaration line for line.
type canonicalSpec struct {
	Trigger       canonicalTrigger `json:"trigger"`
	Catchup       string           `json:"catchup,omitempty"`
	Overlap       string           `json:"overlap,omitempty"`
	Deps          []canonicalDep   `json:"deps,omitempty"`
	MaxAttempts   int              `json:"maxAttempts,omitempty"`
	LeaseTTLMs    int64            `json:"leaseTtlMs,omitempty"`
	MaxConcurrent int              `json:"maxConcurrent,omitempty"`
	Retry         *RetryPolicy     `json:"retry,omitempty"`
}

type canonicalTrigger struct {
	Cron     *canonicalCron     `json:"cron,omitempty"`
	Delayed  *canonicalDelayed  `json:"delayed,omitempty"`
	Interval *canonicalInterval `json:"interval,omitempty"`
}

type canonicalCron struct {
	Expr string `json:"expr"`
	TZ   string `json:"tz,omitempty"`
}

type canonicalDelayed struct {
	RunAtUnixMs int64 `json:"runAtUnixMs"`
}

type canonicalInterval struct {
	EveryMs int64 `json:"everyMs"`
}

type canonicalDep struct {
	Kind   string `json:"kind"`
	Target string `json:"target"`
}

// Validate rejects the shapes the runtime answers with InvalidArgument, plus the
// ones it silently rewrites — a retry policy with no initial delay is replaced
// wholesale by the server default, which loses the rest of the policy without a
// word.
func (s Spec) Validate() error {
	if s.Trigger.kind == triggerNone {
		return fmt.Errorf("job: validate spec: %w", ErrNoTrigger)
	}
	switch s.Catchup {
	case "", CatchupSkip, CatchupFireOnce, CatchupFireAll:
	default:
		return fmt.Errorf("job: validate spec: %w: got %q", ErrCatchupPolicy, s.Catchup)
	}
	switch s.Overlap {
	case "", OverlapSkip, OverlapAllow, OverlapBufferOne:
	default:
		return fmt.Errorf("job: validate spec: %w: got %q", ErrOverlapPolicy, s.Overlap)
	}
	for _, d := range s.Deps {
		switch d.Kind {
		case "rpc", "event", "workflow":
		default:
			return fmt.Errorf("job: validate spec: %w: got %q", ErrDepKind, d.Kind)
		}
		if d.Target == "" {
			return fmt.Errorf("job: validate spec: %w: kind %q", ErrDepTarget, d.Kind)
		}
	}
	if s.MaxAttempts < 0 || s.LeaseTTLMs < 0 || s.MaxConcurrent < 0 {
		return fmt.Errorf("job: validate spec: %w", ErrNegativeLimit)
	}
	if s.Retry != nil && s.Retry.InitialMs <= 0 {
		return fmt.Errorf("job: validate spec: %w", ErrRetryInitial)
	}
	return nil
}

// CanonicalJSON renders the spec exactly as the runtime expects to read it back
// out of input_schema_json. Any drift in a key name, in field order or in what
// is omitted changes the contract hash on one side only, and the job then fails
// to register with an error that points nowhere near the cause.
func (s Spec) CanonicalJSON() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	c := canonicalSpec{
		Trigger:       s.Trigger.canonical(),
		Catchup:       string(s.Catchup),
		Overlap:       string(s.Overlap),
		MaxAttempts:   s.MaxAttempts,
		LeaseTTLMs:    s.LeaseTTLMs,
		MaxConcurrent: s.MaxConcurrent,
		Retry:         s.Retry,
	}
	for _, d := range s.Deps {
		c.Deps = append(c.Deps, canonicalDep{Kind: d.Kind, Target: d.Target})
	}
	out, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("job: encode canonical spec: %w", err)
	}
	return out, nil
}

func (t Trigger) canonical() canonicalTrigger {
	switch t.kind {
	case triggerCron:
		return canonicalTrigger{Cron: &canonicalCron{Expr: t.cronExpr, TZ: t.cronTZ}}
	case triggerDelayed:
		return canonicalTrigger{Delayed: &canonicalDelayed{RunAtUnixMs: t.runAtUnixMs}}
	case triggerInterval:
		return canonicalTrigger{Interval: &canonicalInterval{EveryMs: t.everyMs}}
	default:
		// Unreachable: Validate rejects a spec without a trigger before any
		// caller reaches the encoder.
		panic("job: canonical trigger: no trigger kind")
	}
}

// ContractHash identifies one canonical spec. The runtime routes and versions by
// it, so it is the hash of the very bytes that travel in input_schema_json and
// nothing else.
func ContractHash(canonicalJSON []byte) string {
	sum := sha256.Sum256(canonicalJSON)
	return hex.EncodeToString(sum[:])
}

// Execution is what the runtime says about one attempt of a job. Jobs carry no
// input and no output, so this is the whole context a handler gets besides ctx.
type Execution struct {
	Name                   string
	ID                     string
	ScheduledAtUnixMs      int64
	LocalScheduledAtUnixMs int64
	Attempt                int
	IdempotencyKey         string
}

// Handler runs one execution. ctx is cancelled when the subscriber stops; the
// only outcome is an error or the absence of one.
type Handler func(ctx context.Context, exec Execution) error

// ErrPermanent marks a failure the runtime must not retry. Wrap it to report a
// poisoned input instead of burning every remaining attempt on it.
var ErrPermanent = errors.New("job failure is permanent")

// Declaration is one declared job: the canonical bytes the runtime registers,
// the hash identifying them and the handler that runs them.
type Declaration struct {
	Name         string
	Spec         Spec
	SpecJSON     []byte
	ContractHash string
	Handler      Handler
}

// Declarations holds every job the service declares. Safe for concurrent use:
// jobs are declared from wherever the application wires them up.
type Declarations struct {
	mu     sync.Mutex
	order  []string
	byName map[string]Declaration
}

// NewDeclarations builds an empty set.
func NewDeclarations() *Declarations {
	return &Declarations{byName: make(map[string]Declaration)}
}

// Add validates the spec, freezes its canonical form and binds the handler.
func (d *Declarations) Add(name string, spec Spec, h Handler) (Declaration, error) {
	if name == "" {
		return Declaration{}, fmt.Errorf("job: declare: %w", ErrEmptyName)
	}
	if h == nil {
		return Declaration{}, fmt.Errorf("job: declare %q: %w", name, ErrNoHandler)
	}
	specJSON, err := spec.CanonicalJSON()
	if err != nil {
		return Declaration{}, fmt.Errorf("job: declare %q: %w", name, err)
	}

	decl := Declaration{
		Name:         name,
		Spec:         spec,
		SpecJSON:     specJSON,
		ContractHash: ContractHash(specJSON),
		Handler:      h,
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if _, dup := d.byName[name]; dup {
		return Declaration{}, fmt.Errorf("job: declare %q: %w", name, ErrDuplicateName)
	}
	d.byName[name] = decl
	d.order = append(d.order, name)
	return decl, nil
}

// Lookup resolves an incoming execution to its declaration.
func (d *Declarations) Lookup(name string) (Declaration, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	decl, ok := d.byName[name]
	return decl, ok
}

// Each walks the declarations in the order they were added, so the register
// frame is byte-stable across restarts. Returning false stops the walk.
func (d *Declarations) Each(fn func(Declaration) bool) {
	d.mu.Lock()
	decls := make([]Declaration, 0, len(d.order))
	for _, name := range d.order {
		decls = append(decls, d.byName[name])
	}
	d.mu.Unlock()

	for _, decl := range decls {
		if !fn(decl) {
			return
		}
	}
}

// Len reports how many jobs are declared.
func (d *Declarations) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.byName)
}
