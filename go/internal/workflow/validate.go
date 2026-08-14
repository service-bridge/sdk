package workflow

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	wf "github.com/service-bridge/sdk/go/workflow"
)

// The caps the runtime enforces on a registered graph
// (runtime/internal/workflow/register.go). Checking them here costs nothing and
// turns a failed registration at start into a failed declaration.
const (
	maxDepth = 10
	maxSteps = 500
)

// stepIDPattern is the alphabet the runtime keys steps by. Anything outside it
// would round-trip badly through the step tables.
var stepIDPattern = regexp.MustCompile(`^[a-z0-9_]+$`)

// Reasons a graph is refused. They are checked where the workflow is written,
// not at the first run: a graph that fails to register leaves the service
// unable to start with an error pointing nowhere near the cause.
var (
	ErrEmptyWorkflowName       = errors.New("workflow name must not be empty")
	ErrNoSteps                 = errors.New("workflow needs at least one step")
	ErrStepID                  = errors.New("step id must match ^[a-z0-9_]+$")
	ErrDuplicateStepID         = errors.New("step id is already used")
	ErrUnknownStepKind         = errors.New("step is not one of the graph kinds")
	ErrDepth                   = errors.New("groups are nested deeper than the runtime accepts")
	ErrStepCount               = errors.New("graph holds more steps than the runtime accepts")
	ErrEmptyGroup              = errors.New("group needs at least one step")
	ErrUnknownDependency       = errors.New("waitFor names a step that is not in the graph")
	ErrDependencyCycle         = errors.New("waitFor forms a cycle")
	ErrCompensationUnsupported = errors.New("only call and publish steps can be compensated")
	ErrSelfReference           = errors.New("workflow step starts the workflow it belongs to")
	ErrEmptyTarget             = errors.New("target must not be empty")
	ErrNegativeDuration        = errors.New("sleep duration must not be negative")
	ErrForEachAlias            = errors.New("forEach alias must match ^[a-z0-9_]+$")
	ErrMissingFunc             = errors.New("local step needs a function")
)

// ValidationError reports a graph the SDK refuses to freeze. It names the step
// and the field so the author reads the location off the error, and wraps the
// reason so errors.Is picks it apart.
type ValidationError struct {
	Workflow string
	Step     string
	Field    string
	Reason   error
}

func (e *ValidationError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "workflow: validate %q", e.Workflow)
	if e.Step != "" {
		fmt.Fprintf(&b, ": step %q", e.Step)
	}
	if e.Field != "" {
		fmt.Fprintf(&b, ": %s", e.Field)
	}
	fmt.Fprintf(&b, ": %s", e.Reason)
	return b.String()
}

func (e *ValidationError) Unwrap() error { return e.Reason }

// Validate checks everything about a graph that can be decided without running
// it. It is the whole gate: Freeze refuses to encode a graph that fails here.
func Validate(name string, def wf.Definition) error {
	if name == "" {
		return &ValidationError{Reason: ErrEmptyWorkflowName}
	}
	if len(def.Steps) == 0 {
		return &ValidationError{Workflow: name, Reason: ErrNoSteps}
	}

	v := &validator{
		workflow: name,
		ids:      make(map[string]struct{}),
		deps:     make(map[string][]string),
	}
	if err := v.walk(def.Steps, 0); err != nil {
		return err
	}
	return v.checkDependencies()
}

type validator struct {
	workflow string
	ids      map[string]struct{}
	deps     map[string][]string
	// order keeps declaration order so the cycle walk reports the same step on
	// every run; a map walk would name a different one each time.
	order []string
	total int
}

func (v *validator) fail(step, field string, reason error) error {
	return &ValidationError{Workflow: v.workflow, Step: step, Field: field, Reason: reason}
}

func (v *validator) walk(steps []wf.Step, depth int) error {
	if depth > maxDepth {
		return v.fail("", "", fmt.Errorf("%w: %d levels", ErrDepth, maxDepth))
	}
	for _, step := range steps {
		v.total++
		if v.total > maxSteps {
			return v.fail("", "", fmt.Errorf("%w: %d steps", ErrStepCount, maxSteps))
		}
		if err := v.step(step, depth); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) step(step wf.Step, depth int) error {
	if step == nil {
		return v.fail("", "", ErrUnknownStepKind)
	}

	common := step.Common()
	if !stepIDPattern.MatchString(common.ID) {
		return v.fail(common.ID, "id", fmt.Errorf("%w: got %q", ErrStepID, common.ID))
	}
	if _, dup := v.ids[common.ID]; dup {
		return v.fail(common.ID, "id", ErrDuplicateStepID)
	}
	v.ids[common.ID] = struct{}{}
	v.deps[common.ID] = common.WaitFor
	v.order = append(v.order, common.ID)

	if common.When != nil {
		if err := v.predicate(common.ID, "when", common.When); err != nil {
			return err
		}
	}
	if common.Compensate != nil {
		if err := v.compensation(step, common); err != nil {
			return err
		}
	}

	switch s := step.(type) {
	case wf.Call:
		if err := v.target(s.ID, "service", s.Service); err != nil {
			return err
		}
		if err := v.target(s.ID, "method", s.Method); err != nil {
			return err
		}
		if err := v.value(s.ID, "input", s.Input); err != nil {
			return err
		}
		return v.callOpts(s.ID, s.Opts)

	case wf.Publish:
		if err := v.target(s.ID, "event", s.Event); err != nil {
			return err
		}
		if err := v.value(s.ID, "input", s.Input); err != nil {
			return err
		}
		return v.publishOpts(s.ID, s.Opts)

	case wf.Sleep:
		if s.DurationSec < 0 {
			return v.fail(s.ID, "durationSec", fmt.Errorf("%w: got %d", ErrNegativeDuration, s.DurationSec))
		}
		return nil

	case wf.WaitEvent:
		if err := v.target(s.ID, "event", s.Event); err != nil {
			return err
		}
		for key, filter := range s.Filter {
			if err := v.value(s.ID, "filter."+key, filter); err != nil {
				return err
			}
		}
		return nil

	case wf.WaitSignal:
		if s.Signal == "" {
			return v.fail(s.ID, "signal", ErrEmptyTarget)
		}
		return nil

	case wf.SubWorkflow:
		if err := v.target(s.ID, "workflow", s.Workflow); err != nil {
			return err
		}
		if name, ok := s.Workflow.(wf.Name); ok && string(name) == v.workflow {
			return v.fail(s.ID, "workflow", fmt.Errorf("%w: %q", ErrSelfReference, v.workflow))
		}
		if err := v.value(s.ID, "input", s.Input); err != nil {
			return err
		}
		return v.startOpts(s.ID, s.Opts)

	case wf.Parallel:
		return v.group(s.ID, s.Steps, s.ForEach, depth)

	case wf.Sequence:
		return v.group(s.ID, s.Steps, s.ForEach, depth)

	case wf.Local:
		if s.Fn == nil {
			return v.fail(s.ID, "fn", ErrMissingFunc)
		}
		return nil

	default:
		return v.fail(common.ID, "", fmt.Errorf("%w: %T", ErrUnknownStepKind, step))
	}
}

func (v *validator) group(id string, steps []wf.Step, each *wf.ForEach, depth int) error {
	if len(steps) == 0 {
		return v.fail(id, "steps", ErrEmptyGroup)
	}
	if each != nil {
		if err := v.path(id, "forEach.from", each.From); err != nil {
			return err
		}
		if !stepIDPattern.MatchString(each.As) {
			return v.fail(id, "forEach.as", fmt.Errorf("%w: got %q", ErrForEachAlias, each.As))
		}
	}
	return v.walk(steps, depth+1)
}

func (v *validator) compensation(step wf.Step, common wf.Control) error {
	if step.Kind() != wf.KindCall && step.Kind() != wf.KindPublish {
		return v.fail(common.ID, "compensate", fmt.Errorf("%w: got %s", ErrCompensationUnsupported, step.Kind()))
	}
	comp := common.Compensate
	targets := []struct {
		field  string
		target wf.Target
	}{
		{"compensate.service", comp.Service},
		{"compensate.method", comp.Method},
		{"compensate.event", comp.Event},
	}
	for _, t := range targets {
		if t.target == nil {
			continue
		}
		if err := v.target(common.ID, t.field, t.target); err != nil {
			return err
		}
	}
	if err := v.value(common.ID, "compensate.input", comp.Input); err != nil {
		return err
	}
	return v.value(common.ID, "compensate.idempotencyKey", comp.IdempotencyKey)
}

func (v *validator) callOpts(id string, opts *wf.CallOpts) error {
	if opts == nil {
		return nil
	}
	if err := v.value(id, "opts.idempotencyKey", opts.IdempotencyKey); err != nil {
		return err
	}
	return v.value(id, "opts.requestId", opts.RequestID)
}

func (v *validator) publishOpts(id string, opts *wf.PublishOpts) error {
	if opts == nil {
		return nil
	}
	if err := v.value(id, "opts.idempotencyKey", opts.IdempotencyKey); err != nil {
		return err
	}
	if err := v.value(id, "opts.partitionKey", opts.PartitionKey); err != nil {
		return err
	}
	for key, header := range opts.Headers {
		if err := v.value(id, "opts.headers."+key, header); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) startOpts(id string, opts *wf.StartOpts) error {
	if opts == nil {
		return nil
	}
	return v.value(id, "opts.idempotencyKey", opts.IdempotencyKey)
}

func (v *validator) target(id, field string, t wf.Target) error {
	switch target := t.(type) {
	case nil:
		return v.fail(id, field, ErrEmptyTarget)
	case wf.Name:
		if target == "" {
			return v.fail(id, field, ErrEmptyTarget)
		}
		return nil
	case wf.Path:
		return v.path(id, field, target)
	default:
		return v.fail(id, field, fmt.Errorf("%w: unknown target %T", ErrEmptyTarget, t))
	}
}

func (v *validator) path(id, field string, p wf.Path) error {
	if err := ValidatePath(p); err != nil {
		return v.fail(id, field, err)
	}
	return nil
}

// value walks a declared value tree for the sole purpose of parsing every path
// in it; the result is discarded.
func (v *validator) value(id, field string, val any) error {
	_, err := walkValue(val,
		func(p wf.Path) (any, error) { return nil, ValidatePath(p) },
		func(string) (any, error) { return nil, nil },
	)
	if err != nil {
		return v.fail(id, field, err)
	}
	return nil
}

func (v *validator) predicate(id, field string, p wf.Predicate) error {
	if err := v.predicateNode(id, field, p.Node()); err != nil {
		return err
	}
	return nil
}

func (v *validator) predicateNode(id, field string, node any) error {
	switch n := node.(type) {
	case wf.Path:
		return v.path(id, field, n)

	case map[string]any:
		if len(n) != 1 {
			return v.fail(id, field, ErrPredicateShape)
		}
		for op, operand := range n {
			switch op {
			case "not":
				return v.predicateNode(id, field+".not", operand)
			case "and", "or":
				nodes, ok := operand.([]any)
				if !ok || len(nodes) == 0 {
					return v.fail(id, field+"."+op, ErrPredicateShape)
				}
				for i, sub := range nodes {
					if err := v.predicateNode(id, fmt.Sprintf("%s.%s[%d]", field, op, i), sub); err != nil {
						return err
					}
				}
				return nil
			case "equals", "in":
				pair, ok := operand.([]any)
				if !ok || len(pair) != 2 {
					return v.fail(id, field+"."+op, ErrPredicateShape)
				}
				for i, operandValue := range pair {
					if err := v.value(id, fmt.Sprintf("%s.%s[%d]", field, op, i), operandValue); err != nil {
						return err
					}
				}
				return nil
			default:
				return v.fail(id, field, fmt.Errorf("%w: %q", ErrPredicateShape, op))
			}
		}
	}
	return v.fail(id, field, ErrPredicateShape)
}

// checkDependencies runs after every id is known, because a step may wait for
// one declared later in the graph.
func (v *validator) checkDependencies() error {
	const (
		unvisited = iota
		onStack
		done
	)
	state := make(map[string]int, len(v.deps))

	var visit func(id string) error
	visit = func(id string) error {
		switch state[id] {
		case done:
			return nil
		case onStack:
			return v.fail(id, "waitFor", ErrDependencyCycle)
		}
		state[id] = onStack
		for _, dep := range v.deps[id] {
			if _, known := v.ids[dep]; !known {
				return v.fail(id, "waitFor", fmt.Errorf("%w: %q", ErrUnknownDependency, dep))
			}
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[id] = done
		return nil
	}

	for _, id := range v.order {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}
