package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// Encoded is a definition ready for registration plus what stays local.
type Encoded struct {
	Definition *pb.WorkflowDefinition
	// Locals maps a local step's id to its function, which cannot travel.
	Locals map[string]wf.LocalFunc
}

// Encode turns a declared definition into the WorkflowDefinition the runtime
// validates and freezes. Only what cannot be expressed on the wire is checked
// here; graph rules are the runtime's.
func Encode(name string, def wf.Definition) (Encoded, error) {
	out := Encoded{Locals: map[string]wf.LocalFunc{}}
	var schema []byte
	if def.Input != nil {
		b, err := json.Marshal(def.Input)
		if err != nil {
			return Encoded{}, fmt.Errorf("workflow %q: encode input schema: %w", name, err)
		}
		schema = b
	}
	steps, err := encodeSteps(def.Steps, out.Locals)
	if err != nil {
		return Encoded{}, fmt.Errorf("workflow %q: %w", name, err)
	}
	out.Definition = &pb.WorkflowDefinition{
		Name:            name,
		Version:         def.Version,
		InputSchemaJson: schema,
		Steps:           steps,
		Retry:           encodeRetry(def.Retry),
		MaxParallelism:  uint32(max(def.MaxParallelism, 0)), //nolint:gosec // bounded by the runtime
		TimeoutMs:       def.Timeout.Milliseconds(),
	}
	return out, nil
}

func encodeSteps(steps []wf.Step, locals map[string]wf.LocalFunc) ([]*pb.Step, error) {
	out := make([]*pb.Step, 0, len(steps))
	for _, s := range steps {
		ps, err := encodeStep(s, locals)
		if err != nil {
			return nil, err
		}
		out = append(out, ps)
	}
	return out, nil
}

func encodeStep(s wf.Step, locals map[string]wf.LocalFunc) (*pb.Step, error) {
	if s == nil {
		return nil, fmt.Errorf("nil step")
	}
	c := s.Common()
	when, err := encodePredicate(c.When)
	if err != nil {
		return nil, fmt.Errorf("step %q: when: %w", c.ID, err)
	}
	out := &pb.Step{
		Id:        c.ID,
		WaitFor:   c.WaitFor,
		When:      when,
		TimeoutMs: c.Timeout.Milliseconds(),
		Retry:     encodeRetry(c.Retry),
	}
	fail := func(err error) (*pb.Step, error) { return nil, fmt.Errorf("step %q: %w", c.ID, err) }
	switch v := s.(type) {
	case wf.Call:
		step := &pb.CallStep{Service: encodeTarget(v.Service), Method: encodeTarget(v.Method)}
		if step.Input, err = encodeValue(v.Input); err != nil {
			return fail(err)
		}
		if step.Opts, err = encodeCallOpts(v.Opts); err != nil {
			return fail(err)
		}
		if step.Compensate, err = encodeCompensation(c.Compensate, wf.KindCall); err != nil {
			return fail(err)
		}
		out.Kind = &pb.Step_Call{Call: step}
	case wf.Publish:
		step := &pb.PublishStep{Event: encodeTarget(v.Event)}
		if step.Input, err = encodeValue(v.Input); err != nil {
			return fail(err)
		}
		if step.Opts, err = encodePublishOpts(v.Opts); err != nil {
			return fail(err)
		}
		if step.Compensate, err = encodeCompensation(c.Compensate, wf.KindPublish); err != nil {
			return fail(err)
		}
		out.Kind = &pb.Step_Publish{Publish: step}
	case wf.Local:
		if v.Fn == nil {
			return fail(fmt.Errorf("local step without Fn"))
		}
		locals[c.ID] = v.Fn
		out.Kind = &pb.Step_Local{Local: &pb.LocalStep{}}
	case wf.Sleep:
		out.Kind = &pb.Step_Sleep{Sleep: &pb.SleepStep{DurationMs: v.Duration.Milliseconds()}}
	case wf.WaitEvent:
		filter, err := encodeValueMap(v.Filter)
		if err != nil {
			return fail(err)
		}
		out.Kind = &pb.Step_WaitEvent{WaitEvent: &pb.WaitEventStep{Event: v.Event, Filter: filter}}
	case wf.WaitSignal:
		out.Kind = &pb.Step_WaitSignal{WaitSignal: &pb.WaitSignalStep{Signal: v.Signal}}
	case wf.SubWorkflow:
		step := &pb.SubWorkflowStep{
			Service:   encodeTarget(v.Service),
			Workflow:  encodeTarget(v.Workflow),
			TimeoutMs: v.Timeout.Milliseconds(),
		}
		if step.Input, err = encodeValue(v.Input); err != nil {
			return fail(err)
		}
		if step.IdempotencyKey, err = encodeOptional(v.IdempotencyKey); err != nil {
			return fail(err)
		}
		out.Kind = &pb.Step_Workflow{Workflow: step}
	case wf.Parallel:
		group, err := encodeGroup(v.Steps, v.ForEach, locals)
		if err != nil {
			return nil, err
		}
		out.Kind = &pb.Step_Parallel{Parallel: group}
	case wf.Sequence:
		group, err := encodeGroup(v.Steps, v.ForEach, locals)
		if err != nil {
			return nil, err
		}
		out.Kind = &pb.Step_Sequence{Sequence: group}
	default:
		return fail(fmt.Errorf("unknown step kind %T", s))
	}
	return out, nil
}

func encodeGroup(steps []wf.Step, each *wf.ForEach, locals map[string]wf.LocalFunc) (*pb.GroupStep, error) {
	children, err := encodeSteps(steps, locals)
	if err != nil {
		return nil, err
	}
	group := &pb.GroupStep{Steps: children}
	if each != nil {
		group.ForEach = &pb.ForEach{From: string(each.From), As: each.As}
	}
	return group, nil
}

func encodeTarget(t wf.Target) *pb.Expr {
	switch v := t.(type) {
	case wf.Path:
		return &pb.Expr{Value: &pb.Expr_Path{Path: string(v)}}
	case wf.Name:
		b, _ := json.Marshal(string(v))
		return &pb.Expr{Value: &pb.Expr_Literal{Literal: b}}
	}
	return nil
}

// encodeOptional encodes a value that may be absent (nil → no expression).
func encodeOptional(v any) (*pb.Expr, error) {
	if v == nil {
		return nil, nil
	}
	return encodeValue(v)
}

// encodeValue turns a JSON tree with Paths anywhere inside into an expression.
func encodeValue(v any) (*pb.Expr, error) {
	switch t := v.(type) {
	case wf.Path:
		return &pb.Expr{Value: &pb.Expr_Path{Path: string(t)}}, nil
	case wf.Name:
		return literal(string(t))
	case map[string]any:
		fields, err := encodeValueMap(t)
		if err != nil {
			return nil, err
		}
		return &pb.Expr{Value: &pb.Expr_Object{Object: &pb.ExprMap{Fields: fields}}}, nil
	case []any:
		items := make([]*pb.Expr, 0, len(t))
		for _, el := range t {
			x, err := encodeValue(el)
			if err != nil {
				return nil, err
			}
			items = append(items, x)
		}
		return &pb.Expr{Value: &pb.Expr_List{List: &pb.ExprList{Items: items}}}, nil
	}
	return literal(v)
}

func literal(v any) (*pb.Expr, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode literal: %w", err)
	}
	return &pb.Expr{Value: &pb.Expr_Literal{Literal: b}}, nil
}

func encodeValueMap(m map[string]any) (map[string]*pb.Expr, error) {
	if len(m) == 0 {
		return nil, nil
	}
	out := make(map[string]*pb.Expr, len(m))
	for k, v := range m {
		x, err := encodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		out[k] = x
	}
	return out, nil
}

func encodePredicate(p wf.Predicate) (*pb.Predicate, error) {
	if p == nil {
		return nil, nil
	}
	return encodeNode(p.Node())
}

// encodeNode reads the node form a Predicate exposes: a Path is a truthy test,
// a one-key map is an operator.
func encodeNode(node any) (*pb.Predicate, error) {
	switch v := node.(type) {
	case wf.Path:
		x, _ := encodeValue(v)
		return &pb.Predicate{Op: &pb.Predicate_Truthy{Truthy: x}}, nil
	case map[string]any:
		for op, arg := range v {
			switch op {
			case "not":
				inner, err := encodeNode(arg)
				if err != nil {
					return nil, err
				}
				return &pb.Predicate{Op: &pb.Predicate_Not{Not: inner}}, nil
			case "equals", "in":
				pair, ok := arg.([]any)
				if !ok || len(pair) != 2 {
					return nil, fmt.Errorf("%s needs two operands", op)
				}
				l, err := encodeValue(pair[0])
				if err != nil {
					return nil, err
				}
				r, err := encodeValue(pair[1])
				if err != nil {
					return nil, err
				}
				if op == "equals" {
					return &pb.Predicate{Op: &pb.Predicate_Equals{Equals: &pb.ExprPair{Left: l, Right: r}}}, nil
				}
				return &pb.Predicate{Op: &pb.Predicate_In{In: &pb.ExprPair{Left: l, Right: r}}}, nil
			case "and", "or":
				list, _ := arg.([]any)
				items := make([]*pb.Predicate, 0, len(list))
				for _, n := range list {
					x, err := encodeNode(n)
					if err != nil {
						return nil, err
					}
					items = append(items, x)
				}
				if op == "and" {
					return &pb.Predicate{Op: &pb.Predicate_And{And: &pb.PredicateList{Items: items}}}, nil
				}
				return &pb.Predicate{Op: &pb.Predicate_Or{Or: &pb.PredicateList{Items: items}}}, nil
			}
		}
	}
	return nil, fmt.Errorf("malformed predicate %v", node)
}

func encodeRetry(r *wf.RetryPolicy) *pb.RetryPolicy {
	if r == nil {
		return nil
	}
	return &pb.RetryPolicy{
		MaxAttempts: uint32(max(r.MaxAttempts, 0)), //nolint:gosec // bounded by the runtime
		BaseDelayMs: r.BaseDelay.Milliseconds(),
		Factor:      r.Factor,
		MaxDelayMs:  r.MaxDelay.Milliseconds(),
		Jitter:      r.Jitter,
	}
}

func encodeCallOpts(o *wf.CallOpts) (*pb.CallStepOptions, error) {
	if o == nil {
		return nil, nil
	}
	idem, err := encodeOptional(o.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	reqID, err := encodeOptional(o.RequestID)
	if err != nil {
		return nil, err
	}
	return &pb.CallStepOptions{
		TimeoutMs:      o.Timeout.Milliseconds(),
		Transport:      string(o.Transport),
		IdempotencyKey: idem,
		RequestId:      reqID,
		Retry:          encodeRetry(o.Retry),
	}, nil
}

func encodePublishOpts(o *wf.PublishOpts) (*pb.PublishStepOptions, error) {
	if o == nil {
		return nil, nil
	}
	idem, err := encodeOptional(o.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	pk, err := encodeOptional(o.PartitionKey)
	if err != nil {
		return nil, err
	}
	headers, err := encodeValueMap(o.Headers)
	if err != nil {
		return nil, err
	}
	return &pb.PublishStepOptions{IdempotencyKey: idem, PartitionKey: pk, Headers: headers}, nil
}

func encodeCompensation(c *wf.Compensation, stepKind string) (*pb.Compensation, error) {
	if c == nil {
		return nil, nil
	}
	input, err := encodeValue(c.Input)
	if err != nil {
		return nil, err
	}
	out := &pb.Compensation{Input: input, Retry: encodeRetry(c.Retry)}
	kind := string(c.Kind)
	if kind == "" {
		kind = stepKind
	}
	switch kind {
	case wf.KindCall:
		opts, err := encodeCallOpts(c.CallOpts)
		if err != nil {
			return nil, err
		}
		out.Kind = &pb.Compensation_Call{Call: &pb.CallCompensation{
			Service: encodeTarget(c.Service), Method: encodeTarget(c.Method), Opts: opts,
		}}
	case wf.KindPublish:
		opts, err := encodePublishOpts(c.PublishOpts)
		if err != nil {
			return nil, err
		}
		out.Kind = &pb.Compensation_Publish{Publish: &pb.PublishCompensation{Event: encodeTarget(c.Event), Opts: opts}}
	default:
		return nil, fmt.Errorf("compensation kind %q", kind)
	}
	return out, nil
}

// msDuration converts wire milliseconds back to a duration.
func msDuration(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }
