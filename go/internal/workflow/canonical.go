package workflow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	wf "github.com/service-bridge/sdk/go/workflow"
)

// Frozen is a workflow as it leaves the declaration: the exact bytes that
// travel in IncomingMethod.input_schema_json, the hash of those bytes that
// travels in contract_hash, and the steps the runner executes — the same steps
// the bytes describe, with the workflow-level retry policy already pushed down.
type Frozen struct {
	Name string
	// Steps is the graph after retry materialisation. The runner works off this
	// slice, not off the declared one, so what runs and what was registered
	// cannot diverge.
	Steps []wf.Step
	// JSON is the canonical encoding: keys sorted, no whitespace, zero-valued
	// fields absent.
	JSON []byte
	// Fingerprint is the SHA-256 of JSON, hex-encoded.
	Fingerprint string
}

// Freeze validates a declared workflow and renders it into the form the runtime
// registers it by. It is the only way a graph reaches the wire, so a graph that
// fails validation cannot be registered.
func Freeze(name string, def wf.Definition) (Frozen, error) {
	if err := Validate(name, def); err != nil {
		return Frozen{}, err
	}

	steps := def.Steps
	if def.Retry != nil {
		steps = materializeRetry(steps, def.Retry)
	}

	graph := make([]any, 0, len(steps))
	for _, step := range steps {
		node, err := canonicalStep(step)
		if err != nil {
			return Frozen{}, err
		}
		graph = append(graph, node)
	}

	root := map[string]any{"graph": graph}
	if def.Retry != nil {
		root["retry"] = canonicalRetry(def.Retry)
	}
	if def.MaxParallelism > 0 {
		root["maxParallelism"] = def.MaxParallelism
	}
	if def.TimeoutSec > 0 {
		root["timeoutSec"] = def.TimeoutSec
	}
	if len(def.Input) > 0 {
		// The input schema is JSON Schema, not an expression tree: it travels as
		// written, and only its keys get sorted.
		root["inputSchema"] = def.Input
	}

	blob, err := encodeCanonical(root)
	if err != nil {
		return Frozen{}, fmt.Errorf("workflow: freeze %q: %w", name, err)
	}

	return Frozen{
		Name:        name,
		Steps:       steps,
		JSON:        blob,
		Fingerprint: Fingerprint(blob),
	}, nil
}

// Fingerprint identifies one canonical graph. It hashes the very bytes that
// travel in input_schema_json and nothing else, so a hash that matches means
// the graphs match.
func Fingerprint(canonicalJSON []byte) string {
	sum := sha256.Sum256(canonicalJSON)
	return hex.EncodeToString(sum[:])
}

// encodeCanonical renders v with sorted keys and no whitespace. Go's encoder
// already sorts map keys; HTML escaping is turned off so the bytes are the
// plain JSON every SDK produces for the same graph.
func encodeCanonical(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encode canonical graph: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// materializeRetry pushes the workflow-level policy down onto every operation
// step that declared none.
//
// The runtime seeds a step's attempt budget from the step's own retry block and
// reads nothing off the graph (runtime/internal/workflow/register.go), so a
// policy left at the top is a declaration nothing acts on. Groups are skipped
// because a group is not an operation — its failure is one of its children's,
// already retried on that child's budget — and so are the parked kinds, which
// the runtime resumes rather than re-executes.
func materializeRetry(steps []wf.Step, policy *wf.RetryPolicy) []wf.Step {
	out := make([]wf.Step, 0, len(steps))
	for _, step := range steps {
		out = append(out, applyRetry(step, policy))
	}
	return out
}

func applyRetry(step wf.Step, policy *wf.RetryPolicy) wf.Step {
	switch s := step.(type) {
	case wf.Call:
		if s.Retry == nil {
			s.Retry = policy
		}
		return s
	case wf.Publish:
		if s.Retry == nil {
			s.Retry = policy
		}
		return s
	case wf.SubWorkflow:
		if s.Retry == nil {
			s.Retry = policy
		}
		return s
	case wf.Local:
		if s.Retry == nil {
			s.Retry = policy
		}
		return s
	case wf.Parallel:
		s.Steps = materializeRetry(s.Steps, policy)
		return s
	case wf.Sequence:
		s.Steps = materializeRetry(s.Steps, policy)
		return s
	default:
		return step
	}
}

func canonicalStep(step wf.Step) (map[string]any, error) {
	common := step.Common()
	node := map[string]any{
		"id":   common.ID,
		"type": step.Kind(),
	}

	if len(common.WaitFor) > 0 {
		// waitFor is a set: sorting it keeps the fingerprint from moving when
		// the same dependencies are written in a different order.
		deps := make([]string, len(common.WaitFor))
		copy(deps, common.WaitFor)
		sort.Strings(deps)
		node["waitFor"] = deps
	}
	if common.When != nil {
		when, err := canonicalValue(common.When.Node())
		if err != nil {
			return nil, fmt.Errorf("step %q: when: %w", common.ID, err)
		}
		node["when"] = when
	}
	if common.Compensate != nil {
		comp, err := canonicalCompensation(common.Compensate)
		if err != nil {
			return nil, fmt.Errorf("step %q: compensate: %w", common.ID, err)
		}
		node["compensate"] = comp
	}
	if common.TimeoutSec > 0 {
		node["timeoutSec"] = common.TimeoutSec
	}
	if common.Retry != nil {
		node["retry"] = canonicalRetry(common.Retry)
	}

	if err := encodeKind(step, node); err != nil {
		return nil, fmt.Errorf("step %q: %w", common.ID, err)
	}
	return node, nil
}

func encodeKind(step wf.Step, node map[string]any) error {
	switch s := step.(type) {
	case wf.Call:
		node["service"] = canonicalTarget(s.Service)
		node["method"] = canonicalTarget(s.Method)
		if err := putValue(node, "input", s.Input); err != nil {
			return err
		}
		if s.Opts != nil {
			opts, err := canonicalCallOpts(s.Opts)
			if err != nil {
				return err
			}
			node["opts"] = opts
		}

	case wf.Publish:
		node["event"] = canonicalTarget(s.Event)
		if err := putValue(node, "input", s.Input); err != nil {
			return err
		}
		if s.Opts != nil {
			opts, err := canonicalPublishOpts(s.Opts)
			if err != nil {
				return err
			}
			node["opts"] = opts
		}

	case wf.Sleep:
		node["durationSec"] = s.DurationSec

	case wf.WaitEvent:
		node["event"] = canonicalTarget(s.Event)
		if len(s.Filter) > 0 {
			filter, err := canonicalValue(s.Filter)
			if err != nil {
				return err
			}
			node["filter"] = filter
		}

	case wf.WaitSignal:
		node["signal"] = s.Signal

	case wf.SubWorkflow:
		node["workflow"] = canonicalTarget(s.Workflow)
		if err := putValue(node, "input", s.Input); err != nil {
			return err
		}
		if s.Opts != nil {
			opts, err := canonicalStartOpts(s.Opts)
			if err != nil {
				return err
			}
			node["opts"] = opts
		}

	case wf.Parallel:
		return encodeGroup(node, s.Steps, s.ForEach)

	case wf.Sequence:
		return encodeGroup(node, s.Steps, s.ForEach)

	case wf.Local:
		// Fn is deliberately absent: a Go closure has no serialized form, and
		// the step is identified by its id when the assignment comes back.

	default:
		return fmt.Errorf("%w: %T", ErrUnknownStepKind, step)
	}
	return nil
}

func encodeGroup(node map[string]any, steps []wf.Step, each *wf.ForEach) error {
	children := make([]any, 0, len(steps))
	for _, child := range steps {
		encoded, err := canonicalStep(child)
		if err != nil {
			return err
		}
		children = append(children, encoded)
	}
	node["steps"] = children
	if each != nil {
		node["forEach"] = map[string]any{"from": string(each.From), "as": each.As}
	}
	return nil
}

func canonicalCompensation(c *wf.Compensation) (map[string]any, error) {
	node := map[string]any{}
	if c.Kind != "" {
		node["type"] = string(c.Kind)
	}
	if c.Service != nil {
		node["service"] = canonicalTarget(c.Service)
	}
	if c.Method != nil {
		node["method"] = canonicalTarget(c.Method)
	}
	if c.Event != nil {
		node["event"] = canonicalTarget(c.Event)
	}
	if err := putValue(node, "input", c.Input); err != nil {
		return nil, err
	}
	if err := putValue(node, "idempotencyKey", c.IdempotencyKey); err != nil {
		return nil, err
	}
	if c.Retry != nil {
		node["retry"] = canonicalRetry(c.Retry)
	}
	return node, nil
}

func canonicalCallOpts(o *wf.CallOpts) (map[string]any, error) {
	node := map[string]any{}
	if o.Timeout > 0 {
		node["timeoutMs"] = o.Timeout.Milliseconds()
	}
	if o.Transport != "" {
		node["transport"] = string(o.Transport)
	}
	if err := putValue(node, "idempotencyKey", o.IdempotencyKey); err != nil {
		return nil, err
	}
	if err := putValue(node, "requestId", o.RequestID); err != nil {
		return nil, err
	}
	if o.Retry != nil {
		node["retry"] = canonicalRetry(o.Retry)
	}
	return node, nil
}

func canonicalPublishOpts(o *wf.PublishOpts) (map[string]any, error) {
	node := map[string]any{}
	if err := putValue(node, "idempotencyKey", o.IdempotencyKey); err != nil {
		return nil, err
	}
	if err := putValue(node, "partitionKey", o.PartitionKey); err != nil {
		return nil, err
	}
	if o.FireAndForget {
		node["fireAndForget"] = true
	}
	if len(o.Headers) > 0 {
		headers, err := canonicalValue(o.Headers)
		if err != nil {
			return nil, err
		}
		node["headers"] = headers
	}
	if o.OccurredAtMs > 0 {
		node["occurredAtMs"] = o.OccurredAtMs
	}
	return node, nil
}

func canonicalStartOpts(o *wf.StartOpts) (map[string]any, error) {
	node := map[string]any{}
	if err := putValue(node, "idempotencyKey", o.IdempotencyKey); err != nil {
		return nil, err
	}
	if o.TimeoutSec > 0 {
		node["timeoutSec"] = o.TimeoutSec
	}
	return node, nil
}

func canonicalRetry(r *wf.RetryPolicy) map[string]any {
	node := map[string]any{}
	if r.MaxAttempts > 0 {
		node["maxAttempts"] = r.MaxAttempts
	}
	if r.BaseDelay > 0 {
		node["baseDelayMs"] = r.BaseDelay.Milliseconds()
	}
	if r.Factor > 0 {
		node["factor"] = r.Factor
	}
	if r.MaxDelay > 0 {
		node["maxDelayMs"] = r.MaxDelay.Milliseconds()
	}
	if r.Jitter > 0 {
		node["jitter"] = r.Jitter
	}
	return node
}

// canonicalTarget renders what a step acts on. A Path travels as the bare
// expression the runtime recognises as dynamic; a Name that would read as one
// is wrapped, so a literal is never taken for a path on the way back.
func canonicalTarget(t wf.Target) any {
	switch target := t.(type) {
	case wf.Path:
		return string(target)
	case wf.Name:
		return escapeLiteral(string(target))
	default:
		// Unreachable: the target set is closed and validation rejects an absent
		// one before Freeze reaches the encoder.
		panic(fmt.Sprintf("workflow: canonical target: unknown target %T", t))
	}
}

func putValue(node map[string]any, key string, v any) error {
	if v == nil {
		return nil
	}
	encoded, err := canonicalValue(v)
	if err != nil {
		return err
	}
	node[key] = encoded
	return nil
}

// canonicalValue renders a declared value tree: Path becomes the expression it
// is, and a plain string that would read as an expression is wrapped so it
// stays a literal.
func canonicalValue(v any) (any, error) {
	return walkValue(v,
		func(p wf.Path) (any, error) { return string(p), nil },
		func(s string) (any, error) { return escapeLiteral(s), nil },
	)
}

func escapeLiteral(s string) any {
	if s == "$" || strings.HasPrefix(s, "$.") {
		return map[string]any{"literal": s}
	}
	return s
}
