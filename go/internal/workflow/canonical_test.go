package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	wf "github.com/service-bridge/sdk/go/workflow"
)

// The shapes below are copied from runtime/internal/workflow/register.go. They
// are the only fields the runtime reads out of a registered graph, so decoding
// the canonical bytes through them is the check that the two halves of the
// contract still agree.

type runtimeCanonicalGraph struct {
	Graph          []json.RawMessage `json:"graph"`
	Retry          json.RawMessage   `json:"retry,omitempty"`
	MaxParallelism int               `json:"maxParallelism,omitempty"`
	TimeoutSec     int               `json:"timeoutSec,omitempty"`
	InputSchema    json.RawMessage   `json:"inputSchema,omitempty"`
}

type runtimeGraphStep struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	WaitFor  []string          `json:"waitFor,omitempty"`
	Workflow string            `json:"workflow,omitempty"`
	Steps    []json.RawMessage `json:"steps,omitempty"`
}

type runtimeSeedStep struct {
	ID    string            `json:"id"`
	Type  string            `json:"type"`
	Steps []json.RawMessage `json:"steps,omitempty"`
	Retry *struct {
		MaxAttempts int `json:"maxAttempts,omitempty"`
	} `json:"retry,omitempty"`
	ForEach *struct {
		From string `json:"from,omitempty"`
	} `json:"forEach,omitempty"`
}

type runtimePolicyStep struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Service  string `json:"service"`
	Method   string `json:"method"`
	Event    string `json:"event"`
	Workflow string `json:"workflow"`
}

// fullGraph declares one step of every kind so a single freeze exercises the
// whole encoder.
func fullGraph() wf.Definition {
	return wf.Definition{
		Input: map[string]any{
			"type":       "object",
			"properties": map[string]any{"orderId": map[string]any{"type": "string"}},
		},
		MaxParallelism: 4,
		TimeoutSec:     600,
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{
					ID:         "charge",
					TimeoutSec: 30,
					Compensate: &wf.Compensation{
						Kind:    wf.CompensateCall,
						Service: wf.Name("billing"),
						Method:  wf.Name("Refund"),
						Input:   map[string]any{"txId": wf.Path("$.charge.txId")},
					},
				},
				Service: wf.Name("billing"),
				Method:  wf.Name("Charge"),
				Input:   map[string]any{"orderId": wf.Path("$.input.orderId"), "currency": "EUR"},
				Opts: &wf.CallOpts{
					Timeout:        5 * time.Second,
					Transport:      wf.TransportDirect,
					IdempotencyKey: wf.Path("$.input.orderId"),
				},
			},
			wf.Publish{
				Control: wf.Control{ID: "announce", WaitFor: []string{"charge"}},
				Event:   wf.Name("order.charged"),
				Input:   map[string]any{"orderId": wf.Path("$.input.orderId")},
				Opts:    &wf.PublishOpts{PartitionKey: wf.Path("$.input.orderId"), FireAndForget: true},
			},
			wf.Sleep{
				Control:     wf.Control{ID: "cool_off", WaitFor: []string{"announce"}},
				DurationSec: 60,
			},
			wf.WaitEvent{
				Control: wf.Control{ID: "await_ship", WaitFor: []string{"cool_off"}},
				Event:   wf.Name("order.shipped"),
				Filter:  map[string]any{"orderId": wf.Path("$.input.orderId")},
			},
			wf.WaitSignal{
				Control: wf.Control{ID: "await_ack"},
				Signal:  "human_ack",
			},
			wf.SubWorkflow{
				Control:  wf.Control{ID: "invoice", When: wf.Truthy(wf.Path("$.charge.ok"))},
				Workflow: wf.Name("billing.invoice"),
				Input:    map[string]any{"txId": wf.Path("$.charge.txId")},
				Opts:     &wf.StartOpts{TimeoutSec: 120},
			},
			wf.Parallel{
				Control: wf.Control{ID: "notify_all"},
				ForEach: &wf.ForEach{From: wf.Path("$.input.recipients"), As: "recipient"},
				Steps: []wf.Step{
					wf.Call{
						Control: wf.Control{ID: "notify_one"},
						Service: wf.Name("mailer"),
						Method:  wf.Name("Send"),
						Input:   map[string]any{"to": wf.Path("$.recipient.email")},
					},
				},
			},
			wf.Sequence{
				Control: wf.Control{ID: "wrap_up"},
				Steps: []wf.Step{
					wf.Local{
						Control: wf.Control{ID: "summarize"},
						Fn: func(_ context.Context, _ map[string]any) (any, error) {
							return nil, nil
						},
					},
				},
			},
		},
	}
}

func TestFreezeProducesTheFieldsTheRuntimeReads(t *testing.T) {
	frozen, err := Freeze("order.fulfil", fullGraph())
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}

	var graph runtimeCanonicalGraph
	if err := json.Unmarshal(frozen.JSON, &graph); err != nil {
		t.Fatalf("runtime decode: %v", err)
	}
	if graph.MaxParallelism != 4 {
		t.Errorf("maxParallelism = %d, want 4", graph.MaxParallelism)
	}
	if graph.TimeoutSec != 600 {
		t.Errorf("timeoutSec = %d, want 600", graph.TimeoutSec)
	}
	if len(graph.InputSchema) == 0 {
		t.Error("inputSchema is absent; the runtime stores it as the run input schema")
	}
	if len(graph.Graph) != 8 {
		t.Fatalf("top-level steps = %d, want 8", len(graph.Graph))
	}

	wantKinds := []string{"call", "publish", "sleep", "wait_event", "wait_signal", "workflow", "parallel", "sequence"}
	for i, raw := range graph.Graph {
		var step runtimeGraphStep
		if err := json.Unmarshal(raw, &step); err != nil {
			t.Fatalf("step %d decode: %v", i, err)
		}
		if step.Type != wantKinds[i] {
			t.Errorf("step %d type = %q, want %q", i, step.Type, wantKinds[i])
		}
		if step.ID == "" {
			t.Errorf("step %d has no id; the runtime keys workflow_steps by it", i)
		}
	}

	// The runtime rejects a graph whose sub-workflow names the workflow itself,
	// so the field it compares has to arrive as a plain string.
	var sub runtimeGraphStep
	if err := json.Unmarshal(graph.Graph[5], &sub); err != nil {
		t.Fatalf("sub-workflow decode: %v", err)
	}
	if sub.Workflow != "billing.invoice" {
		t.Errorf("workflow = %q, want %q", sub.Workflow, "billing.invoice")
	}

	// Groups carry their children where the seed walker looks for them.
	var group runtimeGraphStep
	if err := json.Unmarshal(graph.Graph[6], &group); err != nil {
		t.Fatalf("group decode: %v", err)
	}
	if len(group.Steps) != 1 {
		t.Fatalf("group children = %d, want 1", len(group.Steps))
	}
}

func TestFreezeFeedsThePolicyWalker(t *testing.T) {
	frozen, err := Freeze("order.fulfil", fullGraph())
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	var graph runtimeCanonicalGraph
	if err := json.Unmarshal(frozen.JSON, &graph); err != nil {
		t.Fatalf("runtime decode: %v", err)
	}

	claims := map[string]runtimePolicyStep{}
	var walk func(raw []json.RawMessage)
	walk = func(raw []json.RawMessage) {
		for _, item := range raw {
			var step runtimePolicyStep
			if err := json.Unmarshal(item, &step); err != nil {
				t.Fatalf("policy decode: %v", err)
			}
			claims[step.ID] = step
			var group runtimeGraphStep
			if err := json.Unmarshal(item, &group); err != nil {
				t.Fatalf("group decode: %v", err)
			}
			walk(group.Steps)
		}
	}
	walk(graph.Graph)

	if got := claims["charge"]; got.Service != "billing" || got.Method != "Charge" {
		t.Errorf("call claim = %+v, want service=billing method=Charge", got)
	}
	if got := claims["announce"]; got.Event != "order.charged" {
		t.Errorf("publish claim event = %q, want order.charged", got.Event)
	}
	if got := claims["await_ship"]; got.Event != "order.shipped" {
		t.Errorf("wait_event claim event = %q, want order.shipped", got.Event)
	}
	if got := claims["invoice"]; got.Workflow != "billing.invoice" {
		t.Errorf("workflow claim = %q, want billing.invoice", got.Workflow)
	}
	if got := claims["notify_one"]; got.Service != "mailer" {
		t.Errorf("nested call claim service = %q, want mailer", got.Service)
	}
}

func TestFreezeFeedsTheSeedWalker(t *testing.T) {
	def := fullGraph()
	def.Retry = &wf.RetryPolicy{MaxAttempts: 7, BaseDelay: 250 * time.Millisecond, Factor: 2, Jitter: 0.1}

	frozen, err := Freeze("order.fulfil", def)
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	var graph runtimeCanonicalGraph
	if err := json.Unmarshal(frozen.JSON, &graph); err != nil {
		t.Fatalf("runtime decode: %v", err)
	}

	seeds := map[string]int{}
	var walk func(raw []json.RawMessage)
	walk = func(raw []json.RawMessage) {
		for _, item := range raw {
			var step runtimeSeedStep
			if err := json.Unmarshal(item, &step); err != nil {
				t.Fatalf("seed decode: %v", err)
			}
			// The runtime's default when a step declares no retry block.
			attempts := 3
			if step.Retry != nil && step.Retry.MaxAttempts > 0 {
				attempts = step.Retry.MaxAttempts
			}
			seeds[step.ID] = attempts
			if step.Type == "parallel" || step.Type == "sequence" {
				walk(step.Steps)
			}
		}
	}
	walk(graph.Graph)

	// A policy declared once at the top of the graph has to arrive on the steps
	// themselves: the seed walker reads the budget off the step and nowhere else.
	forOperations := []string{"charge", "announce", "invoice", "notify_one", "summarize"}
	for _, id := range forOperations {
		if seeds[id] != 7 {
			t.Errorf("step %q maxAttempts = %d, want 7 from the workflow policy", id, seeds[id])
		}
	}

	// A group is not an operation and a parked step is resumed rather than
	// re-executed, so neither takes the policy.
	forSkipped := []string{"notify_all", "wrap_up", "cool_off", "await_ship", "await_ack"}
	for _, id := range forSkipped {
		if seeds[id] != 3 {
			t.Errorf("step %q maxAttempts = %d, want the runtime default 3", id, seeds[id])
		}
	}

	if graph.Retry == nil {
		t.Error("the graph-level policy is missing; the runtime keeps it for the run")
	}
}

func TestFreezeMaterializesRetryOnTheStepsTheRunnerExecutes(t *testing.T) {
	policy := &wf.RetryPolicy{MaxAttempts: 5}
	own := &wf.RetryPolicy{MaxAttempts: 2}

	def := wf.Definition{
		Retry: policy,
		Steps: []wf.Step{
			wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("s"), Method: wf.Name("M")},
			wf.Call{Control: wf.Control{ID: "b", Retry: own}, Service: wf.Name("s"), Method: wf.Name("M")},
			wf.Sleep{Control: wf.Control{ID: "c"}, DurationSec: 1},
			wf.Sequence{
				Control: wf.Control{ID: "g"},
				Steps: []wf.Step{
					wf.Publish{Control: wf.Control{ID: "d"}, Event: wf.Name("e")},
				},
			},
		},
	}

	frozen, err := Freeze("wf", def)
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}

	byID := map[string]wf.Step{}
	var collect func(steps []wf.Step)
	collect = func(steps []wf.Step) {
		for _, s := range steps {
			byID[s.Common().ID] = s
			switch group := s.(type) {
			case wf.Parallel:
				collect(group.Steps)
			case wf.Sequence:
				collect(group.Steps)
			}
		}
	}
	collect(frozen.Steps)

	if got := byID["a"].Common().Retry; got != policy {
		t.Errorf("step a retry = %v, want the workflow policy", got)
	}
	if got := byID["b"].Common().Retry; got != own {
		t.Errorf("step b retry = %v, want its own policy left alone", got)
	}
	if got := byID["c"].Common().Retry; got != nil {
		t.Errorf("step c retry = %v, want none on a parked step", got)
	}
	if got := byID["g"].Common().Retry; got != nil {
		t.Errorf("group g retry = %v, want none on a group", got)
	}
	if got := byID["d"].Common().Retry; got != policy {
		t.Errorf("nested step d retry = %v, want the workflow policy", got)
	}

	// Materialisation must not write through to what the caller declared.
	declared := def.Steps[0].(wf.Call)
	if declared.Retry != nil {
		t.Error("the declared graph was mutated; freezing has to leave it alone")
	}
}

func TestFingerprintIsStableAcrossRuns(t *testing.T) {
	first, err := Freeze("order.fulfil", fullGraph())
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	second, err := Freeze("order.fulfil", fullGraph())
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if first.Fingerprint != second.Fingerprint {
		t.Errorf("fingerprints differ across runs:\n%s\n%s", first.Fingerprint, second.Fingerprint)
	}
	if string(first.JSON) != string(second.JSON) {
		t.Error("canonical bytes differ across runs")
	}
	if len(first.Fingerprint) != 64 {
		t.Errorf("fingerprint length = %d, want 64 hex characters", len(first.Fingerprint))
	}
}

func TestFingerprintIgnoresOrderThatCarriesNoMeaning(t *testing.T) {
	build := func(deps []string, input map[string]any) wf.Definition {
		return wf.Definition{Steps: []wf.Step{
			wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("s"), Method: wf.Name("M")},
			wf.Call{Control: wf.Control{ID: "b"}, Service: wf.Name("s"), Method: wf.Name("M")},
			wf.Call{
				Control: wf.Control{ID: "c", WaitFor: deps},
				Service: wf.Name("s"),
				Method:  wf.Name("M"),
				Input:   input,
			},
		}}
	}

	forward, err := Freeze("wf", build([]string{"a", "b"}, map[string]any{"x": 1, "y": 2}))
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	reversed, err := Freeze("wf", build([]string{"b", "a"}, map[string]any{"y": 2, "x": 1}))
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if forward.Fingerprint != reversed.Fingerprint {
		t.Error("reordering a dependency set or a value map changed the fingerprint")
	}
}

func TestFingerprintFollowsMeaning(t *testing.T) {
	base := wf.Definition{Steps: []wf.Step{
		wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("billing"), Method: wf.Name("Charge")},
	}}
	baseline, err := Freeze("wf", base)
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}

	changes := map[string]wf.Definition{
		"a different method": {Steps: []wf.Step{
			wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("billing"), Method: wf.Name("Refund")},
		}},
		"a different step id": {Steps: []wf.Step{
			wf.Call{Control: wf.Control{ID: "b"}, Service: wf.Name("billing"), Method: wf.Name("Charge")},
		}},
		"an added input": {Steps: []wf.Step{
			wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("billing"), Method: wf.Name("Charge"), Input: map[string]any{"x": 1}},
		}},
		"a step order swap": {Steps: []wf.Step{
			wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("billing"), Method: wf.Name("Charge")},
			wf.Sleep{Control: wf.Control{ID: "b"}, DurationSec: 1},
		}},
		"a graph timeout": {
			TimeoutSec: 10,
			Steps: []wf.Step{
				wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("billing"), Method: wf.Name("Charge")},
			},
		},
	}

	for name, def := range changes {
		frozen, err := Freeze("wf", def)
		if err != nil {
			t.Fatalf("freeze %s: %v", name, err)
		}
		if frozen.Fingerprint == baseline.Fingerprint {
			t.Errorf("%s left the fingerprint unchanged", name)
		}
	}
}

func TestFreezeLeavesTheLocalFunctionOut(t *testing.T) {
	called := false
	def := wf.Definition{Steps: []wf.Step{
		wf.Local{
			Control: wf.Control{ID: "compute"},
			Fn: func(_ context.Context, _ map[string]any) (any, error) {
				called = true
				return nil, nil
			},
		},
	}}

	frozen, err := Freeze("wf", def)
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if called {
		t.Error("freezing invoked the local function")
	}

	encoded := string(frozen.JSON)
	for _, needle := range []string{`"fn"`, `"Fn"`, "func("} {
		if strings.Contains(encoded, needle) {
			t.Errorf("canonical JSON carries %s: %s", needle, encoded)
		}
	}

	var step map[string]any
	var graph runtimeCanonicalGraph
	if err := json.Unmarshal(frozen.JSON, &graph); err != nil {
		t.Fatalf("runtime decode: %v", err)
	}
	if err := json.Unmarshal(graph.Graph[0], &step); err != nil {
		t.Fatalf("step decode: %v", err)
	}
	if len(step) != 2 || step["id"] != "compute" || step["type"] != "local" {
		t.Errorf("local step = %v, want only id and type", step)
	}

	// Two graphs differing only in the closure describe the same work.
	other := wf.Definition{Steps: []wf.Step{
		wf.Local{
			Control: wf.Control{ID: "compute"},
			Fn:      func(_ context.Context, _ map[string]any) (any, error) { return 42, nil },
		},
	}}
	twin, err := Freeze("wf", other)
	if err != nil {
		t.Fatalf("freeze twin: %v", err)
	}
	if twin.Fingerprint != frozen.Fingerprint {
		t.Error("swapping the closure moved the fingerprint")
	}
}

func TestCanonicalKeysAreSortedAndCompact(t *testing.T) {
	blob, err := encodeCanonical(map[string]any{
		"zulu":  1,
		"alpha": map[string]any{"y": 1, "x": 2},
		"mike":  []any{3, 1, 2},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := `{"alpha":{"x":2,"y":1},"mike":[3,1,2],"zulu":1}`
	if string(blob) != want {
		t.Errorf("canonical = %s, want %s", blob, want)
	}
}

func TestCanonicalOmitsZeroValuedFields(t *testing.T) {
	frozen, err := Freeze("wf", wf.Definition{Steps: []wf.Step{
		wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("s"), Method: wf.Name("M")},
	}})
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	want := `{"graph":[{"id":"a","method":"M","service":"s","type":"call"}]}`
	if string(frozen.JSON) != want {
		t.Errorf("canonical = %s, want %s", frozen.JSON, want)
	}
}

func TestCanonicalKeepsPathsAndEscapesLiteralsThatLookLikeThem(t *testing.T) {
	frozen, err := Freeze("wf", wf.Definition{Steps: []wf.Step{
		wf.Call{
			Control: wf.Control{ID: "a"},
			Service: wf.Name("s"),
			Method:  wf.Name("M"),
			Input: map[string]any{
				"path":    wf.Path("$.input.id"),
				"literal": "$.input.id",
				"plain":   "hello",
			},
		},
	}})
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	encoded := string(frozen.JSON)
	if !strings.Contains(encoded, `"path":"$.input.id"`) {
		t.Errorf("a Path did not travel as an expression: %s", encoded)
	}
	if !strings.Contains(encoded, `"literal":{"literal":"$.input.id"}`) {
		t.Errorf("a string literal was not escaped: %s", encoded)
	}
	if !strings.Contains(encoded, `"plain":"hello"`) {
		t.Errorf("an ordinary string was rewritten: %s", encoded)
	}
}

func TestFreezeRefusesAnInvalidGraph(t *testing.T) {
	_, err := Freeze("wf", wf.Definition{Steps: []wf.Step{
		wf.Call{Control: wf.Control{ID: "Bad ID"}, Service: wf.Name("s"), Method: wf.Name("M")},
	}})
	var invalid *ValidationError
	if err == nil || !errors.As(err, &invalid) {
		t.Fatalf("freeze error = %v, want a ValidationError", err)
	}
}
