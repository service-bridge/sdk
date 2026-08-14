package workflow_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/service-bridge/sdk/go/workflow"
)

// TestKindsMatchTheRuntimeDiscriminators pins the strings the runtime switches
// on when it walks a registered graph (runtime/internal/workflow/register.go).
func TestKindsMatchTheRuntimeDiscriminators(t *testing.T) {
	cases := []struct {
		step wf
		want string
	}{
		{workflow.Call{}, "call"},
		{workflow.Publish{}, "publish"},
		{workflow.Sleep{}, "sleep"},
		{workflow.WaitEvent{}, "wait_event"},
		{workflow.WaitSignal{}, "wait_signal"},
		{workflow.SubWorkflow{}, "workflow"},
		{workflow.Parallel{}, "parallel"},
		{workflow.Sequence{}, "sequence"},
		{workflow.Local{}, "local"},
	}
	for _, tc := range cases {
		if got := tc.step.Kind(); got != tc.want {
			t.Errorf("%T kind = %q, want %q", tc.step, got, tc.want)
		}
	}
	if len(cases) != 9 {
		t.Errorf("the graph has %d kinds; the runtime knows 9", len(cases))
	}
}

// wf is the local alias for the Step interface, kept short so the table above
// reads as a list of kinds.
type wf = workflow.Step

func TestEveryKindCarriesTheCommonFields(t *testing.T) {
	control := workflow.Control{
		ID:         "charge",
		WaitFor:    []string{"validate"},
		TimeoutSec: 30,
	}
	steps := []workflow.Step{
		workflow.Call{Control: control},
		workflow.Publish{Control: control},
		workflow.Sleep{Control: control},
		workflow.WaitEvent{Control: control},
		workflow.WaitSignal{Control: control},
		workflow.SubWorkflow{Control: control},
		workflow.Parallel{Control: control},
		workflow.Sequence{Control: control},
		workflow.Local{Control: control},
	}
	for _, step := range steps {
		got := step.Common()
		if got.ID != "charge" || got.TimeoutSec != 30 || len(got.WaitFor) != 1 {
			t.Errorf("%T common = %+v, want the declared control fields", step, got)
		}
	}
}

func TestForEachBelongsToAGroupRatherThanBeingOne(t *testing.T) {
	// The fan-out is a property of parallel and sequence: the runtime looks for
	// forEach on the group and knows no step kind of that name.
	group := workflow.Parallel{
		Control: workflow.Control{ID: "notify"},
		ForEach: &workflow.ForEach{From: "$.input.recipients", As: "recipient"},
		Steps:   []workflow.Step{workflow.Sleep{Control: workflow.Control{ID: "wait"}}},
	}
	if group.ForEach.As != "recipient" {
		t.Errorf("alias = %q, want recipient", group.ForEach.As)
	}
	if group.Kind() != workflow.KindParallel {
		t.Errorf("kind = %q, want parallel", group.Kind())
	}

	sequence := workflow.Sequence{ForEach: &workflow.ForEach{From: "$.x", As: "item"}}
	if sequence.ForEach == nil {
		t.Error("a sequence has to accept a fan-out too")
	}
}

func TestLocalHoldsTheFunctionOnTheStep(t *testing.T) {
	step := workflow.Local{
		Control: workflow.Control{ID: "compute"},
		Fn: func(_ context.Context, state map[string]any) (any, error) {
			return state["x"], nil
		},
	}
	got, err := step.Fn(context.Background(), map[string]any{"x": 42})
	if err != nil || got != 42 {
		t.Errorf("fn = %v, %v", got, err)
	}
}

// TestPathIsNotAString proves the two are told apart by type, which is what
// removes the escape hack the string-only SDKs need.
func TestPathIsNotAString(t *testing.T) {
	var target workflow.Target = workflow.Path("$.input.service")
	switch target.(type) {
	case workflow.Path:
	default:
		t.Fatalf("a Path resolved to %T", target)
	}

	target = workflow.Name("$.input.service")
	switch target.(type) {
	case workflow.Name:
	case workflow.Path:
		t.Fatal("a literal name was taken for a path")
	default:
		t.Fatalf("a Name resolved to %T", target)
	}

	if reflect.TypeOf(workflow.Path("")) == reflect.TypeOf(workflow.Name("")) {
		t.Error("Path and Name are the same type")
	}
}
