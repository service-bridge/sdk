package workflow_test

import (
	"reflect"
	"testing"

	"github.com/service-bridge/sdk/go/workflow"
)

// TestPredicateNodes pins the wire shape of every predicate. The runner reads
// these nodes and the fingerprint covers them, so the keys are contract.
func TestPredicateNodes(t *testing.T) {
	cases := []struct {
		name string
		pred workflow.Predicate
		want any
	}{
		{
			name: "truthy is the bare expression",
			pred: workflow.Truthy("$.charge.ok"),
			want: workflow.Path("$.charge.ok"),
		},
		{
			name: "negation",
			pred: workflow.Not(workflow.Truthy("$.suspended")),
			want: map[string]any{"not": workflow.Path("$.suspended")},
		},
		{
			name: "equality keeps operand order",
			pred: workflow.Equals(workflow.Path("$.country"), "RU"),
			want: map[string]any{"equals": []any{workflow.Path("$.country"), "RU"}},
		},
		{
			name: "membership is value then list",
			pred: workflow.In(workflow.Path("$.country"), []any{"RU", "DE"}),
			want: map[string]any{"in": []any{workflow.Path("$.country"), []any{"RU", "DE"}}},
		},
		{
			name: "conjunction",
			pred: workflow.And(workflow.Truthy("$.a"), workflow.Truthy("$.b")),
			want: map[string]any{"and": []any{workflow.Path("$.a"), workflow.Path("$.b")}},
		},
		{
			name: "disjunction",
			pred: workflow.Or(workflow.Truthy("$.a"), workflow.Truthy("$.b")),
			want: map[string]any{"or": []any{workflow.Path("$.a"), workflow.Path("$.b")}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.pred.Node(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("node = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestPredicatesNest(t *testing.T) {
	pred := workflow.And(
		workflow.Truthy("$.charge.ok"),
		workflow.Not(workflow.In(workflow.Path("$.country"), []any{"FR"})),
	)
	want := map[string]any{"and": []any{
		workflow.Path("$.charge.ok"),
		map[string]any{"not": map[string]any{"in": []any{workflow.Path("$.country"), []any{"FR"}}}},
	}}
	if got := pred.Node(); !reflect.DeepEqual(got, want) {
		t.Errorf("node = %#v, want %#v", got, want)
	}
}

// TestAMissingOperandStaysVisible keeps a malformed predicate reportable
// instead of panicking where it is built.
func TestAMissingOperandStaysVisible(t *testing.T) {
	if got := workflow.Not(nil).Node(); !reflect.DeepEqual(got, map[string]any{"not": nil}) {
		t.Errorf("node = %#v, want a nil operand", got)
	}
	if got := workflow.And(nil).Node(); !reflect.DeepEqual(got, map[string]any{"and": []any{nil}}) {
		t.Errorf("node = %#v, want a nil operand", got)
	}
	if got := workflow.And().Node(); !reflect.DeepEqual(got, map[string]any{"and": []any{}}) {
		t.Errorf("node = %#v, want an empty operand list", got)
	}
}
