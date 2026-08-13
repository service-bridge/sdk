package workflow

import (
	"errors"
	"math"
	"reflect"
	"testing"

	wf "github.com/service-bridge/sdk/go/workflow"
)

func evalState() map[string]any {
	return map[string]any{
		"input": map[string]any{"userId": "u-1", "amount": float64(100)},
		"list": map[string]any{
			"items": []any{
				map[string]any{"id": "a", "qty": float64(1)},
				map[string]any{"id": "b", "qty": float64(2)},
				map[string]any{"id": "c", "qty": float64(3)},
			},
		},
		"charge":    map[string]any{"ok": true, "txId": "tx-7"},
		"suspended": false,
		"country":   "RU",
		"empty":     "",
	}
}

func TestEvalPathReadsFields(t *testing.T) {
	got, err := EvalPath("$.input.userId", evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != "u-1" {
		t.Errorf("got %v, want u-1", got)
	}
}

func TestEvalPathReadsAnIndex(t *testing.T) {
	got, err := EvalPath("$.list.items[1].id", evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != "b" {
		t.Errorf("got %v, want b", got)
	}

	whole, err := EvalPath("$.list.items[0]", evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !reflect.DeepEqual(whole, map[string]any{"id": "a", "qty": float64(1)}) {
		t.Errorf("got %v, want the first element", whole)
	}
}

func TestEvalPathWildcardKeepsTheWholeArray(t *testing.T) {
	got, err := EvalPath("$.list.items[*]", evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	items, ok := got.([]any)
	if !ok || len(items) != 3 {
		t.Fatalf("got %v, want three elements", got)
	}
}

func TestEvalPathWildcardWithAFieldCollectsIt(t *testing.T) {
	got, err := EvalPath("$.list.items[*].id", evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !reflect.DeepEqual(got, []any{"a", "b", "c"}) {
		t.Errorf("got %v, want [a b c]", got)
	}
}

func TestEvalPathOnAMissingValueYieldsNothing(t *testing.T) {
	cases := []wf.Path{
		"$.input.nope",
		"$.nothing.here.at.all",
		"$.list.items[9]",
		"$.charge.txId.deeper",
	}
	for _, expr := range cases {
		got, err := EvalPath(expr, evalState())
		if err != nil {
			t.Errorf("%s: error %v, want nothing", expr, err)
		}
		if got != nil {
			t.Errorf("%s = %v, want nil", expr, got)
		}
	}
}

func TestEvalPathOverASkippedStepYieldsAnEmptyList(t *testing.T) {
	// A step gated off by its condition leaves no output; fanning out over it
	// has to produce an empty list rather than fail the run.
	got, err := EvalPath("$.skipped.items[*].id", evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	items, ok := got.([]any)
	if !ok || len(items) != 0 {
		t.Errorf("got %v, want an empty list", got)
	}
}

func TestEvalPathRejectsMalformedExpressions(t *testing.T) {
	cases := []wf.Path{
		"foo.bar",
		"$.foo[abc]",
		"$.foo[",
		"$.foo[-1]",
		"$.",
		"$foo",
		"$.foo..bar",
	}
	for _, expr := range cases {
		if _, err := EvalPath(expr, evalState()); !errors.Is(err, ErrPathSyntax) {
			t.Errorf("%s: error %v, want ErrPathSyntax", expr, err)
		}
	}
}

func TestEvalPathRejectsAWildcardOverANonArray(t *testing.T) {
	if _, err := EvalPath("$.input[*]", evalState()); !errors.Is(err, ErrPathType) {
		t.Errorf("error %v, want ErrPathType", err)
	}
}

func TestValidatePathSeparatesSyntaxFromLookup(t *testing.T) {
	if err := ValidatePath("$.a.b[0].c[*].d"); err != nil {
		t.Errorf("valid expression rejected: %v", err)
	}
	if err := ValidatePath("$.a["); err == nil {
		t.Error("malformed expression accepted")
	}
}

func TestEvalValueResolvesOnlyPaths(t *testing.T) {
	got, err := EvalValue(map[string]any{
		"user":    wf.Path("$.input.userId"),
		"literal": "$.input.userId",
		"plain":   "text",
		"nested":  []any{wf.Path("$.charge.txId"), float64(1)},
		"missing": wf.Path("$.nope"),
	}, evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	want := map[string]any{
		"user":    "u-1",
		"literal": "$.input.userId",
		"plain":   "text",
		"nested":  []any{"tx-7", float64(1)},
		"missing": nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestEvalTarget(t *testing.T) {
	name, err := EvalTarget(wf.Name("billing"), evalState())
	if err != nil || name != "billing" {
		t.Errorf("literal target = %q, %v", name, err)
	}

	resolved, err := EvalTarget(wf.Path("$.charge.txId"), evalState())
	if err != nil || resolved != "tx-7" {
		t.Errorf("path target = %q, %v", resolved, err)
	}

	if _, err := EvalTarget(wf.Path("$.charge.ok"), evalState()); !errors.Is(err, ErrTargetShape) {
		t.Errorf("error %v, want ErrTargetShape when a target is not a string", err)
	}
	if _, err := EvalTarget(nil, evalState()); !errors.Is(err, ErrTargetShape) {
		t.Errorf("error %v, want ErrTargetShape for an absent target", err)
	}
}

func TestEvalPredicate(t *testing.T) {
	state := evalState()
	cases := []struct {
		name string
		pred wf.Predicate
		want bool
	}{
		{"truthy on a set flag", wf.Truthy("$.charge.ok"), true},
		{"truthy on a false flag", wf.Truthy("$.suspended"), false},
		{"truthy on a missing value", wf.Truthy("$.nope"), false},
		{"truthy on an empty string", wf.Truthy("$.empty"), false},
		{"truthy on a non-empty string", wf.Truthy("$.country"), true},
		{"negation", wf.Not(wf.Truthy("$.suspended")), true},
		{"double negation", wf.Not(wf.Not(wf.Truthy("$.charge.ok"))), true},
		{"equality on strings", wf.Equals(wf.Path("$.country"), "RU"), true},
		{"inequality on strings", wf.Equals(wf.Path("$.country"), "DE"), false},
		{"equality across number types", wf.Equals(wf.Path("$.input.amount"), 100), true},
		{"equality on maps", wf.Equals(wf.Path("$.charge"), map[string]any{"ok": true, "txId": "tx-7"}), true},
		{"membership", wf.In(wf.Path("$.country"), []any{"RU", "DE"}), true},
		{"absent membership", wf.In(wf.Path("$.country"), []any{"FR"}), false},
		{"membership over a resolved list", wf.In("a", wf.Path("$.list.items[*].id")), true},
		{"membership over a non-list", wf.In("a", "not a list"), false},
		{"conjunction holds", wf.And(wf.Truthy("$.charge.ok"), wf.Equals(wf.Path("$.country"), "RU")), true},
		{"conjunction breaks", wf.And(wf.Truthy("$.charge.ok"), wf.Truthy("$.suspended")), false},
		{"disjunction holds", wf.Or(wf.Truthy("$.suspended"), wf.Truthy("$.charge.ok")), true},
		{"disjunction breaks", wf.Or(wf.Truthy("$.suspended"), wf.Truthy("$.nope")), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EvalPredicate(tc.pred, state)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvalPredicateWithoutACondition(t *testing.T) {
	got, err := EvalPredicate(nil, evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !got {
		t.Error("a step without a condition has to run")
	}
}

func TestEvalPredicateRejectsAMalformedShape(t *testing.T) {
	if _, err := EvalPredicate(wf.Not(nil), evalState()); !errors.Is(err, ErrPredicateShape) {
		t.Errorf("error %v, want ErrPredicateShape", err)
	}
	if _, err := EvalPredicate(wf.And(), evalState()); !errors.Is(err, ErrPredicateShape) {
		t.Errorf("error %v, want ErrPredicateShape for an empty conjunction", err)
	}
}

func TestEvalPredicateReportsABrokenPath(t *testing.T) {
	if _, err := EvalPredicate(wf.Truthy("$.a["), evalState()); !errors.Is(err, ErrPathSyntax) {
		t.Errorf("error %v, want ErrPathSyntax", err)
	}
}

func TestEvalReadsTypedContainers(t *testing.T) {
	// Run state is decoded JSON, but a local step may put Go values back into
	// it; reading them must work the same way.
	state := map[string]any{
		"tags":   []string{"a", "b"},
		"labels": map[string]string{"env": "prod"},
	}
	got, err := EvalPath("$.tags[1]", state)
	if err != nil || got != "b" {
		t.Errorf("typed slice = %v, %v", got, err)
	}
	got, err = EvalPath("$.labels.env", state)
	if err != nil || got != "prod" {
		t.Errorf("typed map = %v, %v", got, err)
	}
}

func TestWalkValueRefusesWhatCannotTravel(t *testing.T) {
	if _, err := EvalValue(map[string]any{"fn": func() {}}, evalState()); !errors.Is(err, ErrGraphValue) {
		t.Errorf("error %v, want ErrGraphValue", err)
	}
	if _, err := EvalValue(map[int]string{1: "x"}, evalState()); !errors.Is(err, ErrGraphValue) {
		t.Errorf("error %v, want ErrGraphValue for a non-string map key", err)
	}
}

func TestEvalPredicateReportsABrokenOperand(t *testing.T) {
	cases := map[string]wf.Predicate{
		"a broken left operand":  wf.Equals(wf.Path("$.a["), 1),
		"a broken right operand": wf.Equals(1, wf.Path("$.a[")),
		"a broken membership":    wf.In(wf.Path("$.a["), []any{1}),
		"a broken conjunction":   wf.And(wf.Truthy("$.charge.ok"), wf.Truthy("$.a[")),
		"a broken disjunction":   wf.Or(wf.Truthy("$.suspended"), wf.Truthy("$.a[")),
		"a broken negation":      wf.Not(wf.Truthy("$.a[")),
	}
	for name, pred := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := EvalPredicate(pred, evalState()); !errors.Is(err, ErrPathSyntax) {
				t.Errorf("error %v, want ErrPathSyntax", err)
			}
		})
	}
}

func TestEvalPredicateRejectsAnEmptyDisjunction(t *testing.T) {
	if _, err := EvalPredicate(wf.Or(), evalState()); !errors.Is(err, ErrPredicateShape) {
		t.Errorf("error %v, want ErrPredicateShape", err)
	}
	if _, err := EvalPredicate(wf.And(nil), evalState()); !errors.Is(err, ErrPredicateShape) {
		t.Errorf("error %v, want ErrPredicateShape for a nil operand", err)
	}
}

func TestEvalValueFollowsPointersAndNils(t *testing.T) {
	text := "x"
	var missing *string
	got, err := EvalValue(map[string]any{"set": &text, "unset": missing}, evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	want := map[string]any{"set": "x", "unset": nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestEvalPathHandlesMoreShapes(t *testing.T) {
	state := map[string]any{
		"labels": map[string]string{"env": "prod"},
		"items":  []any{map[string]any{"id": "a"}},
	}
	// A missing key in a typed map.
	if got, err := EvalPath("$.labels.missing", state); err != nil || got != nil {
		t.Errorf("typed map miss = %v, %v", got, err)
	}
	// An index into something that resolved to nothing.
	if got, err := EvalPath("$.nope[0]", state); err != nil || got != nil {
		t.Errorf("index into nothing = %v, %v", got, err)
	}
	// An empty index is not a number.
	if _, err := EvalPath("$.items[]", state); !errors.Is(err, ErrPathSyntax) {
		t.Errorf("error %v, want ErrPathSyntax", err)
	}
	// A wildcard over a typed slice still collects.
	typed := map[string]any{"tags": []string{"a", "b"}}
	if got, err := EvalPath("$.tags[*]", typed); err != nil || len(got.([]any)) != 2 {
		t.Errorf("typed wildcard = %v, %v", got, err)
	}
}

func TestEvalTargetReportsAMalformedPath(t *testing.T) {
	if _, err := EvalTarget(wf.Path("$.a["), evalState()); !errors.Is(err, ErrPathSyntax) {
		t.Errorf("error %v, want ErrPathSyntax", err)
	}
}

func TestEvalValueTreatsANameAsALiteral(t *testing.T) {
	got, err := EvalValue(map[string]any{"service": wf.Name("billing")}, evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !reflect.DeepEqual(got, map[string]any{"service": "billing"}) {
		t.Errorf("got %#v, want the literal name", got)
	}
}

type namedBool bool
type namedInt int
type namedUint uint
type namedFloat float64

func TestEvalValueReadsNamedScalarTypes(t *testing.T) {
	got, err := EvalValue(map[string]any{
		"flag":  namedBool(true),
		"count": namedInt(3),
		"size":  namedUint(4),
		"ratio": namedFloat(0.5),
	}, evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	want := map[string]any{
		"flag":  true,
		"count": int64(3),
		"size":  uint64(4),
		"ratio": 0.5,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	if _, err := EvalValue(map[string]any{"bad": namedFloat(math.Inf(1))}, evalState()); !errors.Is(err, ErrGraphValue) {
		t.Errorf("error %v, want ErrGraphValue", err)
	}
}
