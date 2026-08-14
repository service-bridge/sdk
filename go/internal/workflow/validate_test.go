package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"

	wf "github.com/service-bridge/sdk/go/workflow"
)

func call(id string, waitFor ...string) wf.Call {
	return wf.Call{
		Control: wf.Control{ID: id, WaitFor: waitFor},
		Service: wf.Name("billing"),
		Method:  wf.Name("Charge"),
	}
}

func TestValidateAcceptsAWholeGraph(t *testing.T) {
	if err := Validate("order.fulfil", fullGraph()); err != nil {
		t.Fatalf("valid graph rejected: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name string
		def  wf.Definition
		want error
	}{
		{
			name: "a step id outside the alphabet",
			def:  wf.Definition{Steps: []wf.Step{call("Charge-Step")}},
			want: ErrStepID,
		},
		{
			name: "a step without an id",
			def:  wf.Definition{Steps: []wf.Step{call("")}},
			want: ErrStepID,
		},
		{
			name: "a duplicated step id",
			def:  wf.Definition{Steps: []wf.Step{call("a"), call("a")}},
			want: ErrDuplicateStepID,
		},
		{
			name: "a duplicate that hides inside a group",
			def: wf.Definition{Steps: []wf.Step{
				call("a"),
				wf.Sequence{Control: wf.Control{ID: "g"}, Steps: []wf.Step{call("a")}},
			}},
			want: ErrDuplicateStepID,
		},
		{
			name: "a dependency on a step that does not exist",
			def:  wf.Definition{Steps: []wf.Step{call("a", "ghost")}},
			want: ErrUnknownDependency,
		},
		{
			name: "a step waiting for itself",
			def:  wf.Definition{Steps: []wf.Step{call("a", "a")}},
			want: ErrDependencyCycle,
		},
		{
			name: "a cycle across two steps",
			def:  wf.Definition{Steps: []wf.Step{call("a", "b"), call("b", "a")}},
			want: ErrDependencyCycle,
		},
		{
			name: "a cycle across three steps",
			def:  wf.Definition{Steps: []wf.Step{call("a", "c"), call("b", "a"), call("c", "b")}},
			want: ErrDependencyCycle,
		},
		{
			name: "compensation on a step with nothing to undo",
			def: wf.Definition{Steps: []wf.Step{
				wf.Sleep{
					Control: wf.Control{
						ID:         "a",
						Compensate: &wf.Compensation{Input: map[string]any{}},
					},
					DurationSec: 1,
				},
			}},
			want: ErrCompensationUnsupported,
		},
		{
			name: "compensation on a group",
			def: wf.Definition{Steps: []wf.Step{
				wf.Parallel{
					Control: wf.Control{
						ID:         "g",
						Compensate: &wf.Compensation{Input: map[string]any{}},
					},
					Steps: []wf.Step{call("a")},
				},
			}},
			want: ErrCompensationUnsupported,
		},
		{
			name: "a workflow that starts itself",
			def: wf.Definition{Steps: []wf.Step{
				wf.SubWorkflow{Control: wf.Control{ID: "a"}, Workflow: wf.Name("order.fulfil")},
			}},
			want: ErrSelfReference,
		},
		{
			name: "a broken path in an input",
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{
					Control: wf.Control{ID: "a"},
					Service: wf.Name("billing"),
					Method:  wf.Name("Charge"),
					Input:   map[string]any{"x": wf.Path("$.foo[abc]")},
				},
			}},
			want: ErrPathSyntax,
		},
		{
			name: "a broken path in a target",
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Path("$.["), Method: wf.Name("Charge")},
			}},
			want: ErrPathSyntax,
		},
		{
			name: "a broken path in a condition",
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{
					Control: wf.Control{ID: "a", When: wf.Truthy("$.a..b")},
					Service: wf.Name("billing"),
					Method:  wf.Name("Charge"),
				},
			}},
			want: ErrPathSyntax,
		},
		{
			name: "a broken path nested in a condition",
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{
					Control: wf.Control{ID: "a", When: wf.And(wf.Equals(wf.Path("$.a["), "x"))},
					Service: wf.Name("billing"),
					Method:  wf.Name("Charge"),
				},
			}},
			want: ErrPathSyntax,
		},
		{
			name: "a broken path in a fan-out",
			def: wf.Definition{Steps: []wf.Step{
				wf.Parallel{
					Control: wf.Control{ID: "g"},
					ForEach: &wf.ForEach{From: "$.items[", As: "item"},
					Steps:   []wf.Step{call("a")},
				},
			}},
			want: ErrPathSyntax,
		},
		{
			name: "a fan-out alias outside the alphabet",
			def: wf.Definition{Steps: []wf.Step{
				wf.Parallel{
					Control: wf.Control{ID: "g"},
					ForEach: &wf.ForEach{From: "$.items", As: "Item"},
					Steps:   []wf.Step{call("a")},
				},
			}},
			want: ErrForEachAlias,
		},
		{
			name: "a target left empty",
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{Control: wf.Control{ID: "a"}, Method: wf.Name("Charge")},
			}},
			want: ErrEmptyTarget,
		},
		{
			name: "an empty group",
			def: wf.Definition{Steps: []wf.Step{
				wf.Sequence{Control: wf.Control{ID: "g"}},
			}},
			want: ErrEmptyGroup,
		},
		{
			name: "a local step without a function",
			def: wf.Definition{Steps: []wf.Step{
				wf.Local{Control: wf.Control{ID: "a"}},
			}},
			want: ErrMissingFunc,
		},
		{
			name: "a negative sleep",
			def: wf.Definition{Steps: []wf.Step{
				wf.Sleep{Control: wf.Control{ID: "a"}, DurationSec: -1},
			}},
			want: ErrNegativeDuration,
		},
		{
			name: "a malformed condition",
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{
					Control: wf.Control{ID: "a", When: wf.Not(nil)},
					Service: wf.Name("billing"),
					Method:  wf.Name("Charge"),
				},
			}},
			want: ErrPredicateShape,
		},
		{
			name: "a value that cannot travel",
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{
					Control: wf.Control{ID: "a"},
					Service: wf.Name("billing"),
					Method:  wf.Name("Charge"),
					Input:   map[string]any{"cb": func() {}},
				},
			}},
			want: ErrGraphValue,
		},
		{
			name: "no steps at all",
			def:  wf.Definition{},
			want: ErrNoSteps,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate("order.fulfil", tc.def)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			var invalid *ValidationError
			if !errors.As(err, &invalid) {
				t.Fatalf("error %v is not a ValidationError; callers cannot tell it apart", err)
			}
			if invalid.Workflow != "order.fulfil" {
				t.Errorf("error names workflow %q, want order.fulfil", invalid.Workflow)
			}
		})
	}
}

func TestValidateRejectsAnEmptyWorkflowName(t *testing.T) {
	err := Validate("", wf.Definition{Steps: []wf.Step{call("a")}})
	if !errors.Is(err, ErrEmptyWorkflowName) {
		t.Errorf("error = %v, want ErrEmptyWorkflowName", err)
	}
}

func TestValidateAcceptsADependencyDeclaredLater(t *testing.T) {
	def := wf.Definition{Steps: []wf.Step{call("a", "b"), call("b")}}
	if err := Validate("wf", def); err != nil {
		t.Errorf("forward reference rejected: %v", err)
	}
}

func TestValidateAcceptsADynamicWorkflowTarget(t *testing.T) {
	// Only a literal self-reference is decidable here; a target resolved from
	// state is checked by the runtime when the run starts.
	def := wf.Definition{Steps: []wf.Step{
		wf.SubWorkflow{Control: wf.Control{ID: "a"}, Workflow: wf.Path("$.input.next")},
	}}
	if err := Validate("order.fulfil", def); err != nil {
		t.Errorf("dynamic sub-workflow rejected: %v", err)
	}
}

func TestValidateHoldsTheCapsTheRuntimeEnforces(t *testing.T) {
	deep := wf.Step(call("leaf"))
	for i := 0; i <= maxDepth; i++ {
		deep = wf.Sequence{Control: wf.Control{ID: fmt.Sprintf("g%d", i)}, Steps: []wf.Step{deep}}
	}
	if err := Validate("wf", wf.Definition{Steps: []wf.Step{deep}}); !errors.Is(err, ErrDepth) {
		t.Errorf("error = %v, want ErrDepth", err)
	}

	wide := make([]wf.Step, maxSteps+1)
	for i := range wide {
		wide[i] = call(fmt.Sprintf("s%d", i))
	}
	if err := Validate("wf", wf.Definition{Steps: wide}); !errors.Is(err, ErrStepCount) {
		t.Errorf("error = %v, want ErrStepCount", err)
	}
}

func TestValidateReadsEveryOptionForPaths(t *testing.T) {
	cases := map[string]wf.Definition{
		"call options": {Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "a"},
				Service: wf.Name("s"),
				Method:  wf.Name("M"),
				Opts:    &wf.CallOpts{IdempotencyKey: wf.Path("$.a[")},
			},
		}},
		"publish options": {Steps: []wf.Step{
			wf.Publish{
				Control: wf.Control{ID: "a"},
				Event:   wf.Name("e"),
				Opts:    &wf.PublishOpts{Headers: map[string]any{"h": wf.Path("$.a[")}},
			},
		}},
		"start options": {Steps: []wf.Step{
			wf.SubWorkflow{
				Control:  wf.Control{ID: "a"},
				Workflow: wf.Name("other"),
				Opts:     &wf.StartOpts{IdempotencyKey: wf.Path("$.a[")},
			},
		}},
		"compensation input": {Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{
					ID:         "a",
					Compensate: &wf.Compensation{Input: map[string]any{"x": wf.Path("$.a[")}},
				},
				Service: wf.Name("s"),
				Method:  wf.Name("M"),
			},
		}},
		"event filter": {Steps: []wf.Step{
			wf.WaitEvent{
				Control: wf.Control{ID: "a"},
				Event:   wf.Name("e"),
				Filter:  map[string]any{"x": wf.Path("$.a[")},
			},
		}},
	}

	for name, def := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Validate("wf", def); !errors.Is(err, ErrPathSyntax) {
				t.Errorf("error = %v, want ErrPathSyntax", err)
			}
		})
	}
}

func TestValidationErrorNamesTheStepAndField(t *testing.T) {
	err := Validate("order.fulfil", wf.Definition{Steps: []wf.Step{
		wf.Call{
			Control: wf.Control{ID: "charge"},
			Service: wf.Path("$.["),
			Method:  wf.Name("Charge"),
		},
	}})
	var invalid *ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("error %v is not a ValidationError", err)
	}
	if invalid.Step != "charge" || invalid.Field != "service" {
		t.Errorf("error points at step %q field %q, want charge/service", invalid.Step, invalid.Field)
	}
	if invalid.Error() == "" {
		t.Error("error message is empty")
	}
}

func TestValidateFailsAtTheDeclarationNotTheRun(t *testing.T) {
	// A local step's closure is never called during validation, so a graph is
	// refused before anything it describes has run.
	ran := false
	def := wf.Definition{Steps: []wf.Step{
		wf.Local{
			Control: wf.Control{ID: "a"},
			Fn: func(context.Context, map[string]any) (any, error) {
				ran = true
				return nil, nil
			},
		},
		call("a"),
	}}
	if err := Validate("wf", def); !errors.Is(err, ErrDuplicateStepID) {
		t.Fatalf("error = %v, want ErrDuplicateStepID", err)
	}
	if ran {
		t.Error("validation executed a step")
	}
}

func TestValidateRejectsAnAbsentStep(t *testing.T) {
	err := Validate("wf", wf.Definition{Steps: []wf.Step{nil}})
	if !errors.Is(err, ErrUnknownStepKind) {
		t.Errorf("error = %v, want ErrUnknownStepKind", err)
	}
}

func TestValidateRejectsAnEmptyLiteralTarget(t *testing.T) {
	err := Validate("wf", wf.Definition{Steps: []wf.Step{
		wf.Publish{Control: wf.Control{ID: "a"}, Event: wf.Name("")},
	}})
	if !errors.Is(err, ErrEmptyTarget) {
		t.Errorf("error = %v, want ErrEmptyTarget", err)
	}
}

func TestValidateRejectsEveryMalformedPredicateShape(t *testing.T) {
	// Every shape the constructors produce is well formed except for the two a
	// caller can still get wrong: a missing operand and an empty operand list.
	cases := map[string]wf.Predicate{
		"an empty conjunction":       wf.And(),
		"an empty disjunction":       wf.Or(),
		"a nil inside a conjunction": wf.And(nil),
		"a nil inside a disjunction": wf.Or(nil),
		"a nil negation":             wf.Not(nil),
	}

	for name, pred := range cases {
		t.Run(name, func(t *testing.T) {
			err := Validate("wf", wf.Definition{Steps: []wf.Step{
				wf.Sleep{Control: wf.Control{ID: "a", When: pred}},
			}})
			if !errors.Is(err, ErrPredicateShape) {
				t.Errorf("error = %v, want ErrPredicateShape", err)
			}
		})
	}
}

func TestValidateRejectsABrokenPathInsideAnOperand(t *testing.T) {
	for name, pred := range map[string]wf.Predicate{
		"equality":   wf.Equals(wf.Path("$.a["), 1),
		"membership": wf.In(wf.Path("$.a["), []any{1}),
		"negation":   wf.Not(wf.Truthy("$.a[")),
		"nested or":  wf.Or(wf.Truthy("$.ok"), wf.Truthy("$.a[")),
	} {
		t.Run(name, func(t *testing.T) {
			err := Validate("wf", wf.Definition{Steps: []wf.Step{
				wf.Sleep{Control: wf.Control{ID: "a", When: pred}},
			}})
			if !errors.Is(err, ErrPathSyntax) {
				t.Errorf("error = %v, want ErrPathSyntax", err)
			}
		})
	}
}

func TestValidateReadsEveryFieldOfEveryKind(t *testing.T) {
	cases := map[string]struct {
		def  wf.Definition
		want error
	}{
		"a call method": {
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{Control: wf.Control{ID: "a"}, Service: wf.Name("s"), Method: wf.Path("$.a[")},
			}},
			want: ErrPathSyntax,
		},
		"a publish input": {
			def: wf.Definition{Steps: []wf.Step{
				wf.Publish{Control: wf.Control{ID: "a"}, Event: wf.Name("e"), Input: wf.Path("$.a[")},
			}},
			want: ErrPathSyntax,
		},
		"a wait_event target": {
			def: wf.Definition{Steps: []wf.Step{
				wf.WaitEvent{Control: wf.Control{ID: "a"}, Event: wf.Path("$.a[")},
			}},
			want: ErrPathSyntax,
		},
		"a wait_signal without a name": {
			def: wf.Definition{Steps: []wf.Step{
				wf.WaitSignal{Control: wf.Control{ID: "a"}},
			}},
			want: ErrEmptyTarget,
		},
		"a sub-workflow target": {
			def: wf.Definition{Steps: []wf.Step{
				wf.SubWorkflow{Control: wf.Control{ID: "a"}, Workflow: wf.Path("$.a[")},
			}},
			want: ErrPathSyntax,
		},
		"a sub-workflow input": {
			def: wf.Definition{Steps: []wf.Step{
				wf.SubWorkflow{Control: wf.Control{ID: "a"}, Workflow: wf.Name("other"), Input: wf.Path("$.a[")},
			}},
			want: ErrPathSyntax,
		},
		"a compensation target": {
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{
					Control: wf.Control{
						ID:         "a",
						Compensate: &wf.Compensation{Service: wf.Path("$.a["), Method: wf.Name("Undo")},
					},
					Service: wf.Name("s"),
					Method:  wf.Name("M"),
				},
			}},
			want: ErrPathSyntax,
		},
		"a publish idempotency key": {
			def: wf.Definition{Steps: []wf.Step{
				wf.Publish{
					Control: wf.Control{ID: "a"},
					Event:   wf.Name("e"),
					Opts:    &wf.PublishOpts{IdempotencyKey: wf.Path("$.a[")},
				},
			}},
			want: ErrPathSyntax,
		},
		"a publish partition key": {
			def: wf.Definition{Steps: []wf.Step{
				wf.Publish{
					Control: wf.Control{ID: "a"},
					Event:   wf.Name("e"),
					Opts:    &wf.PublishOpts{PartitionKey: wf.Path("$.a[")},
				},
			}},
			want: ErrPathSyntax,
		},
		"a call request id": {
			def: wf.Definition{Steps: []wf.Step{
				wf.Call{
					Control: wf.Control{ID: "a"},
					Service: wf.Name("s"),
					Method:  wf.Name("M"),
					Opts:    &wf.CallOpts{RequestID: wf.Path("$.a[")},
				},
			}},
			want: ErrPathSyntax,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Validate("wf", tc.def); !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
		})
	}
}
