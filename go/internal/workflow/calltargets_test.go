package workflow_test

import (
	"testing"

	iwf "github.com/service-bridge/sdk/go/internal/workflow"
	wf "github.com/service-bridge/sdk/go/workflow"
)

func targetStrings(targets []iwf.CallTarget) []string {
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		out = append(out, t.StepID+" -> "+t.Service+"/"+t.Method)
	}
	return out
}

func assertTargets(t *testing.T, steps []wf.Step, want ...string) {
	t.Helper()
	got := targetStrings(iwf.StaticCallTargets(steps))
	if len(got) != len(want) {
		t.Fatalf("targets are %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("targets are %v, want %v", got, want)
		}
	}
}

func TestStaticCallTargetsReachesNestedGroups(t *testing.T) {
	assertTargets(t, []wf.Step{
		wf.Call{
			Control: wf.Control{ID: "top"},
			Service: wf.Name("billing"),
			Method:  wf.Name("Charge"),
		},
		wf.Parallel{
			Control: wf.Control{ID: "group"},
			Steps: []wf.Step{
				wf.Sequence{
					Control: wf.Control{ID: "inner"},
					Steps: []wf.Step{
						wf.Call{
							Control: wf.Control{ID: "deep"},
							Service: wf.Name("shipping"),
							Method:  wf.Name("Ship"),
						},
					},
				},
			},
		},
	},
		"top -> billing/Charge",
		"deep -> shipping/Ship",
	)
}

// TestStaticCallTargetsSkipsComputedNames states the limit of the check plainly:
// a target read out of run state has no name until the step runs.
func TestStaticCallTargetsSkipsComputedNames(t *testing.T) {
	assertTargets(t, []wf.Step{
		wf.Call{
			Control: wf.Control{ID: "by_path"},
			Service: wf.Path("input.service"),
			Method:  wf.Name("Charge"),
		},
		wf.Call{
			Control: wf.Control{ID: "method_by_path"},
			Service: wf.Name("billing"),
			Method:  wf.Path("input.method"),
		},
		wf.Call{
			Control: wf.Control{ID: "literal"},
			Service: wf.Name("billing"),
			Method:  wf.Name("Charge"),
		},
	},
		"literal -> billing/Charge",
	)
}

// TestStaticCallTargetsCoversCompensations matters because a compensation is a
// call like any other: it needs the same declared dependency, and it only runs
// on the day something else already went wrong.
func TestStaticCallTargetsCoversCompensations(t *testing.T) {
	assertTargets(t, []wf.Step{
		wf.Call{
			Control: wf.Control{
				ID: "charge",
				Compensate: &wf.Compensation{
					Method: wf.Name("Refund"),
				},
			},
			Service: wf.Name("billing"),
			Method:  wf.Name("Charge"),
		},
	},
		"charge -> billing/Charge",
		"charge.compensate -> billing/Refund",
	)
}

// A compensation that names neither service nor method undoes the step through
// the very method it called, so the target is the step's own.
func TestStaticCallTargetsInheritsAnUnnamedCompensationTarget(t *testing.T) {
	assertTargets(t, []wf.Step{
		wf.Call{
			Control: wf.Control{ID: "charge", Compensate: &wf.Compensation{}},
			Service: wf.Name("billing"),
			Method:  wf.Name("Charge"),
		},
	},
		"charge -> billing/Charge",
		"charge.compensate -> billing/Charge",
	)
}

// A publish step compensated by an event makes no call, so it contributes no
// target — and a publish compensated by a call does.
func TestStaticCallTargetsReadsTheCompensationKind(t *testing.T) {
	assertTargets(t, []wf.Step{
		wf.Publish{
			Control: wf.Control{ID: "emit", Compensate: &wf.Compensation{Event: wf.Name("order.cancelled")}},
			Event:   wf.Name("order.placed"),
		},
	})

	assertTargets(t, []wf.Step{
		wf.Publish{
			Control: wf.Control{ID: "emit", Compensate: &wf.Compensation{
				Kind:    wf.CompensateCall,
				Service: wf.Name("billing"),
				Method:  wf.Name("Refund"),
			}},
			Event: wf.Name("order.placed"),
		},
	},
		"emit.compensate -> billing/Refund",
	)
}

func TestStaticCallTargetsOfAGraphWithoutCalls(t *testing.T) {
	assertTargets(t, []wf.Step{
		wf.Sleep{Control: wf.Control{ID: "wait"}, DurationSec: 1},
		wf.WaitSignal{Control: wf.Control{ID: "hold"}, Signal: "go"},
	})
}
