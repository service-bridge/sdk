//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	wf "github.com/service-bridge/sdk/go/workflow"
)

// TestLinearWorkflowRunsToSuccess proves the orchestration loop closes: the
// runtime holds the graph, dispatches each step to the instance that declared
// it, records the output it returns and only then releases the step that waited
// on it. The assertion is on the per-step outputs rather than on the run status
// alone — a run can report success while a step it skipped produced nothing.
func TestLinearWorkflowRunsToSuccess(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	name := uniqueName("go.wf.linear")
	executed := make(chan string, 8)

	c := newClient(t, domainWorkflow, 1)
	err := c.Workflow.Handle(name, wf.Definition{
		Steps: []wf.Step{
			wf.Local{
				Control: wf.Control{ID: "first"},
				Fn: func(_ context.Context, state map[string]any) (any, error) {
					executed <- "first"
					input, _ := state["input"].(map[string]any)
					return map[string]any{"seen": input["n"]}, nil
				},
			},
			wf.Local{
				Control: wf.Control{ID: "second", WaitFor: []string{"first"}},
				Fn: func(_ context.Context, _ map[string]any) (any, error) {
					executed <- "second"
					return map[string]any{"stage": 2}, nil
				},
			},
			wf.Local{
				Control: wf.Control{ID: "third", WaitFor: []string{"second"}},
				Fn: func(_ context.Context, _ map[string]any) (any, error) {
					executed <- "third"
					return map[string]any{"stage": 3}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("declare workflow: %v", err)
	}
	start(ctx, t, c)

	runID, err := c.Workflow.Start(ctx, name, map[string]any{"n": 21})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	if runID == "" {
		t.Fatal("start returned an empty run identifier")
	}

	awaitCtx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	if _, err := c.Workflow.Await(awaitCtx, runID); err != nil {
		t.Fatalf("await run %s: %v", runID, err)
	}

	order := drain(executed)
	want := []string{"first", "second", "third"}
	if len(order) != len(want) {
		t.Fatalf("steps executed %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("steps executed %v, want %v — waitFor did not sequence the chain", order, want)
		}
	}

	snap, err := c.Workflow.Query(ctx, runID)
	if err != nil {
		t.Fatalf("query run %s: %v", runID, err)
	}
	if snap.Status != "success" {
		t.Fatalf("run status is %q, want %q", snap.Status, "success")
	}
	byID := map[string]any{}
	for _, s := range snap.Steps {
		if s.Status != "success" {
			t.Errorf("step %s finished as %q (last error: %q)", s.StepID, s.Status, s.LastError)
		}
		byID[s.StepID] = s.Output
	}
	for _, id := range want {
		out, ok := byID[id]
		if !ok {
			t.Errorf("step %s is missing from the run snapshot", id)
			continue
		}
		if out == nil {
			t.Errorf("step %s recorded no output", id)
		}
	}

	rows := waitRows(ctx, t, rowTimeout, "the workflow run operation", fmt.Sprintf(
		`SELECT channel, kind, status FROM operations WHERE subject = %s`,
		lit(t, "workflow.run:"+name)), 1)
	if got := num(t, rows[0], "channel"); got != 4 {
		t.Errorf("run operation is on channel %v, want 4 (WORKFLOW)", got)
	}
	if got := num(t, rows[0], "kind"); got != 1 {
		t.Errorf("run operation kind is %v, want 1 (RUN)", got)
	}
}

func drain(ch chan string) []string {
	var out []string
	for {
		select {
		case v := <-ch:
			out = append(out, v)
		default:
			return out
		}
	}
}
