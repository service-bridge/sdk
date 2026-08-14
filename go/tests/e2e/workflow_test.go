//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
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

// beyondFloat64 is 2^53+1, the first integer a JSON number cannot hold. Run
// state is JSON, so a call step's payload only keeps a 64-bit field intact if
// both directions travel as the JSON mirror of the message, where such a field
// is a string. A value below this bound would pass even if they did not.
const beyondFloat64 = int64(9007199254740993)

// TestWorkflowCallStepReachesTypedHandler is the whole point of a call step: the
// graph names a method, the runtime dispatches the step back to this instance,
// and the step reaches an ordinary Handle[Req, Resp] handler on another service
// — the same handler a direct Call would reach, at the same contract version.
//
// The 64-bit field is asserted on both sides of the trip: it is the value that
// degrades silently if the payload crosses as anything other than the message's
// JSON mirror.
func TestWorkflowCallStepReachesTypedHandler(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("go.wf.call.typed")
	name := uniqueName("go.wf.call.typed.wf")
	calleeName := serviceName(domainWorkflow, 2)
	served := make(chan *e2epb.Echo, 4)

	provider := newClient(t, domainWorkflow, 2, servicebridge.WithAdvertise("127.0.0.1", 0))
	err := servicebridge.Handle(provider, method, func(_ context.Context, req *e2epb.Echo) (*e2epb.EchoReply, error) {
		served <- req
		return &e2epb.EchoReply{Text: req.GetText(), N: req.GetN(), HandledBy: "go"}, nil
	})
	if err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	start(ctx, t, provider)

	owner := newClient(t, domainWorkflow, 1)
	if _, err := servicebridge.NewMethod[*e2epb.Echo, *e2epb.EchoReply](
		servicebridge.NewClient(owner, calleeName), method); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	err = owner.Workflow.Handle(name, wf.Definition{
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "invoke"},
				Service: wf.Name(calleeName),
				Method:  wf.Name(method),
				Input: map[string]any{
					"text": wf.Path("$.input.text"),
					// Written as the JSON mirror writes it: a 64-bit field is a
					// string there, and this is the form the value comes back in.
					"n": fmt.Sprint(beyondFloat64),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("declare workflow: %v", err)
	}
	start(ctx, t, owner)
	waitForMethod(ctx, t, owner, calleeName, method)

	runID, err := owner.Workflow.Start(ctx, name, map[string]any{"text": "from-go-workflow"})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	awaitCtx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	state, err := owner.Workflow.Await(awaitCtx, runID)
	if err != nil {
		t.Fatalf("await run %s: %v", runID, err)
	}

	select {
	case req := <-served:
		if req.GetText() != "from-go-workflow" {
			t.Errorf("the handler saw text %q, want %q", req.GetText(), "from-go-workflow")
		}
		if req.GetN() != beyondFloat64 {
			t.Errorf("the handler saw n=%d, want %d", req.GetN(), beyondFloat64)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run finished but the handler was never invoked")
	}

	output, ok := state["invoke"].(map[string]any)
	if !ok {
		t.Fatalf("run state holds %#v under the step, want the reply object", state["invoke"])
	}
	if got := output["text"]; got != "from-go-workflow" {
		t.Errorf("the reply in run state carries text %#v, want %q", got, "from-go-workflow")
	}
	if got := output["handledBy"]; got != "go" {
		t.Errorf("the reply in run state carries handledBy %#v, want %q", got, "go")
	}
	if got := output["n"]; got != fmt.Sprint(beyondFloat64) {
		t.Errorf("the reply in run state carries n %#v, want the string %q", got, fmt.Sprint(beyondFloat64))
	}
}

// TestWorkflowCallStepRefusesAnUndeclaredDependency proves the refusal lands at
// Start, where the declaration is still in the reader's hands. The alternative
// — calling at the empty contract hash — reaches no typed handler at all and
// would report itself as an unavailable mesh.
func TestWorkflowCallStepRefusesAnUndeclaredDependency(t *testing.T) {
	ctx := testContext(t, time.Minute)

	name := uniqueName("go.wf.call.undeclared.wf")
	method := uniqueName("go.wf.call.undeclared")
	callee := serviceName(domainWorkflow, 2)

	owner := newClient(t, domainWorkflow, 1)
	err := owner.Workflow.Handle(name, wf.Definition{
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "invoke"},
				Service: wf.Name(callee),
				Method:  wf.Name(method),
			},
		},
	})
	if err != nil {
		t.Fatalf("declare workflow: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	err = owner.Start(startCtx)
	if err == nil {
		t.Fatal("the client started with a graph calling a method it never declared")
	}
	if !errors.Is(err, servicebridge.ErrConfig) {
		t.Fatalf("Start failed with %v, want a CONFIG error", err)
	}
	for _, want := range []string{name, "invoke", callee, method, "NewMethod"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// TestWorkflowCallStepFailsOnAnUndeclaredComputedTarget covers the half no check
// at Start can reach: a target read out of run state has no name until the step
// runs. The step must then say the same thing Start would have said.
func TestWorkflowCallStepFailsOnAnUndeclaredComputedTarget(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	name := uniqueName("go.wf.call.computed.wf")
	method := uniqueName("go.wf.call.computed")
	callee := serviceName(domainWorkflow, 2)

	owner := newClient(t, domainWorkflow, 1)
	err := owner.Workflow.Handle(name, wf.Definition{
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "invoke"},
				Service: wf.Path("$.input.service"),
				Method:  wf.Path("$.input.method"),
			},
		},
	})
	if err != nil {
		t.Fatalf("declare workflow: %v", err)
	}
	start(ctx, t, owner)

	runID, err := owner.Workflow.Start(ctx, name, map[string]any{"service": callee, "method": method})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	snap := awaitFailedRun(ctx, t, owner, runID)
	if !strings.Contains(stepError(snap, "invoke"), "not a declared dependency") {
		t.Fatalf("the step failed with %q, want the undeclared-dependency refusal", stepError(snap, "invoke"))
	}
}

// TestWorkflowCallStepSurfacesTheCalleeError proves a business failure travels
// back as the step's failure rather than as a decode error or a silent success.
func TestWorkflowCallStepSurfacesTheCalleeError(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("go.wf.call.failing")
	name := uniqueName("go.wf.call.failing.wf")
	calleeName := serviceName(domainWorkflow, 2)

	provider := newClient(t, domainWorkflow, 2, servicebridge.WithAdvertise("127.0.0.1", 0))
	err := servicebridge.Handle(provider, method, func(_ context.Context, _ *e2epb.Echo) (*e2epb.EchoReply, error) {
		return nil, errors.New("the callee refuses this order")
	})
	if err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	start(ctx, t, provider)

	owner := newClient(t, domainWorkflow, 1)
	if _, err := servicebridge.NewMethod[*e2epb.Echo, *e2epb.EchoReply](
		servicebridge.NewClient(owner, calleeName), method); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	err = owner.Workflow.Handle(name, wf.Definition{
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "invoke"},
				Service: wf.Name(calleeName),
				Method:  wf.Name(method),
				Input:   map[string]any{"text": "doomed"},
			},
		},
	})
	if err != nil {
		t.Fatalf("declare workflow: %v", err)
	}
	start(ctx, t, owner)
	waitForMethod(ctx, t, owner, calleeName, method)

	runID, err := owner.Workflow.Start(ctx, name, map[string]any{})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	snap := awaitFailedRun(ctx, t, owner, runID)
	if !strings.Contains(stepError(snap, "invoke"), "the callee refuses this order") {
		t.Fatalf("the step failed with %q, want the callee's own message", stepError(snap, "invoke"))
	}
}

// awaitFailedRun waits for a run to reach a terminal status and refuses to
// accept success. Await alone is not enough: it reports a terminal run as an
// error without saying which step failed or why.
func awaitFailedRun(ctx context.Context, t *testing.T, c *servicebridge.Client, runID string) servicebridge.RunSnapshot {
	t.Helper()
	var snap servicebridge.RunSnapshot
	waitFor(ctx, t, deliveryTimeout, "run "+runID+" to reach a terminal status", func(ctx context.Context) (bool, error) {
		got, err := c.Workflow.Query(ctx, runID)
		if err != nil {
			return false, err
		}
		snap = got
		return got.Status == "failed" || got.Status == "success" || got.Status == "cancelled", nil
	})
	if snap.Status != "failed" {
		t.Fatalf("run %s finished as %q, want failed", runID, snap.Status)
	}
	return snap
}

func stepError(snap servicebridge.RunSnapshot, stepID string) string {
	for _, s := range snap.Steps {
		if s.StepID == stepID {
			return s.LastError
		}
	}
	return ""
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
