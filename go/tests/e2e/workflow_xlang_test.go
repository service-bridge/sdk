//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// A workflow declared in one language reaches a step served by the other, and
// the same graph declared by both SDKs freezes to the same runtime fingerprint
// (the runtime computes it; neither SDK canonicalizes anything).

// TestWorkflowGoCallStepReachesNodeService closes the mirror of the case below:
// a Go-declared workflow whose step calls a method a Node instance handles. The
// step encodes through the pair of types the dependency was declared with, so
// the contract hash it routes at is computed on the Go side from generated Go
// types while the callee's is computed by protobufjs from the .proto file — the
// run only reaches the handler if those two agree.
func TestWorkflowGoCallStepReachesNodeService(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("wf.xlang.go2node")
	workflowName := uniqueName("wf.xlang.go2node.wf")

	cfg := newAgentConfig(t)
	cfg.RPCMethod = method
	agent := startNodeAgent(ctx, t, cfg)
	callee := agent.Ready.ServiceName

	owner := newClient(t, domainXLang, 3)
	if _, err := servicebridge.NewMethod[*e2epb.Echo, *e2epb.EchoReply](
		servicebridge.NewClient(owner, callee), method); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	err := owner.Workflow.Handle(workflowName, wf.Definition{Version: "test-v1",
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "invoke"},
				Service: wf.Name(callee),
				Method:  wf.Name(method),
				Input:   map[string]any{"text": "from-go-workflow", "n": "77"},
			},
		},
	})
	if err != nil {
		t.Fatalf("declare workflow: %v", err)
	}
	start(ctx, t, owner)
	waitForMethod(ctx, t, owner, callee, method)

	runID, err := owner.Workflow.Start(ctx, serviceName(domainXLang, 3), workflowName, map[string]any{})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	awaitCtx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	state, err := owner.Workflow.Await(awaitCtx, runID)
	if err != nil {
		t.Fatalf("await run %s: %v\nagent stderr:\n%s", runID, err, agent.stderr.String())
	}

	served := agent.waitRPC(t, 10*time.Second)
	if served.Method != method {
		t.Errorf("the Node handler served %q, want %q", served.Method, method)
	}
	if got := served.Req["text"]; got != "from-go-workflow" {
		t.Errorf("the Node handler saw text %#v, want %q", got, "from-go-workflow")
	}

	output, ok := state["invoke"].(map[string]any)
	if !ok {
		t.Fatalf("run state holds %#v under the step, want the reply object", state["invoke"])
	}
	if got := output["handledBy"]; got != "node" {
		t.Errorf("the reply in run state carries handledBy %#v, want %q", got, "node")
	}
	if got := output["n"]; got != "77" {
		t.Errorf("the reply in run state carries n %#v, want the string %q", got, "77")
	}
}

// TestWorkflowNodeCallStepReachesGoService proves a workflow declared by the
// Node SDK, whose only step calls a method a Go instance handles, registers
// and runs to success — the runtime has to accept the graph, dispatch the step
// back to the Node instance, and route that step's call to Go exactly as a
// direct call would.
func TestWorkflowNodeCallStepReachesGoService(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("wf.xlang.node2go")
	workflowName := uniqueName("wf.xlang.node2go.wf")
	callee := serviceName(domainXLang, 1)
	served := make(chan *e2epb.Echo, 4)

	provider := newClient(t, domainXLang, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	err := servicebridge.Handle(provider, method, func(_ context.Context, req *e2epb.Echo) (*e2epb.EchoReply, error) {
		served <- req
		return &e2epb.EchoReply{Text: req.GetText(), N: req.GetN(), HandledBy: "go"}, nil
	})
	if err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	// The same graph the agent declares, declared in Go under another name: the
	// runtime must freeze both to one fingerprint (it computes it; neither SDK
	// canonicalizes anything).
	twin := workflowName + ".go"
	if _, err := servicebridge.NewMethod[*e2epb.Echo, *e2epb.EchoReply](servicebridge.NewClient(provider, callee), method); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	if err := provider.Workflow.Handle(twin, wf.Definition{Version: "test-v1", Steps: []wf.Step{
		wf.Call{
			Control: wf.Control{ID: "call_target"},
			Service: wf.Name(callee),
			Method:  wf.Name(method),
			Input:   map[string]any{"text": "from-node-workflow", "n": 77},
			Opts:    &wf.CallOpts{Transport: wf.TransportProxy, Timeout: 20 * time.Second},
		},
	}}); err != nil {
		t.Fatalf("declare twin workflow: %v", err)
	}
	start(ctx, t, provider)

	cfg := newAgentConfig(t)
	cfg.Deps = []agentDep{{Service: callee, Methods: []string{method}}}
	cfg.WorkflowName = workflowName
	cfg.WorkflowCallService = callee
	cfg.WorkflowCallMethod = method
	agent := startNodeAgent(ctx, t, cfg)

	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	agent.awaitMethod(callCtx, t, callee, method)

	rows := waitRows(ctx, t, rowTimeout, "both definitions", fmt.Sprintf(
		`SELECT name, fingerprint FROM workflow_definitions WHERE name IN (%s, %s)`,
		lit(t, workflowName), lit(t, twin)), 2)
	if str(rows[0], "fingerprint") != str(rows[1], "fingerprint") {
		t.Fatalf("the same graph froze to different fingerprints: %v", rows)
	}

	runID, err := agent.startWorkflow(callCtx, workflowName, map[string]any{})
	if err != nil {
		t.Fatalf("the Node agent could not start the workflow: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}
	if runID == "" {
		t.Fatal("startWorkflow returned an empty run identifier")
	}

	awaitCtx, awaitCancel := context.WithTimeout(ctx, deliveryTimeout)
	defer awaitCancel()
	finalState, err := agent.awaitWorkflow(awaitCtx, runID)
	if err != nil {
		t.Fatalf("the Node agent's run did not finish: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}
	_ = finalState

	select {
	case req := <-served:
		if req.GetText() != "from-node-workflow" {
			t.Errorf("the Go handler saw text %q, want %q", req.GetText(), "from-node-workflow")
		}
		if req.GetN() != 77 {
			t.Errorf("the Go handler saw n=%d, want 77", req.GetN())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the Go handler was never invoked even though the run finished")
	}
}
