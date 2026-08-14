//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	intwf "github.com/service-bridge/sdk/go/internal/workflow"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// The runtime parses and validates the canonical workflow graph
// (runtime/internal/workflow/register.go): the serialized shape the Go DSL
// produces has to match what the Node SDK writes for the same graph, byte for
// byte, because the persisted `graph` column and the `fingerprint` the runtime
// stores are exactly those bytes and their hash. These tests close two gaps
// left uncovered by workflow_test.go (same-language only) and crosslang_test.go
// (RPC/events only, no workflow orchestration): a workflow declared in one
// language reaching a step in the other, and canonical-byte agreement for an
// equivalent graph.

// TestWorkflowCanonicalFingerprintMatchesAcrossLanguages proves
// go/internal/workflow/canonical.go and node/src/workflow/canonical.ts render
// the identical canonical JSON — and therefore the identical fingerprint — for
// the same graph. It needs no runtime: both sides are pure functions of the
// declared graph, so this is the most direct test of the actual risk (a key
// renamed, a field re-cased, a value re-typed silently changes contract_hash on
// one side only) without a live registration's error message standing between
// the assertion and the cause.
func TestWorkflowCanonicalFingerprintMatchesAcrossLanguages(t *testing.T) {
	ctx := testContext(t, time.Minute)

	def := wf.Definition{
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "invoke"},
				Service: wf.Name("billing-svc"),
				Method:  wf.Name("Charge"),
				Input: map[string]any{
					"amount":   4200,
					"currency": "USD",
					"orderId":  "ord-77",
				},
			},
		},
	}
	frozen, err := intwf.Freeze("xlang-fingerprint-check", def)
	if err != nil {
		t.Fatalf("freeze Go graph: %v", err)
	}

	// The exact JS object node/src/workflow/domain.ts::handle builds before
	// calling canonicalize/fingerprint: {graph, retry?, maxParallelism?,
	// timeoutSec?, inputSchema?} with every absent field omitted. This graph
	// declares none of them, so only "graph" is present — mirroring what Go's
	// Freeze leaves in root when Definition carries no Retry/MaxParallelism/
	// TimeoutSec/Input.
	nodeGraph := map[string]any{
		"graph": []map[string]any{
			{
				"id":      "invoke",
				"type":    "call",
				"service": "billing-svc",
				"method":  "Charge",
				"input": map[string]any{
					"amount":   4200,
					"currency": "USD",
					"orderId":  "ord-77",
				},
			},
		},
	}
	nodeCanonical, nodeFingerprint := nodeWorkflowFingerprint(ctx, t, nodeGraph)

	if string(frozen.JSON) != nodeCanonical {
		t.Fatalf("canonical graphs differ:\n  go:   %s\n  node: %s", frozen.JSON, nodeCanonical)
	}
	if frozen.Fingerprint != nodeFingerprint {
		t.Fatalf("fingerprints differ for byte-identical graphs (should be unreachable): go=%s node=%s",
			frozen.Fingerprint, nodeFingerprint)
	}
}

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
	err := owner.Workflow.Handle(workflowName, wf.Definition{
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

	runID, err := owner.Workflow.Start(ctx, workflowName, map[string]any{})
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
