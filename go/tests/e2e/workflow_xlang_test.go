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

// A Go-declared workflow whose step calls a Node handler is deliberately not
// covered here: it cannot pass against the current SDK, and a red test left
// in the suite is worse than no test. The Go workflow executor's Call
// (go/generic.go, executor.Call) always encodes a step's input as a JSON tree
// via serde.Encode's non-proto.Message fallback and always decodes the reply
// with decodeJSONValue's plain json.Unmarshal — there is no path that carries
// the target method's real protobuf schema, so the proxy call the runtime
// receives has no (or an empty) contract hash. The runtime's resolver
// (runtime/internal/rpc/server.go prepareCall → Resolver.Resolve) matches
// live instances by that hash, so it finds nothing for a method actually
// registered with a real schema and answers Unavailable "no compatible
// instance". This reproduces the same way for a same-language wf.Call against
// any Handle[Req,Resp]-declared method — it is not a cross-language gap, and
// workflow_test.go's own coverage never exercises wf.Call against a live
// handler either (every step there is wf.Local). Fixing it means deciding how
// a dynamic call step learns the target's real schema, which is a
// go/generic.go and/or runtime routing change outside this task's file
// ownership; reported instead of patched. TestWorkflowNodeCallStepReachesGoService
// below is unaffected: Node's workflow runner reuses the same schema-aware
// sb.rpc.call() a direct call goes through, useSchema()-registered up front.

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
