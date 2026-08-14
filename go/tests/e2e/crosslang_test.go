//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/internal/serde"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// Each SDK's own suite stays green while the two drift apart: a contract hash
// derived differently, a trace header formatted differently or a canonical job
// specification spelled differently are all invisible from inside one language.
// These tests put a Go and a Node instance on one runtime and make them talk.

// goContractHash is what the Go SDK routes a call to Echo/EchoReply on.
func goContractHash() string {
	return serde.ContractHash(
		(&e2epb.Echo{}).ProtoReflect().Descriptor(),
		(&e2epb.EchoReply{}).ProtoReflect().Descriptor(),
	)
}

// goEventContractHash is what the Go SDK stamps on a published OrderEvent.
func goEventContractHash() string {
	return serde.EventContractHash((&e2epb.OrderEvent{}).ProtoReflect().Descriptor())
}

func newAgentConfig(t *testing.T) agentConfig {
	t.Helper()
	return agentConfig{
		URL:       suite.runtimeURL,
		Key:       bootstrapKey(t, domainXLang, 2),
		DataDir:   t.TempDir(),
		ProtoFile: suite.protoFile,
	}
}

// TestGoCallsNodeHandler is the first half of the cross-language contract: a
// method declared by the Node SDK is reachable from Go under the schema Go
// derives from the same .proto. The contract hashes are compared directly as
// well, because a mismatch would otherwise surface only as "no live instance"
// and read like a discovery problem.
func TestGoCallsNodeHandler(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("xlang.go2node")
	cfg := newAgentConfig(t)
	cfg.RPCMethod = method
	cfg.SubOpSubject = uniqueName("xlang.node.handler")
	agent := startNodeAgent(ctx, t, cfg)

	if want := serviceName(domainXLang, 2); agent.Ready.ServiceName != want {
		t.Fatalf("the agent connected as %q, want %q", agent.Ready.ServiceName, want)
	}
	if agent.Ready.RPCContractHash != goContractHash() {
		t.Fatalf("the two SDKs derive different contract hashes from %s:\n  node: %s\n  go:   %s",
			suite.protoFile, agent.Ready.RPCContractHash, goContractHash())
	}

	caller := newClient(t, domainXLang, 1)
	if err := caller.Service(agent.Ready.ServiceName, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, caller)
	waitForMethod(ctx, t, caller, agent.Ready.ServiceName, method)

	resp, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](ctx, caller,
		agent.Ready.ServiceName, method, &e2epb.Echo{Text: "from-go", N: 11},
		servicebridge.WithTimeout(20*time.Second))
	if err != nil {
		t.Fatalf("call the Node handler: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}
	if resp.GetHandledBy() != "node" {
		t.Errorf("the reply says it was handled by %q, want %q", resp.GetHandledBy(), "node")
	}
	if resp.GetText() != "from-go" {
		t.Errorf("text came back as %q, want %q", resp.GetText(), "from-go")
	}
	if resp.GetN() != 11 {
		t.Errorf("n came back as %d, want 11", resp.GetN())
	}

	served := agent.waitRPC(t, 10*time.Second)
	if served.Method != method {
		t.Errorf("the agent served %q, want %q", served.Method, method)
	}
	if got, _ := served.Req["text"].(string); got != "from-go" {
		t.Errorf("the Node handler saw text %q, want %q", got, "from-go")
	}
}

// TestNodeCallsGoHandler is the other half: a method declared by the Go SDK,
// with Go type parameters as its only schema source, is reachable from Node.
func TestNodeCallsGoHandler(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("xlang.node2go")
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
	cfg.CallSubOpSubject = uniqueName("xlang.node.caller")
	cfg.Deps = []agentDep{{Service: callee, Methods: []string{method}}}
	agent := startNodeAgent(ctx, t, cfg)

	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	agent.awaitMethod(callCtx, t, callee, method)
	reply, err := agent.call(callCtx, callee, method, map[string]any{"text": "from-node", "n": 5})
	if err != nil {
		t.Fatalf("the Node agent could not call the Go handler: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}
	if got, _ := reply["handledBy"].(string); got != "go" {
		t.Errorf("the reply says it was handled by %q, want %q", got, "go")
	}
	if got, _ := reply["text"].(string); got != "from-node" {
		t.Errorf("text came back as %q, want %q", got, "from-node")
	}

	select {
	case req := <-served:
		if req.GetText() != "from-node" {
			t.Errorf("the Go handler saw text %q, want %q", req.GetText(), "from-node")
		}
		if req.GetN() != 5 {
			t.Errorf("the Go handler saw n=%d, want 5", req.GetN())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the Go handler was never invoked even though the call returned")
	}
}

// TestTraceIsSharedGoToNode proves the two SDKs speak one X-SB-Trace. A
// disagreement on that header does not fail a call — it splits one request into
// two unrelated traces, which no functional assertion can see. The chain is
// checked link by link: the Go operation that made the call, the call itself,
// and the operation the Node handler opened while serving it.
func TestTraceIsSharedGoToNode(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("xlang.trace.go2node")
	handlerSubject := uniqueName("xlang.trace.node.handler")
	callerSubject := uniqueName("xlang.trace.go.caller")

	cfg := newAgentConfig(t)
	cfg.RPCMethod = method
	cfg.SubOpSubject = handlerSubject
	agent := startNodeAgent(ctx, t, cfg)

	caller := newClient(t, domainXLang, 1)
	if err := caller.Service(agent.Ready.ServiceName, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, caller)
	waitForMethod(ctx, t, caller, agent.Ready.ServiceName, method)

	opCtx, op := caller.Telemetry.StartOp(ctx, callerSubject)
	_, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](opCtx, caller,
		agent.Ready.ServiceName, method, &e2epb.Echo{Text: "traced"},
		servicebridge.WithTimeout(20*time.Second))
	op.End()
	if err != nil {
		t.Fatalf("call the Node handler: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}
	agent.waitRPC(t, 10*time.Second)

	root := opRow(ctx, t, callerSubject)
	call := opRow(ctx, t, "rpc.call:"+agent.Ready.ServiceName+"/"+method)
	handler := opRow(ctx, t, handlerSubject)

	if call["trace_id"] != root["trace_id"] {
		t.Fatalf("the Go call is on trace %s, the operation that made it on %s",
			call["trace_id"], root["trace_id"])
	}
	if call["parent_op_id"] != root["op_id"] {
		t.Errorf("the Go call hangs under %s, want %s", call["parent_op_id"], root["op_id"])
	}
	if handler["trace_id"] != root["trace_id"] {
		t.Fatalf("the Node handler ran on trace %s while the Go caller was on %s: "+
			"the two SDKs did not agree on X-SB-Trace, so this request is two trees",
			handler["trace_id"], root["trace_id"])
	}
	if handler["parent_op_id"] != call["op_id"] {
		t.Errorf("the Node handler's operation hangs under %s, want the call %s",
			handler["parent_op_id"], call["op_id"])
	}
}

// TestTraceIsSharedNodeToGo is the same proof in the other direction: the trace
// a Node caller minted has to be the one the Go handler continues.
func TestTraceIsSharedNodeToGo(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	method := uniqueName("xlang.trace.node2go")
	callee := serviceName(domainXLang, 1)
	handlerSubject := uniqueName("xlang.trace.go.handler")
	callerSubject := uniqueName("xlang.trace.node.caller")

	provider := newClient(t, domainXLang, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	var telemetry *servicebridge.TelemetryDomain
	err := servicebridge.Handle(provider, method, func(ctx context.Context, req *e2epb.Echo) (*e2epb.EchoReply, error) {
		// The callee emits no operation of its own for an RPC (ADR-0001), so the
		// handler opens one: it is the only row that can show which trace the Go
		// side continued.
		_, op := telemetry.StartOp(ctx, handlerSubject)
		op.End()
		return &e2epb.EchoReply{Text: req.GetText(), HandledBy: "go"}, nil
	})
	if err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	telemetry = provider.Telemetry
	start(ctx, t, provider)

	cfg := newAgentConfig(t)
	cfg.CallSubOpSubject = callerSubject
	cfg.Deps = []agentDep{{Service: callee, Methods: []string{method}}}
	agent := startNodeAgent(ctx, t, cfg)

	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	agent.awaitMethod(callCtx, t, callee, method)
	if _, err := agent.call(callCtx, callee, method, map[string]any{"text": "traced"}); err != nil {
		t.Fatalf("the Node agent could not call the Go handler: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}

	root := opRow(ctx, t, callerSubject)
	call := opRow(ctx, t, "rpc.call:"+callee+"/"+method)
	handler := opRow(ctx, t, handlerSubject)

	if call["trace_id"] != root["trace_id"] {
		t.Fatalf("the Node call is on trace %s, the operation that made it on %s",
			call["trace_id"], root["trace_id"])
	}
	if call["parent_op_id"] != root["op_id"] {
		t.Errorf("the Node call hangs under %s, want %s", call["parent_op_id"], root["op_id"])
	}
	if handler["trace_id"] != root["trace_id"] {
		t.Fatalf("the Go handler ran on trace %s while the Node caller was on %s: "+
			"the two SDKs did not agree on X-SB-Trace, so this request is two trees",
			handler["trace_id"], root["trace_id"])
	}
	if handler["parent_op_id"] != call["op_id"] {
		t.Errorf("the Go handler's operation hangs under %s, want the call %s",
			handler["parent_op_id"], call["op_id"])
	}
}

// TestGoPublishNodeConsumes proves an event crosses the language boundary
// payload-intact, and that both SDKs stamp the same contract hash on it.
func TestGoPublishNodeConsumes(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	name := uniqueName("xlang.go2node.event")
	cfg := newAgentConfig(t)
	cfg.SubscribeEvents = []string{name}
	agent := startNodeAgent(ctx, t, cfg)

	if agent.Ready.EventContractHash != goEventContractHash() {
		t.Fatalf("the two SDKs derive different event contract hashes from %s:\n  node: %s\n  go:   %s",
			suite.protoFile, agent.Ready.EventContractHash, goEventContractHash())
	}

	publisher := newClient(t, domainXLang, 1)
	event, err := servicebridge.DefineEvent[*e2epb.OrderEvent](publisher, name)
	if err != nil {
		t.Fatalf("define event: %v", err)
	}
	start(ctx, t, publisher)

	if _, err := event.Publish(ctx, &e2epb.OrderEvent{OrderId: "xlang-1", Amount: 12.5, Currency: "CHF"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	got := agent.waitEvent(t, deliveryTimeout)
	if got.Name != name {
		t.Errorf("the agent received event %q, want %q", got.Name, name)
	}
	if v, _ := got.Payload["orderId"].(string); v != "xlang-1" {
		t.Errorf("order id arrived as %v, want %q", got.Payload["orderId"], "xlang-1")
	}
	if v, _ := got.Payload["amount"].(float64); v != 12.5 {
		t.Errorf("amount arrived as %v, want 12.5", got.Payload["amount"])
	}
	if v, _ := got.Payload["currency"].(string); v != "CHF" {
		t.Errorf("currency arrived as %v, want %q", got.Payload["currency"], "CHF")
	}
}

// TestNodePublishGoConsumes is the reverse: an event the Node SDK encoded is
// decoded by the Go SDK into the type its subscription was declared with.
func TestNodePublishGoConsumes(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	name := uniqueName("xlang.node2go.event")
	got := &collector{}

	subscriber := newClient(t, domainXLang, 1)
	if err := servicebridge.SubscribeEvent(subscriber, name, func(_ context.Context, e *e2epb.OrderEvent) error {
		got.add(e)
		return nil
	}); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	start(ctx, t, subscriber)

	cfg := newAgentConfig(t)
	cfg.PublishEvents = []string{name}
	agent := startNodeAgent(ctx, t, cfg)

	publishCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err := agent.publish(publishCtx, name, map[string]any{
		"orderId": "xlang-2", "amount": 7.25, "currency": "SEK",
	})
	if err != nil {
		t.Fatalf("the Node agent could not publish: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}

	waitFor(ctx, t, deliveryTimeout, "the Node-published event to reach the Go subscriber",
		func(context.Context) (bool, error) { return got.len() > 0, nil })

	order := got.snapshot()[0]
	if order.GetOrderId() != "xlang-2" {
		t.Errorf("order id decoded as %q, want %q", order.GetOrderId(), "xlang-2")
	}
	if order.GetAmount() != 7.25 {
		t.Errorf("amount decoded as %v, want 7.25", order.GetAmount())
	}
	if order.GetCurrency() != "SEK" {
		t.Errorf("currency decoded as %q, want %q", order.GetCurrency(), "SEK")
	}
}

// opRow reads the one operation row a subject identifies. Subjects here are
// minted per test, so more than one row means an earlier run leaked into this
// one and every conclusion drawn from the rows would be about the wrong call.
func opRow(ctx context.Context, t *testing.T, subject string) map[string]any {
	t.Helper()
	rows := waitRows(ctx, t, rowTimeout, "the operation for "+subject, fmt.Sprintf(
		`SELECT trace_id::text AS trace_id, op_id::text AS op_id,
		        coalesce(parent_op_id::text, '') AS parent_op_id, channel, kind
		   FROM operations WHERE subject = %s`, lit(t, subject)), 1)
	if len(rows) != 1 {
		t.Fatalf("subject %s matches %d operation rows, want exactly 1", subject, len(rows))
	}
	return rows[0]
}
