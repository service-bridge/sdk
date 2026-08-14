package registry_test

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
)

// runtimeRejections mirrors the InvalidArgument gate in
// runtime/internal/registry/server.go: RegisterAndWatch. Every rule listed here
// takes the whole registration down on the server, so a frame the builder
// produces must never trip one.
func runtimeRejections(req *pb.RegisterRequest) []string {
	var bad []string
	for _, dep := range req.GetOutgoing() {
		switch dep.GetType() {
		case pb.MethodType_METHOD_TYPE_EVENT, pb.MethodType_METHOD_TYPE_JOB:
			bad = append(bad, fmt.Sprintf("outgoing %q has forbidden type %s", dep.GetMethodName(), dep.GetType()))
		}
	}
	for _, m := range req.GetIncoming() {
		switch m.GetType() {
		case pb.MethodType_METHOD_TYPE_EVENT:
			bad = append(bad, fmt.Sprintf("incoming %q is an event subscription", m.GetName()))
		case pb.MethodType_METHOD_TYPE_JOB, pb.MethodType_METHOD_TYPE_HTTP:
			if len(m.GetOutputSchemaJson()) > 0 {
				bad = append(bad, fmt.Sprintf("incoming %q (%s) declares output_schema_json", m.GetName(), m.GetType()))
			}
		}
		if m.GetType() == pb.MethodType_METHOD_TYPE_JOB && len(m.GetInputSchemaJson()) == 0 {
			bad = append(bad, fmt.Sprintf("job %q carries no canonical spec", m.GetName()))
		}
	}
	seenSubs := make(map[string]int)
	for _, s := range req.GetEventSubscriptions() {
		seenSubs[s.GetPattern()]++
		if seenSubs[s.GetPattern()] > 1 {
			bad = append(bad, fmt.Sprintf("duplicate subscription pattern %q breaks (subscriber_id, pattern)", s.GetPattern()))
		}
	}
	return bad
}

// TestBuildRegisterRequestNeverProducesAFrameTheRuntimeRejects is the contract
// test: whatever sequence of accepted declarations the application writes, the
// assembled frame passes every server-side gate.
func TestBuildRegisterRequestNeverProducesAFrameTheRuntimeRejects(t *testing.T) {
	d := registry.NewDeclarations()

	mustAdd := func(spec registry.IncomingSpec) {
		t.Helper()
		if err := d.AddIncoming(spec); err != nil {
			t.Fatalf("add incoming %q: %v", spec.Name, err)
		}
	}
	mustAdd(registry.IncomingSpec{
		Type:             pb.MethodType_METHOD_TYPE_RPC,
		Name:             "charge",
		InputSchemaJSON:  []byte(`{"type":"object"}`),
		OutputSchemaJSON: []byte(`{"type":"object"}`),
		ContractHash:     "h1",
	})
	mustAdd(registry.IncomingSpec{
		Type:            pb.MethodType_METHOD_TYPE_JOB,
		Name:            "nightly-reconcile",
		InputSchemaJSON: []byte(`{"cron":"0 3 * * *"}`),
	})
	mustAdd(registry.IncomingSpec{
		Type:            pb.MethodType_METHOD_TYPE_WORKFLOW,
		Name:            "checkout",
		InputSchemaJSON: []byte(`{"steps":[]}`),
	})
	if err := d.AddHTTPRoute("GET", "/health"); err != nil {
		t.Fatalf("add http route: %v", err)
	}
	if err := d.PublishEvent("order.created", []byte(`{}`), "h2"); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if err := d.SubscribeEvent("order.*", true); err != nil {
		t.Fatalf("subscribe event: %v", err)
	}
	if err := d.AddOutgoing("billing", "charge", pb.MethodType_METHOD_TYPE_RPC); err != nil {
		t.Fatalf("add outgoing: %v", err)
	}

	if bad := runtimeRejections(d.BuildRegisterRequest()); len(bad) > 0 {
		t.Fatalf("assembled frame trips the runtime gate: %v", bad)
	}
}

func TestAddIncomingRejectsEventBecauseSubscriptionsTravelSeparately(t *testing.T) {
	d := registry.NewDeclarations()
	err := d.AddIncoming(registry.IncomingSpec{
		Type: pb.MethodType_METHOD_TYPE_EVENT,
		Name: "order.created",
	})
	if !errors.Is(err, registry.ErrEventAsIncoming) {
		t.Fatalf("got %v, want ErrEventAsIncoming", err)
	}
	if got := len(d.BuildRegisterRequest().GetIncoming()); got != 0 {
		t.Fatalf("rejected declaration still reached the frame (%d incoming)", got)
	}
}

func TestAddIncomingRejectsOutputSchemaOnJobAndHTTP(t *testing.T) {
	for _, typ := range []pb.MethodType{
		pb.MethodType_METHOD_TYPE_JOB,
		pb.MethodType_METHOD_TYPE_HTTP,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			d := registry.NewDeclarations()
			err := d.AddIncoming(registry.IncomingSpec{
				Type:             typ,
				Name:             "thing",
				InputSchemaJSON:  []byte(`{}`),
				OutputSchemaJSON: []byte(`{"type":"object"}`),
			})
			if !errors.Is(err, registry.ErrOutputSchema) {
				t.Fatalf("got %v, want ErrOutputSchema", err)
			}
			if got := len(d.BuildRegisterRequest().GetIncoming()); got != 0 {
				t.Fatalf("rejected declaration still reached the frame (%d incoming)", got)
			}
		})
	}
}

// TestAddIncomingRejectsJobWithoutCanonicalSpec pins where a job's schedule
// lives: the runtime reads it out of input_schema_json, so an empty one is not
// "no schema" but "no job".
func TestAddIncomingRejectsJobWithoutCanonicalSpec(t *testing.T) {
	d := registry.NewDeclarations()
	err := d.AddIncoming(registry.IncomingSpec{
		Type: pb.MethodType_METHOD_TYPE_JOB,
		Name: "nightly",
	})
	if !errors.Is(err, registry.ErrEmptyJobSpec) {
		t.Fatalf("got %v, want ErrEmptyJobSpec", err)
	}
	if got := len(d.BuildRegisterRequest().GetIncoming()); got != 0 {
		t.Fatalf("rejected declaration still reached the frame (%d incoming)", got)
	}
}

func TestAddIncomingRejectsNamelessAndUntypedDeclarations(t *testing.T) {
	d := registry.NewDeclarations()
	if err := d.AddIncoming(registry.IncomingSpec{Type: pb.MethodType_METHOD_TYPE_RPC}); !errors.Is(err, registry.ErrEmptyName) {
		t.Fatalf("empty name: got %v, want ErrEmptyName", err)
	}
	if err := d.AddIncoming(registry.IncomingSpec{Name: "x"}); !errors.Is(err, registry.ErrUnspecifiedType) {
		t.Fatalf("unspecified type: got %v, want ErrUnspecifiedType", err)
	}
	if err := d.PublishEvent("", nil, ""); !errors.Is(err, registry.ErrEmptyName) {
		t.Fatalf("empty event name: got %v, want ErrEmptyName", err)
	}
	if err := d.SubscribeEvent("", false); !errors.Is(err, registry.ErrEmptyName) {
		t.Fatalf("empty pattern: got %v, want ErrEmptyName", err)
	}
	if err := d.AddOutgoing("", "m", pb.MethodType_METHOD_TYPE_RPC); !errors.Is(err, registry.ErrEmptyName) {
		t.Fatalf("empty target service: got %v, want ErrEmptyName", err)
	}
	if err := d.AddOutgoing("s", "", pb.MethodType_METHOD_TYPE_RPC); !errors.Is(err, registry.ErrEmptyName) {
		t.Fatalf("empty target method: got %v, want ErrEmptyName", err)
	}
	if err := d.AddHTTPRoute("", "/x"); !errors.Is(err, registry.ErrEmptyHTTPPattern) {
		t.Fatalf("empty http method: got %v, want ErrEmptyHTTPPattern", err)
	}
	if err := d.AddHTTPRoute("GET", ""); !errors.Is(err, registry.ErrEmptyHTTPPattern) {
		t.Fatalf("empty http pattern: got %v, want ErrEmptyHTTPPattern", err)
	}

	req := d.BuildRegisterRequest()
	if len(req.GetIncoming())+len(req.GetPublished())+len(req.GetOutgoing())+len(req.GetEventSubscriptions()) != 0 {
		t.Fatalf("rejected declarations reached the frame: %v", req)
	}
}

// TestAddOutgoingRejectsEventAndJobDependencies guards the asymmetry that is
// easy to get wrong: an event or a job is delivered *to* the service by the
// runtime, so it can never be an outgoing dependency.
func TestAddOutgoingRejectsEventAndJobDependencies(t *testing.T) {
	for _, typ := range []pb.MethodType{
		pb.MethodType_METHOD_TYPE_EVENT,
		pb.MethodType_METHOD_TYPE_JOB,
		pb.MethodType_METHOD_TYPE_UNSPECIFIED,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			d := registry.NewDeclarations()
			if err := d.AddOutgoing("billing", "charge", typ); !errors.Is(err, registry.ErrOutgoingType) {
				t.Fatalf("got %v, want ErrOutgoingType", err)
			}
			if got := len(d.BuildRegisterRequest().GetOutgoing()); got != 0 {
				t.Fatalf("rejected dependency still reached the frame (%d outgoing)", got)
			}
		})
	}
}

func TestAddOutgoingAcceptsRPCWorkflowAndHTTP(t *testing.T) {
	d := registry.NewDeclarations()
	for _, typ := range []pb.MethodType{
		pb.MethodType_METHOD_TYPE_RPC,
		pb.MethodType_METHOD_TYPE_WORKFLOW,
		pb.MethodType_METHOD_TYPE_HTTP,
	} {
		if err := d.AddOutgoing("billing", "charge", typ); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
	}
	if got := len(d.BuildRegisterRequest().GetOutgoing()); got != 3 {
		t.Fatalf("got %d outgoing deps, want 3 — the type is part of the identity", got)
	}
}

// TestSubscribeEventCollapsesDuplicatePatterns protects the registration from
// the server's PRIMARY KEY (subscriber_id, pattern): a second row for the same
// pattern rolls the whole registration back.
func TestSubscribeEventCollapsesDuplicatePatterns(t *testing.T) {
	d := registry.NewDeclarations()
	for range 3 {
		if err := d.SubscribeEvent("order.*", true); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}
	// A second handler on the same pattern may ask for a different durability;
	// the pattern is still one row on the wire.
	if err := d.SubscribeEvent("order.*", false); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := d.SubscribeEvent("payment.*", false); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	subs := d.BuildRegisterRequest().GetEventSubscriptions()
	if len(subs) != 2 {
		t.Fatalf("got %d subscriptions, want 2", len(subs))
	}
	if subs[0].GetPattern() != "order.*" || !subs[0].GetDurable() {
		t.Fatalf("first subscription %v lost the durability of the first declaration", subs[0])
	}
	if bad := runtimeRejections(d.BuildRegisterRequest()); len(bad) > 0 {
		t.Fatalf("duplicate subscriptions reached the frame: %v", bad)
	}
}

// TestAddOutgoingCollapsesDuplicateDependencies keeps the register frame from
// inflating: a generated client and a hand-written dependency declaration for
// the same target both land here.
func TestAddOutgoingCollapsesDuplicateDependencies(t *testing.T) {
	d := registry.NewDeclarations()
	for range 4 {
		if err := d.AddOutgoing("billing", "charge", pb.MethodType_METHOD_TYPE_RPC); err != nil {
			t.Fatalf("add outgoing: %v", err)
		}
	}
	if err := d.AddOutgoing("billing", "refund", pb.MethodType_METHOD_TYPE_RPC); err != nil {
		t.Fatalf("add outgoing: %v", err)
	}
	if err := d.AddOutgoing("shipping", "charge", pb.MethodType_METHOD_TYPE_RPC); err != nil {
		t.Fatalf("add outgoing: %v", err)
	}

	deps := d.BuildRegisterRequest().GetOutgoing()
	if len(deps) != 3 {
		t.Fatalf("got %d outgoing deps, want 3: %v", len(deps), deps)
	}
	if deps[0].GetServiceName() != "billing" || deps[0].GetMethodName() != "charge" {
		t.Fatalf("declaration order lost: first dep is %v", deps[0])
	}
}

func TestAddHTTPRouteDeclaresRouteWithoutTransportSchema(t *testing.T) {
	d := registry.NewDeclarations()
	if err := d.AddHTTPRoute("POST", "/orders/:id"); err != nil {
		t.Fatalf("add http route: %v", err)
	}

	incoming := d.BuildRegisterRequest().GetIncoming()
	if len(incoming) != 1 {
		t.Fatalf("got %d incoming, want 1", len(incoming))
	}
	got := incoming[0]
	if got.GetType() != pb.MethodType_METHOD_TYPE_HTTP {
		t.Fatalf("type %s, want HTTP", got.GetType())
	}
	if got.GetName() != "POST /orders/:id" {
		t.Fatalf("name %q, want %q", got.GetName(), "POST /orders/:id")
	}
	if len(got.GetInputSchemaJson()) != 0 || len(got.GetOutputSchemaJson()) != 0 || got.GetContractHash() != "" {
		t.Fatalf("http route carries transport metadata it must not: %v", got)
	}
}

// TestBuildRegisterRequestSnapshotsTheDeclarationSet matters because the watch
// rebuilds the request on every reopen while integrations keep declaring HTTP
// routes: a frame already in flight must not grow a row underneath it.
func TestBuildRegisterRequestSnapshotsTheDeclarationSet(t *testing.T) {
	d := registry.NewDeclarations()
	d.SetCallEndpoint("10.0.0.1:9000")
	d.SetHTTPEndpoint("10.0.0.1:8080")
	if err := d.AddHTTPRoute("GET", "/health"); err != nil {
		t.Fatalf("add http route: %v", err)
	}

	first := d.BuildRegisterRequest()
	if err := d.AddHTTPRoute("GET", "/metrics"); err != nil {
		t.Fatalf("add http route: %v", err)
	}
	second := d.BuildRegisterRequest()

	if len(first.GetIncoming()) != 1 {
		t.Fatalf("the in-flight frame grew to %d incoming", len(first.GetIncoming()))
	}
	if len(second.GetIncoming()) != 2 {
		t.Fatalf("the reopened frame has %d incoming, want 2", len(second.GetIncoming()))
	}
	if first.GetCallEndpoint() != "10.0.0.1:9000" || first.GetHttpEndpoint() != "10.0.0.1:8080" {
		t.Fatalf("endpoints lost: %v", first)
	}
}

// TestDeclarationsAreSafeForConcurrentDeclaration covers the wiring the SDK
// actually does: handlers are registered from wherever the application creates
// them while the watch may be rebuilding the frame.
func TestDeclarationsAreSafeForConcurrentDeclaration(t *testing.T) {
	d := registry.NewDeclarations()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := "rpc-" + strconv.Itoa(i)
			if err := d.AddIncoming(registry.IncomingSpec{
				Type: pb.MethodType_METHOD_TYPE_RPC,
				Name: name,
			}); err != nil {
				t.Errorf("add incoming: %v", err)
			}
			if err := d.SubscribeEvent("order.*", true); err != nil {
				t.Errorf("subscribe: %v", err)
			}
			if err := d.AddOutgoing("billing", "charge", pb.MethodType_METHOD_TYPE_RPC); err != nil {
				t.Errorf("add outgoing: %v", err)
			}
			d.SetCallEndpoint("10.0.0.1:9000")
			_ = d.BuildRegisterRequest()
		}()
	}
	wg.Wait()

	req := d.BuildRegisterRequest()
	if len(req.GetIncoming()) != 8 {
		t.Fatalf("got %d incoming, want 8", len(req.GetIncoming()))
	}
	if len(req.GetEventSubscriptions()) != 1 {
		t.Fatalf("got %d subscriptions, want 1 — dedup raced", len(req.GetEventSubscriptions()))
	}
	if len(req.GetOutgoing()) != 1 {
		t.Fatalf("got %d outgoing deps, want 1 — dedup raced", len(req.GetOutgoing()))
	}
}
