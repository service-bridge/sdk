//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// TestConnectPublishesIdentity proves the control-plane handshake completed:
// the runtime only answers Welcome with a session, a service and an instance
// once it has accepted the provisioned certificate and the registration.
func TestConnectPublishesIdentity(t *testing.T) {
	ctx := testContext(t, time.Minute)

	c := newClient(t, domainMisc, 1)
	start(ctx, t, c)

	id := c.Identity()
	if id.SessionID == "" {
		t.Error("session id is empty after a successful start")
	}
	if id.ServiceID == "" {
		t.Error("service id is empty after a successful start")
	}
	if id.InstanceID == "" {
		t.Error("instance id is empty after a successful start")
	}
	if want := serviceName(domainMisc, 1); id.ServiceName != want {
		t.Errorf("service name is %q, want %q — the key belongs to a different service row", id.ServiceName, want)
	}
}

// TestHandlerReachesPeerServiceMap proves registration propagated: what one
// instance declared before Start is what another instance discovers, down to
// the contract hash that decides routing.
func TestHandlerReachesPeerServiceMap(t *testing.T) {
	ctx := testContext(t, time.Minute)

	method := uniqueName("map.echo")
	callee := serviceName(domainRPC, 1)

	provider := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	if err := servicebridge.Handle(provider, method, echoHandler("provider")); err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	start(ctx, t, provider)

	observer := newClient(t, domainRPC, 2)
	if err := observer.Service(callee, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, observer)

	waitForMethod(ctx, t, observer, callee, method)

	var found servicebridge.MethodInfo
	for _, m := range observer.ServiceMap().Methods {
		if m.ServiceName == callee && m.Name == method {
			found = m
			break
		}
	}
	if found.ContractHash == "" {
		t.Fatalf("discovered method %s carries no contract hash: routing has nothing to match on", method)
	}
	if found.InstanceID != provider.Identity().InstanceID {
		t.Errorf("method is advertised by instance %q, but the provider is %q",
			found.InstanceID, provider.Identity().InstanceID)
	}
	if found.Streaming {
		t.Error("a unary handler is advertised as streaming")
	}

	var providerListed bool
	for _, inst := range observer.ServiceMap().Instances {
		if inst.InstanceID == provider.Identity().InstanceID {
			providerListed = true
			if inst.CallEndpoint == "" {
				t.Error("the provider advertises no call endpoint: a direct call has nowhere to dial")
			}
		}
	}
	if !providerListed {
		t.Error("the provider instance is missing from the observer's mesh view")
	}
}

// echoHandler answers with the request's own fields plus a marker naming which
// registration served the call. Tests that route between competing handlers
// read the marker back.
func echoHandler(marker string) func(context.Context, *e2epb.Echo) (*e2epb.EchoReply, error) {
	return func(_ context.Context, req *e2epb.Echo) (*e2epb.EchoReply, error) {
		return &e2epb.EchoReply{Text: req.GetText(), N: req.GetN(), HandledBy: marker}, nil
	}
}
