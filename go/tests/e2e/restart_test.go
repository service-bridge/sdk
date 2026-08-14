//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// The Node SDK has dedicated lifecycle e2e tests (node/tests/e2e/*-lifecycle.
// test.ts, via _helpers/dedicated-runtime.ts) that kill and restart the
// runtime and prove the SDK reconnects on its own. The Go suite has no
// equivalent anywhere: connect_test.go proves the first handshake, but nothing
// proves a live Go client survives the runtime going away and coming back —
// cert rotation and session supervision are the most expensive part of this
// SDK, and a regression there is invisible without a test that actually kills
// the process out from under a connected client.
//
// This test spawns its own isolated runtime (own DB, own ports) via
// dedicated_runtime_test.go so killing it cannot affect the ambient runtime
// this whole suite, or any other agent working in the repository, depends on.

func TestClientReconnectsAfterRuntimeRestart(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	binary := t.TempDir() + "/sb-runtime-restart-test"
	buildDedicatedRuntimeBinary(t, binary)

	rt := spawnDedicatedRuntime(ctx, t, spawnDedicatedRuntimeOpts{
		Name:       "restart",
		GRPCPort:   25445,
		UIPort:     25444,
		BinaryPath: binary,
	})
	t.Cleanup(rt.Cleanup)

	key := bootstrapKey(t, domainMisc, 3)
	method := uniqueName("restart.echo")

	connected := make(chan servicebridge.Identity, 8)
	reconnecting := make(chan struct {
		attempt int
		cause   error
	}, 8)
	disconnected := make(chan error, 8)

	c, err := servicebridge.New(rt.URL, key,
		servicebridge.WithDataDir(t.TempDir()),
		servicebridge.WithLogger(logger(t)),
		servicebridge.WithReconnectAttempts(30),
		servicebridge.WithReconnectLadder(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	c.OnConnected(func(id servicebridge.Identity) { connected <- id })
	c.OnReconnecting(func(attempt int, cause error) {
		reconnecting <- struct {
			attempt int
			cause   error
		}{attempt, cause}
	})
	c.OnDisconnected(func(cause error) { disconnected <- cause })

	if err := servicebridge.Handle(c, method, func(_ context.Context, req *e2epb.Echo) (*e2epb.EchoReply, error) {
		return &e2epb.EchoReply{Text: req.GetText(), N: req.GetN(), HandledBy: "restarted-instance"}, nil
	}); err != nil {
		t.Fatalf("declare handler: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	if err := c.Start(startCtx); err != nil {
		t.Fatalf("start client: %v", err)
	}
	cancel()
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		_ = c.Stop(stopCtx)
	})

	var firstIdentity servicebridge.Identity
	select {
	case firstIdentity = <-connected:
	case <-time.After(20 * time.Second):
		t.Fatal("client never reported connected before the runtime was killed")
	}
	if firstIdentity.SessionID == "" || firstIdentity.InstanceID == "" {
		t.Fatalf("first identity is incomplete: %+v", firstIdentity)
	}

	rt.Kill()

	select {
	case rc := <-reconnecting:
		if rc.attempt < 1 {
			t.Errorf("reconnecting event carries attempt %d, want >= 1", rc.attempt)
		}
	case cause := <-disconnected:
		t.Fatalf("client gave up (disconnected: %v) before reconnect attempts were exhausted — "+
			"reconnectAttempts=30 should have covered the restart gap", cause)
	case <-time.After(20 * time.Second):
		t.Fatal("client never noticed the runtime was gone (no reconnecting event within 20s)")
	}

	rt.Restart(ctx)

	select {
	case id := <-connected:
		if id.SessionID == "" || id.InstanceID == "" {
			t.Fatalf("post-restart identity is incomplete: %+v", id)
		}
		if id.ServiceID != firstIdentity.ServiceID {
			t.Errorf("service id changed across reconnect: before=%s after=%s", firstIdentity.ServiceID, id.ServiceID)
		}
	case cause := <-disconnected:
		t.Fatalf("client gave up reconnecting instead of recovering once the runtime came back: %v", cause)
	case <-time.After(40 * time.Second):
		t.Fatal("client never reconnected within 40s of the runtime restarting")
	}

	// Incoming calls are served again: a fresh caller against the restarted
	// runtime has to discover the handler and reach it, proving the client's
	// registration survived the round trip, not just its control stream.
	caller, err := servicebridge.New(rt.URL, key,
		servicebridge.WithDataDir(t.TempDir()),
		servicebridge.WithLogger(logger(t)),
		servicebridge.WithReconnectAttempts(3),
		servicebridge.WithAdvertise("127.0.0.1", 0),
	)
	if err != nil {
		t.Fatalf("new caller client: %v", err)
	}
	selfName := serviceName(domainMisc, 3)
	if err := caller.Service(selfName, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	callerCtx, callerCancel := context.WithTimeout(ctx, connectTimeout)
	if err := caller.Start(callerCtx); err != nil {
		t.Fatalf("start caller: %v", err)
	}
	callerCancel()
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		_ = caller.Stop(stopCtx)
	})
	waitForMethod(ctx, t, caller, selfName, method)

	resp, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](ctx, caller, selfName, method,
		&e2epb.Echo{Text: "post-restart", N: 1}, servicebridge.WithTimeout(20*time.Second))
	if err != nil {
		t.Fatalf("call the reconnected instance's handler: %v", err)
	}
	if resp.GetHandledBy() != "restarted-instance" {
		t.Errorf("reply says handledBy %q, want %q", resp.GetHandledBy(), "restarted-instance")
	}
	if resp.GetText() != "post-restart" {
		t.Errorf("reply text is %q, want %q", resp.GetText(), "post-restart")
	}
}
