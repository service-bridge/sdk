package connection_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// baselineGoroutines settles the runtime and reports the count to compare
// against after the lifecycle is stopped.
func baselineGoroutines(t *testing.T) int {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	return runtime.NumGoroutine()
}

func assertNoLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		got := runtime.NumGoroutine()
		if got <= before {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutines leaked: %d before, %d after\n%s", before, got, buf)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStreamDeathReconnects(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.start()

	first := h.control.awaitOpen(t)
	conns, _ := h.dialer.dialed()
	first.die(t, status.Error(codes.Unavailable, "runtime restarted"))

	waitFor(t, "a second session", func() bool { return len(h.observer.connectedIDs()) == 2 })
	if h.observer.reconnectCount() == 0 {
		t.Error("Reconnecting never fired")
	}
	if got := h.observer.connectedIDs()[1].SessionID; got != "session-2" {
		t.Errorf("second session id: %q", got)
	}
	assertClosed(t, conns[0], "the channel of the dead session")

	for _, name := range consumerNames {
		if got := h.consumers[name].count(); got != 2 {
			t.Errorf("consumer %s got %d credential updates, want 2", name, got)
		}
	}
}

func TestReconnectReusesTheCachedLeaf(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.start()

	for i := 0; i < 3; i++ {
		call := h.control.awaitOpen(t)
		call.die(t, status.Error(codes.Unavailable, "transport closing"))
		want := i + 2
		waitFor(t, "session after a transport break", func() bool { return len(h.observer.connectedIDs()) == want })
	}

	// Bootstrap.Provision is the 64 MiB argon2id path on the runtime. Calling it
	// per transport reconnect turns a reconnect storm into a self-inflicted DoS.
	if got := h.prov.count(); got != 1 {
		t.Errorf("Provision called %d times across 3 reconnects, want 1", got)
	}
	registrars := h.registrars.registrars()
	if len(registrars) != 4 {
		t.Errorf("registrars built: %d, want one per session (4)", len(registrars))
	}
	if starts, _ := h.inbound.counts(); starts != 1 {
		t.Errorf("inbound rebound %d times: peers would be dialling a stale endpoint", starts)
	}
}

func TestReconnectReprovisionsAnExpiringLeaf(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	// Inside the renewal window: the cached leaf is no longer worth reusing.
	h.prov.setNotAfter(time.Now().Add(10 * time.Minute))
	h.start()

	call := h.control.awaitOpen(t)
	call.die(t, status.Error(codes.Unavailable, "transport closing"))
	waitFor(t, "the second session", func() bool { return len(h.observer.connectedIDs()) == 2 })

	if got := h.prov.count(); got != 2 {
		t.Errorf("Provision called %d times, want 2 for a leaf inside the renewal window", got)
	}
}

func TestNonRetryableStreamCodeStopsTheLadder(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.start()

	call := h.control.awaitOpen(t)
	call.die(t, status.Error(codes.PermissionDenied, "policy rejects this identity"))

	waitFor(t, "Disconnected", func() bool { return len(h.observer.disconnects()) == 1 })
	cause := h.observer.disconnects()[0]
	if status.Code(cause) != codes.PermissionDenied {
		t.Errorf("cause reported outward: %v", cause)
	}

	time.Sleep(200 * time.Millisecond) // several ladder rungs
	if got := h.control.openCount(); got != 1 {
		t.Errorf("Control.Open called %d times after a terminal code, want 1", got)
	}
	if got := h.observer.reconnectCount(); got != 0 {
		t.Errorf("Reconnecting fired %d times after a terminal code", got)
	}
	if _, err := h.life.Conn(); err == nil {
		t.Error("a lifecycle that gave up still reports a live session")
	}
}

func TestNonRetryableProvisionErrorStopsTheLadder(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.prov.setNotAfter(time.Now().Add(10 * time.Minute)) // forces a reprovision
	h.start()

	h.prov.setError(status.Error(codes.Unauthenticated, "bootstrap key revoked"))
	call := h.control.awaitOpen(t)
	call.die(t, status.Error(codes.Unavailable, "transport closing"))

	waitFor(t, "Disconnected", func() bool { return len(h.observer.disconnects()) == 1 })
	if code := status.Code(h.observer.disconnects()[0]); code != codes.Unauthenticated {
		t.Errorf("cause code: got %s want Unauthenticated", code)
	}
	if got := h.prov.count(); got != 2 {
		t.Errorf("Provision called %d times, want 2: the revoked key must not be retried", got)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(cfg *connection.LifecycleConfig) { cfg.MaxAttempts = 2 })
	h.start()

	h.dialer.setFail(errors.New("no route to the runtime"))
	call := h.control.awaitOpen(t)
	call.die(t, status.Error(codes.Unavailable, "transport closing"))

	waitFor(t, "Disconnected", func() bool { return len(h.observer.disconnects()) == 1 })
	if got := h.observer.reconnectCount(); got != 2 {
		t.Errorf("attempts before giving up: got %d want 2", got)
	}
}

func TestStopDuringAnInFlightConnect(t *testing.T) {
	t.Parallel()
	before := baselineGoroutines(t)
	h := newHarness(t, func(cfg *connection.LifecycleConfig) {
		// Long enough that only Stop can end the wait.
		cfg.WelcomeTimeout = 30 * time.Second
	})
	h.control.mu.Lock()
	h.control.welcomes = 0
	h.control.mu.Unlock()

	started := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		started <- h.life.Start(ctx)
	}()

	h.control.awaitOpen(t)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.life.Stop(stopCtx); err != nil {
		t.Fatalf("Stop during an in-flight connect: %v", err)
	}

	select {
	case err := <-started:
		if err == nil {
			t.Fatal("Start succeeded after Stop")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start never returned after Stop")
	}

	conns, _ := h.dialer.dialed()
	for _, conn := range conns {
		assertClosed(t, conn, "channel opened by the aborted connect")
	}
	if len(conns) != 1 {
		t.Errorf("dialed %d channels during one aborted connect", len(conns))
	}
	if _, closes := h.inbound.counts(); closes != 1 {
		t.Errorf("inbound closed %d times after an aborted start", closes)
	}
	if _, err := h.life.Conn(); err == nil {
		t.Error("a stopped lifecycle reports a live session")
	}
	assertNoLeak(t, before)
}

func TestStartRefusedAfterStop(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.life.Stop(ctx); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
	err := h.life.Start(ctx)
	if err == nil {
		t.Fatal("a stopped lifecycle started")
	}
	if !errors.Is(err, connection.ErrSession) {
		t.Errorf("kind: got %v want ErrSession", err)
	}
	if h.control.openCount() != 0 {
		t.Error("a stopped lifecycle opened a control stream")
	}
	if starts, _ := h.inbound.counts(); starts != 0 {
		t.Error("a stopped lifecycle bound the inbound listener")
	}
}

func TestFullCycleLeavesNoGoroutines(t *testing.T) {
	t.Parallel()
	before := baselineGoroutines(t)

	h := newHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.life.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	call := h.control.awaitOpen(t)
	call.die(t, status.Error(codes.Unavailable, "transport closing"))
	waitFor(t, "the second session", func() bool { return len(h.observer.connectedIDs()) == 2 })

	if err := h.life.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertNoLeak(t, before)
}
