//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// The guarantees of the runtime-interpreted DAG with per-step leases (runtime
// ADR 0003), mirrored from the Node suite so both SDKs prove the same
// behaviour against the same runtime.

func terminal(status string) bool {
	switch status {
	case "success", "failed", "cancelled", "timed_out", "failed_compensated":
		return true
	}
	return false
}

func awaitStatus(ctx context.Context, t *testing.T, c *servicebridge.Client, runID string) servicebridge.RunSnapshot {
	t.Helper()
	var snap servicebridge.RunSnapshot
	waitFor(ctx, t, deliveryTimeout, "run "+runID+" to finish", func(ctx context.Context) (bool, error) {
		got, err := c.Workflow.Query(ctx, runID)
		if err != nil {
			return false, err
		}
		snap = got
		return terminal(got.Status), nil
	})
	return snap
}

func awaitParked(ctx context.Context, t *testing.T, c *servicebridge.Client, runID string) {
	t.Helper()
	waitFor(ctx, t, deliveryTimeout, "run "+runID+" to park", func(ctx context.Context) (bool, error) {
		got, err := c.Workflow.Query(ctx, runID)
		if err != nil {
			return false, err
		}
		switch got.WaitingReason {
		case "sleep", "signal", "event", "child":
			return got.Status == "active", nil
		}
		return false, nil
	})
}

func stepOf(snap servicebridge.RunSnapshot, id string) servicebridge.StepSnapshot {
	for _, s := range snap.Steps {
		if s.StepID == id {
			return s
		}
	}
	return servicebridge.StepSnapshot{}
}

func owner1() string { return serviceName(domainWorkflow, 1) }

// A parked sibling waking up never re-executes a live branch (WF-01).
func TestWorkflowParkedSiblingDoesNotFenceLiveBranch(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	name := uniqueName("go.wf.parlive")
	release := make(chan struct{})
	var executions atomic.Int32

	c := newClient(t, domainWorkflow, 1)
	if err := c.Workflow.Handle(name, wf.Definition{Version: "v1", Steps: []wf.Step{
		wf.Sleep{Control: wf.Control{ID: "nap"}, Duration: 300 * time.Millisecond},
		wf.Local{Control: wf.Control{ID: "work"}, Fn: func(ctx context.Context, _ map[string]any) (any, error) {
			executions.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return "done", nil
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, c)
	runID, err := c.Workflow.Start(ctx, owner1(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(ctx, t, deliveryTimeout, "sleep to fire", func(ctx context.Context) (bool, error) {
		snap, err := c.Workflow.Query(ctx, runID)
		return err == nil && stepOf(snap, "nap").Status == "success", err
	})
	close(release)
	snap := awaitStatus(ctx, t, c, runID)
	if snap.Status != "success" || executions.Load() != 1 || stepOf(snap, "work").Attempt != 1 {
		t.Fatalf("status=%s executions=%d attempt=%d", snap.Status, executions.Load(), stepOf(snap, "work").Attempt)
	}
}

// Two parallel wait_event steps both receive their events (WF-02).
func TestWorkflowParallelWaitEventsBothDelivered(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	name := uniqueName("go.wf.parevents")
	evA, evB := uniqueName("go.wf.ev.a"), uniqueName("go.wf.ev.b")

	c := newClient(t, domainWorkflow, 1)
	if err := c.Workflow.Handle(name, wf.Definition{Version: "v1", Steps: []wf.Step{
		wf.WaitEvent{Control: wf.Control{ID: "a"}, Event: evA},
		wf.WaitEvent{Control: wf.Control{ID: "b"}, Event: evB, Filter: map[string]any{"$.orderId": wf.Path("$.input.id")}},
	}}); err != nil {
		t.Fatal(err)
	}
	a, err := servicebridge.DefineEvent[*e2epb.OrderEvent](c, evA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := servicebridge.DefineEvent[*e2epb.OrderEvent](c, evB)
	if err != nil {
		t.Fatal(err)
	}
	start(ctx, t, c)
	runID, err := c.Workflow.Start(ctx, owner1(), name, map[string]any{"id": "o-7"})
	if err != nil {
		t.Fatal(err)
	}
	awaitParked(ctx, t, c, runID)
	if _, err := b.Publish(ctx, &e2epb.OrderEvent{OrderId: "other", Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, &e2epb.OrderEvent{OrderId: "o-7", Currency: "EUR"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Publish(ctx, &e2epb.OrderEvent{OrderId: "x", Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	out, err := c.Workflow.Await(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if got := out["b"].(map[string]any)["orderId"]; got != "o-7" {
		t.Fatalf("b received %v", out["b"])
	}
	if got := out["a"].(map[string]any)["currency"]; got != "USD" {
		t.Fatalf("a received %v", out["a"])
	}
}

// An owner instance dying while the run is parked does not strand it (WF-03).
func TestWorkflowParkSurvivesOwnerInstance(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)
	name := uniqueName("go.wf.parkcrash")
	var ranOn sync.Map
	def := func(who string) wf.Definition {
		return wf.Definition{Version: "v1", Steps: []wf.Step{
			wf.WaitSignal{Control: wf.Control{ID: "wait"}, Signal: "go"},
			wf.Local{Control: wf.Control{ID: "tail", WaitFor: []string{"wait"}}, Fn: func(context.Context, map[string]any) (any, error) {
				ranOn.Store("tail", who)
				return who, nil
			}},
		}}
	}
	first := newClient(t, domainWorkflow, 1)
	if err := first.Workflow.Handle(name, def("first")); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, first)
	runID, err := first.Workflow.Start(ctx, owner1(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	awaitParked(ctx, t, first, runID)
	stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := first.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}

	second := newClient(t, domainWorkflow, 1)
	if err := second.Workflow.Handle(name, def("second")); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, second)
	if _, err := second.Workflow.Signal(ctx, runID, "go", map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	if snap := awaitStatus(ctx, t, second, runID); snap.Status != "success" {
		t.Fatalf("status %s", snap.Status)
	}
	if who, _ := ranOn.Load("tail"); who != "second" {
		t.Fatalf("tail ran on %v", who)
	}
}

// Signals are a queue with signal_id dedup (decision 5b).
func TestWorkflowSignalsQueueAndDedup(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	name := uniqueName("go.wf.signals")
	c := newClient(t, domainWorkflow, 1)
	if err := c.Workflow.Handle(name, wf.Definition{Version: "v1", Steps: []wf.Step{
		wf.WaitSignal{Control: wf.Control{ID: "first"}, Signal: "approve"},
		wf.WaitSignal{Control: wf.Control{ID: "second", WaitFor: []string{"first"}}, Signal: "approve"},
	}}); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, c)
	runID, err := c.Workflow.Start(ctx, owner1(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if dup, err := c.Workflow.Signal(ctx, runID, "approve", map[string]any{"n": 1}, servicebridge.WithSignalID("s-1")); err != nil || dup {
		t.Fatalf("first signal: dup=%v err=%v", dup, err)
	}
	if dup, err := c.Workflow.Signal(ctx, runID, "approve", map[string]any{"n": 1}, servicebridge.WithSignalID("s-1")); err != nil || !dup {
		t.Fatalf("resend: dup=%v err=%v", dup, err)
	}
	if _, err := c.Workflow.Signal(ctx, runID, "approve", map[string]any{"n": 2}); err != nil {
		t.Fatal(err)
	}
	out, err := c.Workflow.Await(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(out["first"]) != "map[n:1]" || fmt.Sprint(out["second"]) != "map[n:2]" {
		t.Fatalf("output %v", out)
	}
}

// A service that is neither owner, starter nor granted is refused (SEC-01);
// the run timeout ends timed_out (WF-16).
func TestWorkflowStrangerRefusedAndTimeout(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	name := uniqueName("go.wf.stranger")
	c := newClient(t, domainWorkflow, 1)
	if err := c.Workflow.Handle(name, wf.Definition{Version: "v1", Steps: []wf.Step{
		wf.WaitSignal{Control: wf.Control{ID: "w"}, Signal: "never"},
	}}); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, c)
	stranger := newClient(t, domainWorkflow, 3)
	start(ctx, t, stranger)

	runID, err := c.Workflow.Start(ctx, owner1(), name, nil, servicebridge.WithRunTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stranger.Workflow.Query(ctx, runID); !errors.Is(err, servicebridge.ErrAccessDenied) {
		t.Fatalf("stranger query: %v", err)
	}
	if _, err := stranger.Workflow.Signal(ctx, runID, "never", nil); !errors.Is(err, servicebridge.ErrAccessDenied) {
		t.Fatalf("stranger signal: %v", err)
	}
	if err := stranger.Workflow.Cancel(ctx, runID); !errors.Is(err, servicebridge.ErrAccessDenied) {
		t.Fatalf("stranger cancel: %v", err)
	}
	if snap := awaitStatus(ctx, t, c, runID); snap.Status != "timed_out" {
		t.Fatalf("status %s", snap.Status)
	}
	_, err = c.Workflow.Await(ctx, runID)
	var failed *servicebridge.RunFailedError
	if !errors.As(err, &failed) || failed.Status != "timed_out" {
		t.Fatalf("await: %v", err)
	}
}

// Cancelling a parent cancels its unfinished child (WF-16).
func TestWorkflowCancelCascadesToChild(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	parent, child := uniqueName("go.wf.parent"), uniqueName("go.wf.child")
	c := newClient(t, domainWorkflow, 1)
	if err := c.Workflow.Handle(parent, wf.Definition{Version: "v1", Steps: []wf.Step{
		wf.SubWorkflow{Control: wf.Control{ID: "kid"}, Workflow: wf.Name(child)},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Workflow.Handle(child, wf.Definition{Version: "v1", Steps: []wf.Step{
		wf.WaitSignal{Control: wf.Control{ID: "w"}, Signal: "never"},
	}}); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, c)
	runID, err := c.Workflow.Start(ctx, owner1(), parent, nil)
	if err != nil {
		t.Fatal(err)
	}
	var kid string
	waitFor(ctx, t, deliveryTimeout, "child run", func(ctx context.Context) (bool, error) {
		snap, err := c.Workflow.Query(ctx, runID)
		kid = stepOf(snap, "kid").ChildRunID
		return err == nil && kid != "", err
	})
	if err := c.Workflow.Cancel(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if snap := awaitStatus(ctx, t, c, runID); snap.Status != "cancelled" {
		t.Fatalf("parent %s", snap.Status)
	}
	if snap := awaitStatus(ctx, t, c, kid); snap.Status != "cancelled" {
		t.Fatalf("child %s", snap.Status)
	}
}

// A failing compensation ends failed_compensated and RetryCompensation
// recovers it (WF-04).
func TestWorkflowFailedCompensationRetry(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)
	reserve, release := uniqueName("go.wf.reserve"), uniqueName("go.wf.release")
	name := uniqueName("go.wf.comp")
	callee := serviceName(domainWorkflow, 2)
	var releaseFailures atomic.Int32
	releaseFailures.Store(1)

	provider := newClient(t, domainWorkflow, 2, servicebridge.WithAdvertise("127.0.0.1", 0))
	if err := servicebridge.Handle(provider, reserve, func(_ context.Context, req *e2epb.Echo) (*e2epb.EchoReply, error) {
		return &e2epb.EchoReply{Text: "rsv-" + req.GetText(), HandledBy: "go"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := servicebridge.Handle(provider, release, func(_ context.Context, _ *e2epb.Echo) (*e2epb.EchoReply, error) {
		if releaseFailures.Add(-1) >= 0 {
			return nil, errors.New("release is down")
		}
		return &e2epb.EchoReply{HandledBy: "go"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, provider)

	owner := newClient(t, domainWorkflow, 1)
	for _, m := range []string{reserve, release} {
		if _, err := servicebridge.NewMethod[*e2epb.Echo, *e2epb.EchoReply](servicebridge.NewClient(owner, callee), m); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Workflow.Handle(name, wf.Definition{Version: "v1", Steps: []wf.Step{
		wf.Call{
			Control: wf.Control{ID: "reserve", Compensate: &wf.Compensation{
				Method: wf.Name(release), Input: map[string]any{"text": wf.Path("$.reserve.text")},
			}},
			Service: wf.Name(callee), Method: wf.Name(reserve), Input: map[string]any{"text": "item"},
		},
		wf.Local{Control: wf.Control{ID: "boom", WaitFor: []string{"reserve"}}, Fn: func(context.Context, map[string]any) (any, error) {
			return nil, errors.New("declined")
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	start(ctx, t, owner)
	waitForMethod(ctx, t, owner, callee, reserve)
	waitForMethod(ctx, t, owner, callee, release)

	runID, err := owner.Workflow.Start(ctx, owner1(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if snap := awaitStatus(ctx, t, owner, runID); snap.Status != "failed_compensated" {
		t.Fatalf("status %s (%s)", snap.Status, snap.ErrorMessage)
	}
	if err := owner.Workflow.RetryCompensation(ctx, runID); err != nil {
		t.Fatal(err)
	}
	snap := awaitStatus(ctx, t, owner, runID)
	if snap.Status != "failed" || stepOf(snap, "reserve").Status != "compensated" {
		t.Fatalf("after retry: %s reserve=%s", snap.Status, stepOf(snap, "reserve").Status)
	}
}
