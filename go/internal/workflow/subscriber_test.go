package workflow_test

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	iwf "github.com/service-bridge/sdk/go/internal/workflow"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// staticGraphs is the local declaration registry: the only place a graph with
// live Go closures in it exists.
type staticGraphs struct {
	mu     sync.Mutex
	graphs map[string][]wf.Step
}

func (g *staticGraphs) Steps(name string) ([]wf.Step, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	steps, ok := g.graphs[name]
	return steps, ok
}

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (h *logSink) Enabled(context.Context, slog.Level) bool { return true }

func (h *logSink) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Level.String())
	b.WriteString(" ")
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" ")
		b.WriteString(a.String())
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, b.String())
	return nil
}

func (h *logSink) contains(fragment string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, line := range h.lines {
		if strings.Contains(line, fragment) {
			return true
		}
	}
	return false
}

func (h *logSink) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logSink) WithGroup(string) slog.Handler      { return h }

func fastBackoff() stream.Backoff {
	return stream.NewBackoff(stream.WithLadder(time.Millisecond), stream.WithJitterRatio(0))
}

type subscriberHarness struct {
	srv    *scriptedWorkflows
	sub    *iwf.Subscriber
	graphs *staticGraphs
	exec   *scriptedExec
	logs   *logSink
	id     *rotatingIdentity
}

func startSubscriber(t *testing.T, graphs map[string][]wf.Step) *subscriberHarness {
	t.Helper()

	srv, client := startWorkflows(t)
	id := &rotatingIdentity{service: "svc-1", instance: "inst-1"}
	ops := newCheckpoints(t, client, id)
	exec := &scriptedExec{}
	logs := &logSink{}

	runner, err := iwf.NewRunner(iwf.RunnerConfig{Ops: ops, Executor: exec, Logger: slog.New(logs)})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	source := &staticGraphs{graphs: graphs}
	sub, err := iwf.NewSubscriber(iwf.SubscriberConfig{
		Clients:           staticClients{client: client},
		Identity:          id.get,
		Graphs:            source,
		Runner:            runner,
		Ops:               ops,
		HeartbeatInterval: 10 * time.Millisecond,
		Backoff:           fastBackoff(),
		Logger:            slog.New(logs),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(sub.Stop)

	return &subscriberHarness{srv: srv, sub: sub, graphs: source, exec: exec, logs: logs, id: id}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (h *subscriberHarness) heartbeatsFor(runID string, epoch uint64) int {
	n := 0
	for _, beat := range h.srv.snapshotHeartbeats() {
		if beat.GetRunId() == runID && beat.GetLeaseEpoch() == epoch {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestLocalGraphReplacesTheWireGraph pins the reason the frozen plan is not
// executed: a Local step carries a Go closure, and no JSON can carry one back.
func TestLocalGraphReplacesTheWireGraph(t *testing.T) {
	t.Parallel()

	ran := make(chan map[string]any, 1)
	graph := []wf.Step{localStep("work", nil, func(_ context.Context, state map[string]any) (any, error) {
		ran <- state
		return map[string]any{"done": true}, nil
	})}
	h := startSubscriber(t, map[string][]wf.Step{"order_flow": graph})

	waitFor(t, "the subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) > 0
	})

	h.srv.assignments <- &pb.RunAssignment{
		RunId:        "r-1",
		WorkflowName: "order_flow",
		// The wire graph is deliberately unusable: nothing may be read out of it.
		FrozenPlan: []byte("not json at all"),
		Input:      []byte(`{"id":"o-1"}`),
		LeaseEpoch: 3,
	}

	select {
	case state := <-ran:
		input, _ := state["input"].(map[string]any)
		if input["id"] != "o-1" {
			t.Fatalf("run input did not reach the step: %#v", state)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the locally declared graph never ran")
	}

	waitFor(t, "the run to be completed", func() bool { return len(h.srv.snapshotCompleteRuns()) == 1 })
	done := h.srv.snapshotCompleteRuns()[0]
	if done.GetTerminalStatus() != "success" || done.GetLeaseEpoch() != 3 {
		t.Fatalf("complete run = %#v", done)
	}
}

func TestAssignmentForAnUndeclaredWorkflowIsDropped(t *testing.T) {
	t.Parallel()

	ran := make(chan struct{}, 1)
	graph := []wf.Step{localStep("work", nil, func(context.Context, map[string]any) (any, error) {
		ran <- struct{}{}
		return nil, nil
	})}
	h := startSubscriber(t, map[string][]wf.Step{"known": graph})

	waitFor(t, "the subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) > 0
	})

	h.srv.assignments <- &pb.RunAssignment{RunId: "r-1", WorkflowName: "unknown", LeaseEpoch: 1}
	// The subscription survives it: a known assignment right behind still runs.
	h.srv.assignments <- &pb.RunAssignment{RunId: "r-2", WorkflowName: "known", LeaseEpoch: 1}

	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("an unknown workflow tore down the subscription")
	}

	waitFor(t, "the known run to complete", func() bool { return len(h.srv.snapshotCompleteRuns()) == 1 })
	if got := h.srv.snapshotCompleteRuns()[0].GetRunId(); got != "r-2" {
		t.Fatalf("completed the wrong run: %s", got)
	}
	if !h.logs.contains("no graph declared") {
		t.Fatal("the dropped assignment was not explained in the log")
	}
}

// TestHeartbeatIsKeyedByRunAndEpoch pins the livelock the Node subscriber had:
// the old execution's exit killed the re-assignment's heartbeat, the lease
// expired, the run was assigned again, and around it went.
func TestHeartbeatIsKeyedByRunAndEpoch(t *testing.T) {
	t.Parallel()

	gates := map[float64]chan struct{}{1: make(chan struct{}), 2: make(chan struct{})}
	graph := []wf.Step{localStep("hold", nil, func(ctx context.Context, state map[string]any) (any, error) {
		input, _ := state["input"].(map[string]any)
		gate := gates[input["gate"].(float64)]
		select {
		case <-gate:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})}
	h := startSubscriber(t, map[string][]wf.Step{"held": graph})

	waitFor(t, "the subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) > 0
	})

	h.srv.assignments <- &pb.RunAssignment{
		RunId: "r-1", WorkflowName: "held", Input: []byte(`{"gate":1}`), LeaseEpoch: 1,
	}
	waitFor(t, "the first lease to beat", func() bool { return h.heartbeatsFor("r-1", 1) > 0 })

	// The runtime reclaims the lease and re-assigns the same run at a higher
	// epoch while the first execution is still inside its step.
	h.srv.assignments <- &pb.RunAssignment{
		RunId: "r-1", WorkflowName: "held", Input: []byte(`{"gate":2}`), LeaseEpoch: 2,
	}
	waitFor(t, "the second lease to beat", func() bool { return h.heartbeatsFor("r-1", 2) > 0 })

	// The superseded execution now finishes. Its exit must not retire the lease
	// the live execution is holding.
	close(gates[1])
	waitFor(t, "the superseded execution to finish", func() bool { return len(h.srv.snapshotCompleteRuns()) > 0 })

	settled := h.heartbeatsFor("r-1", 2)
	waitFor(t, "the live lease to keep beating", func() bool { return h.heartbeatsFor("r-1", 2) > settled+1 })

	close(gates[2])
}

// TestUnrenewableLeaseRetiresItsHeartbeat covers the runtime's own finding: a
// run that was never parked may hold no lease at all, and its heartbeat is then
// refused with Aborted forever.
func TestUnrenewableLeaseRetiresItsHeartbeat(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	graph := []wf.Step{localStep("hold", nil, func(ctx context.Context, _ map[string]any) (any, error) {
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})}
	h := startSubscriber(t, map[string][]wf.Step{"held": graph})
	h.srv.mu.Lock()
	h.srv.onHeartbeat = func(*pb.HeartbeatRequest) error {
		return status.Error(codes.Aborted, "no lease holder")
	}
	h.srv.mu.Unlock()

	waitFor(t, "the subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) > 0
	})
	h.srv.assignments <- &pb.RunAssignment{RunId: "r-1", WorkflowName: "held", LeaseEpoch: 1}

	waitFor(t, "the refused heartbeat", func() bool { return h.heartbeatsFor("r-1", 1) > 0 })
	waitFor(t, "the explanation in the log", func() bool { return h.logs.contains("lease not renewable") })

	// Retired, not repeated: the count stops moving.
	settled := h.heartbeatsFor("r-1", 1)
	time.Sleep(60 * time.Millisecond)
	if got := h.heartbeatsFor("r-1", 1); got != settled {
		t.Fatalf("a refused heartbeat kept repeating: %d then %d", settled, got)
	}

	// And the run itself is still running: the subscription was not torn down.
	close(release)
	waitFor(t, "the run to finish", func() bool { return len(h.srv.snapshotCompleteRuns()) == 1 })
}

func TestCompensatingAssignmentReportsItsTerminalStatus(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		reason string
		want   string
	}{
		{"after a step failure", "step_failure", "failed_compensated"},
		{"after a cancellation", "user_cancel", "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			charge := callStep("charge", "billing", "charge")
			charge.Compensate = &wf.Compensation{Kind: wf.CompensateCall, Method: wf.Name("refund")}
			h := startSubscriber(t, map[string][]wf.Step{"order_flow": {charge}})

			waitFor(t, "the subscription", func() bool {
				h.srv.mu.Lock()
				defer h.srv.mu.Unlock()
				return len(h.srv.subscribes) > 0
			})
			h.srv.assignments <- &pb.RunAssignment{
				RunId:        "r-1",
				WorkflowName: "order_flow",
				Input:        []byte(`{"id":"o-1"}`),
				State:        []byte(`{"charge":{"txn":"t-1"}}`),
				LeaseEpoch:   5,
				Compensating: true,
				CancelReason: tc.reason,
			}

			waitFor(t, "the compensation to finish", func() bool { return len(h.srv.snapshotCompleteRuns()) == 1 })
			done := h.srv.snapshotCompleteRuns()[0]
			if done.GetTerminalStatus() != tc.want {
				t.Fatalf("terminal status = %q, want %q", done.GetTerminalStatus(), tc.want)
			}
			if got := h.exec.calledMethods(); len(got) != 1 || got[0] != "billing/refund" {
				t.Fatalf("compensation calls = %v", got)
			}
		})
	}
}

func TestParkedRunIsNotCompleted(t *testing.T) {
	t.Parallel()

	graph := []wf.Step{wf.Sleep{Control: wf.Control{ID: "wait"}, DurationSec: 60}}
	h := startSubscriber(t, map[string][]wf.Step{"delayed": graph})

	waitFor(t, "the subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) > 0
	})
	h.srv.assignments <- &pb.RunAssignment{RunId: "r-1", WorkflowName: "delayed", LeaseEpoch: 1}

	waitFor(t, "the park", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.parkCalls) == 1
	})
	time.Sleep(50 * time.Millisecond)
	if got := h.srv.snapshotCompleteRuns(); len(got) != 0 {
		t.Fatalf("a parked run was completed: %#v", got)
	}
}

func TestIdentityRotationReopensTheStream(t *testing.T) {
	t.Parallel()

	h := startSubscriber(t, map[string][]wf.Step{})
	waitFor(t, "the first subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) == 1
	})

	h.id.rotate("inst-2")
	waitFor(t, "the reopened subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) >= 2 && h.srv.subscribes[1].GetInstanceId() == "inst-2"
	})
}

func TestRunFailureIsReportedToTheOwner(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onFail = func(*pb.FailStepRequest) (*pb.FailStepResponse, error) {
		return &pb.FailStepResponse{NextAction: "compensate"}, nil
	}

	id := &rotatingIdentity{service: "svc-1", instance: "inst-1"}
	ops := newCheckpoints(t, client, id)
	logs := &logSink{}
	runner, err := iwf.NewRunner(iwf.RunnerConfig{
		Ops:      ops,
		Executor: &scriptedExec{onCall: func(context.Context, iwf.CallSpec) (any, error) { return nil, errors.New("boom") }},
		Logger:   slog.New(logs),
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	failures := make(chan error, 8)
	sub, err := iwf.NewSubscriber(iwf.SubscriberConfig{
		Clients:           staticClients{client: client},
		Identity:          id.get,
		Graphs:            &staticGraphs{graphs: map[string][]wf.Step{"w": {callStep("charge", "billing", "charge")}}},
		Runner:            runner,
		Ops:               ops,
		HeartbeatInterval: 10 * time.Millisecond,
		Backoff:           fastBackoff(),
		OnError:           func(err error) { failures <- err },
		Logger:            slog.New(logs),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(sub.Stop)

	waitFor(t, "the subscription", func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.subscribes) > 0
	})
	srv.assignments <- &pb.RunAssignment{RunId: "r-1", WorkflowName: "w", Input: []byte(`{}`), LeaseEpoch: 1}

	select {
	case err := <-failures:
		var stepErr *iwf.StepError
		if !errors.As(err, &stepErr) || stepErr.Action != iwf.ActionCompensate {
			t.Fatalf("reported error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a run handed back to the runtime was reported to nobody")
	}
	if got := srv.snapshotCompleteRuns(); len(got) != 0 {
		t.Fatalf("a failed run was completed: %#v", got)
	}
}

func TestUnreadableAssignmentStateIsReported(t *testing.T) {
	t.Parallel()

	h := startSubscriber(t, map[string][]wf.Step{"w": {
		localStep("work", nil, func(context.Context, map[string]any) (any, error) { return nil, nil }),
	}})
	waitFor(t, "the subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) > 0
	})

	h.srv.assignments <- &pb.RunAssignment{
		RunId: "r-1", WorkflowName: "w", State: []byte("{not json"), LeaseEpoch: 1,
	}
	waitFor(t, "the explanation in the log", func() bool { return h.logs.contains("unreadable state") })

	if got := h.srv.snapshotCompleteRuns(); len(got) != 0 {
		t.Fatalf("a run that never started was completed: %#v", got)
	}
}

func TestStartTwiceIsRejected(t *testing.T) {
	t.Parallel()

	h := startSubscriber(t, map[string][]wf.Step{})
	if err := h.sub.Start(t.Context()); !errors.Is(err, stream.ErrAlreadyStarted) {
		t.Fatalf("second start: %v", err)
	}
}

func TestSubscriberRefusesAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	_, client := startWorkflows(t)
	id := &rotatingIdentity{service: "svc-1", instance: "inst-1"}
	ops := newCheckpoints(t, client, id)
	runner, err := iwf.NewRunner(iwf.RunnerConfig{Ops: ops, Executor: &scriptedExec{}})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	full := iwf.SubscriberConfig{
		Clients:  staticClients{client: client},
		Identity: id.get,
		Graphs:   &staticGraphs{},
		Runner:   runner,
		Ops:      ops,
	}

	for name, drop := range map[string]func(*iwf.SubscriberConfig){
		"clients":  func(c *iwf.SubscriberConfig) { c.Clients = nil },
		"identity": func(c *iwf.SubscriberConfig) { c.Identity = nil },
		"graphs":   func(c *iwf.SubscriberConfig) { c.Graphs = nil },
		"runner":   func(c *iwf.SubscriberConfig) { c.Runner = nil },
		"ops":      func(c *iwf.SubscriberConfig) { c.Ops = nil },
	} {
		cfg := full
		drop(&cfg)
		if _, err := iwf.NewSubscriber(cfg); !errors.Is(err, iwf.ErrInvalidConfig) {
			t.Fatalf("missing %s: want ErrInvalidConfig, got %v", name, err)
		}
	}
}

func TestAnUnreadableTraceHeaderDoesNotStopTheRun(t *testing.T) {
	t.Parallel()

	h := startSubscriber(t, map[string][]wf.Step{"w": {
		localStep("work", nil, func(context.Context, map[string]any) (any, error) { return nil, nil }),
	}})
	waitFor(t, "the subscription", func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return len(h.srv.subscribes) > 0
	})

	// A header the SDK cannot read means the caller sent no usable trace, not
	// that the assignment is broken: the run joins a fresh tree and goes on.
	h.srv.assignments <- &pb.RunAssignment{
		RunId: "r-1", WorkflowName: "w", XSbTrace: "not-a-trace", LeaseEpoch: 1,
	}
	waitFor(t, "the run to complete", func() bool { return len(h.srv.snapshotCompleteRuns()) == 1 })
}

func TestStopLeavesNoGoroutines(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	id := &rotatingIdentity{service: "svc-1", instance: "inst-1"}
	ops := newCheckpoints(t, client, id)
	// One warm-up call so the channel's own goroutines exist before the count.
	if err := ops.Heartbeat(t.Context(), iwf.HeartbeatArgs{RunID: "warm", LeaseEpoch: 1}); err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	before := runtime.NumGoroutine()

	runner, err := iwf.NewRunner(iwf.RunnerConfig{Ops: ops, Executor: &scriptedExec{}})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	graph := []wf.Step{localStep("work", nil, func(context.Context, map[string]any) (any, error) { return nil, nil })}
	sub, err := iwf.NewSubscriber(iwf.SubscriberConfig{
		Clients:           staticClients{client: client},
		Identity:          id.get,
		Graphs:            &staticGraphs{graphs: map[string][]wf.Step{"w": graph}},
		Runner:            runner,
		Ops:               ops,
		HeartbeatInterval: 5 * time.Millisecond,
		Backoff:           fastBackoff(),
		Logger:            slog.New(&logSink{}),
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	if err := sub.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, "the subscription", func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return len(srv.subscribes) > 0
	})
	srv.assignments <- &pb.RunAssignment{RunId: "r-1", WorkflowName: "w", LeaseEpoch: 1}
	waitFor(t, "the run", func() bool { return len(srv.snapshotCompleteRuns()) == 1 })
	sub.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines leaked after Stop: %d before, %d after", before, runtime.NumGoroutine())
}
