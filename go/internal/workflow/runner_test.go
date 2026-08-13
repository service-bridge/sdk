package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	iwf "github.com/service-bridge/sdk/go/internal/workflow"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// ---------------------------------------------------------------------------
// scripted checkpoint surface and executor
// ---------------------------------------------------------------------------

// scriptedOps is the runtime as the runner sees it: it records every checkpoint
// and answers with whatever the test scripted. Ops is declared at the consumer
// precisely so the runner can be driven through every answer the runtime is
// allowed to give.
type scriptedOps struct {
	mu sync.Mutex

	begins       []iwf.BeginStepArgs
	completes    []iwf.CompleteStepArgs
	fails        []iwf.FailStepArgs
	parks        []iwf.ParkArgs
	completeRuns []iwf.CompleteRunArgs

	onBegin func(ctx context.Context, args iwf.BeginStepArgs) (iwf.BeginResult, error)
	onFail  func(ctx context.Context, args iwf.FailStepArgs) (iwf.Decision, error)
}

func (o *scriptedOps) BeginStep(ctx context.Context, args iwf.BeginStepArgs) (iwf.BeginResult, error) {
	o.mu.Lock()
	o.begins = append(o.begins, args)
	hook := o.onBegin
	o.mu.Unlock()
	if hook != nil {
		return hook(ctx, args)
	}
	return iwf.BeginResult{LeaseEpoch: args.LeaseEpoch}, nil
}

func (o *scriptedOps) CompleteStep(_ context.Context, args iwf.CompleteStepArgs) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.completes = append(o.completes, args)
	return nil
}

func (o *scriptedOps) FailStep(ctx context.Context, args iwf.FailStepArgs) (iwf.Decision, error) {
	o.mu.Lock()
	o.fails = append(o.fails, args)
	hook := o.onFail
	o.mu.Unlock()
	if hook != nil {
		return hook(ctx, args)
	}
	return iwf.Decision{Action: iwf.ActionFailRun}, nil
}

func (o *scriptedOps) Park(_ context.Context, args iwf.ParkArgs) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.parks = append(o.parks, args)
	return nil
}

func (o *scriptedOps) CompleteRun(_ context.Context, args iwf.CompleteRunArgs) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.completeRuns = append(o.completeRuns, args)
	return nil
}

func (o *scriptedOps) Heartbeat(context.Context, iwf.HeartbeatArgs) error { return nil }

func (o *scriptedOps) beganSteps() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, 0, len(o.begins))
	for _, b := range o.begins {
		out = append(out, b.StepID)
	}
	return out
}

func (o *scriptedOps) completedSteps() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, 0, len(o.completes))
	for _, c := range o.completes {
		out = append(out, c.StepID)
	}
	return out
}

func (o *scriptedOps) failedSteps() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, 0, len(o.fails))
	for _, f := range o.fails {
		out = append(out, f.StepID)
	}
	return out
}

// scriptedExec records what actually reached the outside world. What it did not
// record is the point of most of these tests.
type scriptedExec struct {
	mu sync.Mutex

	calls    []iwf.CallSpec
	publishs []iwf.PublishSpec
	starts   []iwf.StartSpec

	onCall    func(ctx context.Context, spec iwf.CallSpec) (any, error)
	onPublish func(ctx context.Context, spec iwf.PublishSpec) (any, error)
	onStart   func(ctx context.Context, spec iwf.StartSpec) (string, error)
}

func (e *scriptedExec) Call(ctx context.Context, spec iwf.CallSpec) (any, error) {
	e.mu.Lock()
	e.calls = append(e.calls, spec)
	hook := e.onCall
	e.mu.Unlock()
	if hook != nil {
		return hook(ctx, spec)
	}
	return map[string]any{"ok": true}, nil
}

func (e *scriptedExec) Publish(ctx context.Context, spec iwf.PublishSpec) (any, error) {
	e.mu.Lock()
	e.publishs = append(e.publishs, spec)
	hook := e.onPublish
	e.mu.Unlock()
	if hook != nil {
		return hook(ctx, spec)
	}
	return nil, nil
}

func (e *scriptedExec) StartRun(ctx context.Context, spec iwf.StartSpec) (string, error) {
	e.mu.Lock()
	e.starts = append(e.starts, spec)
	hook := e.onStart
	e.mu.Unlock()
	if hook != nil {
		return hook(ctx, spec)
	}
	return "child-1", nil
}

func (e *scriptedExec) calledMethods() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.calls))
	for _, c := range e.calls {
		out = append(out, c.Service+"/"+c.Method)
	}
	return out
}

func newRunner(t *testing.T, ops iwf.Ops, exec iwf.Executor) *iwf.Runner {
	t.Helper()
	r, err := iwf.NewRunner(iwf.RunnerConfig{
		Ops:      ops,
		Executor: exec,
		// The retry delay is the runtime's decision, not a wall clock this test
		// has to sit through.
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	return r
}

func localStep(id string, waitFor []string, fn wf.LocalFunc) wf.Local {
	return wf.Local{Control: wf.Control{ID: id, WaitFor: waitFor}, Fn: fn}
}

func callStep(id, service, method string) wf.Call {
	return wf.Call{
		Control: wf.Control{ID: id},
		Service: wf.Name(service),
		Method:  wf.Name(method),
		Input:   map[string]any{"id": wf.Path("$.input.id")},
	}
}

// ---------------------------------------------------------------------------
// scheduling
// ---------------------------------------------------------------------------

func TestRunnerHonoursReadinessLevels(t *testing.T) {
	t.Parallel()

	var order []string
	var mu sync.Mutex
	record := func(id string) wf.LocalFunc {
		return func(context.Context, map[string]any) (any, error) {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, id)
			return id, nil
		}
	}

	ops := &scriptedOps{}
	steps := []wf.Step{
		localStep("c", []string{"b"}, record("c")),
		localStep("b", []string{"a"}, record("b")),
		localStep("a", nil, record("a")),
	}

	out, err := newRunner(t, ops, &scriptedExec{}).Run(t.Context(), steps, iwf.RunContext{
		RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.Parked {
		t.Fatal("nothing parked")
	}
	if got := fmt.Sprint(order); got != "[a b c]" {
		t.Fatalf("execution order = %s", got)
	}
	if got := out.State["c"]; got != "c" {
		t.Fatalf("state = %#v", out.State)
	}
}

func TestRunnerCapsTheParallelBatch(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	live, peak := 0, 0
	release := make(chan struct{})
	fn := func(ctx context.Context, _ map[string]any) (any, error) {
		mu.Lock()
		live++
		if live > peak {
			peak = live
		}
		mu.Unlock()

		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
			return nil, errors.New("release never came")
		}

		mu.Lock()
		live--
		mu.Unlock()
		return nil, nil
	}

	// Two of the four steps may run at once, so the first pair has to finish
	// before the second starts. Releasing on the second entry proves the cap:
	// with no cap all four would be live and the release would come at once.
	go func() {
		for {
			mu.Lock()
			reached := peak >= 2
			mu.Unlock()
			if reached {
				close(release)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	steps := []wf.Step{
		localStep("a", nil, fn),
		localStep("b", nil, fn),
		localStep("c", nil, fn),
		localStep("d", nil, fn),
	}
	ops := &scriptedOps{}
	if _, err := newRunner(t, ops, &scriptedExec{}).Run(t.Context(), steps, iwf.RunContext{
		RunID: "r1", LeaseEpoch: 1, MaxParallelism: 2,
		State: map[string]any{"input": map[string]any{}},
	}); err != nil {
		t.Fatalf("run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if peak > 2 {
		t.Fatalf("max parallelism 2 was exceeded: peak %d", peak)
	}
	if len(ops.beganSteps()) != 4 {
		t.Fatalf("want every step begun, got %v", ops.beganSteps())
	}
}

// ---------------------------------------------------------------------------
// idempotent replay
// ---------------------------------------------------------------------------

func TestAlreadyDoneStepTakesTheCacheAndDoesNotRun(t *testing.T) {
	t.Parallel()

	var ran int
	ops := &scriptedOps{
		onBegin: func(_ context.Context, args iwf.BeginStepArgs) (iwf.BeginResult, error) {
			if args.StepID == "charge" {
				return iwf.BeginResult{AlreadyDone: true, Output: map[string]any{"txn": "t-1"}}, nil
			}
			return iwf.BeginResult{}, nil
		},
	}
	exec := &scriptedExec{
		onCall: func(context.Context, iwf.CallSpec) (any, error) {
			ran++
			return nil, nil
		},
	}

	steps := []wf.Step{callStep("charge", "billing", "charge")}
	out, err := newRunner(t, ops, exec).Run(t.Context(), steps, iwf.RunContext{
		RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{"id": "o-1"}},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if ran != 0 {
		t.Fatalf("a checkpointed step was executed again %d times", ran)
	}
	if len(ops.completedSteps()) != 0 {
		t.Fatalf("a cached step must not be checkpointed again: %v", ops.completedSteps())
	}
	cached, ok := out.State["charge"].(map[string]any)
	if !ok || cached["txn"] != "t-1" {
		t.Fatalf("cached output was not adopted: %#v", out.State["charge"])
	}
}

// TestReplayedCompensationDoesNotRepeatItself is the regression on the most
// expensive bug the Node runner had: the forward path honoured the "already
// done" checkpoint and the backward path did not, so every reassignment of a
// compensating run refunded the customer once more.
func TestReplayedCompensationDoesNotRepeatItself(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{
		onBegin: func(_ context.Context, args iwf.BeginStepArgs) (iwf.BeginResult, error) {
			// The previous holder got as far as undoing "ship" before its lease
			// expired.
			if args.StepID == "ship.compensate" {
				return iwf.BeginResult{AlreadyDone: true, Output: map[string]any{"cancelled": true}}, nil
			}
			return iwf.BeginResult{}, nil
		},
	}
	exec := &scriptedExec{}

	charge := callStep("charge", "billing", "charge")
	charge.Compensate = &wf.Compensation{
		Kind:    wf.CompensateCall,
		Service: wf.Name("billing"),
		Method:  wf.Name("refund"),
		Input:   map[string]any{"txn": wf.Path("$.charge.txn")},
	}
	ship := callStep("ship", "shipping", "ship")
	ship.Compensate = &wf.Compensation{
		Kind:    wf.CompensateCall,
		Service: wf.Name("shipping"),
		Method:  wf.Name("cancel"),
	}

	state := map[string]any{
		"input":  map[string]any{"id": "o-1"},
		"charge": map[string]any{"txn": "t-1"},
		"ship":   map[string]any{"label": "l-1"},
	}
	_, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{charge, ship}, iwf.RunContext{
		RunID: "r1", LeaseEpoch: 4, State: state, Compensating: true, CancelReason: "step_failure",
	})
	if err != nil {
		t.Fatalf("compensate: %v", err)
	}

	if got := exec.calledMethods(); len(got) != 1 || got[0] != "billing/refund" {
		t.Fatalf("compensation replay executed the wrong set: %v", got)
	}
	if got := ops.completedSteps(); len(got) != 1 || got[0] != "charge.compensate" {
		t.Fatalf("checkpointed compensations = %v", got)
	}
	// The refund has to be shaped against the run state it is undoing.
	exec.mu.Lock()
	defer exec.mu.Unlock()
	input, _ := exec.calls[0].Input.(map[string]any)
	if input["txn"] != "t-1" {
		t.Fatalf("refund input = %#v", exec.calls[0].Input)
	}
}

func TestCompensationSkipsStepsThatNeverCompleted(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{}

	charge := callStep("charge", "billing", "charge")
	charge.Compensate = &wf.Compensation{Kind: wf.CompensateCall, Method: wf.Name("refund")}
	ship := callStep("ship", "shipping", "ship")
	ship.Compensate = &wf.Compensation{Kind: wf.CompensateCall, Method: wf.Name("cancel")}

	// "ship" is absent from state: the run failed on it, so it never completed.
	state := map[string]any{"input": map[string]any{}, "charge": map[string]any{"txn": "t-1"}}
	if _, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{charge, ship}, iwf.RunContext{
		RunID: "r1", LeaseEpoch: 1, State: state, Compensating: true,
	}); err != nil {
		t.Fatalf("compensate: %v", err)
	}

	if got := exec.calledMethods(); len(got) != 1 || got[0] != "billing/refund" {
		t.Fatalf("compensated the wrong set: %v", got)
	}
}

// TestCompensationCoversAStepThatProducedNothing guards the publish case: a
// publish checkpoints a null output, and reading the value instead of its
// presence would leave every published effect standing.
func TestCompensationCoversAStepThatProducedNothing(t *testing.T) {
	t.Parallel()

	exec := &scriptedExec{}
	publish := wf.Publish{
		Control: wf.Control{ID: "reserve", Compensate: &wf.Compensation{
			Kind:  wf.CompensatePublish,
			Event: wf.Name("stock.released"),
		}},
		Event: wf.Name("stock.reserved"),
	}

	state := map[string]any{"input": map[string]any{}, "reserve": nil}
	if _, err := newRunner(t, &scriptedOps{}, exec).Run(t.Context(), []wf.Step{publish},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: state, Compensating: true}); err != nil {
		t.Fatalf("compensate: %v", err)
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	if len(exec.publishs) != 1 || exec.publishs[0].Event != "stock.released" {
		t.Fatalf("publishes = %#v", exec.publishs)
	}
}

func TestFailedCompensationIsReportedAndTheWalkGoesOn(t *testing.T) {
	t.Parallel()

	boom := errors.New("refund endpoint is down")
	ops := &scriptedOps{}
	exec := &scriptedExec{
		onCall: func(_ context.Context, spec iwf.CallSpec) (any, error) {
			if spec.Method == "cancel" {
				return nil, boom
			}
			return nil, nil
		},
	}

	charge := callStep("charge", "billing", "charge")
	charge.Compensate = &wf.Compensation{Kind: wf.CompensateCall, Method: wf.Name("refund")}
	ship := callStep("ship", "shipping", "ship")
	ship.Compensate = &wf.Compensation{Kind: wf.CompensateCall, Method: wf.Name("cancel")}

	state := map[string]any{
		"input":  map[string]any{},
		"charge": map[string]any{"txn": "t-1"},
		"ship":   map[string]any{"label": "l-1"},
	}
	_, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{charge, ship}, iwf.RunContext{
		RunID: "r1", LeaseEpoch: 1, State: state, Compensating: true,
	})
	if err == nil {
		t.Fatal("a failed compensation must not let the run be completed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("the cause was lost: %v", err)
	}
	// The step before it was still compensated.
	if got := exec.calledMethods(); len(got) != 2 {
		t.Fatalf("the walk stopped early: %v", got)
	}
	if got := ops.failedSteps(); len(got) != 1 || got[0] != "ship.compensate" {
		t.Fatalf("the failed compensation was not reported: %v", got)
	}
	if got := ops.completedSteps(); len(got) != 1 || got[0] != "charge.compensate" {
		t.Fatalf("checkpointed compensations = %v", got)
	}
}

// ---------------------------------------------------------------------------
// the runtime's decision after a failure
// ---------------------------------------------------------------------------

func TestRuntimeDecisionRetryIsExecuted(t *testing.T) {
	t.Parallel()

	var attempts int
	ops := &scriptedOps{
		onFail: func(context.Context, iwf.FailStepArgs) (iwf.Decision, error) {
			return iwf.Decision{Action: iwf.ActionRetry, RetryDelaySec: 3}, nil
		},
	}
	exec := &scriptedExec{
		onCall: func(context.Context, iwf.CallSpec) (any, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("transient")
			}
			return map[string]any{"txn": "t-2"}, nil
		},
	}

	out, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{callStep("charge", "billing", "charge")},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("the retry decision was not executed: %d attempts", attempts)
	}
	if got := ops.beganSteps(); len(got) != 2 {
		t.Fatalf("a retry must re-claim the step: %v", got)
	}
	if got := ops.completedSteps(); len(got) != 1 {
		t.Fatalf("completions = %v", got)
	}
	if out.State["charge"].(map[string]any)["txn"] != "t-2" {
		t.Fatalf("state = %#v", out.State)
	}
}

func TestRuntimeDecisionCompensateAndFailRunAreExecuted(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		action iwf.NextAction
	}{
		{"compensate", iwf.ActionCompensate},
		{"fail run", iwf.ActionFailRun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attempts int
			ops := &scriptedOps{
				onFail: func(context.Context, iwf.FailStepArgs) (iwf.Decision, error) {
					return iwf.Decision{Action: tc.action}, nil
				},
			}
			exec := &scriptedExec{
				onCall: func(context.Context, iwf.CallSpec) (any, error) {
					attempts++
					return nil, errors.New("boom")
				},
			}

			_, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{callStep("charge", "billing", "charge")},
				iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
			var stepErr *iwf.StepError
			if !errors.As(err, &stepErr) {
				t.Fatalf("want *StepError, got %v", err)
			}
			if stepErr.Action != tc.action || stepErr.StepID != "charge" {
				t.Fatalf("decision was lost: %#v", stepErr)
			}
			if attempts != 1 {
				t.Fatalf("the step ran %d times after a terminal decision", attempts)
			}
			if len(ops.completedSteps()) != 0 {
				t.Fatalf("a failed step must not be completed: %v", ops.completedSteps())
			}
		})
	}
}

func TestUnknownDecisionIsRefused(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{
		onFail: func(context.Context, iwf.FailStepArgs) (iwf.Decision, error) {
			return iwf.Decision{Action: "sleep_on_it"}, nil
		},
	}
	exec := &scriptedExec{
		onCall: func(context.Context, iwf.CallSpec) (any, error) { return nil, errors.New("boom") },
	}

	_, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{callStep("charge", "billing", "charge")},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
	if !errors.Is(err, iwf.ErrUnknownAction) {
		t.Fatalf("want ErrUnknownAction, got %v", err)
	}
}

func TestRetriableMirrorsTheDeclaredBudget(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{
		onCall: func(context.Context, iwf.CallSpec) (any, error) { return nil, errors.New("boom") },
	}

	step := callStep("charge", "billing", "charge")
	step.Retry = &wf.RetryPolicy{MaxAttempts: 3}
	_, _ = newRunner(t, ops, exec).Run(t.Context(), []wf.Step{step},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})

	ops.mu.Lock()
	defer ops.mu.Unlock()
	if len(ops.fails) != 1 || !ops.fails[0].Retriable {
		t.Fatalf("a declared budget must be reported: %#v", ops.fails)
	}
}

// ---------------------------------------------------------------------------
// a failing step takes its neighbours with it
// ---------------------------------------------------------------------------

// TestFailingStepCancelsItsNeighbours pins the second Node bug: Promise.all
// returned on the first rejection while the siblings kept running, so they
// dispatched their calls and checkpointed against a run the runtime had already
// moved to compensating.
func TestFailingStepCancelsItsNeighbours(t *testing.T) {
	t.Parallel()

	failed := make(chan struct{})
	var ran int
	var mu sync.Mutex

	ops := &scriptedOps{
		onFail: func(context.Context, iwf.FailStepArgs) (iwf.Decision, error) {
			close(failed)
			return iwf.Decision{Action: iwf.ActionCompensate}, nil
		},
	}
	// The neighbour is held inside its claim until the failure has landed, and
	// released by the cancellation that the failure causes. It must never reach
	// its own dispatch.
	ops.onBegin = func(ctx context.Context, args iwf.BeginStepArgs) (iwf.BeginResult, error) {
		if args.StepID != "neighbour" {
			return iwf.BeginResult{}, nil
		}
		select {
		case <-failed:
		case <-time.After(2 * time.Second):
			return iwf.BeginResult{}, errors.New("the failure never landed")
		}
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
		return iwf.BeginResult{}, nil
	}

	exec := &scriptedExec{
		onCall: func(_ context.Context, spec iwf.CallSpec) (any, error) {
			mu.Lock()
			ran++
			mu.Unlock()
			if spec.Method == "boom" {
				return nil, errors.New("boom")
			}
			return nil, nil
		},
	}

	steps := []wf.Step{callStep("boom", "svc", "boom"), callStep("neighbour", "svc", "work")}
	_, err := newRunner(t, ops, exec).Run(t.Context(), steps,
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
	if err == nil {
		t.Fatal("want the run to be handed back")
	}

	mu.Lock()
	defer mu.Unlock()
	if ran != 1 {
		t.Fatalf("the neighbour reached the outside world: %d dispatches", ran)
	}
	if got := ops.completedSteps(); len(got) != 0 {
		t.Fatalf("the neighbour checkpointed against a failing run: %v", got)
	}
}

// ---------------------------------------------------------------------------
// parking
// ---------------------------------------------------------------------------

func TestSleepParksAndLeavesTheRunUnfinished(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	steps := []wf.Step{
		wf.Sleep{Control: wf.Control{ID: "wait"}, DurationSec: 30},
		localStep("after", []string{"wait"}, func(context.Context, map[string]any) (any, error) {
			return nil, errors.New("a parked run must not schedule the next level")
		}),
	}

	out, err := newRunner(t, ops, &scriptedExec{}).Run(t.Context(), steps,
		iwf.RunContext{RunID: "r1", LeaseEpoch: 2, State: map[string]any{"input": map[string]any{}}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !out.Parked || out.ParkedStepID != "wait" {
		t.Fatalf("outcome = %#v", out)
	}
	if got := ops.beganSteps(); len(got) != 1 || got[0] != "wait" {
		t.Fatalf("steps claimed after the park: %v", got)
	}
	if len(ops.completedSteps()) != 0 {
		t.Fatal("a parked step has no output to checkpoint")
	}
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if len(ops.parks) != 1 || ops.parks[0].Sleep == nil || ops.parks[0].Sleep.DurationSec != 30 {
		t.Fatalf("parks = %#v", ops.parks)
	}
	if len(ops.completeRuns) != 0 {
		t.Fatal("the runner never completes a run itself")
	}
}

func TestWaitEventParksWithAResolvedFilter(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	steps := []wf.Step{wf.WaitEvent{
		Control: wf.Control{ID: "await_payment", TimeoutSec: 120},
		Event:   wf.Name("payment.settled"),
		Filter:  map[string]any{"$.order_id": wf.Path("$.input.id")},
	}}

	out, err := newRunner(t, ops, &scriptedExec{}).Run(t.Context(), steps,
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{"id": "o-9"}}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !out.Parked {
		t.Fatal("a wait must park")
	}

	ops.mu.Lock()
	defer ops.mu.Unlock()
	wait := ops.parks[0].Event
	if wait == nil || wait.Event != "payment.settled" || wait.TimeoutSec != 120 {
		t.Fatalf("event wait = %#v", wait)
	}
	if wait.FilterJSON != `{"$.order_id":"o-9"}` {
		t.Fatalf("filter was not resolved against run state: %s", wait.FilterJSON)
	}
}

func TestSubWorkflowStartsTheChildAndParksOnIt(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{
		onStart: func(context.Context, iwf.StartSpec) (string, error) { return "child-7", nil },
	}
	steps := []wf.Step{wf.SubWorkflow{
		Control:  wf.Control{ID: "nested"},
		Workflow: wf.Name("refund_flow"),
		Input:    map[string]any{"order": wf.Path("$.input.id")},
	}}

	out, err := newRunner(t, ops, exec).Run(t.Context(), steps,
		iwf.RunContext{RunID: "parent-1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{"id": "o-1"}}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !out.Parked {
		t.Fatal("a nested run must park the parent, not block it")
	}

	exec.mu.Lock()
	start := exec.starts[0]
	exec.mu.Unlock()
	if start.Workflow != "refund_flow" || start.ParentRunID != "parent-1" {
		t.Fatalf("start spec = %#v", start)
	}
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if ops.parks[0].Nested == nil || ops.parks[0].Nested.ChildRunID != "child-7" {
		t.Fatalf("nested park = %#v", ops.parks[0])
	}
}

// ---------------------------------------------------------------------------
// groups, fanout and resolution
// ---------------------------------------------------------------------------

func TestForEachGivesEveryIterationItsOwnStepIDs(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{}
	group := wf.Parallel{
		Control: wf.Control{ID: "fan"},
		ForEach: &wf.ForEach{From: wf.Path("$.input.items"), As: "item"},
		Steps: []wf.Step{wf.Call{
			Control: wf.Control{ID: "ship"},
			Service: wf.Name("shipping"),
			Method:  wf.Name("ship"),
			Input:   map[string]any{"sku": wf.Path("$.item.sku")},
		}},
	}

	state := map[string]any{"input": map[string]any{"items": []any{
		map[string]any{"sku": "a"},
		map[string]any{"sku": "b"},
	}}}
	out, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{group},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: state})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	began := map[string]bool{}
	for _, id := range ops.beganSteps() {
		began[id] = true
	}
	for _, want := range []string{"fan", "ship:0", "ship:1"} {
		if !began[want] {
			t.Fatalf("step %q was never claimed: %v", want, ops.beganSteps())
		}
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	skus := map[string]bool{}
	for _, c := range exec.calls {
		skus[c.Input.(map[string]any)["sku"].(string)] = true
	}
	if !skus["a"] || !skus["b"] {
		t.Fatalf("the fanout element was not bound: %#v", exec.calls)
	}
	groupOut, ok := out.State["fan"].(map[string]any)
	if !ok || len(groupOut) != 2 {
		t.Fatalf("group output = %#v", out.State["fan"])
	}
}

func TestSequenceRunsItsChildrenInOrder(t *testing.T) {
	t.Parallel()

	var order []string
	var mu sync.Mutex
	record := func(id string) wf.LocalFunc {
		return func(context.Context, map[string]any) (any, error) {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, id)
			return id, nil
		}
	}

	group := wf.Sequence{
		Control: wf.Control{ID: "seq"},
		Steps:   []wf.Step{localStep("one", nil, record("one")), localStep("two", nil, record("two"))},
	}
	if _, err := newRunner(t, &scriptedOps{}, &scriptedExec{}).Run(t.Context(), []wf.Step{group},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := fmt.Sprint(order); got != "[one two]" {
		t.Fatalf("sequence order = %s", got)
	}
}

func TestSkippedStepUnblocksItsDependents(t *testing.T) {
	t.Parallel()

	var ran []string
	var mu sync.Mutex
	record := func(id string) wf.LocalFunc {
		return func(context.Context, map[string]any) (any, error) {
			mu.Lock()
			defer mu.Unlock()
			ran = append(ran, id)
			return id, nil
		}
	}

	gated := localStep("gated", nil, record("gated"))
	gated.When = wf.Truthy("$.input.premium")
	after := localStep("after", []string{"gated"}, record("after"))

	out, err := newRunner(t, &scriptedOps{}, &scriptedExec{}).Run(t.Context(), []wf.Step{gated, after},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{"premium": false}}})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := fmt.Sprint(ran); got != "[after]" {
		t.Fatalf("ran = %s", got)
	}
	if out.State["gated"] != nil {
		t.Fatalf("a skipped step resolves to nothing, got %#v", out.State["gated"])
	}
}

func TestCallOptionsAreResolvedAgainstRunState(t *testing.T) {
	t.Parallel()

	exec := &scriptedExec{}
	step := callStep("charge", "billing", "charge")
	step.Opts = &wf.CallOpts{
		Timeout:        2 * time.Second,
		Transport:      wf.TransportProxy,
		IdempotencyKey: wf.Path("$.input.id"),
		RequestID:      "req-static",
	}

	if _, err := newRunner(t, &scriptedOps{}, exec).Run(t.Context(), []wf.Step{step},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{"id": "o-1"}}}); err != nil {
		t.Fatalf("run: %v", err)
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	spec := exec.calls[0]
	if spec.IdempotencyKey != "o-1" || spec.RequestID != "req-static" {
		t.Fatalf("options = %#v", spec)
	}
	if spec.Transport != "proxy" || spec.Timeout != 2*time.Second {
		t.Fatalf("options = %#v", spec)
	}
}

func TestPublishOptionsAreResolvedAgainstRunState(t *testing.T) {
	t.Parallel()

	exec := &scriptedExec{}
	step := wf.Publish{
		Control: wf.Control{ID: "notify"},
		Event:   wf.Name("order.shipped"),
		Input:   map[string]any{"id": wf.Path("$.input.id")},
		Opts: &wf.PublishOpts{
			PartitionKey: wf.Path("$.input.id"),
			Headers:      map[string]any{"tenant": wf.Path("$.input.tenant")},
			OccurredAtMs: 1_700_000_000_000,
		},
	}

	if _, err := newRunner(t, &scriptedOps{}, exec).Run(t.Context(), []wf.Step{step},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1,
			State: map[string]any{"input": map[string]any{"id": "o-1", "tenant": "acme"}}}); err != nil {
		t.Fatalf("run: %v", err)
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	spec := exec.publishs[0]
	if spec.Event != "order.shipped" || spec.PartitionKey != "o-1" || spec.Headers["tenant"] != "acme" {
		t.Fatalf("publish spec = %#v", spec)
	}
	if spec.OccurredAtMs != 1_700_000_000_000 {
		t.Fatalf("occurred at = %d", spec.OccurredAtMs)
	}
}

func TestWrapStepSeesEveryUnit(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var roles []string
	r, err := iwf.NewRunner(iwf.RunnerConfig{
		Ops:      &scriptedOps{},
		Executor: &scriptedExec{},
		WrapStep: func(ctx context.Context, span iwf.StepSpan, fn func(context.Context) (any, error)) (any, error) {
			mu.Lock()
			roles = append(roles, string(span.Role)+":"+span.StepID)
			mu.Unlock()
			return fn(ctx)
		},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	group := wf.Parallel{
		Control: wf.Control{ID: "fan"},
		ForEach: &wf.ForEach{From: wf.Path("$.input.items"), As: "item"},
		Steps:   []wf.Step{callStep("ship", "shipping", "ship")},
	}
	state := map[string]any{"input": map[string]any{"id": "o-1", "items": []any{1, 2}}}
	if _, err := r.Run(t.Context(), []wf.Step{group}, iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: state}); err != nil {
		t.Fatalf("run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	seen := map[string]bool{}
	for _, role := range roles {
		seen[role] = true
	}
	for _, want := range []string{"group:fan", "branch:fan:0", "branch:fan:1", "step:ship:0", "step:ship:1"} {
		if !seen[want] {
			t.Fatalf("span %q missing from %v", want, roles)
		}
	}
}

// TestFanoutParksEveryKindOfWait also pins that a fanout iteration renames the
// steps of every kind, not only the ones the happy path happens to use.
func TestFanoutParksEveryKindOfWait(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{onStart: func(context.Context, iwf.StartSpec) (string, error) { return "child-3", nil }}
	group := wf.Parallel{
		Control: wf.Control{ID: "waits"},
		ForEach: &wf.ForEach{From: wf.Path("$.input.items"), As: "item"},
		Steps: []wf.Step{
			wf.Sleep{Control: wf.Control{ID: "nap"}, DurationSec: 5},
			wf.WaitSignal{Control: wf.Control{ID: "sig", TimeoutSec: 90}, Signal: "approve"},
			wf.SubWorkflow{Control: wf.Control{ID: "child"}, Workflow: wf.Name("sub")},
		},
	}

	state := map[string]any{"input": map[string]any{"items": []any{"only"}}}
	out, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{group},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: state})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !out.Parked {
		t.Fatal("a group whose children all park is not finished")
	}
	if _, ok := out.State["waits"]; ok {
		t.Fatalf("a parked group must not be checkpointed: %#v", out.State["waits"])
	}

	ops.mu.Lock()
	defer ops.mu.Unlock()
	parked := map[string]iwf.ParkArgs{}
	for _, p := range ops.parks {
		parked[p.StepID] = p
	}
	if len(parked) != 3 {
		t.Fatalf("parks = %#v", ops.parks)
	}
	if parked["nap:0"].Sleep == nil || parked["nap:0"].Sleep.DurationSec != 5 {
		t.Fatalf("sleep park = %#v", parked["nap:0"])
	}
	signal := parked["sig:0"].Signal
	if signal == nil || signal.Signal != "approve" || signal.TimeoutSec != 90 {
		t.Fatalf("signal park = %#v", parked["sig:0"])
	}
	if parked["child:0"].Nested == nil || parked["child:0"].Nested.ChildRunID != "child-3" {
		t.Fatalf("nested park = %#v", parked["child:0"])
	}
}

func TestFanoutRenamesNestedGroups(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	group := wf.Parallel{
		Control: wf.Control{ID: "outer"},
		ForEach: &wf.ForEach{From: wf.Path("$.input.items"), As: "item"},
		Steps: []wf.Step{wf.Sequence{
			Control: wf.Control{ID: "inner"},
			Steps: []wf.Step{
				localStep("one", nil, func(context.Context, map[string]any) (any, error) { return 1, nil }),
				wf.Publish{Control: wf.Control{ID: "two"}, Event: wf.Name("done")},
			},
		}},
	}

	state := map[string]any{"input": map[string]any{"items": []any{"a"}}}
	if _, err := newRunner(t, ops, &scriptedExec{}).Run(t.Context(), []wf.Step{group},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: state}); err != nil {
		t.Fatalf("run: %v", err)
	}

	// The runtime seeds a fanout child's row lazily off the claim, so the claim
	// has to name its parent or the whole branch lands unattached.
	parents := map[string]string{}
	ops.mu.Lock()
	for _, begin := range ops.begins {
		parents[begin.StepID] = begin.ParentStepID
	}
	ops.mu.Unlock()

	for stepID, wantParent := range map[string]string{
		"outer":   "",
		"inner:0": "outer",
		"one:0":   "inner:0",
		"two:0":   "inner:0",
	} {
		parent, claimed := parents[stepID]
		if !claimed {
			t.Fatalf("step %q was never claimed: %v", stepID, parents)
		}
		if parent != wantParent {
			t.Fatalf("step %q claims parent %q, want %q", stepID, parent, wantParent)
		}
	}
}

func TestPublishCompensationRepublishesOnTheStepsOwnEvent(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{}
	publish := wf.Publish{
		Control: wf.Control{ID: "reserve", Compensate: &wf.Compensation{
			Kind:           wf.CompensatePublish,
			Input:          map[string]any{"id": wf.Path("$.input.id")},
			IdempotencyKey: wf.Path("$.input.id"),
		}},
		Event: wf.Name("stock.reserved"),
	}

	state := map[string]any{"input": map[string]any{"id": "o-1"}, "reserve": map[string]any{"ok": true}}
	if _, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{publish},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: state, Compensating: true}); err != nil {
		t.Fatalf("compensate: %v", err)
	}

	exec.mu.Lock()
	defer exec.mu.Unlock()
	if len(exec.publishs) != 1 {
		t.Fatalf("publishes = %#v", exec.publishs)
	}
	spec := exec.publishs[0]
	if spec.Event != "stock.reserved" || spec.IdempotencyKey != "o-1" {
		t.Fatalf("compensating publish = %#v", spec)
	}
	if spec.Payload.(map[string]any)["id"] != "o-1" {
		t.Fatalf("compensating payload = %#v", spec.Payload)
	}
}

func TestRetryDelayGivesUpWithTheRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// The default delay is a real timer. Cancelling the run has to cut it short
	// instead of holding a lease nobody is renewing.
	ops := &scriptedOps{
		onFail: func(context.Context, iwf.FailStepArgs) (iwf.Decision, error) {
			cancel()
			return iwf.Decision{Action: iwf.ActionRetry, RetryDelaySec: 300}, nil
		},
	}
	exec := &scriptedExec{
		onCall: func(context.Context, iwf.CallSpec) (any, error) { return nil, errors.New("boom") },
	}
	runner, err := iwf.NewRunner(iwf.RunnerConfig{Ops: ops, Executor: exec})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(ctx, []wf.Step{callStep("charge", "billing", "charge")},
			iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
		done <- runErr
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want a cancelled run, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the retry delay outlived the run")
	}
}

func TestUnresolvableTargetFailsTheStep(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{}
	step := wf.Call{
		Control: wf.Control{ID: "charge"},
		Service: wf.Path("$.input.missing"),
		Method:  wf.Name("charge"),
	}

	_, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{step},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
	if err == nil {
		t.Fatal("want the step to fail")
	}
	if len(exec.calledMethods()) != 0 {
		t.Fatal("an unresolved target must not reach the outside world")
	}
	if got := ops.failedSteps(); len(got) != 1 || got[0] != "charge" {
		t.Fatalf("the failure was not reported: %v", got)
	}
}

func TestLocalStepWithoutAFunctionFails(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	_, err := newRunner(t, ops, &scriptedExec{}).Run(t.Context(),
		[]wf.Step{wf.Local{Control: wf.Control{ID: "work"}}},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
	if err == nil {
		t.Fatal("a local step with no closure cannot run")
	}
	if len(ops.failedSteps()) != 1 {
		t.Fatalf("the failure was not reported: %v", ops.failedSteps())
	}
}

func TestSubWorkflowStartFailureFailsTheStep(t *testing.T) {
	t.Parallel()

	ops := &scriptedOps{}
	exec := &scriptedExec{
		onStart: func(context.Context, iwf.StartSpec) (string, error) {
			return "", status.Error(codes.NotFound, "no such workflow")
		},
	}
	step := wf.SubWorkflow{Control: wf.Control{ID: "child"}, Workflow: wf.Name("missing")}

	_, err := newRunner(t, ops, exec).Run(t.Context(), []wf.Step{step},
		iwf.RunContext{RunID: "r1", LeaseEpoch: 1, State: map[string]any{"input": map[string]any{}}})
	if err == nil {
		t.Fatal("want the step to fail")
	}
	ops.mu.Lock()
	defer ops.mu.Unlock()
	if len(ops.parks) != 0 {
		t.Fatal("a child that never started must not park the parent")
	}
	// The callee's own code has to survive to the checkpoint, otherwise the
	// runtime stores an unclassified failure for a classified one.
	if ops.fails[0].ErrorCode != codes.NotFound.String() {
		t.Fatalf("error code = %q", ops.fails[0].ErrorCode)
	}
}

func TestStepErrorNamesWhatFailed(t *testing.T) {
	t.Parallel()

	err := &iwf.StepError{RunID: "r-1", StepID: "charge", Action: iwf.ActionCompensate, Err: errors.New("boom")}
	msg := err.Error()
	for _, want := range []string{"r-1", "charge", "compensate", "boom"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("%q missing from %q", want, msg)
		}
	}
	if !errors.Is(err, err.Err) {
		t.Fatal("the cause must stay reachable")
	}
}

func TestRunnerRefusesAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	if _, err := iwf.NewRunner(iwf.RunnerConfig{}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	if _, err := iwf.NewRunner(iwf.RunnerConfig{Ops: &scriptedOps{}}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	r := newRunner(t, &scriptedOps{}, &scriptedExec{})
	if _, err := r.Run(t.Context(), nil, iwf.RunContext{}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig for an empty run id, got %v", err)
	}
}
