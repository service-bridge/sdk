package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	iwf "github.com/service-bridge/sdk/go/internal/workflow"
)

func newCaller(t *testing.T, client pb.WorkflowsClient) *iwf.Caller {
	t.Helper()
	caller, err := iwf.NewCaller(iwf.CallerConfig{Clients: staticClients{client: client}})
	if err != nil {
		t.Fatalf("new caller: %v", err)
	}
	return caller
}

func TestStartEncodesInputAndReturnsTheRunID(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onStart = func(*pb.StartRunRequest) (*pb.StartRunResponse, error) {
		return &pb.StartRunResponse{RunId: "run-42"}, nil
	}

	runID, err := newCaller(t, client).Start(t.Context(), iwf.StartArgs{
		Workflow:       "order_flow",
		Input:          map[string]any{"id": "o-1"},
		IdempotencyKey: "k-1",
		TimeoutSec:     30,
		ParentRunID:    "parent-1",
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if runID != "run-42" {
		t.Fatalf("run id = %q", runID)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	req := srv.starts[0]
	if req.GetIdempotencyKey() != "k-1" || req.GetTimeoutSec() != 30 || req.GetParentRunId() != "parent-1" {
		t.Fatalf("start request = %#v", req)
	}
	var input map[string]any
	if err := json.Unmarshal(req.GetInput(), &input); err != nil {
		t.Fatalf("input is not json: %v", err)
	}
	if input["id"] != "o-1" {
		t.Fatalf("input = %#v", input)
	}
}

func TestStartCarriesTheCallersTrace(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	tc := telemetry.TraceContext{TraceID: uuid.New(), ParentOpID: uuid.New()}
	ctx := telemetry.WithTraceContext(t.Context(), tc)

	if _, err := newCaller(t, client).Start(ctx, iwf.StartArgs{Workflow: "order_flow"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if got := srv.starts[0].GetXSbTrace(); got != telemetry.FormatHeader(tc) {
		t.Fatalf("trace header = %q", got)
	}
}

// TestStartDistinguishesItsRefusals pins the taxonomy: a policy denial, an
// unknown workflow and anything else have opposite fixes, so a caller has to be
// able to tell them apart without reading a message.
func TestStartDistinguishesItsRefusals(t *testing.T) {
	t.Parallel()

	t.Run("policy denial", func(t *testing.T) {
		t.Parallel()

		srv, client := startWorkflows(t)
		srv.onStart = func(*pb.StartRunRequest) (*pb.StartRunResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "workflow.run is not granted")
		}

		_, err := newCaller(t, client).Start(t.Context(), iwf.StartArgs{Workflow: "order_flow"})
		var denied *iwf.AccessDeniedError
		if !errors.As(err, &denied) {
			t.Fatalf("want *AccessDeniedError, got %T: %v", err, err)
		}
		if denied.Workflow != "order_flow" || denied.Reason != "workflow.run is not granted" {
			t.Fatalf("denial lost its detail: %#v", denied)
		}
		if !errors.Is(err, iwf.ErrAccessDenied) {
			t.Fatal("the denial does not match its sentinel")
		}
		if errors.Is(err, iwf.ErrWorkflowNotFound) {
			t.Fatal("a denial must not read as a missing workflow")
		}
	})

	t.Run("unknown workflow", func(t *testing.T) {
		t.Parallel()

		srv, client := startWorkflows(t)
		srv.onStart = func(*pb.StartRunRequest) (*pb.StartRunResponse, error) {
			return nil, status.Error(codes.NotFound, "no definition")
		}

		_, err := newCaller(t, client).Start(t.Context(), iwf.StartArgs{Workflow: "order_flow"})
		var missing *iwf.NotFoundError
		if !errors.As(err, &missing) {
			t.Fatalf("want *NotFoundError, got %T: %v", err, err)
		}
		if !errors.Is(err, iwf.ErrWorkflowNotFound) || errors.Is(err, iwf.ErrAccessDenied) {
			t.Fatalf("sentinels crossed: %v", err)
		}
	})

	t.Run("anything else", func(t *testing.T) {
		t.Parallel()

		srv, client := startWorkflows(t)
		srv.onStart = func(*pb.StartRunRequest) (*pb.StartRunResponse, error) {
			return nil, status.Error(codes.Unavailable, "runtime restarting")
		}

		_, err := newCaller(t, client).Start(t.Context(), iwf.StartArgs{Workflow: "order_flow"})
		if err == nil {
			t.Fatal("want an error")
		}
		if errors.Is(err, iwf.ErrAccessDenied) || errors.Is(err, iwf.ErrWorkflowNotFound) {
			t.Fatalf("a transport failure was classified: %v", err)
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("the status was lost: %v", err)
		}
	})
}

func TestSignalAgainstATerminalRunIsTyped(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onSignal = func(*pb.SignalRunRequest) error {
		return status.Error(codes.FailedPrecondition, "run is already success")
	}

	err := newCaller(t, client).Signal(t.Context(), iwf.SignalArgs{
		RunID: "r-1", Signal: "approve", Payload: map[string]any{"by": "ops"},
	})
	var terminal *iwf.TerminalError
	if !errors.As(err, &terminal) {
		t.Fatalf("want *TerminalError, got %T: %v", err, err)
	}
	if !errors.Is(err, iwf.ErrRunTerminal) {
		t.Fatal("the terminal error does not match its sentinel")
	}
}

func TestSignalAndCancelReachTheRuntime(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	caller := newCaller(t, client)

	if err := caller.Signal(t.Context(), iwf.SignalArgs{RunID: "r-1", Signal: "approve", Payload: 7}); err != nil {
		t.Fatalf("signal: %v", err)
	}
	if err := caller.Cancel(t.Context(), "r-1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.signals[0].GetSignalName() != "approve" || string(srv.signals[0].GetPayload()) != "7" {
		t.Fatalf("signal = %#v", srv.signals[0])
	}
	if srv.cancels[0].GetRunId() != "r-1" {
		t.Fatalf("cancel = %#v", srv.cancels[0])
	}
}

// TestAwaitHandsBackStateOnlyOnSuccess pins the contract: a compensated run has
// no result, and returning its half-built state as if it were one is how a
// caller acts on a run that failed.
func TestAwaitHandsBackStateOnlyOnSuccess(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		srv, client := startWorkflows(t)
		srv.onAwait = func(_ *pb.AwaitRunRequest, srv grpc.ServerStreamingServer[pb.RunStatusUpdate]) error {
			if err := srv.Send(&pb.RunStatusUpdate{RunId: "r-1", Status: "running"}); err != nil {
				return err
			}
			return srv.Send(&pb.RunStatusUpdate{
				RunId: "r-1", Status: "success", State: []byte(`{"charge":{"txn":"t-1"}}`),
			})
		}

		state, err := newCaller(t, client).Await(t.Context(), "r-1")
		if err != nil {
			t.Fatalf("await: %v", err)
		}
		charge, ok := state["charge"].(map[string]any)
		if !ok || charge["txn"] != "t-1" {
			t.Fatalf("final state = %#v", state)
		}
	})

	for _, terminalStatus := range []string{"failed", "failed_compensated", "cancelled"} {
		t.Run(terminalStatus, func(t *testing.T) {
			t.Parallel()

			srv, client := startWorkflows(t)
			srv.onAwait = func(_ *pb.AwaitRunRequest, srv grpc.ServerStreamingServer[pb.RunStatusUpdate]) error {
				return srv.Send(&pb.RunStatusUpdate{
					RunId: "r-1", Status: terminalStatus, State: []byte(`{"charge":{"txn":"t-1"}}`),
				})
			}

			state, err := newCaller(t, client).Await(t.Context(), "r-1")
			if state != nil {
				t.Fatalf("a non-successful run handed back state: %#v", state)
			}
			var terminal *iwf.TerminalError
			if !errors.As(err, &terminal) {
				t.Fatalf("want *TerminalError, got %T: %v", err, err)
			}
			if terminal.Status != terminalStatus {
				t.Fatalf("status = %q, want %q", terminal.Status, terminalStatus)
			}
		})
	}
}

func TestAwaitWithoutAnyUpdateIsAnError(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onAwait = func(*pb.AwaitRunRequest, grpc.ServerStreamingServer[pb.RunStatusUpdate]) error { return nil }

	if _, err := newCaller(t, client).Await(t.Context(), "r-1"); err == nil {
		t.Fatal("a stream that says nothing is not a successful run")
	}
}

func TestAwaitWithUnreadableStateIsAnError(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onAwait = func(_ *pb.AwaitRunRequest, srv grpc.ServerStreamingServer[pb.RunStatusUpdate]) error {
		return srv.Send(&pb.RunStatusUpdate{RunId: "r-1", Status: "success", State: []byte("{not json")})
	}

	if _, err := newCaller(t, client).Await(t.Context(), "r-1"); err == nil {
		t.Fatal("state that cannot be read is not a result")
	}
}

func TestQueryDecodesStateAndSteps(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onQuery = func(req *pb.QueryRunRequest) (*pb.QueryRunResponse, error) {
		return &pb.QueryRunResponse{
			RunId:  req.GetRunId(),
			Status: "compensating",
			State:  []byte(`{"charge":{"txn":"t-1"}}`),
			Steps: []*pb.StepInfo{
				{StepId: "charge", Status: "compensated", Output: []byte(`{"txn":"t-1"}`), CompensatedBy: "charge.compensate"},
				{StepId: "ship", Status: "failed", LastError: "carrier down"},
			},
		}, nil
	}

	snapshot, err := newCaller(t, client).Query(t.Context(), "r-1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if snapshot.Status != "compensating" || len(snapshot.Steps) != 2 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if snapshot.State["charge"].(map[string]any)["txn"] != "t-1" {
		t.Fatalf("state = %#v", snapshot.State)
	}
	if snapshot.Steps[0].CompensatedBy != "charge.compensate" {
		t.Fatalf("step = %#v", snapshot.Steps[0])
	}
	if snapshot.Steps[1].Output != nil || snapshot.Steps[1].LastError != "carrier down" {
		t.Fatalf("step = %#v", snapshot.Steps[1])
	}
}

func TestReplayForksTheRun(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onReplay = func(*pb.ReplayRunRequest) (*pb.ReplayRunResponse, error) {
		return &pb.ReplayRunResponse{RunId: "run-copy"}, nil
	}

	runID, err := newCaller(t, client).Replay(t.Context(), "r-1", "ship")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if runID != "run-copy" {
		t.Fatalf("run id = %q", runID)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.replays[0].GetFromStepId() != "ship" {
		t.Fatalf("replay = %#v", srv.replays[0])
	}
}

func TestRunLevelRefusalsAreClassified(t *testing.T) {
	t.Parallel()

	t.Run("unknown run", func(t *testing.T) {
		t.Parallel()

		srv, client := startWorkflows(t)
		srv.onQuery = func(*pb.QueryRunRequest) (*pb.QueryRunResponse, error) {
			return nil, status.Error(codes.NotFound, "no such run")
		}
		if _, err := newCaller(t, client).Query(t.Context(), "r-1"); !errors.Is(err, iwf.ErrWorkflowNotFound) {
			t.Fatalf("want ErrWorkflowNotFound, got %v", err)
		}
	})

	t.Run("denied", func(t *testing.T) {
		t.Parallel()

		srv, client := startWorkflows(t)
		srv.onSignal = func(*pb.SignalRunRequest) error {
			return status.Error(codes.PermissionDenied, "not the owner")
		}
		err := newCaller(t, client).Signal(t.Context(), iwf.SignalArgs{RunID: "r-1", Signal: "go"})
		if !errors.Is(err, iwf.ErrAccessDenied) {
			t.Fatalf("want ErrAccessDenied, got %v", err)
		}
	})

	t.Run("anything else", func(t *testing.T) {
		t.Parallel()

		srv, client := startWorkflows(t)
		srv.onQuery = func(*pb.QueryRunRequest) (*pb.QueryRunResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		}
		_, err := newCaller(t, client).Query(t.Context(), "r-1")
		if err == nil || errors.Is(err, iwf.ErrRunTerminal) || errors.Is(err, iwf.ErrWorkflowNotFound) {
			t.Fatalf("a plain failure was classified: %v", err)
		}
	})
}

func TestCallerErrorsNameWhatTheyRefused(t *testing.T) {
	t.Parallel()

	denied := &iwf.AccessDeniedError{Workflow: "order_flow", Reason: "not granted"}
	if !strings.Contains(denied.Error(), "order_flow") || !strings.Contains(denied.Error(), "not granted") {
		t.Fatalf("denial message = %q", denied.Error())
	}
	missing := &iwf.NotFoundError{Workflow: "order_flow"}
	if !strings.Contains(missing.Error(), "order_flow") {
		t.Fatalf("not-found message = %q", missing.Error())
	}
	terminal := &iwf.TerminalError{RunID: "r-1", Status: "cancelled"}
	if !strings.Contains(terminal.Error(), "r-1") || !strings.Contains(terminal.Error(), "cancelled") {
		t.Fatalf("terminal message = %q", terminal.Error())
	}
}

func TestCallerRefusesAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	if _, err := iwf.NewCaller(iwf.CallerConfig{}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
}

// brokenClients models the channel going away underneath a caller.
type brokenClients struct{}

func (brokenClients) WorkflowsClient(context.Context) (pb.WorkflowsClient, error) {
	return nil, errors.New("channel is down")
}

func TestCallerSurfacesAMissingChannel(t *testing.T) {
	t.Parallel()

	caller, err := iwf.NewCaller(iwf.CallerConfig{Clients: brokenClients{}})
	if err != nil {
		t.Fatalf("new caller: %v", err)
	}
	if _, err := caller.Start(t.Context(), iwf.StartArgs{Workflow: "w"}); err == nil {
		t.Fatal("want an error")
	}
	if err := caller.Cancel(t.Context(), "r-1"); err == nil {
		t.Fatal("want an error")
	}
	if _, err := caller.Query(t.Context(), "r-1"); err == nil {
		t.Fatal("want an error")
	}
	if _, err := caller.Await(t.Context(), "r-1"); err == nil {
		t.Fatal("want an error")
	}
	if _, err := caller.Replay(t.Context(), "r-1", ""); err == nil {
		t.Fatal("want an error")
	}
	if err := caller.Signal(t.Context(), iwf.SignalArgs{RunID: "r-1"}); err == nil {
		t.Fatal("want an error")
	}
}
