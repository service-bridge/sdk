package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	iwf "github.com/service-bridge/sdk/go/internal/workflow"
)

// ---------------------------------------------------------------------------
// harness: a real Workflows server over bufconn
// ---------------------------------------------------------------------------

// scriptedWorkflows is a real Workflows server. Every test that talks to the
// runtime reaches it through the generated stub over bufconn, so what is under
// test is the production call path and not a hand-rolled stand-in.
type scriptedWorkflows struct {
	pb.UnimplementedWorkflowsServer

	mu           sync.Mutex
	beginCalls   []*pb.BeginStepRequest
	completeStep []*pb.CompleteStepRequest
	failCalls    []*pb.FailStepRequest
	parkCalls    []*pb.ParkRequest
	completeRuns []*pb.CompleteRunRequest
	heartbeats   []*pb.HeartbeatRequest
	subscribes   []*pb.SubscribeRequest
	starts       []*pb.StartRunRequest
	signals      []*pb.SignalRunRequest
	cancels      []*pb.CancelRunRequest
	replays      []*pb.ReplayRunRequest

	// Hooks. Nil means "answer with the zero value".
	onBegin     func(*pb.BeginStepRequest) (*pb.BeginStepResponse, error)
	onFail      func(*pb.FailStepRequest) (*pb.FailStepResponse, error)
	onHeartbeat func(*pb.HeartbeatRequest) error
	onStart     func(*pb.StartRunRequest) (*pb.StartRunResponse, error)
	onSignal    func(*pb.SignalRunRequest) error
	onQuery     func(*pb.QueryRunRequest) (*pb.QueryRunResponse, error)
	onAwait     func(*pb.AwaitRunRequest, grpc.ServerStreamingServer[pb.RunStatusUpdate]) error
	onReplay    func(*pb.ReplayRunRequest) (*pb.ReplayRunResponse, error)

	assignments chan *pb.RunAssignment
}

func newScriptedWorkflows() *scriptedWorkflows {
	return &scriptedWorkflows{assignments: make(chan *pb.RunAssignment, 32)}
}

func (s *scriptedWorkflows) BeginStep(_ context.Context, req *pb.BeginStepRequest) (*pb.BeginStepResponse, error) {
	s.mu.Lock()
	s.beginCalls = append(s.beginCalls, req)
	hook := s.onBegin
	s.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return &pb.BeginStepResponse{LeaseEpoch: req.GetLeaseEpoch()}, nil
}

func (s *scriptedWorkflows) CompleteStep(_ context.Context, req *pb.CompleteStepRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	s.completeStep = append(s.completeStep, req)
	s.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (s *scriptedWorkflows) FailStep(_ context.Context, req *pb.FailStepRequest) (*pb.FailStepResponse, error) {
	s.mu.Lock()
	s.failCalls = append(s.failCalls, req)
	hook := s.onFail
	s.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return &pb.FailStepResponse{NextAction: "fail_run"}, nil
}

func (s *scriptedWorkflows) Park(_ context.Context, req *pb.ParkRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	s.parkCalls = append(s.parkCalls, req)
	s.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (s *scriptedWorkflows) CompleteRun(_ context.Context, req *pb.CompleteRunRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	s.completeRuns = append(s.completeRuns, req)
	s.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (s *scriptedWorkflows) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	s.heartbeats = append(s.heartbeats, req)
	hook := s.onHeartbeat
	s.mu.Unlock()
	if hook != nil {
		if err := hook(req); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *scriptedWorkflows) Subscribe(req *pb.SubscribeRequest, srv grpc.ServerStreamingServer[pb.RunAssignment]) error {
	s.mu.Lock()
	s.subscribes = append(s.subscribes, req)
	s.mu.Unlock()
	for {
		select {
		case <-srv.Context().Done():
			return nil
		case msg := <-s.assignments:
			if err := srv.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (s *scriptedWorkflows) Start(_ context.Context, req *pb.StartRunRequest) (*pb.StartRunResponse, error) {
	s.mu.Lock()
	s.starts = append(s.starts, req)
	hook := s.onStart
	s.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return &pb.StartRunResponse{RunId: "run-1"}, nil
}

func (s *scriptedWorkflows) Signal(_ context.Context, req *pb.SignalRunRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	s.signals = append(s.signals, req)
	hook := s.onSignal
	s.mu.Unlock()
	if hook != nil {
		if err := hook(req); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *scriptedWorkflows) Cancel(_ context.Context, req *pb.CancelRunRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	s.cancels = append(s.cancels, req)
	s.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (s *scriptedWorkflows) Query(_ context.Context, req *pb.QueryRunRequest) (*pb.QueryRunResponse, error) {
	s.mu.Lock()
	hook := s.onQuery
	s.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return &pb.QueryRunResponse{RunId: req.GetRunId()}, nil
}

func (s *scriptedWorkflows) Await(req *pb.AwaitRunRequest, srv grpc.ServerStreamingServer[pb.RunStatusUpdate]) error {
	s.mu.Lock()
	hook := s.onAwait
	s.mu.Unlock()
	if hook != nil {
		return hook(req, srv)
	}
	return nil
}

func (s *scriptedWorkflows) Replay(_ context.Context, req *pb.ReplayRunRequest) (*pb.ReplayRunResponse, error) {
	s.mu.Lock()
	s.replays = append(s.replays, req)
	hook := s.onReplay
	s.mu.Unlock()
	if hook != nil {
		return hook(req)
	}
	return &pb.ReplayRunResponse{RunId: "replay-1"}, nil
}

func (s *scriptedWorkflows) snapshotHeartbeats() []*pb.HeartbeatRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.HeartbeatRequest, len(s.heartbeats))
	copy(out, s.heartbeats)
	return out
}

func (s *scriptedWorkflows) snapshotCompleteRuns() []*pb.CompleteRunRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.CompleteRunRequest, len(s.completeRuns))
	copy(out, s.completeRuns)
	return out
}

func startWorkflows(t *testing.T) (*scriptedWorkflows, pb.WorkflowsClient) {
	t.Helper()

	srv := newScriptedWorkflows()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterWorkflowsServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})
	return srv, pb.NewWorkflowsClient(conn)
}

// staticClients hands out one stub, the way the connection layer hands out the
// current one.
type staticClients struct{ client pb.WorkflowsClient }

func (c staticClients) WorkflowsClient(context.Context) (pb.WorkflowsClient, error) {
	return c.client, nil
}

// rotatingIdentity mints a fresh instance id on demand, the way a certificate
// rotation does.
type rotatingIdentity struct {
	mu       sync.Mutex
	service  string
	instance string
}

func (r *rotatingIdentity) get() iwf.Identity {
	r.mu.Lock()
	defer r.mu.Unlock()
	return iwf.Identity{ServiceID: r.service, InstanceID: r.instance}
}

func (r *rotatingIdentity) rotate(instance string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.instance = instance
}

func newCheckpoints(t *testing.T, client pb.WorkflowsClient, id *rotatingIdentity) *iwf.Checkpoints {
	t.Helper()
	ops, err := iwf.NewCheckpoints(iwf.CheckpointConfig{
		Clients:  staticClients{client: client},
		Identity: id.get,
	})
	if err != nil {
		t.Fatalf("new checkpoints: %v", err)
	}
	return ops
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestBeginStepReturnsCachedOutput(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onBegin = func(*pb.BeginStepRequest) (*pb.BeginStepResponse, error) {
		return &pb.BeginStepResponse{
			AlreadyDone:  true,
			CachedOutput: []byte(`{"refunded":true}`),
			LeaseEpoch:   7,
		}, nil
	}
	ops := newCheckpoints(t, client, &rotatingIdentity{service: "svc", instance: "inst-1"})

	got, err := ops.BeginStep(t.Context(), iwf.BeginStepArgs{
		RunID: "r1", StepID: "charge", Kind: "call", Input: map[string]any{"amount": 10}, LeaseEpoch: 7,
	})
	if err != nil {
		t.Fatalf("begin step: %v", err)
	}
	if !got.AlreadyDone {
		t.Fatal("want already done")
	}
	out, ok := got.Output.(map[string]any)
	if !ok || out["refunded"] != true {
		t.Fatalf("cached output not decoded: %#v", got.Output)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.beginCalls) != 1 {
		t.Fatalf("want 1 begin call, got %d", len(srv.beginCalls))
	}
	if got := srv.beginCalls[0].GetInstanceId(); got != "inst-1" {
		t.Fatalf("instance id = %q", got)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(srv.beginCalls[0].GetInputSnapshot(), &snapshot); err != nil {
		t.Fatalf("input snapshot is not json: %v", err)
	}
	if snapshot["amount"] != float64(10) {
		t.Fatalf("input snapshot = %#v", snapshot)
	}
}

func TestStaleEpochIsLostOwnership(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onBegin = func(*pb.BeginStepRequest) (*pb.BeginStepResponse, error) {
		return nil, status.Error(codes.Aborted, "lease fenced")
	}
	ops := newCheckpoints(t, client, &rotatingIdentity{service: "svc", instance: "inst-1"})

	_, err := ops.BeginStep(t.Context(), iwf.BeginStepArgs{RunID: "r1", StepID: "a", LeaseEpoch: 3})
	if !errors.Is(err, iwf.ErrLeaseLost) {
		t.Fatalf("want ErrLeaseLost, got %v", err)
	}
	var fenced *iwf.FencedError
	if !errors.As(err, &fenced) {
		t.Fatalf("want *FencedError, got %T", err)
	}
	if fenced.LeaseEpoch != 3 || fenced.RunID != "r1" || fenced.StepID != "a" {
		t.Fatalf("fenced error lost its identity: %#v", fenced)
	}
	// The transport status has to survive too, otherwise a log line says only
	// that something was fenced.
	if status.Code(fenced.Err) != codes.Aborted {
		t.Fatalf("underlying status lost: %v", fenced.Err)
	}
}

func TestTransportFailureIsNotFencing(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onBegin = func(*pb.BeginStepRequest) (*pb.BeginStepResponse, error) {
		return nil, status.Error(codes.Unavailable, "runtime restarting")
	}
	ops := newCheckpoints(t, client, &rotatingIdentity{service: "svc", instance: "inst-1"})

	_, err := ops.BeginStep(t.Context(), iwf.BeginStepArgs{RunID: "r1", StepID: "a", LeaseEpoch: 3})
	if err == nil {
		t.Fatal("want an error")
	}
	if errors.Is(err, iwf.ErrLeaseLost) {
		t.Fatal("an unavailable runtime is not lost ownership")
	}
}

func TestFailStepCarriesTheDecision(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	srv.onFail = func(*pb.FailStepRequest) (*pb.FailStepResponse, error) {
		return &pb.FailStepResponse{NextAction: "retry", RetryDelaySec: 4}, nil
	}
	ops := newCheckpoints(t, client, &rotatingIdentity{service: "svc", instance: "inst-1"})

	decision, err := ops.FailStep(t.Context(), iwf.FailStepArgs{
		RunID: "r1", StepID: "a", ErrorCode: "BOOM", ErrorMessage: "boom", LeaseEpoch: 2, Retriable: true,
	})
	if err != nil {
		t.Fatalf("fail step: %v", err)
	}
	if decision.Action != iwf.ActionRetry || decision.RetryDelaySec != 4 {
		t.Fatalf("decision = %#v", decision)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if !srv.failCalls[0].GetRetriable() {
		t.Fatal("retriable flag was dropped")
	}
}

func TestParkEncodesEveryWait(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	ops := newCheckpoints(t, client, &rotatingIdentity{service: "svc", instance: "inst-1"})
	ctx := t.Context()

	if err := ops.Park(ctx, iwf.ParkArgs{RunID: "r", StepID: "s1", LeaseEpoch: 1, Sleep: &iwf.SleepWait{DurationSec: 30}}); err != nil {
		t.Fatalf("park sleep: %v", err)
	}
	if err := ops.Park(ctx, iwf.ParkArgs{RunID: "r", StepID: "s2", LeaseEpoch: 1, Event: &iwf.EventWait{Event: "paid", FilterJSON: `{"$.id":"1"}`, TimeoutSec: 60}}); err != nil {
		t.Fatalf("park event: %v", err)
	}
	if err := ops.Park(ctx, iwf.ParkArgs{RunID: "r", StepID: "s3", LeaseEpoch: 1, Signal: &iwf.SignalWait{Signal: "approve", TimeoutSec: 5}}); err != nil {
		t.Fatalf("park signal: %v", err)
	}
	if err := ops.Park(ctx, iwf.ParkArgs{RunID: "r", StepID: "s4", LeaseEpoch: 1, Nested: &iwf.NestedWait{ChildRunID: "child-1"}}); err != nil {
		t.Fatalf("park nested: %v", err)
	}
	if err := ops.Park(ctx, iwf.ParkArgs{RunID: "r", StepID: "s5", LeaseEpoch: 1}); err == nil {
		t.Fatal("a park with no wait must be refused")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.parkCalls) != 4 {
		t.Fatalf("want 4 parks, got %d", len(srv.parkCalls))
	}
	if got := srv.parkCalls[0].GetSleep().GetDurationSec(); got != 30 {
		t.Fatalf("sleep duration = %d", got)
	}
	if got := srv.parkCalls[1].GetEventWait(); got.GetEvent() != "paid" || got.GetTimeoutSec() != 60 {
		t.Fatalf("event wait = %#v", got)
	}
	if got := srv.parkCalls[2].GetSignalWait(); got.GetSignal() != "approve" || got.GetTimeoutSec() != 5 {
		t.Fatalf("signal wait = %#v", got)
	}
	if got := srv.parkCalls[3].GetNestedRun().GetChildRunId(); got != "child-1" {
		t.Fatalf("nested run = %q", got)
	}
}

func TestHeartbeatReadsIdentityPerCall(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	id := &rotatingIdentity{service: "svc", instance: "inst-1"}
	ops := newCheckpoints(t, client, id)

	if err := ops.Heartbeat(t.Context(), iwf.HeartbeatArgs{RunID: "r1", LeaseEpoch: 1}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	id.rotate("inst-2")
	if err := ops.Heartbeat(t.Context(), iwf.HeartbeatArgs{RunID: "r1", LeaseEpoch: 1}); err != nil {
		t.Fatalf("heartbeat after rotation: %v", err)
	}

	beats := srv.snapshotHeartbeats()
	if len(beats) != 2 {
		t.Fatalf("want 2 heartbeats, got %d", len(beats))
	}
	if beats[0].GetInstanceId() != "inst-1" || beats[1].GetInstanceId() != "inst-2" {
		t.Fatalf("identity was cached: %q then %q", beats[0].GetInstanceId(), beats[1].GetInstanceId())
	}
}

func TestCompleteRunSendsTerminalStatus(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	ops := newCheckpoints(t, client, &rotatingIdentity{service: "svc", instance: "inst-1"})

	if err := ops.CompleteRun(t.Context(), iwf.CompleteRunArgs{
		RunID:          "r1",
		FinalState:     map[string]any{"charge": "ok"},
		LeaseEpoch:     9,
		TerminalStatus: "failed_compensated",
	}); err != nil {
		t.Fatalf("complete run: %v", err)
	}

	runs := srv.snapshotCompleteRuns()
	if len(runs) != 1 {
		t.Fatalf("want 1 complete run, got %d", len(runs))
	}
	if runs[0].GetTerminalStatus() != "failed_compensated" || runs[0].GetLeaseEpoch() != 9 {
		t.Fatalf("complete run = %#v", runs[0])
	}
	var state map[string]any
	if err := json.Unmarshal(runs[0].GetFinalState(), &state); err != nil {
		t.Fatalf("final state is not json: %v", err)
	}
	if state["charge"] != "ok" {
		t.Fatalf("final state = %#v", state)
	}
}

func TestEveryCheckpointSurfacesFencing(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	fence := func() error { return status.Error(codes.Aborted, "lease fenced") }
	srv.onHeartbeat = func(*pb.HeartbeatRequest) error { return fence() }
	srv.onFail = func(*pb.FailStepRequest) (*pb.FailStepResponse, error) { return nil, fence() }
	ops := newCheckpoints(t, client, &rotatingIdentity{service: "svc", instance: "inst-1"})
	ctx := t.Context()

	if _, err := ops.FailStep(ctx, iwf.FailStepArgs{RunID: "r", StepID: "a", LeaseEpoch: 1}); !errors.Is(err, iwf.ErrLeaseLost) {
		t.Fatalf("fail step: want ErrLeaseLost, got %v", err)
	}
	if err := ops.Heartbeat(ctx, iwf.HeartbeatArgs{RunID: "r", LeaseEpoch: 1}); !errors.Is(err, iwf.ErrLeaseLost) {
		t.Fatalf("heartbeat: want ErrLeaseLost, got %v", err)
	}
}

func TestHeartbeatWithoutIdentityIsRefusedLocally(t *testing.T) {
	t.Parallel()

	srv, client := startWorkflows(t)
	ops := newCheckpoints(t, client, &rotatingIdentity{})

	if err := ops.Heartbeat(t.Context(), iwf.HeartbeatArgs{RunID: "r", LeaseEpoch: 1}); !errors.Is(err, iwf.ErrNoIdentity) {
		t.Fatalf("want ErrNoIdentity, got %v", err)
	}
	if len(srv.snapshotHeartbeats()) != 0 {
		t.Fatal("a heartbeat with no identity must not reach the runtime")
	}
}

func TestCheckpointsSurfaceAMissingChannel(t *testing.T) {
	t.Parallel()

	ops, err := iwf.NewCheckpoints(iwf.CheckpointConfig{
		Clients:  brokenClients{},
		Identity: func() iwf.Identity { return iwf.Identity{ServiceID: "s", InstanceID: "i"} },
	})
	if err != nil {
		t.Fatalf("new checkpoints: %v", err)
	}
	ctx := t.Context()

	if _, err := ops.BeginStep(ctx, iwf.BeginStepArgs{RunID: "r", StepID: "a"}); err == nil {
		t.Fatal("begin step: want an error")
	}
	if err := ops.CompleteStep(ctx, iwf.CompleteStepArgs{RunID: "r", StepID: "a"}); err == nil {
		t.Fatal("complete step: want an error")
	}
	if _, err := ops.FailStep(ctx, iwf.FailStepArgs{RunID: "r", StepID: "a"}); err == nil {
		t.Fatal("fail step: want an error")
	}
	if err := ops.Park(ctx, iwf.ParkArgs{RunID: "r", StepID: "a", Sleep: &iwf.SleepWait{}}); err == nil {
		t.Fatal("park: want an error")
	}
	if err := ops.CompleteRun(ctx, iwf.CompleteRunArgs{RunID: "r"}); err == nil {
		t.Fatal("complete run: want an error")
	}
	if err := ops.Heartbeat(ctx, iwf.HeartbeatArgs{RunID: "r"}); err == nil {
		t.Fatal("heartbeat: want an error")
	}
}

func TestNewCheckpointsRefusesAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	if _, err := iwf.NewCheckpoints(iwf.CheckpointConfig{}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	if _, err := iwf.NewCheckpoints(iwf.CheckpointConfig{Clients: staticClients{}}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
}
