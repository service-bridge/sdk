package workflow_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	iwf "github.com/service-bridge/sdk/go/internal/workflow"
)

func (f *fakeClient) Start(_ context.Context, r *pb.StartRunRequest, _ ...grpc.CallOption) (*pb.StartRunResponse, error) {
	f.last = r
	if f.err != nil {
		return nil, f.err
	}
	return &pb.StartRunResponse{RunId: "r1"}, nil
}

func (f *fakeClient) Signal(_ context.Context, r *pb.SignalRunRequest, _ ...grpc.CallOption) (*pb.SignalRunResponse, error) {
	f.last = r
	if f.err != nil {
		return nil, f.err
	}
	return &pb.SignalRunResponse{Duplicate: r.GetSignalId() == "dup"}, nil
}

func (f *fakeClient) Cancel(context.Context, *pb.CancelRunRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, f.err
}

func (f *fakeClient) RetryCompensation(context.Context, *pb.RetryCompensationRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, f.err
}

func (f *fakeClient) Replay(_ context.Context, r *pb.ReplayRunRequest, _ ...grpc.CallOption) (*pb.ReplayRunResponse, error) {
	f.last = r
	return &pb.ReplayRunResponse{RunId: "r2"}, f.err
}

func (f *fakeClient) Query(context.Context, *pb.QueryRunRequest, ...grpc.CallOption) (*pb.RunSnapshot, error) {
	return f.snapshot, f.err
}

func (f *fakeClient) Await(context.Context, *pb.AwaitRunRequest, ...grpc.CallOption) (pb.Workflows_AwaitClient, error) {
	return &awaitStream{updates: f.await}, f.err
}

type awaitStream struct {
	grpc.ClientStream
	updates []*pb.RunStatusUpdate
}

func (s *awaitStream) Recv() (*pb.RunStatusUpdate, error) {
	if len(s.updates) == 0 {
		return nil, io.EOF
	}
	u := s.updates[0]
	s.updates = s.updates[1:]
	return u, nil
}

func caller(t *testing.T, f *fakeClient) *iwf.Caller {
	t.Helper()
	c, err := iwf.NewCaller(iwf.CallerConfig{Clients: source{f}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCallerStartAndSignal(t *testing.T) {
	f := newFake()
	c := caller(t, f)
	id, err := c.Start(context.Background(), iwf.StartArgs{Service: "orders", Workflow: "flow", Input: map[string]any{"a": 1}, IdempotencyKey: "k", TimeoutMs: 10})
	if err != nil || id != "r1" {
		t.Fatal(id, err)
	}
	req := f.last.(*pb.StartRunRequest)
	if req.GetService() != "orders" || req.GetWorkflow() != "flow" || string(req.GetInput()) != `{"a":1}` || req.GetTimeoutMs() != 10 {
		t.Fatalf("start request %+v", req)
	}
	if dup, err := c.Signal(context.Background(), iwf.SignalArgs{RunID: "r", Signal: "go", SignalID: "dup"}); err != nil || !dup {
		t.Fatalf("dup=%v err=%v", dup, err)
	}
	if id, err := c.Replay(context.Background(), "r", "b"); err != nil || id != "r2" {
		t.Fatal(id, err)
	}
	if err := c.Cancel(context.Background(), "r"); err != nil {
		t.Fatal(err)
	}
	if err := c.RetryCompensation(context.Background(), "r"); err != nil {
		t.Fatal(err)
	}
}

func TestCallerMapsStatusCodes(t *testing.T) {
	cases := map[codes.Code]error{
		codes.PermissionDenied:   iwf.ErrAccessDenied,
		codes.NotFound:           iwf.ErrWorkflowNotFound,
		codes.FailedPrecondition: iwf.ErrRunTerminal,
	}
	for code, want := range cases {
		f := newFake()
		f.err = status.Error(code, "x")
		c := caller(t, f)
		if _, err := c.Start(context.Background(), iwf.StartArgs{Service: "s", Workflow: "w"}); !errors.Is(err, want) {
			t.Errorf("%v: %v", code, err)
		}
		if err := c.Cancel(context.Background(), "r"); !errors.Is(err, want) {
			t.Errorf("%v cancel: %v", code, err)
		}
	}
	f := newFake()
	f.err = status.Error(codes.Internal, "boom")
	if _, err := caller(t, f).Signal(context.Background(), iwf.SignalArgs{RunID: "r"}); err == nil || errors.Is(err, iwf.ErrRunTerminal) {
		t.Fatalf("internal: %v", err)
	}
}

func TestCallerAwait(t *testing.T) {
	f := newFake()
	f.await = []*pb.RunStatusUpdate{{Status: "active"}, {Status: "success", Terminal: true, Output: []byte(`{"a":1}`)}}
	out, err := caller(t, f).Await(context.Background(), "r")
	if err != nil || out["a"] != float64(1) {
		t.Fatal(out, err)
	}
	f.await = []*pb.RunStatusUpdate{{Status: "timed_out", Terminal: true, ErrorCode: "TIMEOUT"}}
	_, err = caller(t, f).Await(context.Background(), "r")
	var failed *iwf.RunFailedError
	if !errors.As(err, &failed) || failed.Status != "timed_out" || !errors.Is(err, iwf.ErrRunFailed) {
		t.Fatalf("failed await: %v", err)
	}
	f.await = nil
	if _, err := caller(t, f).Await(context.Background(), "r"); !errors.Is(err, iwf.ErrRunTerminal) {
		t.Fatalf("empty stream: %v", err)
	}
}

func TestCallerQuery(t *testing.T) {
	f := newFake()
	f.snapshot = &pb.RunSnapshot{RunId: "r", Status: "active", WaitingReason: "signal", Input: []byte(`{}`),
		Steps:   []*pb.StepInfo{{StepId: "w", Status: "parked", WaitKey: "go", Output: []byte(`null`)}},
		Signals: []*pb.PendingSignal{{SignalName: "x", Payload: []byte(`1`), EnqueuedAtUnixMs: 5}}}
	snap, err := caller(t, f).Query(context.Background(), "r")
	if err != nil || snap.WaitingReason != "signal" || snap.Steps[0].WaitKey != "go" || snap.Signals[0].Payload != float64(1) || snap.Output != nil {
		t.Fatalf("%+v %v", snap, err)
	}
	f.snapshot.Output = []byte(`{`)
	if _, err := caller(t, f).Query(context.Background(), "r"); err == nil {
		t.Fatal("bad output accepted")
	}
}

func TestNewCallerValidates(t *testing.T) {
	if _, err := iwf.NewCaller(iwf.CallerConfig{}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatal(err)
	}
}
