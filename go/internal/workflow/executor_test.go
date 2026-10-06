package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	iwf "github.com/service-bridge/sdk/go/internal/workflow"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// fakeClient is an in-memory Workflows stub.
type fakeClient struct {
	pb.WorkflowsClient
	mu        sync.Mutex
	tasks     chan *pb.StepTask
	completed map[string][]byte
	failed    map[string]*pb.FailTaskRequest
	beats     [][]string
	lost      map[string]bool
	failOnce  int
	err       error
	last      any
	await     []*pb.RunStatusUpdate
	snapshot  *pb.RunSnapshot
}

func newFake() *fakeClient {
	return &fakeClient{tasks: make(chan *pb.StepTask, 16), completed: map[string][]byte{}, failed: map[string]*pb.FailTaskRequest{}, lost: map[string]bool{}}
}

// source hands the fake out as the ClientSource.
type source struct{ f *fakeClient }

func (s source) WorkflowsClient(context.Context) (pb.WorkflowsClient, error) { return s.f, nil }

func (f *fakeClient) Subscribe(ctx context.Context, _ *pb.SubscribeRequest, _ ...grpc.CallOption) (pb.Workflows_SubscribeClient, error) {
	return &taskStream{ctx: ctx, tasks: f.tasks}, nil
}

func (f *fakeClient) CompleteTask(_ context.Context, r *pb.CompleteTaskRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOnce > 0 {
		f.failOnce--
		return nil, status.Error(codes.Unavailable, "down")
	}
	f.completed[r.GetTaskToken()] = r.GetOutput()
	return &emptypb.Empty{}, nil
}

func (f *fakeClient) FailTask(_ context.Context, r *pb.FailTaskRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[r.GetTaskToken()] = r
	return &emptypb.Empty{}, nil
}

func (f *fakeClient) Heartbeat(_ context.Context, r *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beats = append(f.beats, r.GetTaskTokens())
	var lost []string
	for _, tok := range r.GetTaskTokens() {
		if f.lost[tok] {
			lost = append(lost, tok)
		}
	}
	return &pb.HeartbeatResponse{LostTokens: lost}, nil
}

type taskStream struct {
	grpc.ClientStream
	ctx   context.Context
	tasks chan *pb.StepTask
}

func (s *taskStream) Recv() (*pb.StepTask, error) {
	select {
	case t := <-s.tasks:
		return t, nil
	case <-s.ctx.Done():
		return nil, io.EOF
	}
}
func (s *taskStream) Header() (metadata.MD, error) { return nil, nil }
func (s *taskStream) Context() context.Context     { return s.ctx }

type fakeDefs map[string]wf.LocalFunc

func (d fakeDefs) Local(workflow, version, step string) (wf.LocalFunc, bool) {
	fn, ok := d[workflow+"@"+version+"/"+step]
	return fn, ok
}

type fakeEffects struct {
	mu        sync.Mutex
	calls     []iwf.CallSpec
	publishes []iwf.PublishSpec
	callErr   error
	traced    bool
}

func (e *fakeEffects) Call(ctx context.Context, s iwf.CallSpec) (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, e.traced = telemetry.FromContext(ctx)
	e.calls = append(e.calls, s)
	if e.callErr != nil {
		return nil, e.callErr
	}
	return map[string]any{"ok": true}, nil
}

func (e *fakeEffects) Publish(_ context.Context, s iwf.PublishSpec) (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.publishes = append(e.publishes, s)
	return map[string]any{"eventId": "e1"}, nil
}

func newExecutor(t *testing.T, f *fakeClient, defs fakeDefs, eff *fakeEffects, spans *[]iwf.Span) *iwf.Executor {
	t.Helper()
	var mu sync.Mutex
	ex, err := iwf.NewExecutor(iwf.ExecutorConfig{
		Clients:     source{f},
		Identity:    func() iwf.Identity { return iwf.Identity{ServiceID: "s", InstanceID: "i"} },
		Definitions: defs,
		Effects:     eff,
		WrapSpan: func(ctx context.Context, s iwf.Span, fn func(context.Context) (any, error)) (any, error) {
			mu.Lock()
			*spans = append(*spans, s)
			mu.Unlock()
			return fn(ctx)
		},
		ErrorCode: func(err error) string {
			if errors.Is(err, errDeclined) {
				return "DECLINED"
			}
			return ""
		},
		Backoff: stream.NewBackoff(stream.WithLadder(10 * time.Millisecond)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ex.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ex.Stop)
	return ex
}

var errDeclined = errors.New("declined")

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *fakeClient) done(token string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, c := f.completed[token]
	_, x := f.failed[token]
	return c || x
}

func TestExecutorRunsTasks(t *testing.T) {
	f := newFake()
	eff := &fakeEffects{}
	var spans []iwf.Span
	defs := fakeDefs{"wf@v1/calc": func(ctx context.Context, state map[string]any) (any, error) {
		task, _ := wf.TaskOf(ctx)
		return map[string]any{"n": state["input"], "attempt": task.Attempt}, nil
	}}
	newExecutor(t, f, defs, eff, &spans)

	f.tasks <- &pb.StepTask{TaskToken: "l", RunId: "r", Workflow: "wf", Version: "v1", StepId: "calc", TemplateStepId: "calc",
		Kind: pb.TaskKind_TASK_KIND_LOCAL, Attempt: 2, State: []byte(`{"input":5}`), HeartbeatIntervalMs: 10_000}
	f.tasks <- &pb.StepTask{TaskToken: "c", Kind: pb.TaskKind_TASK_KIND_CALL, Service: "billing", Method: "charge", Input: []byte(`{"a":1}`),
		CallOpts: &pb.ResolvedCallOptions{TimeoutMs: 500, Transport: "proxy", IdempotencyKey: "k", Retry: &pb.RetryPolicy{MaxAttempts: 2}},
		XSbTrace: "01a11369-e617-7683-9b36-3551ddd8a6a8-01a11369-e617-7683-9b36-3551ddd8a6a9"}
	f.tasks <- &pb.StepTask{TaskToken: "p", Kind: pb.TaskKind_TASK_KIND_PUBLISH, Event: "e", Input: []byte(`[1]`),
		PublishOpts: &pb.ResolvedPublishOptions{PartitionKey: "pk", Headers: map[string]string{"h": "v"}}}
	f.tasks <- &pb.StepTask{TaskToken: "x", Kind: pb.TaskKind_TASK_KIND_CALL, Service: "b", Method: "refund", IsCompensation: true, CompensatesStepId: "charge"}
	eventually(t, func() bool { return f.done("l") && f.done("c") && f.done("p") && f.done("x") })

	var out map[string]any
	_ = json.Unmarshal(f.completed["l"], &out)
	if out["n"] != float64(5) || out["attempt"] != float64(2) {
		t.Fatalf("local output %s", f.completed["l"])
	}
	var charge iwf.CallSpec
	for _, c := range eff.calls {
		if c.Method == "charge" {
			charge = c
		}
	}
	if len(eff.calls) != 2 || charge.Timeout != 500*time.Millisecond || charge.Retry.MaxAttempts != 2 || charge.Transport != "proxy" {
		t.Fatalf("calls %+v", eff.calls)
	}
	if eff.publishes[0].PartitionKey != "pk" || eff.publishes[0].Headers["h"] != "v" {
		t.Fatalf("publish %+v", eff.publishes)
	}
	comp := 0
	for _, sp := range spans {
		if sp.IsCompensation && sp.CompensatesStepID == "charge" {
			comp++
		}
	}
	if len(spans) != 2 || comp != 1 {
		t.Fatalf("spans %+v (local and compensation only)", spans)
	}
}

func TestExecutorReportsFailures(t *testing.T) {
	f := newFake()
	eff := &fakeEffects{callErr: errDeclined}
	var spans []iwf.Span
	newExecutor(t, f, fakeDefs{}, eff, &spans)
	f.tasks <- &pb.StepTask{TaskToken: "c", Kind: pb.TaskKind_TASK_KIND_CALL, Service: "b", Method: "m"}
	f.tasks <- &pb.StepTask{TaskToken: "v", Kind: pb.TaskKind_TASK_KIND_LOCAL, Workflow: "wf", Version: "v9", TemplateStepId: "s"}
	f.tasks <- &pb.StepTask{TaskToken: "u", Kind: pb.TaskKind_TASK_KIND_UNSPECIFIED}
	eventually(t, func() bool { return f.done("c") && f.done("v") && f.done("u") })
	if f.failed["c"].GetErrorCode() != "DECLINED" || f.failed["c"].GetNonRetriable() {
		t.Fatalf("call failure %+v", f.failed["c"])
	}
	if f.failed["v"].GetErrorCode() != "UNSUPPORTED_VERSION" || !f.failed["v"].GetNonRetriable() {
		t.Fatalf("version failure %+v", f.failed["v"])
	}
}

func TestExecutorAbortsOnLostLeaseAndDeadline(t *testing.T) {
	f := newFake()
	f.lost["gone"] = true
	aborted := make(chan string, 2)
	defs := fakeDefs{"wf@v1/s": func(ctx context.Context, _ map[string]any) (any, error) {
		<-ctx.Done()
		task, _ := wf.TaskOf(ctx)
		aborted <- task.StepID
		return nil, ctx.Err()
	}}
	var spans []iwf.Span
	ex := newExecutor(t, f, defs, &fakeEffects{}, &spans)
	f.tasks <- &pb.StepTask{TaskToken: "gone", RunId: "r", StepId: "lost", Kind: pb.TaskKind_TASK_KIND_LOCAL, Workflow: "wf", Version: "v1", TemplateStepId: "s", HeartbeatIntervalMs: 250}
	f.tasks <- &pb.StepTask{TaskToken: "late", RunId: "r", StepId: "late", Kind: pb.TaskKind_TASK_KIND_LOCAL, Workflow: "wf", Version: "v1", TemplateStepId: "s",
		HeartbeatIntervalMs: 60_000, DeadlineUnixMs: time.Now().Add(50 * time.Millisecond).UnixMilli()}
	got := map[string]bool{<-aborted: true, <-aborted: true}
	if !got["lost"] || !got["late"] {
		t.Fatalf("aborted %v", got)
	}
	eventually(t, func() bool { return f.done("late") && ex.InFlight() == 0 })
	if f.done("gone") {
		t.Fatal("a task with a lost lease reported a result")
	}
}

func TestExecutorRetriesTransientReport(t *testing.T) {
	f := newFake()
	f.failOnce = 1
	var spans []iwf.Span
	newExecutor(t, f, fakeDefs{"wf@v1/s": func(context.Context, map[string]any) (any, error) { return "x", nil }}, &fakeEffects{}, &spans)
	f.tasks <- &pb.StepTask{TaskToken: "t", Kind: pb.TaskKind_TASK_KIND_LOCAL, Workflow: "wf", Version: "v1", TemplateStepId: "s"}
	eventually(t, func() bool { return f.done("t") })
}

func TestNewExecutorValidates(t *testing.T) {
	if _, err := iwf.NewExecutor(iwf.ExecutorConfig{}); !errors.Is(err, iwf.ErrInvalidConfig) {
		t.Fatalf("empty config: %v", err)
	}
}
