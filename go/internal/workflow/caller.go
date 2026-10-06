package workflow

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// Reasons a caller-side operation is refused. They have opposite fixes: a
// denied policy needs a grant, an unknown workflow a deployment, a terminal run
// neither.
var (
	ErrAccessDenied     = errors.New("workflow: access denied")
	ErrWorkflowNotFound = errors.New("workflow: not found")
	ErrRunTerminal      = errors.New("workflow: run is terminal")
	ErrRunFailed        = errors.New("workflow: run did not succeed")
)

// AccessDeniedError names what the policy refused and why.
type AccessDeniedError struct {
	Target string
	Reason string
}

func (e *AccessDeniedError) Error() string {
	return fmt.Sprintf("workflow %s: access denied: %s", e.Target, e.Reason)
}

func (e *AccessDeniedError) Unwrap() error { return ErrAccessDenied }

// NotFoundError names a workflow or run the runtime does not know.
type NotFoundError struct{ Target string }

func (e *NotFoundError) Error() string { return fmt.Sprintf("workflow %s: not found", e.Target) }

func (e *NotFoundError) Unwrap() error { return ErrWorkflowNotFound }

// TerminalError reports an operation that needs a run in another state.
type TerminalError struct {
	RunID  string
	Detail string
}

func (e *TerminalError) Error() string { return fmt.Sprintf("workflow run %s: %s", e.RunID, e.Detail) }

func (e *TerminalError) Unwrap() error { return ErrRunTerminal }

// RunFailedError is what Await returns for a run that ended other than success.
type RunFailedError struct {
	RunID        string
	Status       string
	ErrorCode    string
	ErrorMessage string
}

func (e *RunFailedError) Error() string {
	return fmt.Sprintf("workflow run %s ended %s: %s %s", e.RunID, e.Status, e.ErrorCode, e.ErrorMessage)
}

func (e *RunFailedError) Unwrap() error { return ErrRunFailed }

// StartArgs starts one run.
type StartArgs struct {
	Service        string
	Workflow       string
	Input          any
	IdempotencyKey string
	TimeoutMs      int64
}

// SignalArgs enqueues one signal.
type SignalArgs struct {
	RunID    string
	Signal   string
	Payload  any
	SignalID string
}

// StepSnapshot is what Query reports about one step.
type StepSnapshot struct {
	StepID            string
	ParentStepID      string
	Kind              string
	Status            string
	Attempt           int
	Output            any
	ErrorCode         string
	ErrorMessage      string
	WaitingReason     string
	WaitKey           string
	ChildRunID        string
	CompensatesStepID string
	StartedAtMs       int64
	EndedAtMs         int64
}

// PendingSignal is a signal queued for a run and not consumed yet.
type PendingSignal struct {
	Name         string
	SignalID     string
	Payload      any
	EnqueuedAtMs int64
}

// RunSnapshot is a point-in-time view of a run.
type RunSnapshot struct {
	RunID         string
	Service       string
	Workflow      string
	Status        string
	StopReason    string
	WaitingReason string
	Input         any
	Output        map[string]any
	ErrorCode     string
	ErrorMessage  string
	ParentRunID   string
	StartedAtMs   int64
	EndedAtMs     int64
	Steps         []StepSnapshot
	Signals       []PendingSignal
}

// CallerConfig wires the caller side.
type CallerConfig struct {
	Clients ClientSource
}

// Caller starts runs, steers them and reads them back.
type Caller struct {
	clients ClientSource
}

// NewCaller validates the config.
func NewCaller(cfg CallerConfig) (*Caller, error) {
	if cfg.Clients == nil {
		return nil, fmt.Errorf("workflow: new caller: missing client source: %w", ErrInvalidConfig)
	}
	return &Caller{clients: cfg.Clients}, nil
}

// Start creates a run of the service's workflow and returns its id.
func (c *Caller) Start(ctx context.Context, args StartArgs) (string, error) {
	target := args.Service + "/" + args.Workflow
	client, err := c.client(ctx, "start")
	if err != nil {
		return "", err
	}
	input, err := encodeJSON(args.Input)
	if err != nil {
		return "", fmt.Errorf("workflow: start %s: encode input: %w", target, err)
	}
	resp, err := client.Start(ctx, &pb.StartRunRequest{
		Service:        args.Service,
		Workflow:       args.Workflow,
		Input:          input,
		IdempotencyKey: args.IdempotencyKey,
		TimeoutMs:      args.TimeoutMs,
		XSbTrace:       traceHeader(ctx),
	})
	if err != nil {
		return "", callError("start", target, err)
	}
	return resp.GetRunId(), nil
}

// Signal enqueues a signal; duplicate reports a repeated SignalID.
func (c *Caller) Signal(ctx context.Context, args SignalArgs) (bool, error) {
	client, err := c.client(ctx, "signal")
	if err != nil {
		return false, err
	}
	payload, err := encodeJSON(args.Payload)
	if err != nil {
		return false, fmt.Errorf("workflow: signal run %s: encode payload: %w", args.RunID, err)
	}
	resp, err := client.Signal(ctx, &pb.SignalRunRequest{
		RunId: args.RunID, SignalName: args.Signal, Payload: payload, SignalId: args.SignalID,
	})
	if err != nil {
		return false, callError("signal", args.RunID, err)
	}
	return resp.GetDuplicate(), nil
}

// Cancel stops a run; its compensations run before it ends cancelled.
func (c *Caller) Cancel(ctx context.Context, runID string) error {
	client, err := c.client(ctx, "cancel")
	if err != nil {
		return err
	}
	if _, err := client.Cancel(ctx, &pb.CancelRunRequest{RunId: runID}); err != nil {
		return callError("cancel", runID, err)
	}
	return nil
}

// RetryCompensation re-runs the failed compensations of a failed_compensated run.
func (c *Caller) RetryCompensation(ctx context.Context, runID string) error {
	client, err := c.client(ctx, "retry compensation")
	if err != nil {
		return err
	}
	if _, err := client.RetryCompensation(ctx, &pb.RetryCompensationRequest{RunId: runID}); err != nil {
		return callError("retry compensation", runID, err)
	}
	return nil
}

// Query reads a run without waiting for it.
func (c *Caller) Query(ctx context.Context, runID string) (RunSnapshot, error) {
	client, err := c.client(ctx, "query")
	if err != nil {
		return RunSnapshot{}, err
	}
	resp, err := client.Query(ctx, &pb.QueryRunRequest{RunId: runID})
	if err != nil {
		return RunSnapshot{}, callError("query", runID, err)
	}
	return toSnapshot(resp)
}

// Await blocks until the run ends. It returns the run output on success and a
// RunFailedError naming any other terminal status.
func (c *Caller) Await(ctx context.Context, runID string) (map[string]any, error) {
	client, err := c.client(ctx, "await")
	if err != nil {
		return nil, err
	}
	st, err := client.Await(ctx, &pb.AwaitRunRequest{RunId: runID})
	if err != nil {
		return nil, callError("await", runID, err)
	}
	var last *pb.RunStatusUpdate
	for {
		update, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, callError("await", runID, err)
		}
		last = update
	}
	if last == nil || !last.GetTerminal() {
		return nil, &TerminalError{RunID: runID, Detail: "await stream ended before a terminal status"}
	}
	if last.GetStatus() != "success" {
		return nil, &RunFailedError{RunID: runID, Status: last.GetStatus(), ErrorCode: last.GetErrorCode(), ErrorMessage: last.GetErrorMessage()}
	}
	return decodeObject(last.GetOutput())
}

// Replay starts a new run from the source's frozen definition and input; a
// non-empty fromStepID keeps the steps that do not depend on it.
func (c *Caller) Replay(ctx context.Context, runID, fromStepID string) (string, error) {
	client, err := c.client(ctx, "replay")
	if err != nil {
		return "", err
	}
	resp, err := client.Replay(ctx, &pb.ReplayRunRequest{RunId: runID, FromStepId: fromStepID})
	if err != nil {
		return "", callError("replay", runID, err)
	}
	return resp.GetRunId(), nil
}

func (c *Caller) client(ctx context.Context, op string) (pb.WorkflowsClient, error) {
	client, err := c.clients.WorkflowsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("workflow: %s: workflows client: %w", op, err)
	}
	return client, nil
}

// traceHeader carries the caller's trace so the run hangs under it.
func traceHeader(ctx context.Context) string {
	tc, ok := telemetry.FromContext(ctx)
	if !ok {
		return ""
	}
	return telemetry.FormatHeader(tc)
}

func callError(op, target string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("workflow: %s %s: %w", op, target, err)
	}
	switch st.Code() {
	case codes.PermissionDenied:
		return &AccessDeniedError{Target: target, Reason: st.Message()}
	case codes.NotFound:
		return &NotFoundError{Target: target}
	case codes.FailedPrecondition:
		return &TerminalError{RunID: target, Detail: st.Message()}
	}
	return fmt.Errorf("workflow: %s %s: %w", op, target, err)
}

func decodeObject(blob []byte) (map[string]any, error) {
	v, err := decodeJSON(blob)
	if err != nil {
		return nil, err
	}
	m, _ := v.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

func toSnapshot(r *pb.RunSnapshot) (RunSnapshot, error) {
	input, err := decodeJSON(r.GetInput())
	if err != nil {
		return RunSnapshot{}, fmt.Errorf("workflow: query run %s: input: %w", r.GetRunId(), err)
	}
	out := RunSnapshot{
		RunID: r.GetRunId(), Service: r.GetService(), Workflow: r.GetWorkflow(), Status: r.GetStatus(),
		StopReason: r.GetStopReason(), WaitingReason: r.GetWaitingReason(), Input: input,
		ErrorCode: r.GetErrorCode(), ErrorMessage: r.GetErrorMessage(), ParentRunID: r.GetParentRunId(),
		StartedAtMs: r.GetStartedAtUnixMs(), EndedAtMs: r.GetEndedAtUnixMs(),
	}
	if len(r.GetOutput()) > 0 {
		if out.Output, err = decodeObject(r.GetOutput()); err != nil {
			return RunSnapshot{}, fmt.Errorf("workflow: query run %s: output: %w", r.GetRunId(), err)
		}
	}
	for _, s := range r.GetSteps() {
		output, err := decodeJSON(s.GetOutput())
		if err != nil {
			return RunSnapshot{}, fmt.Errorf("workflow: query run %s: step %q: %w", r.GetRunId(), s.GetStepId(), err)
		}
		out.Steps = append(out.Steps, StepSnapshot{
			StepID: s.GetStepId(), ParentStepID: s.GetParentStepId(), Kind: s.GetKind(), Status: s.GetStatus(),
			Attempt: int(s.GetAttempt()), Output: output, ErrorCode: s.GetErrorCode(), ErrorMessage: s.GetErrorMessage(),
			WaitingReason: s.GetWaitingReason(), WaitKey: s.GetWaitKey(), ChildRunID: s.GetChildRunId(),
			CompensatesStepID: s.GetCompensatesStepId(), StartedAtMs: s.GetStartedAtUnixMs(), EndedAtMs: s.GetEndedAtUnixMs(),
		})
	}
	for _, sig := range r.GetSignals() {
		payload, err := decodeJSON(sig.GetPayload())
		if err != nil {
			return RunSnapshot{}, fmt.Errorf("workflow: query run %s: signal: %w", r.GetRunId(), err)
		}
		out.Signals = append(out.Signals, PendingSignal{Name: sig.GetSignalName(), SignalID: sig.GetSignalId(), Payload: payload, EnqueuedAtMs: sig.GetEnqueuedAtUnixMs()})
	}
	return out, nil
}
