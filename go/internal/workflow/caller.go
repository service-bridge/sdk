package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reasons a caller-side operation is refused. They are separate sentinels
// because they have opposite fixes and an operator sent to the wrong one wastes
// an outage: a denied policy needs a grant, an unknown workflow needs a
// deployment, a terminal run needs neither.
var (
	// ErrAccessDenied marks a run refused by the bilateral access check.
	ErrAccessDenied = errors.New("workflow: access denied")
	// ErrWorkflowNotFound marks a workflow no service has registered.
	ErrWorkflowNotFound = errors.New("workflow: no such workflow")
	// ErrRunTerminal marks a run that has already finished.
	ErrRunTerminal = errors.New("workflow: run is already terminal")
)

// AccessDeniedError names the workflow the policy refused and why.
type AccessDeniedError struct {
	Workflow string
	Reason   string
}

func (e *AccessDeniedError) Error() string {
	return fmt.Sprintf("workflow: start %q: access denied: %s", e.Workflow, e.Reason)
}

func (e *AccessDeniedError) Unwrap() error { return ErrAccessDenied }

// NotFoundError names a workflow the runtime has no definition for.
type NotFoundError struct {
	Workflow string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("workflow: start %q: no service registers this workflow", e.Workflow)
}

func (e *NotFoundError) Unwrap() error { return ErrWorkflowNotFound }

// TerminalError reports a run that ended in something other than success, or an
// operation attempted against a run that had already ended.
type TerminalError struct {
	RunID  string
	Status string
}

func (e *TerminalError) Error() string {
	return fmt.Sprintf("workflow: run %s is terminal: %s", e.RunID, e.Status)
}

func (e *TerminalError) Unwrap() error { return ErrRunTerminal }

// StartArgs starts one run.
type StartArgs struct {
	Workflow       string
	Input          any
	IdempotencyKey string
	TimeoutSec     int
	// ParentRunID is set when a workflow step starts this run. The runtime walks
	// the ancestor chain through it to refuse a cycle.
	ParentRunID string
}

// SignalArgs delivers one signal to a parked run.
type SignalArgs struct {
	RunID   string
	Signal  string
	Payload any
}

// StepSnapshot is what Query reports about one step.
type StepSnapshot struct {
	StepID string
	Status string
	Output any
	// LastError is the failure the step reported, empty while it has not failed.
	LastError string
	// CompensatedBy names the compensation step that undid this one.
	CompensatedBy string
}

// RunSnapshot is a point-in-time view of a run.
type RunSnapshot struct {
	RunID  string
	Status string
	State  map[string]any
	Steps  []StepSnapshot
}

// CallerConfig wires the caller side. See ./README.md.
type CallerConfig struct {
	Clients ClientSource
}

// Caller is the outward half of the workflow API: starting runs, steering them
// and reading them back. It holds nothing — every call takes the current stub
// from the connection layer.
type Caller struct {
	clients ClientSource
}

// NewCaller validates the config and builds the caller side.
func NewCaller(cfg CallerConfig) (*Caller, error) {
	if cfg.Clients == nil {
		return nil, fmt.Errorf("workflow: new caller: missing client source: %w", ErrInvalidConfig)
	}
	return &Caller{clients: cfg.Clients}, nil
}

// Start schedules a run and returns its id.
func (c *Caller) Start(ctx context.Context, args StartArgs) (string, error) {
	client, err := c.client(ctx, "start")
	if err != nil {
		return "", err
	}
	input, err := encodeJSON(args.Input)
	if err != nil {
		return "", fmt.Errorf("workflow: start %q: encode input: %w", args.Workflow, err)
	}

	resp, err := client.Start(ctx, &pb.StartRunRequest{
		WorkflowName:   args.Workflow,
		Input:          input,
		IdempotencyKey: args.IdempotencyKey,
		TimeoutSec:     uint32(args.TimeoutSec),
		XSbTrace:       traceHeader(ctx),
		ParentRunId:    args.ParentRunID,
	})
	if err != nil {
		return "", startError(args.Workflow, err)
	}
	return resp.GetRunId(), nil
}

// Signal delivers a payload to a run parked on a matching wait.
func (c *Caller) Signal(ctx context.Context, args SignalArgs) error {
	client, err := c.client(ctx, "signal")
	if err != nil {
		return err
	}
	payload, err := encodeJSON(args.Payload)
	if err != nil {
		return fmt.Errorf("workflow: signal run %s: encode payload: %w", args.RunID, err)
	}
	if _, err := client.Signal(ctx, &pb.SignalRunRequest{
		RunId:      args.RunID,
		SignalName: args.Signal,
		Payload:    payload,
	}); err != nil {
		return runError("signal", args.RunID, err)
	}
	return nil
}

// Cancel asks the runtime to stop a run. The run compensates what it has
// already done before it ends.
func (c *Caller) Cancel(ctx context.Context, runID string) error {
	client, err := c.client(ctx, "cancel")
	if err != nil {
		return err
	}
	if _, err := client.Cancel(ctx, &pb.CancelRunRequest{RunId: runID}); err != nil {
		return runError("cancel", runID, err)
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
		return RunSnapshot{}, runError("query", runID, err)
	}

	state, err := decodeState(resp.GetState())
	if err != nil {
		return RunSnapshot{}, fmt.Errorf("workflow: query run %s: %w", runID, err)
	}
	snapshot := RunSnapshot{
		RunID:  resp.GetRunId(),
		Status: resp.GetStatus(),
		State:  state,
		Steps:  make([]StepSnapshot, 0, len(resp.GetSteps())),
	}
	for _, step := range resp.GetSteps() {
		output, err := decodeJSON(step.GetOutput())
		if err != nil {
			return RunSnapshot{}, fmt.Errorf("workflow: query run %s: step %q: %w", runID, step.GetStepId(), err)
		}
		snapshot.Steps = append(snapshot.Steps, StepSnapshot{
			StepID:        step.GetStepId(),
			Status:        step.GetStatus(),
			Output:        output,
			LastError:     step.GetLastError(),
			CompensatedBy: step.GetCompensatedBy(),
		})
	}
	return snapshot, nil
}

// Await blocks until the run ends and returns its final state.
//
// The state is returned for a successful run only. Every other terminal status
// is a TerminalError naming it: a compensated run has no result to hand back,
// and returning its half-built state as if it were an answer is how a caller
// ends up acting on a run that failed.
func (c *Caller) Await(ctx context.Context, runID string) (map[string]any, error) {
	client, err := c.client(ctx, "await")
	if err != nil {
		return nil, err
	}
	st, err := client.Await(ctx, &pb.AwaitRunRequest{RunId: runID})
	if err != nil {
		return nil, runError("await", runID, err)
	}

	var last *pb.RunStatusUpdate
	for {
		update, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, runError("await", runID, err)
		}
		last = update
	}
	if last == nil {
		return nil, fmt.Errorf("workflow: await run %s: the runtime closed the stream without a status", runID)
	}
	if last.GetStatus() != statusSuccess {
		return nil, &TerminalError{RunID: runID, Status: last.GetStatus()}
	}

	state, err := decodeState(last.GetState())
	if err != nil {
		return nil, fmt.Errorf("workflow: await run %s: %w", runID, err)
	}
	return state, nil
}

// Replay forks a finished run into a new one, re-executing from fromStepID. An
// empty fromStepID replays the whole run.
func (c *Caller) Replay(ctx context.Context, runID, fromStepID string) (string, error) {
	client, err := c.client(ctx, "replay")
	if err != nil {
		return "", err
	}
	resp, err := client.Replay(ctx, &pb.ReplayRunRequest{RunId: runID, FromStepId: fromStepID})
	if err != nil {
		return "", runError("replay", runID, err)
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

// traceHeader carries the caller's trace forward so a run started inside a
// handler hangs under it. Empty when nothing is in flight: the runtime then
// mints a root of its own.
func traceHeader(ctx context.Context) string {
	tc, ok := telemetry.FromContext(ctx)
	if !ok {
		return ""
	}
	return telemetry.FormatHeader(tc)
}

func startError(name string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("workflow: start %q: %w", name, err)
	}
	switch st.Code() {
	case codes.PermissionDenied:
		return &AccessDeniedError{Workflow: name, Reason: st.Message()}
	case codes.NotFound:
		return &NotFoundError{Workflow: name}
	default:
		return fmt.Errorf("workflow: start %q: %w", name, err)
	}
}

func runError(op, runID string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("workflow: %s run %s: %w", op, runID, err)
	}
	switch st.Code() {
	case codes.FailedPrecondition:
		return &TerminalError{RunID: runID, Status: st.Message()}
	case codes.PermissionDenied:
		return &AccessDeniedError{Workflow: runID, Reason: st.Message()}
	case codes.NotFound:
		return fmt.Errorf("workflow: %s run %s: %w", op, runID, ErrWorkflowNotFound)
	default:
		return fmt.Errorf("workflow: %s run %s: %w", op, runID, err)
	}
}

func decodeState(blob []byte) (map[string]any, error) {
	state := map[string]any{}
	if len(blob) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(blob, &state); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	return state, nil
}
