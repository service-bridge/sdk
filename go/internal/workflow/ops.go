package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reasons a checkpoint does not land. They are separate from the graph errors
// above because they say nothing about the workflow and everything about who
// owns the run right now.
var (
	// ErrLeaseLost marks a checkpoint the runtime refused because the lease
	// epoch it carried is no longer the current one. It is not a transport
	// failure and retrying it is meaningless: another instance holds the run,
	// and everything this one still had in flight is fenced off.
	ErrLeaseLost = errors.New("workflow: the run is held at a newer lease epoch")
	// ErrInvalidConfig marks a missing dependency at construction time.
	ErrInvalidConfig = errors.New("workflow: invalid config")
	// ErrNoIdentity marks a session with no identity yet, so there is nothing
	// to check point or subscribe as.
	ErrNoIdentity = errors.New("workflow: no session identity")
	// ErrUnknownAction marks a FailStep answer the SDK cannot execute. It is
	// loud rather than assumed: guessing would either re-run a step the runtime
	// wants compensated, or abandon a run the runtime expects to be retried.
	ErrUnknownAction = errors.New("workflow: runtime answered with an unknown next action")
)

// NextAction is the runtime's decision after a step failed. The runner executes
// it; it never decides on its own (ADR 0003).
type NextAction string

const (
	// ActionRetry re-executes the step after RetryDelaySec.
	ActionRetry NextAction = "retry"
	// ActionCompensate hands the run to the runtime, which re-assigns it with
	// the compensating flag.
	ActionCompensate NextAction = "compensate"
	// ActionFailRun ends the run without compensation.
	ActionFailRun NextAction = "fail_run"
)

// FencedError reports a checkpoint refused for a stale lease epoch. It carries
// the epoch that was refused so a log line says which holder was fenced, and
// unwraps to both ErrLeaseLost and the transport error underneath.
type FencedError struct {
	Op         string
	RunID      string
	StepID     string
	LeaseEpoch uint64
	Err        error
}

func (e *FencedError) Error() string {
	if e.StepID == "" {
		return fmt.Sprintf("workflow: %s: run %s at lease epoch %d is fenced: %s",
			e.Op, e.RunID, e.LeaseEpoch, e.Err)
	}
	return fmt.Sprintf("workflow: %s: run %s step %q at lease epoch %d is fenced: %s",
		e.Op, e.RunID, e.StepID, e.LeaseEpoch, e.Err)
}

func (e *FencedError) Unwrap() []error { return []error{ErrLeaseLost, e.Err} }

// ClientSource yields the Workflows stub to talk to the runtime with. The
// connection layer owns the channel and replaces it on rotation, so the stub is
// asked for per call instead of captured once.
type ClientSource interface {
	WorkflowsClient(ctx context.Context) (pb.WorkflowsClient, error)
}

// Identity names the live session. Read per use, never cached: a certificate
// rotation mints a fresh InstanceID and the runtime authenticates against the
// current one.
type Identity struct {
	ServiceID  string
	InstanceID string
}

// BeginStepArgs claims one step of a run.
type BeginStepArgs struct {
	RunID string
	// StepID is the step's own id; for a compensation it is the synthetic
	// "<step>.compensate" id, and ParentStepID names the step being undone.
	StepID       string
	ParentStepID string
	Kind         string
	Input        any
	LeaseEpoch   uint64
}

// BeginResult is what the runtime knows about the step already.
type BeginResult struct {
	// AlreadyDone means the step is checkpointed as complete. The step must not
	// execute again: the run is being replayed, and Output is what it produced
	// the first time.
	AlreadyDone bool
	Output      any
	LeaseEpoch  uint64
}

// CompleteStepArgs checkpoints a step's output.
type CompleteStepArgs struct {
	RunID      string
	StepID     string
	Output     any
	LeaseEpoch uint64
}

// FailStepArgs reports a failed step and asks the runtime what to do.
type FailStepArgs struct {
	RunID        string
	StepID       string
	ErrorCode    string
	ErrorMessage string
	LeaseEpoch   uint64
	// Retriable states whether the step declared a budget for another attempt.
	// The runtime still owns the decision; this only says whether asking makes
	// sense.
	Retriable bool
}

// Decision is the runtime's answer to a failed step.
type Decision struct {
	Action        NextAction
	RetryDelaySec uint32
}

// SleepWait parks a run on a durable timer.
type SleepWait struct{ DurationSec uint32 }

// EventWait parks a run until a matching event is ingested.
type EventWait struct {
	Event string
	// FilterJSON is the resolved filter as JSON; empty matches every event of
	// that name.
	FilterJSON string
	TimeoutSec uint32
}

// SignalWait parks a run until it is signalled by name.
type SignalWait struct {
	Signal     string
	TimeoutSec uint32
}

// NestedWait parks a run until the child run it started finishes.
type NestedWait struct{ ChildRunID string }

// ParkArgs surrenders the run to the runtime until the wait it describes is
// over. Exactly one wait must be set.
type ParkArgs struct {
	RunID      string
	StepID     string
	LeaseEpoch uint64
	Sleep      *SleepWait
	Event      *EventWait
	Signal     *SignalWait
	Nested     *NestedWait
}

// CompleteRunArgs ends a run.
type CompleteRunArgs struct {
	RunID      string
	FinalState map[string]any
	LeaseEpoch uint64
	// TerminalStatus is 'success', 'failed_compensated' or 'cancelled'.
	TerminalStatus string
}

// HeartbeatArgs renews the lease on one run.
type HeartbeatArgs struct {
	RunID      string
	LeaseEpoch uint64
}

// Ops is the checkpoint surface the runner and the subscriber drive the runtime
// through. Declared here, at the consumer, so the transport can be swapped and
// scripted in tests.
type Ops interface {
	BeginStep(ctx context.Context, args BeginStepArgs) (BeginResult, error)
	CompleteStep(ctx context.Context, args CompleteStepArgs) error
	FailStep(ctx context.Context, args FailStepArgs) (Decision, error)
	Park(ctx context.Context, args ParkArgs) error
	CompleteRun(ctx context.Context, args CompleteRunArgs) error
	Heartbeat(ctx context.Context, args HeartbeatArgs) error
}

// CheckpointConfig wires the gRPC implementation of Ops. See ./README.md.
type CheckpointConfig struct {
	Clients  ClientSource
	Identity func() Identity
}

// Checkpoints is Ops over the generated Workflows stub. Every call carries the
// lease epoch of the assignment it belongs to, which is what the runtime fences
// a superseded holder by.
type Checkpoints struct {
	clients  ClientSource
	identity func() Identity
}

// NewCheckpoints validates the config and builds the gRPC checkpoint surface.
func NewCheckpoints(cfg CheckpointConfig) (*Checkpoints, error) {
	if cfg.Clients == nil {
		return nil, fmt.Errorf("workflow: new checkpoints: missing client source: %w", ErrInvalidConfig)
	}
	if cfg.Identity == nil {
		return nil, fmt.Errorf("workflow: new checkpoints: missing identity source: %w", ErrInvalidConfig)
	}
	return &Checkpoints{clients: cfg.Clients, identity: cfg.Identity}, nil
}

// BeginStep claims a step. A step the runtime already has an output for comes
// back AlreadyDone with that output, and must not be executed again.
func (c *Checkpoints) BeginStep(ctx context.Context, args BeginStepArgs) (BeginResult, error) {
	client, err := c.client(ctx, "begin step")
	if err != nil {
		return BeginResult{}, err
	}
	snapshot, err := encodeJSON(args.Input)
	if err != nil {
		return BeginResult{}, fmt.Errorf("workflow: begin step %q: encode input: %w", args.StepID, err)
	}

	resp, err := client.BeginStep(ctx, &pb.BeginStepRequest{
		RunId:         args.RunID,
		StepId:        args.StepID,
		ParentStepId:  args.ParentStepID,
		Kind:          args.Kind,
		InputSnapshot: snapshot,
		LeaseEpoch:    args.LeaseEpoch,
		InstanceId:    c.identity().InstanceID,
	})
	if err != nil {
		return BeginResult{}, c.wrap("begin step", args.RunID, args.StepID, args.LeaseEpoch, err)
	}

	out := BeginResult{AlreadyDone: resp.GetAlreadyDone(), LeaseEpoch: resp.GetLeaseEpoch()}
	if out.AlreadyDone {
		cached, err := decodeJSON(resp.GetCachedOutput())
		if err != nil {
			return BeginResult{}, fmt.Errorf("workflow: begin step %q: decode cached output: %w", args.StepID, err)
		}
		out.Output = cached
	}
	return out, nil
}

// CompleteStep checkpoints the step's output.
func (c *Checkpoints) CompleteStep(ctx context.Context, args CompleteStepArgs) error {
	client, err := c.client(ctx, "complete step")
	if err != nil {
		return err
	}
	output, err := encodeJSON(args.Output)
	if err != nil {
		return fmt.Errorf("workflow: complete step %q: encode output: %w", args.StepID, err)
	}
	if _, err := client.CompleteStep(ctx, &pb.CompleteStepRequest{
		RunId:      args.RunID,
		StepId:     args.StepID,
		Output:     output,
		LeaseEpoch: args.LeaseEpoch,
	}); err != nil {
		return c.wrap("complete step", args.RunID, args.StepID, args.LeaseEpoch, err)
	}
	return nil
}

// FailStep reports a failed step and returns the runtime's decision for it.
func (c *Checkpoints) FailStep(ctx context.Context, args FailStepArgs) (Decision, error) {
	client, err := c.client(ctx, "fail step")
	if err != nil {
		return Decision{}, err
	}
	resp, err := client.FailStep(ctx, &pb.FailStepRequest{
		RunId:        args.RunID,
		StepId:       args.StepID,
		ErrorCode:    args.ErrorCode,
		ErrorMessage: args.ErrorMessage,
		LeaseEpoch:   args.LeaseEpoch,
		Retriable:    args.Retriable,
	})
	if err != nil {
		return Decision{}, c.wrap("fail step", args.RunID, args.StepID, args.LeaseEpoch, err)
	}
	return Decision{
		Action:        NextAction(resp.GetNextAction()),
		RetryDelaySec: resp.GetRetryDelaySec(),
	}, nil
}

// Park surrenders the run until the wait it carries is over.
func (c *Checkpoints) Park(ctx context.Context, args ParkArgs) error {
	client, err := c.client(ctx, "park")
	if err != nil {
		return err
	}
	req := &pb.ParkRequest{RunId: args.RunID, StepId: args.StepID, LeaseEpoch: args.LeaseEpoch}
	switch {
	case args.Sleep != nil:
		req.WaitSpec = &pb.ParkRequest_Sleep{Sleep: &pb.SleepSpec{DurationSec: args.Sleep.DurationSec}}
	case args.Event != nil:
		req.WaitSpec = &pb.ParkRequest_EventWait{EventWait: &pb.EventWaitSpec{
			Event:      args.Event.Event,
			FilterJson: args.Event.FilterJSON,
			TimeoutSec: args.Event.TimeoutSec,
		}}
	case args.Signal != nil:
		req.WaitSpec = &pb.ParkRequest_SignalWait{SignalWait: &pb.SignalWaitSpec{
			Signal:     args.Signal.Signal,
			TimeoutSec: args.Signal.TimeoutSec,
		}}
	case args.Nested != nil:
		req.WaitSpec = &pb.ParkRequest_NestedRun{NestedRun: &pb.NestedRunSpec{
			ChildRunId: args.Nested.ChildRunID,
		}}
	default:
		return fmt.Errorf("workflow: park step %q: %w: no wait spec", args.StepID, ErrInvalidConfig)
	}

	if _, err := client.Park(ctx, req); err != nil {
		return c.wrap("park", args.RunID, args.StepID, args.LeaseEpoch, err)
	}
	return nil
}

// CompleteRun ends the run with the terminal status the caller derived.
func (c *Checkpoints) CompleteRun(ctx context.Context, args CompleteRunArgs) error {
	client, err := c.client(ctx, "complete run")
	if err != nil {
		return err
	}
	state, err := encodeJSON(args.FinalState)
	if err != nil {
		return fmt.Errorf("workflow: complete run %s: encode final state: %w", args.RunID, err)
	}
	if _, err := client.CompleteRun(ctx, &pb.CompleteRunRequest{
		RunId:          args.RunID,
		FinalState:     state,
		LeaseEpoch:     args.LeaseEpoch,
		TerminalStatus: args.TerminalStatus,
	}); err != nil {
		return c.wrap("complete run", args.RunID, "", args.LeaseEpoch, err)
	}
	return nil
}

// Heartbeat renews the lease on one run.
func (c *Checkpoints) Heartbeat(ctx context.Context, args HeartbeatArgs) error {
	client, err := c.client(ctx, "heartbeat")
	if err != nil {
		return err
	}
	id := c.identity()
	if id.InstanceID == "" {
		return fmt.Errorf("workflow: heartbeat run %s: %w", args.RunID, ErrNoIdentity)
	}
	if _, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
		RunId:      args.RunID,
		InstanceId: id.InstanceID,
		LeaseEpoch: args.LeaseEpoch,
	}); err != nil {
		return c.wrap("heartbeat", args.RunID, "", args.LeaseEpoch, err)
	}
	return nil
}

func (c *Checkpoints) client(ctx context.Context, op string) (pb.WorkflowsClient, error) {
	client, err := c.clients.WorkflowsClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("workflow: %s: workflows client: %w", op, err)
	}
	return client, nil
}

// wrap turns the runtime's fencing status into ErrLeaseLost and leaves every
// other failure a plain transport error. The two demand opposite reactions: a
// fenced holder must stop touching the run, a broken channel must be retried.
func (c *Checkpoints) wrap(op, runID, stepID string, epoch uint64, err error) error {
	if status.Code(err) == codes.Aborted {
		return &FencedError{Op: op, RunID: runID, StepID: stepID, LeaseEpoch: epoch, Err: err}
	}
	if stepID == "" {
		return fmt.Errorf("workflow: %s: run %s: %w", op, runID, err)
	}
	return fmt.Errorf("workflow: %s: run %s step %q: %w", op, runID, stepID, err)
}

// encodeJSON renders a value for the wire. A nil value is the JSON null the
// runtime stores for a step that produced nothing, not an absent field.
func encodeJSON(v any) ([]byte, error) {
	blob, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal json: %w", err)
	}
	return blob, nil
}

func decodeJSON(blob []byte) (any, error) {
	if len(blob) == 0 {
		return nil, nil
	}
	var out any
	if err := json.Unmarshal(blob, &out); err != nil {
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}
	return out, nil
}
