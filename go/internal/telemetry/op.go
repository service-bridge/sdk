package telemetry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// OpKind is the per-channel classification of an operation. Values mirror
// runtime/internal/telemetry/enums.go and the CHECK constraint behind it.
type OpKind uint32

const (
	OpKindHTTPHandle OpKind = 1

	OpKindRPCCall OpKind = 1

	OpKindEventPublish OpKind = 1
	OpKindEventDeliver OpKind = 2

	OpKindWorkflowRun        OpKind = 1
	OpKindWorkflowSleep      OpKind = 2
	OpKindWorkflowWaitEvent  OpKind = 3
	OpKindWorkflowWaitSignal OpKind = 4

	OpKindJobExec OpKind = 1

	OpKindUserSubOp OpKind = 1
)

// Payload directions on the wire.
const (
	DirectionIn  uint32 = 1
	DirectionOut uint32 = 2
)

// emptyJSONObject is what meta and attrs default to. The runtime turns an empty
// buffer into SQL NULL, and both columns are NOT NULL, so a START frame must
// carry a real JSON object.
const emptyJSONObject = "{}"

// OpSpec describes an operation at its start.
type OpSpec struct {
	Channel       pb.Channel
	Kind          OpKind
	Subject       string
	PeerServiceID string
	BusinessKey   string
	Attempt       int32
	// StartedAtMs is unix-ms; zero means now.
	StartedAtMs int64
	MetaJSON    []byte
	AttrsJSON   []byte
	// CaptureOverride narrows the runtime-pushed capture mode for this one
	// operation. It can never widen it.
	CaptureOverride Mode
	// OpID pins the operation identity; zero mints a fresh UUIDv7.
	OpID uuid.UUID
}

// Recorder is what the rest of the SDK emits telemetry through: the buffer the
// transport drains plus the runtime-pushed capture authority.
type Recorder struct {
	ring   *Ring
	policy *Policy
}

// NewRecorder binds a ring and a policy.
func NewRecorder(ring *Ring, policy *Policy) *Recorder {
	return &Recorder{ring: ring, policy: policy}
}

// Ring returns the buffer the transport drains.
func (r *Recorder) Ring() *Ring { return r.ring }

// Policy returns the runtime-pushed capture authority.
func (r *Recorder) Policy() *Policy { return r.policy }

// Capturing answers whether an operation on this channel would keep a payload,
// before one is built.
func (r *Recorder) Capturing(ch pb.Channel, override Mode) bool {
	return r.policy.Capturing(ch, override)
}

// Start emits the START frame and returns the handle that closes the operation
// plus the context nested operations inherit — the new operation is their
// parent. The trace comes from ctx; without one a fresh root trace is minted.
//
// The ownership matrix (ADR-0001) decides who calls this: RPC.CALL is emitted
// by the calling SDK only, once per logical call; HTTP.HANDLE and USER.SUBOP by
// the SDK; EVENT.PUBLISH, EVENT.DELIVER, WORKFLOW.RUN and JOB.EXEC belong to
// the runtime and must not be duplicated here.
func (r *Recorder) Start(ctx context.Context, spec OpSpec) (context.Context, *Op, error) {
	tc, ok := FromContext(ctx)
	if !ok {
		root, err := NewRootContext()
		if err != nil {
			return ctx, nil, fmt.Errorf("telemetry: start op: %w", err)
		}
		tc = root
	}

	opID := spec.OpID
	if opID == uuid.Nil {
		id, err := newUUIDv7()
		if err != nil {
			return ctx, nil, fmt.Errorf("telemetry: start op: %w", err)
		}
		opID = id
	}

	startedAtMs := spec.StartedAtMs
	if startedAtMs == 0 {
		startedAtMs = nowUnixMs()
	}

	op := &Op{
		ring:        r.ring,
		traceID:     tc.TraceID,
		opID:        opID,
		parentOpID:  tc.ParentOpID,
		channel:     spec.Channel,
		kind:        spec.Kind,
		attempt:     spec.Attempt,
		startedAtMs: startedAtMs,
		mode:        r.policy.Resolve(spec.Channel, spec.CaptureOverride),
		maxBytes:    r.policy.PayloadMaxBytes(),
	}
	op.pushStart(spec)

	return WithTraceContext(ctx, tc.Child(opID)), op, nil
}

// Op is one in-flight operation. Its START frame is already buffered; End
// buffers the closing delta.
type Op struct {
	ring *Ring

	traceID     uuid.UUID
	opID        uuid.UUID
	parentOpID  uuid.UUID
	channel     pb.Channel
	kind        OpKind
	startedAtMs int64
	mode        Mode
	maxBytes    int32

	mu       sync.Mutex
	attempt  int32
	ended    bool
	buffered map[uint32]capturedPayload
}

type capturedPayload struct {
	direction    uint32
	bytes        []byte
	originalSize int32
	contractHash string
}

// ID returns the operation identifier.
func (o *Op) ID() uuid.UUID { return o.opID }

// TraceID returns the trace this operation belongs to.
func (o *Op) TraceID() uuid.UUID { return o.traceID }

// Context returns the trace context nested operations run under.
func (o *Op) Context() TraceContext {
	return TraceContext{TraceID: o.traceID, ParentOpID: o.opID}
}

// Capturing reports whether building a payload for this operation would keep
// anything. It reads the already-resolved mode, so a handler's narrowing is
// honoured — asking the policy for the channel's mode again would miss it.
func (o *Op) Capturing() bool { return o.mode != ModeNone }

// SetAttempt records the retry counter on this operation. A retry mutates the
// same operation rather than minting a new one, so one logical call stays one
// row (ADR-0001); the END frame carries the final count.
func (o *Op) SetAttempt(attempt int32) {
	o.mu.Lock()
	o.attempt = attempt
	o.mu.Unlock()
}

// CaptureIn records the inbound payload — a request or an input.
func (o *Op) CaptureIn(payload []byte, contractHash string) {
	o.capture(DirectionIn, payload, contractHash)
}

// CaptureOut records the outbound payload — a response or an output.
func (o *Op) CaptureOut(payload []byte, contractHash string) {
	o.capture(DirectionOut, payload, contractHash)
}

func (o *Op) capture(direction uint32, payload []byte, contractHash string) {
	if o.mode == ModeNone {
		return
	}
	capped, originalSize := CapPayload(payload, o.maxBytes)
	att := capturedPayload{
		direction: direction,
		// The caller owns and may reuse payload; what outlives this call is
		// buffered until flush, so it is copied here rather than aliased.
		bytes:        append([]byte(nil), capped...),
		originalSize: originalSize,
		contractHash: contractHash,
	}
	if o.mode == ModeAll {
		o.emitPayload(att)
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ended {
		return
	}
	if o.buffered == nil {
		o.buffered = make(map[uint32]capturedPayload, 2)
	}
	// Last write per direction wins: a retry re-capturing its output replaces
	// the previous one instead of stacking duplicates.
	o.buffered[direction] = att
}

// End buffers the END frame. Calling it again does nothing.
func (o *Op) End(status pb.Status, statusMessage string) {
	o.mu.Lock()
	if o.ended {
		o.mu.Unlock()
		return
	}
	o.ended = true
	attempt := o.attempt
	var pending []capturedPayload
	if failed(status) {
		pending = make([]capturedPayload, 0, len(o.buffered))
		for _, att := range o.buffered {
			pending = append(pending, att)
		}
	}
	o.buffered = nil
	o.mu.Unlock()

	for _, att := range pending {
		o.emitPayload(att)
	}
	o.pushEnd(status, statusMessage, attempt)
}

// failed reports whether a terminal status is worth capturing payloads for.
// PENDING is not terminal; everything past SUCCESS is a failure.
func failed(status pb.Status) bool {
	return status != pb.Status_SUCCESS && status != pb.Status_PENDING
}

func (o *Op) emitPayload(att capturedPayload) {
	o.ring.PushPayload(&pb.PayloadAttachment{
		TraceId:      o.traceID.String(),
		OpId:         o.opID.String(),
		Direction:    att.direction,
		Bytes:        att.bytes,
		OriginalSize: att.originalSize,
		ContractHash: att.contractHash,
	})
}

// pushStart buffers the opening frame: no finished_at_ms, status PENDING.
func (o *Op) pushStart(spec OpSpec) {
	o.ring.PushOp(&pb.OpReport{
		TraceId:       o.traceID.String(),
		OpId:          o.opID.String(),
		ParentOpId:    parentOpIDString(o.parentOpID),
		Channel:       o.channel,
		Kind:          uint32(o.kind),
		Subject:       spec.Subject,
		PeerServiceId: spec.PeerServiceID,
		BusinessKey:   spec.BusinessKey,
		Attempt:       spec.Attempt,
		StartedAtMs:   o.startedAtMs,
		Status:        pb.Status_PENDING,
		MetaJson:      jsonOrEmptyObject(spec.MetaJSON),
		AttrsJson:     jsonOrEmptyObject(spec.AttrsJSON),
	})
}

// pushEnd buffers the closing delta: only the fields that changed since START.
// The runtime upserts them onto the existing row.
func (o *Op) pushEnd(status pb.Status, statusMessage string, attempt int32) {
	finishedAtMs := nowUnixMs()
	o.ring.PushOp(&pb.OpReport{
		TraceId:       o.traceID.String(),
		OpId:          o.opID.String(),
		Channel:       o.channel,
		Kind:          uint32(o.kind),
		Attempt:       attempt,
		FinishedAtMs:  &finishedAtMs,
		Status:        status,
		StatusMessage: statusMessage,
	})
}

// parentOpIDString renders a root's absent parent as the empty string, which is
// what the runtime stores as NULL.
func parentOpIDString(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func jsonOrEmptyObject(b []byte) []byte {
	if len(b) == 0 {
		return []byte(emptyJSONObject)
	}
	return b
}

func nowUnixMs() int64 { return time.Now().UnixMilli() }
