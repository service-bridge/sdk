package telemetry

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

func recorderWith(mode Mode, maxBytes int32) (*Recorder, *Ring) {
	ring := NewRing(Budgets{})
	policy := NewPolicy()
	modes := DefaultModes()
	modes.RPC = mode
	modes.PayloadMaxBytes = maxBytes
	policy.Set(modes)
	return NewRecorder(ring, policy), ring
}

func drainOps(t *testing.T, r *Ring) []*pb.OpReport {
	t.Helper()
	batch := r.Peek(100)
	out := make([]*pb.OpReport, 0, len(batch.Ops))
	for _, it := range batch.Ops {
		out = append(out, it.Msg)
	}
	return out
}

func drainPayloads(t *testing.T, r *Ring) []*pb.PayloadAttachment {
	t.Helper()
	batch := r.Peek(100)
	out := make([]*pb.PayloadAttachment, 0, len(batch.Payloads))
	for _, it := range batch.Payloads {
		out = append(out, it.Msg)
	}
	return out
}

func TestStartEmitsStartFrame(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)
	parent := TraceContext{
		TraceID:    uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcde"),
		ParentOpID: uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcdf"),
	}
	ctx := WithTraceContext(context.Background(), parent)

	child, op, err := rec.Start(ctx, OpSpec{
		Channel:       pb.Channel_RPC,
		Kind:          OpKindRPCCall,
		Subject:       "rpc.call:svc-a/svc-b.method",
		PeerServiceID: "018f3a2b-1c4d-7e8f-9012-3456789abce1",
		BusinessKey:   "order-1",
		StartedAtMs:   1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	frames := drainOps(t, ring)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	f := frames[0]
	if f.GetTraceId() != parent.TraceID.String() {
		t.Fatalf("trace id = %s, want %s", f.GetTraceId(), parent.TraceID)
	}
	if f.GetOpId() != op.ID().String() {
		t.Fatalf("op id = %s, want %s", f.GetOpId(), op.ID())
	}
	if f.GetParentOpId() != parent.ParentOpID.String() {
		t.Fatalf("parent = %s, want %s", f.GetParentOpId(), parent.ParentOpID)
	}
	if f.FinishedAtMs != nil {
		t.Fatalf("START frame carries finished_at_ms = %d", f.GetFinishedAtMs())
	}
	if f.GetStatus() != pb.Status_PENDING {
		t.Fatalf("status = %s, want PENDING", f.GetStatus())
	}
	if f.GetStartedAtMs() != 1_700_000_000_000 {
		t.Fatalf("started_at_ms = %d", f.GetStartedAtMs())
	}
	if f.GetChannel() != pb.Channel_RPC || f.GetKind() != uint32(OpKindRPCCall) {
		t.Fatalf("channel/kind = %s/%d", f.GetChannel(), f.GetKind())
	}
	if f.GetSubject() != "rpc.call:svc-a/svc-b.method" || f.GetBusinessKey() != "order-1" {
		t.Fatalf("subject/business key = %q/%q", f.GetSubject(), f.GetBusinessKey())
	}

	// The started operation becomes the parent of everything nested under it.
	nested, ok := FromContext(child)
	if !ok {
		t.Fatal("Start returned a context without a trace")
	}
	if nested.TraceID != parent.TraceID || nested.ParentOpID != op.ID() {
		t.Fatalf("child context = %+v, want parent %s", nested, op.ID())
	}
}

// The runtime turns an empty buffer into SQL NULL and both columns are NOT NULL.
func TestStartFrameDefaultsJSONToEmptyObject(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)

	_, _, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	f := drainOps(t, ring)[0]
	if string(f.GetMetaJson()) != "{}" {
		t.Fatalf("meta_json = %q, want {}", f.GetMetaJson())
	}
	if string(f.GetAttrsJson()) != "{}" {
		t.Fatalf("attrs_json = %q, want {}", f.GetAttrsJson())
	}
}

func TestStartFrameKeepsExplicitJSON(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)

	_, _, err := rec.Start(context.Background(), OpSpec{
		Channel:   pb.Channel_RPC,
		Kind:      OpKindRPCCall,
		MetaJSON:  []byte(`{"target":"svc-b"}`),
		AttrsJSON: []byte(`{"region":"eu"}`),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	f := drainOps(t, ring)[0]
	if string(f.GetMetaJson()) != `{"target":"svc-b"}` {
		t.Fatalf("meta_json = %q", f.GetMetaJson())
	}
	if string(f.GetAttrsJson()) != `{"region":"eu"}` {
		t.Fatalf("attrs_json = %q", f.GetAttrsJson())
	}
}

func TestStartWithoutTraceMintsRoot(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)

	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	f := drainOps(t, ring)[0]
	if f.GetParentOpId() != "" {
		t.Fatalf("root parent = %q, want empty", f.GetParentOpId())
	}
	if op.TraceID() == uuid.Nil || op.TraceID().Version() != 7 {
		t.Fatalf("trace id = %s, want a UUIDv7", op.TraceID())
	}
	if op.ID().Version() != 7 {
		t.Fatalf("op id = %s, want a UUIDv7", op.ID())
	}
}

func TestStartHonoursExplicitOpID(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)
	want := uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abce2")

	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall, OpID: want})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if op.ID() != want {
		t.Fatalf("op id = %s, want %s", op.ID(), want)
	}
	if drainOps(t, ring)[0].GetOpId() != want.String() {
		t.Fatal("START frame carries a different op id")
	}
}

func TestEndEmitsDeltaFrame(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{
		Channel: pb.Channel_RPC,
		Kind:    OpKindRPCCall,
		Subject: "rpc.call:svc-a/svc-b.method",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.End(pb.Status_ERROR, "boom")

	frames := drainOps(t, ring)
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2", len(frames))
	}
	end := frames[1]
	if end.GetOpId() != op.ID().String() {
		t.Fatal("END frame belongs to another operation")
	}
	if end.FinishedAtMs == nil || end.GetFinishedAtMs() == 0 {
		t.Fatal("END frame carries no finished_at_ms")
	}
	if end.GetStatus() != pb.Status_ERROR || end.GetStatusMessage() != "boom" {
		t.Fatalf("status = %s/%q", end.GetStatus(), end.GetStatusMessage())
	}
	// Delta: fields that cannot change are left out.
	if end.GetSubject() != "" || end.GetParentOpId() != "" || end.GetStartedAtMs() != 0 {
		t.Fatalf("END frame repeats START fields: %+v", end)
	}
	if len(end.GetMetaJson()) != 0 || len(end.GetAttrsJson()) != 0 {
		t.Fatal("END frame repeats meta/attrs")
	}
}

func TestEndIsIdempotent(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.End(pb.Status_SUCCESS, "")
	op.End(pb.Status_ERROR, "second call")

	frames := drainOps(t, ring)
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2", len(frames))
	}
	if frames[1].GetStatus() != pb.Status_SUCCESS {
		t.Fatalf("status = %s, want the first End to win", frames[1].GetStatus())
	}
}

// A retry mutates the same operation: one logical call stays one row, with the
// attempt counter carried by the END frame (ADR-0001).
func TestRetriesMutateTheSameOperation(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.SetAttempt(1)
	op.SetAttempt(2)
	op.End(pb.Status_SUCCESS, "")

	frames := drainOps(t, ring)
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2 — a retry must not mint a row", len(frames))
	}
	if frames[0].GetOpId() != frames[1].GetOpId() {
		t.Fatal("frames describe different operations")
	}
	if frames[0].GetAttempt() != 0 {
		t.Fatalf("START attempt = %d, want 0", frames[0].GetAttempt())
	}
	if frames[1].GetAttempt() != 2 {
		t.Fatalf("END attempt = %d, want 2", frames[1].GetAttempt())
	}
}

func TestCaptureNoneKeepsNothing(t *testing.T) {
	rec, ring := recorderWith(ModeNone, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if op.Capturing() {
		t.Fatal("Capturing reported true in none mode — the caller would serialise for nothing")
	}
	op.CaptureIn([]byte(`{"a":1}`), "hash")
	op.CaptureOut([]byte(`{"b":2}`), "hash")
	op.End(pb.Status_ERROR, "boom")

	if got := drainPayloads(t, ring); len(got) != 0 {
		t.Fatalf("payloads = %d, want 0", len(got))
	}
}

func TestCaptureAllEmitsImmediately(t *testing.T) {
	rec, ring := recorderWith(ModeAll, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !op.Capturing() {
		t.Fatal("Capturing reported false in all mode")
	}
	op.CaptureIn([]byte(`{"a":1}`), "hash-in")

	got := drainPayloads(t, ring)
	if len(got) != 1 {
		t.Fatalf("payloads = %d, want 1 before End", len(got))
	}
	if got[0].GetDirection() != DirectionIn {
		t.Fatalf("direction = %d, want %d", got[0].GetDirection(), DirectionIn)
	}
	if got[0].GetOpId() != op.ID().String() || got[0].GetTraceId() != op.TraceID().String() {
		t.Fatal("payload is not stamped with its operation")
	}
	if string(got[0].GetBytes()) != `{"a":1}` || got[0].GetContractHash() != "hash-in" {
		t.Fatalf("payload = %q/%q", got[0].GetBytes(), got[0].GetContractHash())
	}
}

func TestCaptureAllTruncatesToTheCap(t *testing.T) {
	rec, ring := recorderWith(ModeAll, 4)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.CaptureOut(bytes.Repeat([]byte("x"), 100), "hash")

	got := drainPayloads(t, ring)[0]
	if len(got.GetBytes()) != 4 {
		t.Fatalf("payload length = %d, want 4", len(got.GetBytes()))
	}
	if got.GetOriginalSize() != 100 {
		t.Fatalf("original size = %d, want 100", got.GetOriginalSize())
	}
}

func TestCaptureErrorsHoldsUntilFailure(t *testing.T) {
	rec, ring := recorderWith(ModeErrors, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.CaptureIn([]byte(`{"a":1}`), "hash")
	if got := drainPayloads(t, ring); len(got) != 0 {
		t.Fatalf("payloads = %d before End, want 0", len(got))
	}

	op.End(pb.Status_TIMEOUT, "deadline")

	got := drainPayloads(t, ring)
	if len(got) != 1 {
		t.Fatalf("payloads = %d after a failure, want 1", len(got))
	}
	if string(got[0].GetBytes()) != `{"a":1}` {
		t.Fatalf("payload = %q", got[0].GetBytes())
	}
}

func TestCaptureErrorsDropsOnSuccess(t *testing.T) {
	rec, ring := recorderWith(ModeErrors, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.CaptureIn([]byte(`{"a":1}`), "hash")
	op.End(pb.Status_SUCCESS, "")

	if got := drainPayloads(t, ring); len(got) != 0 {
		t.Fatalf("payloads = %d after success, want 0", len(got))
	}
}

// A retry re-capturing the same direction replaces the previous buffer instead
// of stacking duplicates.
func TestCaptureErrorsKeepsLastPerDirection(t *testing.T) {
	rec, ring := recorderWith(ModeErrors, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.CaptureOut([]byte("attempt-1"), "hash")
	op.CaptureOut([]byte("attempt-2"), "hash")
	op.CaptureIn([]byte("input"), "hash")
	op.End(pb.Status_ERROR, "boom")

	got := drainPayloads(t, ring)
	if len(got) != 2 {
		t.Fatalf("payloads = %d, want one per direction", len(got))
	}
	byDirection := map[uint32]string{}
	for _, p := range got {
		byDirection[p.GetDirection()] = string(p.GetBytes())
	}
	if byDirection[DirectionOut] != "attempt-2" {
		t.Fatalf("out payload = %q, want the last capture", byDirection[DirectionOut])
	}
	if byDirection[DirectionIn] != "input" {
		t.Fatalf("in payload = %q", byDirection[DirectionIn])
	}
}

func TestCaptureCopiesTheCallerBuffer(t *testing.T) {
	rec, ring := recorderWith(ModeAll, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	payload := []byte("original")
	op.CaptureIn(payload, "hash")
	copy(payload, "OVERWRIT")

	if got := string(drainPayloads(t, ring)[0].GetBytes()); got != "original" {
		t.Fatalf("captured payload = %q, want a copy taken at capture time", got)
	}
}

// A handler override narrows what the runtime pushed and can never widen it.
func TestOpCaptureOverrideNarrowsOnly(t *testing.T) {
	rec, ring := recorderWith(ModeAll, DefaultPayloadMaxBytes)

	_, narrowed, err := rec.Start(context.Background(), OpSpec{
		Channel:         pb.Channel_RPC,
		Kind:            OpKindRPCCall,
		CaptureOverride: ModeNone,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if narrowed.Capturing() {
		t.Fatal("an override of none still captures")
	}
	narrowed.CaptureIn([]byte("x"), "hash")
	if got := drainPayloads(t, ring); len(got) != 0 {
		t.Fatalf("payloads = %d, want 0", len(got))
	}

	rec, ring = recorderWith(ModeErrors, DefaultPayloadMaxBytes)
	_, widened, err := rec.Start(context.Background(), OpSpec{
		Channel:         pb.Channel_RPC,
		Kind:            OpKindRPCCall,
		CaptureOverride: ModeAll,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	widened.CaptureIn([]byte("x"), "hash")
	if got := drainPayloads(t, ring); len(got) != 0 {
		t.Fatal("an override widened capture past what the runtime pushed")
	}
}

// Buffering a payload after the operation closed would keep it forever: End
// already decided what to flush.
func TestCaptureAfterEndIsDropped(t *testing.T) {
	rec, ring := recorderWith(ModeErrors, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	op.End(pb.Status_ERROR, "boom")
	op.CaptureIn([]byte("late"), "hash")

	if got := drainPayloads(t, ring); len(got) != 0 {
		t.Fatalf("payloads = %d, want 0", len(got))
	}
}

func TestOpContextNamesItselfAsParent(t *testing.T) {
	rec, _ := recorderWith(ModeNone, DefaultPayloadMaxBytes)
	_, op, err := rec.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Kind: OpKindRPCCall})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	tc := op.Context()
	if tc.TraceID != op.TraceID() || tc.ParentOpID != op.ID() {
		t.Fatalf("op context = %+v, want trace %s parent %s", tc, op.TraceID(), op.ID())
	}
}

func TestRecorderCapturingAnswersBeforeStart(t *testing.T) {
	rec, _ := recorderWith(ModeAll, DefaultPayloadMaxBytes)

	if !rec.Capturing(pb.Channel_RPC, ModeInherit) {
		t.Fatal("Capturing = false for a channel the runtime enabled")
	}
	if rec.Capturing(pb.Channel_RPC, ModeNone) {
		t.Fatal("Capturing ignored the narrowing override")
	}
	if rec.Capturing(pb.Channel_EVENT, ModeInherit) {
		t.Fatal("Capturing = true for a channel with no pushed mode")
	}
	if rec.Ring() == nil || rec.Policy() == nil {
		t.Fatal("Recorder does not expose its ring and policy")
	}
}
