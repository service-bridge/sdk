// Package telemetry holds the SDK-side trace context, the X-SB-Trace wire
// format, the byte-budgeted buffers the transport drains and the operation
// frames the runtime persists.
package telemetry

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// TraceContext names the trace an operation belongs to and the operation it
// hangs under. Both are UUIDv7 in canonical form (ADR-0006); a zero ParentOpID
// marks a root operation, which the runtime stores as NULL.
type TraceContext struct {
	TraceID    uuid.UUID
	ParentOpID uuid.UUID
}

// Root reports whether the context has no parent operation.
func (tc TraceContext) Root() bool { return tc.ParentOpID == uuid.Nil }

// Child derives the context nested operations run under: the same trace, with
// opID as their parent.
func (tc TraceContext) Child(opID uuid.UUID) TraceContext {
	return TraceContext{TraceID: tc.TraceID, ParentOpID: opID}
}

type traceContextKey struct{}

// WithTraceContext returns a ctx carrying tc. Propagation is explicit on
// purpose: an ambient store would have to guess which goroutine inherits a
// trace, and ctx is the only carrier that survives a goroutine boundary
// unambiguously.
func WithTraceContext(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey{}, tc)
}

// FromContext returns the trace context ctx carries.
func FromContext(ctx context.Context) (TraceContext, bool) {
	tc, ok := ctx.Value(traceContextKey{}).(TraceContext)
	return tc, ok
}

// NewRootContext mints a fresh trace with no parent operation.
func NewRootContext() (TraceContext, error) {
	id, err := newUUIDv7()
	if err != nil {
		return TraceContext{}, fmt.Errorf("telemetry: mint root trace: %w", err)
	}
	return TraceContext{TraceID: id}, nil
}

// newUUIDv7 mints one identifier. A failure here means the entropy source is
// broken; the caller propagates it instead of stamping uuid.Nil, which would
// silently merge unrelated traces into one tree on the runtime side.
func newUUIDv7() (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("telemetry: generate uuidv7: %w", err)
	}
	return id, nil
}
