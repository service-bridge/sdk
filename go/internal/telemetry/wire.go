package telemetry

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const (
	// HeaderName is the HTTP header carrying the trace context.
	HeaderName = "X-SB-Trace"

	// MetadataKey is the same value on a gRPC call. gRPC lowercases metadata
	// keys, so the constant is already lowercase.
	MetadataKey = "x-sb-trace"

	// uuidLen is the canonical UUID string length; the header is two of them
	// joined by one separator.
	uuidLen   = 36
	headerLen = uuidLen + 1 + uuidLen
)

// FormatHeader renders tc as the X-SB-Trace value: "<traceID>-<parentOpID>".
// It is the only formatter in the SDK (ADR-0006 §7); the same string goes into
// the HTTP header, the gRPC metadata key and every x_sb_trace proto field
// (CallRequest, InvokeRequest, EventEnvelope, RunAssignment, JobExecution,
// StartRunRequest).
func FormatHeader(tc TraceContext) string {
	return tc.TraceID.String() + "-" + tc.ParentOpID.String()
}

// ParseHeader reads an X-SB-Trace value. It is the only parser in the SDK
// (ADR-0006 §7) and mirrors runtime/internal/telemetry/header.go.
//
// A malformed value is not a broken request — it means the caller sent no
// usable trace, so the operation starts a new tree (ADR-0006 §6). Every
// deviation therefore yields a fresh root context instead of an error. The
// returned error only ever reports a failure to mint that replacement.
func ParseHeader(value string) (TraceContext, error) {
	if tc, ok := parseHeaderStrict(value); ok {
		return tc, nil
	}
	tc, err := NewRootContext()
	if err != nil {
		return TraceContext{}, fmt.Errorf("telemetry: parse header: %w", err)
	}
	return tc, nil
}

// parseHeaderStrict accepts only the exact wire shape. The separator is taken
// by position: a UUID contains hyphens of its own, so searching for one finds
// the wrong offset.
func parseHeaderStrict(value string) (TraceContext, bool) {
	if len(value) != headerLen || value[uuidLen] != '-' {
		return TraceContext{}, false
	}
	traceID, err := uuid.Parse(value[:uuidLen])
	if err != nil {
		return TraceContext{}, false
	}
	parentOpID, err := uuid.Parse(value[uuidLen+1:])
	if err != nil {
		return TraceContext{}, false
	}
	return TraceContext{TraceID: traceID, ParentOpID: parentOpID}, true
}

// InjectMetadata appends the trace context to the outgoing gRPC metadata of ctx.
func InjectMetadata(ctx context.Context, tc TraceContext) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataKey, FormatHeader(tc))
}

// TraceFromMetadata reads the trace context off the incoming gRPC metadata of
// ctx, minting a fresh root when the key is absent or malformed.
func TraceFromMetadata(ctx context.Context) (TraceContext, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ParseHeader("")
	}
	values := md.Get(MetadataKey)
	if len(values) == 0 {
		return ParseHeader("")
	}
	return ParseHeader(values[0])
}
