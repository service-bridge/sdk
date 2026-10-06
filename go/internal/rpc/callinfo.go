package rpc

import (
	"context"
	"time"
)

// CallInfo describes the inbound call a handler is serving. See ./README.md.
type CallInfo struct {
	// RequestID is the caller's identity for this logical call; it stays the
	// same across the caller's retries.
	RequestID string
	// IdempotencyKey is the key the caller supplied, empty when none.
	IdempotencyKey string
	// CallerServiceID is the calling service: the SPIFFE identity of a direct
	// caller, or the service the runtime names on the proxy path.
	CallerServiceID string
	// CallerInstanceID is the calling instance. Empty on the proxy path, where
	// the peer is the runtime.
	CallerInstanceID string
	// Deadline is the caller's deadline; zero when the call has none.
	Deadline time.Time
}

type callInfoKey struct{}

// WithCallInfo puts info into ctx.
func WithCallInfo(ctx context.Context, info CallInfo) context.Context {
	return context.WithValue(ctx, callInfoKey{}, info)
}

// CallInfoFromContext returns the CallInfo of the inbound call ctx belongs to.
func CallInfoFromContext(ctx context.Context) (CallInfo, bool) {
	info, ok := ctx.Value(callInfoKey{}).(CallInfo)
	return info, ok
}
