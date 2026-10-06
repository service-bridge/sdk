// Package testkit is the seam between the client and the sbtest harness: the
// harness hands the client an in-memory transport, and the client hands the
// harness the entry points of its own dispatch.
package testkit

import (
	"context"

	"github.com/service-bridge/sdk/go/internal/events"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/rpc"
)

// Memory replaces every network edge of one client. See ./README.md.
type Memory struct {
	// Set by the harness.
	Call    func(ctx context.Context, req rpc.Request) ([]byte, error)
	Stream  func(ctx context.Context, req rpc.Request) (*rpc.Stream, error)
	Publish events.PublishFunc

	// Set by the client when it is built with the memory transport.
	Unary         func(ctx context.Context, method string, payload []byte) rpc.Outcome
	ServeStream   func(ctx context.Context, method string, payload []byte, send rpc.Sender) rpc.Outcome
	Deliver       func(ctx context.Context, d *pb.EventDelivery) (acked bool, reason string)
	Subscriptions func() []events.Subscription
	// Wrap presents an error the way the client's public API does.
	Wrap func(op string, err error) error
}

// NewOption builds the client option that installs m. The root package sets it
// at init; it returns servicebridge.Option as any, because this package cannot
// import the root package.
var NewOption func(m *Memory) any
