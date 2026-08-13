// Package servicebridge is the Go SDK for the ServiceBridge runtime.
package servicebridge

import "context"

// Client owns every resource the SDK holds: the control-plane connection, the
// inbound Call server and the local outbox. Stop releases them all.
type Client struct{}

// New builds a client for the runtime at url, authenticating with the service
// bootstrap key.
func New(url, key string, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return nil, newError(CodeUnimplemented, "servicebridge.New", "connection layer is not implemented", nil)
}

// Start provisions the mTLS identity, opens the control stream and registers
// the declared handlers.
func (c *Client) Start(ctx context.Context) error {
	return newError(CodeUnimplemented, "Client.Start", "connection layer is not implemented", nil)
}

// Stop drains in-flight work and closes every resource the client owns.
func (c *Client) Stop(ctx context.Context) error {
	return newError(CodeUnimplemented, "Client.Stop", "connection layer is not implemented", nil)
}
