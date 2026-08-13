package rpc

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// ErrInvalidConfig means a required dependency is missing.
var ErrInvalidConfig = errors.New("rpc: invalid configuration")

// InvokeClientSource yields the Invoke stub of the current session. It is asked
// per call rather than cached: every rotation replaces the channel, and a stub
// captured once keeps talking over a channel that is already closing.
type InvokeClientSource interface {
	InvokeClient(ctx context.Context) (pb.InvokeClient, error)
}

// Proxy calls the callee through the runtime. The runtime resolves the target
// instance itself, which is why this path — unlike the direct one — carries the
// contract hash on the wire and owns the idempotency claim.
type Proxy struct {
	clients InvokeClientSource
}

// NewProxy builds the proxy transport over a stub source.
func NewProxy(clients InvokeClientSource) (*Proxy, error) {
	if clients == nil {
		return nil, fmt.Errorf("rpc: new proxy: no invoke client source: %w", ErrInvalidConfig)
	}
	return &Proxy{clients: clients}, nil
}

// EncodeContractHash renders the registry's string hash as the bytes field the
// wire declares.
//
// InvokeRequest.contract_hash is bytes while the registry carries the same hash
// as a string, and the runtime compares it with string(contract_hash) against a
// text column (runtime/internal/registry/repo.go). So the conversion is a plain
// UTF-8 round trip of the "v2:<hex>" string — decoding the hex here would
// produce bytes that match nothing.
func EncodeContractHash(hash string) []byte {
	if hash == "" {
		return nil
	}
	return []byte(hash)
}

// Unary invokes the method through the runtime and returns its response
// payload.
func (p *Proxy) Unary(ctx context.Context, req *pb.InvokeRequest) ([]byte, error) {
	client, err := p.clients.InvokeClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("rpc: proxy unary %s: %w", req.GetMethod(), err)
	}
	resp, err := client.Unary(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("rpc: proxy unary %s: %w", req.GetMethod(), err)
	}
	if resp.GetErrorCode() != "" {
		return nil, handlerError(resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetPayload(), nil
}

// Stream opens a server-side stream through the runtime.
func (p *Proxy) Stream(ctx context.Context, req *pb.InvokeRequest) (*Stream, error) {
	client, err := p.clients.InvokeClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("rpc: proxy stream %s: %w", req.GetMethod(), err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	cs, err := client.Stream(streamCtx, req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("rpc: proxy stream %s: %w", req.GetMethod(), err)
	}

	return newStream(
		func() ([]byte, error) {
			chunk, rerr := cs.Recv()
			if rerr != nil {
				return nil, rerr
			}
			if chunk.GetErrorCode() != "" {
				return nil, handlerError(chunk.GetErrorCode(), chunk.GetErrorMessage())
			}
			return chunk.GetPayload(), nil
		},
		cancel,
	), nil
}
