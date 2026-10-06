package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

var (
	// ErrInvalidConfig marks a missing dependency at construction time.
	ErrInvalidConfig = errors.New("workflow: invalid config")
	// ErrNoIdentity marks a session with no identity yet.
	ErrNoIdentity = errors.New("workflow: no session identity")
)

// ClientSource yields the Workflows stub. The connection layer owns the
// channel, so the stub is asked for per call instead of captured once.
type ClientSource interface {
	WorkflowsClient(ctx context.Context) (pb.WorkflowsClient, error)
}

// Identity names the live session.
type Identity struct {
	ServiceID  string
	InstanceID string
}

// encodeJSON renders a value for the wire; nil is JSON null.
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
