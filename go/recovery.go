package servicebridge

import (
	"context"

	"github.com/service-bridge/sdk/go/internal/outbox"
)

// FailedOutboxEvent is an event rejected by the runtime and retained locally.
type FailedOutboxEvent = outbox.Record

// FailedOutboxEvents returns a bounded page. Use the last event ID as cursor.
func (c *Client) FailedOutboxEvents(ctx context.Context, limit int, cursor string) ([]FailedOutboxEvent, error) {
	if c.buffer == nil {
		return nil, newError(CodeState, "FailedOutboxEvents", "client outbox is not open", nil)
	}
	return c.buffer.ListFailed(ctx, limit, cursor)
}

// RetryFailedEvent explicitly rearms a failed event with its original identity.
func (c *Client) RetryFailedEvent(ctx context.Context, id string) (bool, error) {
	if c.buffer == nil {
		return false, newError(CodeState, "RetryFailedEvent", "client outbox is not open", nil)
	}
	changed, err := c.buffer.RetryFailed(ctx, id)
	if changed && c.drainer != nil {
		c.drainer.Kick()
	}
	return changed, err
}

// DiscardFailedEvent removes one failed event. Pending or inflight rows are protected.
func (c *Client) DiscardFailedEvent(ctx context.Context, id string) (bool, error) {
	if c.buffer == nil {
		return false, newError(CodeState, "DiscardFailedEvent", "client outbox is not open", nil)
	}
	return c.buffer.DiscardFailed(ctx, id)
}
