package servicebridge

import (
	"context"

	"github.com/service-bridge/sdk/go/internal/events"
)

// DeliveryInfo identifies the current event attempt. LeaseToken is an opaque
// delivery generation, not a business idempotency guarantee.
type DeliveryInfo = events.DeliveryInfo

// DeliveryFromContext returns metadata for a subscribed event handler.
func DeliveryFromContext(ctx context.Context) (DeliveryInfo, bool) {
	return events.DeliveryFromContext(ctx)
}
