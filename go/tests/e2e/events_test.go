//go:build e2e

package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// collector records deliveries from the subscriber goroutines the SDK runs
// them on, so a test reads them without racing the delivery path.
type collector struct {
	mu     sync.Mutex
	orders []*e2epb.OrderEvent
}

func (c *collector) add(o *e2epb.OrderEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orders = append(c.orders, o)
}

func (c *collector) snapshot() []*e2epb.OrderEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*e2epb.OrderEvent(nil), c.orders...)
}

func (c *collector) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.orders)
}

// TestPublishReturnsOnlyOnceTheRuntimeHoldsTheEvent: PublishEvent resolves
// after the runtime acknowledged the event, so by the time it returns the row
// is in event_log under the returned id — no polling needed. Ids are uuidv7
// and rise in publish order.
func TestPublishReturnsOnlyOnceTheRuntimeHoldsTheEvent(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	name := uniqueName("go.events.acked")

	publisher := newClient(t, domainEvents, 1)
	if _, err := servicebridge.DefineEvent[*e2epb.OrderEvent](publisher, name); err != nil {
		t.Fatalf("define event: %v", err)
	}
	start(ctx, t, publisher)

	var ids []string
	for i := range 3 {
		id, err := servicebridge.PublishEvent(ctx, publisher, name, &e2epb.OrderEvent{OrderId: fmt.Sprintf("acked-%d", i)})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		ids = append(ids, id)
		rows := mustQuery(ctx, t, "SELECT id::text AS id FROM event_log WHERE id = "+lit(t, id)+"::uuid")
		if len(rows) != 1 {
			t.Fatalf("event %s is not in event_log when PublishEvent returned", id)
		}
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("event ids are not monotonic: %v", ids)
		}
	}
}

func orderIDs(orders []*e2epb.OrderEvent) []string {
	out := make([]string, 0, len(orders))
	for _, o := range orders {
		out = append(out, o.GetOrderId())
	}
	return out
}
