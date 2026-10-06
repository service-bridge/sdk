//go:build e2e && runtime_next

// Needs a runtime that fills EventDelivery.matched_patterns; see
// events_next_test.go.

package e2e

import (
	"context"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// TestNodePublishGoConsumes is the reverse: an event the Node SDK encoded is
// decoded by the Go SDK into the type its subscription was declared with.
func TestNodePublishGoConsumes(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	name := uniqueName("xlang.node2go.event")
	got := &collector{}

	subscriber := newClient(t, domainXLang, 1)
	if err := servicebridge.SubscribeEvent(subscriber, name, func(_ context.Context, e *e2epb.OrderEvent) error {
		got.add(e)
		return nil
	}); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	start(ctx, t, subscriber)

	cfg := newAgentConfig(t)
	cfg.PublishEvents = []string{name}
	agent := startNodeAgent(ctx, t, cfg)

	publishCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err := agent.publish(publishCtx, name, map[string]any{
		"orderId": "xlang-2", "amount": 7.25, "currency": "SEK",
	})
	if err != nil {
		t.Fatalf("the Node agent could not publish: %v\nagent stderr:\n%s", err, agent.stderr.String())
	}

	waitFor(ctx, t, deliveryTimeout, "the Node-published event to reach the Go subscriber",
		func(context.Context) (bool, error) { return got.len() > 0, nil })

	order := got.snapshot()[0]
	if order.GetOrderId() != "xlang-2" {
		t.Errorf("order id decoded as %q, want %q", order.GetOrderId(), "xlang-2")
	}
	if order.GetAmount() != 7.25 {
		t.Errorf("amount decoded as %v, want 7.25", order.GetAmount())
	}
	if order.GetCurrency() != "SEK" {
		t.Errorf("currency decoded as %q, want %q", order.GetCurrency(), "SEK")
	}
}
