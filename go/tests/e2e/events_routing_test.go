//go:build e2e

// Delivery routing by EventDelivery.matched_patterns and subscription filters
// the runtime evaluates.

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// TestEventPublishDeliver proves the path end to end: an acknowledged
// publication reaches a subscriber in another process with every field of the
// protobuf payload intact.
func TestEventPublishDeliver(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	name := uniqueName("go.events.happy")
	got := &collector{}

	subscriber := newClient(t, domainEvents, 2)
	if err := servicebridge.SubscribeEvent(subscriber, name, func(_ context.Context, e *e2epb.OrderEvent) error {
		got.add(e)
		return nil
	}); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	start(ctx, t, subscriber)

	publisher := newClient(t, domainEvents, 1)
	event, err := servicebridge.DefineEvent[*e2epb.OrderEvent](publisher, name)
	if err != nil {
		t.Fatalf("define event: %v", err)
	}
	start(ctx, t, publisher)

	id, err := event.Publish(ctx, &e2epb.OrderEvent{OrderId: "ord-1", Amount: 42.75, Currency: "EUR"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Errorf("publish returned %q, which is not an event identifier: %v", id, err)
	}

	waitFor(ctx, t, deliveryTimeout, "delivery of "+name,
		func(context.Context) (bool, error) { return got.len() > 0, nil })

	orders := got.snapshot()
	if len(orders) != 1 {
		t.Fatalf("received %d deliveries, want 1", len(orders))
	}
	if orders[0].GetOrderId() != "ord-1" {
		t.Errorf("order id is %q, want %q", orders[0].GetOrderId(), "ord-1")
	}
	if orders[0].GetAmount() != 42.75 {
		t.Errorf("amount is %v, want 42.75", orders[0].GetAmount())
	}
	if orders[0].GetCurrency() != "EUR" {
		t.Errorf("currency is %q, want %q", orders[0].GetCurrency(), "EUR")
	}
}

// TestEventPartitionOrdering proves the FIFO guarantee that makes a partition
// key worth using: events sharing one are delivered in publication order, not
// merely delivered.
func TestEventPartitionOrdering(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	name := uniqueName("go.events.ordered")
	partition := uniqueID("go-partition")
	const count = 8
	got := &collector{}

	subscriber := newClient(t, domainEvents, 2)
	if err := servicebridge.SubscribeEvent(subscriber, name, func(_ context.Context, e *e2epb.OrderEvent) error {
		got.add(e)
		return nil
	}); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	start(ctx, t, subscriber)

	publisher := newClient(t, domainEvents, 1)
	event, err := servicebridge.DefineEvent[*e2epb.OrderEvent](publisher, name)
	if err != nil {
		t.Fatalf("define event: %v", err)
	}
	start(ctx, t, publisher)

	for i := range count {
		if _, err := event.Publish(ctx,
			&e2epb.OrderEvent{OrderId: fmt.Sprintf("ord-%d", i), Amount: float64(i), Currency: "USD"},
			servicebridge.WithPartitionKey(partition)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	waitFor(ctx, t, deliveryTimeout, fmt.Sprintf("all %d events on partition %s", count, partition),
		func(context.Context) (bool, error) { return got.len() >= count, nil })

	orders := got.snapshot()
	if len(orders) != count {
		t.Fatalf("received %d deliveries, want %d", len(orders), count)
	}
	for i, o := range orders {
		if want := fmt.Sprintf("ord-%d", i); o.GetOrderId() != want {
			t.Fatalf("delivery %d is %q, want %q — the partition lane reordered: %v",
				i, o.GetOrderId(), want, orderIDs(orders))
		}
	}
}

// TestWildcardSubscriptionRunsByMatchedPattern: the runtime matches the
// pattern and names it in the delivery; the SDK runs the handler of exactly
// that pattern, with the concrete event name in DeliveryFromContext.
func TestWildcardSubscriptionRunsByMatchedPattern(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	family := uniqueName("go.events.family")
	name := family + ".created"
	got := &collector{}
	names := make(chan string, 4)

	subscriber := newClient(t, domainEvents, 2)
	if err := servicebridge.SubscribeEvent(subscriber, family+".*", func(ctx context.Context, e *e2epb.OrderEvent) error {
		info, _ := servicebridge.DeliveryFromContext(ctx)
		names <- info.EventName
		got.add(e)
		return nil
	}); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	start(ctx, t, subscriber)

	publisher := newClient(t, domainEvents, 1)
	if _, err := servicebridge.DefineEvent[*e2epb.OrderEvent](publisher, name); err != nil {
		t.Fatalf("define event: %v", err)
	}
	start(ctx, t, publisher)

	if _, err := servicebridge.PublishEvent(ctx, publisher, name, &e2epb.OrderEvent{OrderId: "wild-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(ctx, t, deliveryTimeout, "the wildcard delivery",
		func(context.Context) (bool, error) { return got.len() > 0, nil })
	if n := <-names; n != name {
		t.Fatalf("DeliveryFromContext names %q, want the concrete %q", n, name)
	}
}

// TestSubscriptionFilterIsEvaluatedByTheRuntime: only events whose payload
// matches every filter path are delivered.
func TestSubscriptionFilterIsEvaluatedByTheRuntime(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	name := uniqueName("go.events.filtered")
	got := &collector{}

	subscriber := newClient(t, domainEvents, 2)
	if err := servicebridge.SubscribeEvent(subscriber, name, func(_ context.Context, e *e2epb.OrderEvent) error {
		got.add(e)
		return nil
	}, servicebridge.WithFilter(map[string]any{"$.currency": "EUR"})); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	start(ctx, t, subscriber)

	publisher := newClient(t, domainEvents, 1)
	if _, err := servicebridge.DefineEvent[*e2epb.OrderEvent](publisher, name); err != nil {
		t.Fatalf("define event: %v", err)
	}
	start(ctx, t, publisher)

	for _, currency := range []string{"USD", "EUR", "GBP"} {
		if _, err := servicebridge.PublishEvent(ctx, publisher, name, &e2epb.OrderEvent{OrderId: "f-" + currency, Currency: currency}); err != nil {
			t.Fatalf("publish %s: %v", currency, err)
		}
	}
	waitFor(ctx, t, deliveryTimeout, "the EUR delivery",
		func(context.Context) (bool, error) { return got.len() > 0, nil })
	time.Sleep(2 * time.Second)
	orders := got.snapshot()
	if len(orders) != 1 || orders[0].GetCurrency() != "EUR" {
		t.Fatalf("delivered %v, want only the EUR order", orderIDs(orders))
	}
}

// TestMalformedFilterStopsTheClient: the runtime refuses the registration with
// INVALID_ARGUMENT, which is terminal — Start fails instead of reconnecting.
func TestMalformedFilterStopsTheClient(t *testing.T) {
	ctx := testContext(t, time.Minute)

	subscriber := newClient(t, domainEvents, 2)
	if err := servicebridge.SubscribeEvent(subscriber, uniqueName("go.events.badfilter"),
		func(context.Context, *e2epb.OrderEvent) error { return nil },
		servicebridge.WithFilter(map[string]any{"not a path": map[string]any{"nested": true}})); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	startCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := subscriber.Start(startCtx); err == nil {
		t.Fatal("a registration the runtime refuses must fail Start")
	} else if fmt.Sprint(err) == "" {
		t.Fatal("empty error")
	}
}
