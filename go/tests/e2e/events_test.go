//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/internal/outbox"
	"github.com/service-bridge/sdk/go/internal/serde"
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

// TestEventPublishDeliver proves the durable path end to end: a publication
// buffered locally reaches a subscriber in another process with every field of
// the protobuf payload intact.
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

// TestOutboxSurvivesRestart proves the buffer is what makes a publication
// durable rather than the connection that carries it. The row is written by a
// process that never reaches the runtime; a later client opening the same
// directory is the one that delivers it.
//
// The row is seeded directly instead of being published and raced against the
// drain: a publish followed by a fast stop delivers most of the time, and a
// test that only sometimes exercises recovery reports green for the wrong
// reason.
func TestOutboxSurvivesRestart(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	name := uniqueName("go.events.recovered")
	dataDir := t.TempDir()
	got := &collector{}

	subscriber := newClient(t, domainEvents, 2)
	if err := servicebridge.SubscribeEvent(subscriber, name, func(_ context.Context, e *e2epb.OrderEvent) error {
		got.add(e)
		return nil
	}); err != nil {
		t.Fatalf("declare subscription: %v", err)
	}
	start(ctx, t, subscriber)

	seedOutbox(ctx, t, dataDir, name, &e2epb.OrderEvent{OrderId: "recovered-1", Amount: 9.5, Currency: "GBP"})

	publisher := newClient(t, domainEvents, 1, servicebridge.WithDataDir(dataDir))
	if _, err := servicebridge.DefineEvent[*e2epb.OrderEvent](publisher, name); err != nil {
		t.Fatalf("define event: %v", err)
	}
	start(ctx, t, publisher)

	waitFor(ctx, t, deliveryTimeout, "the pre-existing outbox row to be drained and delivered",
		func(context.Context) (bool, error) { return got.len() > 0, nil })

	orders := got.snapshot()
	if orders[0].GetOrderId() != "recovered-1" {
		t.Errorf("delivered order id is %q, want %q", orders[0].GetOrderId(), "recovered-1")
	}
	if orders[0].GetAmount() != 9.5 {
		t.Errorf("delivered amount is %v, want 9.5", orders[0].GetAmount())
	}
}

// seedOutbox writes one pending row into the outbox database of dataDir and
// closes it again, which is the state a process that died before its drain
// leaves behind.
func seedOutbox(ctx context.Context, t *testing.T, dataDir, name string, payload *e2epb.OrderEvent) {
	t.Helper()
	storage, err := outbox.Open(ctx, outbox.Config{Dir: dataDir})
	if err != nil {
		t.Fatalf("open outbox in %s: %v", dataDir, err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			t.Fatalf("close seeded outbox: %v", err)
		}
	}()

	encoded, err := serde.Encode(payload)
	if err != nil {
		t.Fatalf("encode seeded payload: %v", err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("mint seeded event id: %v", err)
	}
	now := time.Now().UnixMilli()
	err = storage.Enqueue(ctx, outbox.Record{
		ID:           id.String(),
		Name:         name,
		Payload:      encoded.Proto,
		PayloadJSON:  encoded.JSON,
		ContractHash: encoded.ContractHash,
		OccurredAtMs: now,
		EnqueuedAtMs: now,
	}, 0)
	if err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}
}

func orderIDs(orders []*e2epb.OrderEvent) []string {
	out := make([]string, 0, len(orders))
	for _, o := range orders {
		out = append(out, o.GetOrderId())
	}
	return out
}
