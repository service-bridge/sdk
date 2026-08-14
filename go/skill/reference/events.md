# Events — Go SDK reference

Durable publish/subscribe, at-least-once. Publishing is a local SQLite insert; delivery is asynchronous.

## Signatures

```go signature
func DefineEvent[T proto.Message](c *Client, name string) (*Event[T], error)
func (e *Event[T]) Name() string
func (e *Event[T]) Publish(ctx context.Context, payload T, opts ...PublishOption) (string, error)

func PublishEvent[T proto.Message](ctx context.Context, c *Client, name string, payload T, opts ...PublishOption) (string, error)
func SubscribeEvent[T proto.Message](c *Client, name string, fn func(ctx context.Context, event T) error) error
func SubscribeEventRaw(c *Client, name string, fn func(ctx context.Context, payload []byte) error) error
```

## The mental model

`Publish` writes to an on-disk outbox and returns. **It does not touch the network.** A successful return means "written durably", not "delivered". An unreachable runtime never slows a publish down or fails it; a background drain ships batches when the connection is there.

Consequences:
- Declare with `DefineEvent` **before** `Start`; publish **after** `Start` (before it there is no buffer → `CodeState`).
- A policy refusal cannot be returned to the caller — it arrives through `c.OnPolicyViolation` and a Warn log.

## Names and patterns

Published names must match `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`, checked locally → `CodeInvalidEventName`. Wildcards are not allowed in a published name.

Subscriptions **may** carry a pattern:

| Token | Covers |
|---|---|
| `*` | exactly one segment |
| `#` | zero or more segments |

The runtime routes on the pattern; the SDK also matches it locally when picking handlers for an arriving delivery, so a family subscription works end to end. The delivery carries the **concrete** name the publisher used — if payload shapes differ across the family, use `SubscribeEventRaw`.

## PublishOption

| Option | Default | Effect |
|---|---|---|
| `sb.WithEventIdempotencyKey(k)` | none | Runtime-side dedup of the publish. Named apart from `sb.WithIdempotencyKey` because the two travel to different places. |
| `sb.WithPartitionKey(k)` | none | FIFO lane: events sharing a key are handled in publication order, serially. |
| `sb.WithFireAndForget()` | off | Straight to the runtime, skipping the buffer and every retry with it. Lossy by design. |
| `sb.WithHeaders(map[string]string)` | none | Envelope metadata. |
| `sb.WithOccurredAt(unixMs)` | now | When the event happened, unix-ms. |

## Delivery

- At-least-once. **Handlers must be idempotent.**
- `nil` acks; an error nacks and the runtime redelivers later.
- Several handlers may share a name; they run in registration order and the first failure nacks the whole delivery.
- A delivery with no matching handler is acked, not nacked — routing is the runtime's.
- A handler panic becomes a nack, not a process crash.
- `sb.WithMaxInFlightEvents(n)` (default 32) is real backpressure: at the cap the SDK stops reading the delivery stream.
- Retries, fan-out and DLQ belong to the runtime. There is no DLQ API in the SDK — operate it in the dashboard.

## Outbox

| Knob | Default |
|---|---|
| `sb.WithDataDir(dir)` | `./.servicebridge` (the file is `sdk.db`) |
| `sb.WithMaxOutboxRows(n)` | `10000`; `0` lifts the cap |
| `sb.WithDrainBatchSize(n)` | `100` |

At the cap `Publish` returns `CodeOutboxFull` and nothing already buffered is discarded. The drain ladder is 1 s · 5 s · 30 s · 2 min · 10 min with jitter and the last rung repeats forever — a transport failure never exhausts a budget, because the publish already promised durability. Only meaningful verdicts are terminal: an invalid name and a policy refusal.

## Complete program

```go
package main

import (
	"context"
	"errors"
	"log"
	"os"

	"example.com/orders/orderpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051),
		sb.WithDataDir("/var/lib/orders/sb"),
		sb.WithMaxOutboxRows(50_000),
		sb.WithMaxInFlightEvents(64),
	)
	if err != nil {
		log.Fatal(err)
	}

	// A refused publish cannot be returned to the caller — it arrives here.
	c.OnPolicyViolation(func(v sb.PolicyViolation) {
		log.Printf("policy refused %s %q (%s): %s", v.Declaration, v.Value, v.DenySide, v.Reason)
	})

	// Declare what we publish. Before Start.
	placed, err := sb.DefineEvent[*orderpb.OrderPlaced](c, "order.placed")
	if err != nil {
		log.Fatal(err)
	}

	// Exact-name subscription.
	if err := sb.SubscribeEvent(c, "order.shipped",
		func(ctx context.Context, e *orderpb.OrderShipped) error {
			// At-least-once: dedup on a domain key before doing anything
			// that is not naturally idempotent.
			fresh, err := insertIfAbsent(ctx, "shipped:"+e.GetOrderId())
			if err != nil {
				return err // nack, the runtime redelivers
			}
			if !fresh {
				return nil // already handled, ack
			}
			return notifyCustomer(ctx, e.GetOrderId(), e.GetCarrier())
		}); err != nil {
		log.Fatal(err)
	}

	// Pattern subscription: one segment.
	if err := sb.SubscribeEvent(c, "order.*",
		func(ctx context.Context, e *orderpb.OrderPlaced) error {
			return audit(ctx, e.GetOrderId())
		}); err != nil {
		log.Fatal(err)
	}

	// Payload shape varies across the family: take it undecoded.
	if err := sb.SubscribeEventRaw(c, "audit.#",
		func(ctx context.Context, payload []byte) error {
			return archive(ctx, payload)
		}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	// Publishing happens only after Start.
	id, err := placed.Publish(ctx,
		&orderpb.OrderPlaced{OrderId: "o-1", Total: 4200, UserId: "u-1"},
		sb.WithPartitionKey("o-1"),                     // serialises this order's events
		sb.WithEventIdempotencyKey("order-o-1-placed"), // dedup at the runtime
	)
	if errors.Is(err, sb.ErrOutboxFull) {
		log.Println("local buffer is full — the runtime has been unreachable too long")
	} else if err != nil {
		log.Fatal(err)
	}
	log.Println("buffered event", id)

	select {}
}

func insertIfAbsent(ctx context.Context, key string) (bool, error) { return true, nil }
func notifyCustomer(ctx context.Context, orderID, carrier string) error { return nil }
func audit(ctx context.Context, orderID string) error                   { return nil }
func archive(ctx context.Context, payload []byte) error                 { return nil }
```

## Gotchas

- `DefineEvent` / `SubscribeEvent` after `Start` → `CodeState`. `Publish` before `Start` → `CodeState`.
- Publishing a name with `*` or `#` → `CodeInvalidEventName`.
- A partition key serialises its lane — pick it as fine-grained as the domain allows (`orderID`, not `"orders"`).
- `sb.WithFireAndForget()` drops the buffer **and** the retries: use it for telemetry, never for domain state.
- Runtime-side dedup (`WithEventIdempotencyKey`) protects against double **publish**; only your handler protects against double **delivery**.
