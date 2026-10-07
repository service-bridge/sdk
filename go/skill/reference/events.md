# Events — Go SDK reference

Durable publish/subscribe, at-least-once. `PublishEvent` returns once the runtime has stored the event; delivery to subscribers is asynchronous.

## Signatures

```go signature
func DefineEvent[T proto.Message](c *Client, name string) (*Event[T], error)
func (e *Event[T]) Name() string
func (e *Event[T]) Publish(ctx context.Context, payload T, opts ...PublishOption) (string, error)

func PublishEvent[T proto.Message](ctx context.Context, c *Client, name string, payload T, opts ...PublishOption) (string, error)
func SubscribeEvent[T proto.Message](c *Client, pattern string, fn func(ctx context.Context, event T) error, opts ...SubscribeOption) error
func SubscribeEventRaw(c *Client, pattern string, fn func(ctx context.Context, payload []byte) error, opts ...SubscribeOption) error
func WithFilter(filter map[string]any) SubscribeOption

func DeliveryFromContext(ctx context.Context) (DeliveryInfo, bool)
type DeliveryInfo struct {
	EventID      string
	EventName    string // the concrete name the publisher used
	Attempt      int32
	DeliveryID   string
	LeaseToken   string // opaque delivery generation, not a business idempotency key
	PartitionKey string
	Headers      map[string]string
	OccurredAtMs int64
}
```

## The mental model

`Publish` hands the event to an in-memory queue and **waits for the runtime's acknowledgement**; the returned id means the event is in the runtime's store. While the runtime is unreachable the event waits in the queue. The queue is bounded (`sb.WithMaxPendingPublishes`, default `10000` → `CodeQueueFull` at once when full) and so is the wait (`sb.WithPublishTimeout`, default `30s` → `CodeTimeout`, and the message says whether the event was ever sent). Nothing is written to disk: an unacknowledged event dies with the process.

Consequences:
- Declare with `DefineEvent` **before** `Start`; publish **after** `Start` (before it → `CodeState`). `DefineEvent` is the publisher's declaration only — a subscriber never needs it.
- A `CodeTimeout` after the event was sent means the outcome is unknown: repeat it only with `sb.WithEventIdempotencyKey`.
- `Stop` flushes the queue within its deadline; leftovers fail with `CodeConnection` "client stopped".

Runtime verdicts:

| Verdict | `Publish` returns |
|---|---|
| accepted | the event id (UUIDv7, increasing in publish order) |
| duplicate (same idempotency key) | success, with the **original** event's id |
| conflict (same id, other content) | `CodeConflict` |
| invalid name | `CodeInvalidEventName` |
| forbidden by policy | `CodeAccessDenied`, and `c.OnPolicyViolation` fires |
| no verdict / transport failure | retried with the same id (100 ms … 5 s, immediately on reconnect) |

One batch of up to 100 events is in flight at a time, with at most one event per partition key per batch, so each key keeps its order across retries.

## Names and patterns

Published names must match `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$` → otherwise `CodeInvalidEventName`. A publish takes a name, never a pattern.

Subscriptions **may** carry a pattern:

| Token | Covers |
|---|---|
| `*` | exactly one segment |
| `#` | zero or more segments |

**Routing is the runtime's.** Each delivery lists the patterns of this service it matched, and the SDK runs the handlers of exactly those patterns, each once; it does no wildcard matching of its own. The delivery is acked only if every one of them returns `nil`. If none of the matched patterns has a handler in this process, the delivery is nacked with `no handler for matched patterns`.

**One pattern, one handler per process.** A second handler for the same pattern is `CodeValidation` at declaration. Several handlers for one event come from different patterns (`order.placed` and `order.*`).

The payload is decoded into the subscriber's own type parameter. If payload shapes differ across a pattern's family, use `SubscribeEventRaw`.

## Filters

```go
sb.SubscribeEvent(c, "order.placed", handle,
	sb.WithFilter(map[string]any{"$.status": "paid", "$.region": "eu"}))
```

Only events whose JSON payload has every given path equal to its literal are delivered. The runtime evaluates the filter; the SDK only sends it. A filter the runtime refuses fails the registration and stops the client.

## PublishOption

| Option | Default | Effect |
|---|---|---|
| `sb.WithEventIdempotencyKey(k)` | none | Runtime-side dedup of the publish: a repeat returns the original event's id. Named apart from `sb.WithIdempotencyKey` because the two travel to different places. |
| `sb.WithPartitionKey(k)` | none | FIFO lane: events sharing a key are handled in publication order, serially. |
| `sb.WithFireAndForget()` | off | Return the id as soon as the event is queued, without waiting for the acknowledgement. Accepts loss: until acknowledged the event lives only in process memory, and a delivery failure is only logged. A full queue still fails. |
| `sb.WithHeaders(map[string]string)` | none | Envelope metadata. |
| `sb.WithOccurredAt(unixMs)` | now | When the event happened, unix-ms. |

## Delivery

- At-least-once. **Handlers must be idempotent.**
- `nil` acks; an error nacks and the runtime redelivers later.
- A handler panic becomes a nack, not a process crash.
- `sb.WithMaxInFlightEvents(n)` (default 32) is real backpressure: at the cap the SDK stops reading the delivery stream.
- During `Stop` the subscriber takes no new deliveries and leaves them unanswered, so the runtime redelivers them without burning an attempt.
- Retries, fan-out and DLQ belong to the runtime. There is no DLQ API in the SDK — operate it in the dashboard.

## Complete program

```go
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"example.com/orders/orderpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051),
		sb.WithMaxPendingPublishes(50_000),
		sb.WithPublishTimeout(10*time.Second),
		sb.WithMaxInFlightEvents(64),
	)
	if err != nil {
		log.Fatal(err)
	}

	// A refused publish also arrives here — the only signal for fire-and-forget.
	c.OnPolicyViolation(func(v sb.PolicyViolation) {
		log.Printf("policy refused %s %q (%s): %s", v.Declaration, v.Value, v.DenySide, v.Reason)
	})

	// Declare what we publish. Before Start.
	placed, err := sb.DefineEvent[*orderpb.OrderPlaced](c, "order.placed")
	if err != nil {
		log.Fatal(err)
	}

	// Exact-name subscription, narrowed on the runtime by a filter.
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
		},
		sb.WithFilter(map[string]any{"$.region": "eu"})); err != nil {
		log.Fatal(err)
	}

	// Pattern subscription; the payload shape varies across the family, so
	// take it undecoded.
	if err := sb.SubscribeEventRaw(c, "audit.#",
		func(ctx context.Context, payload []byte) error {
			info, _ := sb.DeliveryFromContext(ctx)
			return archive(ctx, info.EventName, payload)
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
	switch {
	case errors.Is(err, sb.ErrQueueFull):
		log.Println("publish queue is full — the runtime has been unreachable too long")
	case errors.Is(err, sb.ErrTimeout):
		log.Println("no acknowledgement in time:", err) // the message says whether it was sent
	case err != nil:
		log.Fatal(err)
	default:
		log.Println("stored event", id)
	}

	select {}
}

func insertIfAbsent(ctx context.Context, key string) (bool, error) { return true, nil }
func notifyCustomer(ctx context.Context, orderID, carrier string) error { return nil }
func archive(ctx context.Context, name string, payload []byte) error    { return nil }
```

## Gotchas

- `DefineEvent` / `SubscribeEvent` after `Start` → `CodeState`. `Publish` before `Start` → `CodeState`.
- Publishing a name with `*` or `#` → `CodeInvalidEventName`.
- A second handler for the same pattern → `CodeValidation`.
- A partition key serialises its lane — pick it as fine-grained as the domain allows (`orderID`, not `"orders"`).
- `sb.WithFireAndForget()` does not wait for the acknowledgement and accepts loss on a crash: use it for telemetry, never for domain state.
- Runtime-side dedup (`WithEventIdempotencyKey`) protects against double **publish**; only your handler protects against double **delivery**.
