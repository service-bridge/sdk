# Events — durable pub/sub

At-least-once delivery through a local SQLite outbox → runtime → subscribers. Fan-out to every matching subscriber, retries, and a dead-letter queue (operated from the dashboard, not the SDK).

## Declare an event

```ts
sb.event.define(name: string, spec?: SchemaSpec): void
```

- Call before `await sb.start()`, on **both** publisher and subscriber (each indexes schemas locally to encode/decode; there's no global decoder registry).
- `name` must match `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$` (dotted segments, lowercase). A bad name throws `InvalidEventNameError`.
- **Schema:** the proto needs a `service` block, and you reference the message by `method` — the SDK resolves the input message from the rpc of that name. (Event names contain dots, so they can't be rpc names directly; pick a valid rpc identifier for `method`.) Alternatively pass explicit `input` **and** `output`. Passing `input` alone does **not** resolve.
- **Only the payload is the contract.** The reply half the spec format demands is never encoded, decoded or hashed: an event's `contract_hash` pairs the payload with the empty message, exactly as the Go SDK derives it from `google.protobuf.Empty`. Pick any message for `output`; changing it reroutes nothing.

```proto
// events.proto
syntax = "proto3";
package billing;
message ChargedEvent { string charge_id = 1; double amount = 2; }
service BillingEvents {
  // method for event.define; the block requires a reply type, the event identity ignores it.
  rpc billing_charged (ChargedEvent) returns (ChargedEvent);
}
```

## Handle (subscribe)

```ts
sb.event.handle(name: string, fn: (payload: unknown) => Promise<void> | void): void
```

- **Exact event name** — not a wildcard. (Wildcard subscription routing is a server-side concern, not the handler string.)
- Register before `start()`. A single instance may register several handlers; each matching event fires all of them.
- **Handlers must be idempotent.** Delivery is at-least-once. Throwing causes a Nack → retry → DLQ after the runtime's max attempts. Returning normally Acks.

```ts
sb.event.define("billing.charged", { protoFile: "./events.proto", method: "billing_charged" });
sb.event.handle("billing.charged", async (payload) => {
  const e = payload as { charge_id: string; amount: number };
  await applyOnce(e.charge_id, e.amount); // idempotent by charge_id
});
await sb.start();
```

## Publish

```ts
await sb.event.publish<T>(
  name: string,
  payload: T,
  opts?: PublishOpts,
): Promise<{ eventId: string }>
```

- Call **after** `await sb.start()`. The event must be `define()`d first.
- The payload is validated against the schema before it enters the outbox; encoding errors throw synchronously.
- Returns `{ eventId }` (a monotonic UUID).

```ts
sb.event.define("billing.charged", { protoFile: "./events.proto", method: "billing_charged" });
await sb.start();
const { eventId } = await sb.event.publish(
  "billing.charged",
  { charge_id: "ch-123", amount: 100 },
  { idempotencyKey: "ch-123", partitionKey: "user-42" },
);
```

> **Start the subscriber before the publisher.** Fan-out matches against registered subscriptions at publish time — a publish to a name nobody subscribes to yet is accepted but delivered to no one. After `start()`, the subscription registers asynchronously; in a script that publishes microseconds later, wait for the `connected` event (or briefly settle) before the first publish. Long-running services never hit this.

### PublishOpts

```ts
interface PublishOpts {
  idempotencyKey?: string;            // runtime dedups same key (24h window)
  partitionKey?: string;             // FIFO ordering per key per consumer
  fireAndForget?: boolean;           // skip the durable outbox (best-effort); default false
  headers?: Record<string, string>; // metadata; not passed to the handler payload
  occurredAtMs?: number;             // business timestamp; default now
}
```

## Delivery semantics (what to rely on)

- **At-least-once**: a handler may see the same event more than once → make it idempotent (use `idempotencyKey` on publish and/or a dedup key in the handler).
- **Fan-out**: every subscriber whose subscription matches gets its own delivery.
- **Ordering**: only guaranteed per `partitionKey`, per consumer.
- **Retries + DLQ**: a throwing handler is retried by the runtime; after max attempts the delivery goes to the DLQ. Replay/purge the DLQ from the dashboard — the SDK has no DLQ API.

## Errors

```ts
import { InvalidEventNameError, OutboxFullError } from "service-bridge";
```

- `InvalidEventNameError` — from `define()`/`publish()` when the name fails the regex.
- `OutboxFullError` — from `publish()` when the local outbox hits `maxOutboxRows` (backpressure; default 100000). Slow down or raise the cap.
- "no schema registered" — publishing/handling a name that was never `define()`d with a schema.

## Capacity knobs (constructor options)

`dataDir` (outbox location, default `./.servicebridge`), `maxOutboxRows` (100000), `eventsDrainerBatch` (50 rows/tick), `eventsMaxInFlight` (32 concurrent inbound handlers). See [configuration.md](configuration.md).
