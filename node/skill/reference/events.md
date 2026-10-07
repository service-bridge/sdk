# Events — durable pub/sub

`publish()` resolves once the runtime has stored the event in Postgres; the runtime fans it out to every matching subscriber with at-least-once delivery, retries, and a dead-letter queue (operated from the dashboard, not the SDK). The SDK keeps nothing on disk.

## Declare a published event

```ts
sb.event.define(name: string, spec: SchemaSpec): void
```

- Call before `await sb.start()`, on the **publisher** only. It declares "this service publishes `name`" and the schema used to encode it. Subscribers do not call `define` — they pass their own schema to `handle()`.
- `name` must match `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$` (dotted segments, lowercase). `publish()` with a bad name throws `InvalidEventNameError`.
- **Schema:** reference the payload message by `method` (the SDK resolves the input message from the rpc of that name in the `service` block — event names contain dots, so pick a valid rpc identifier), or pass explicit `input` **and** `output`. Passing `input` alone does **not** resolve.
- **Only the payload is the contract.** The reply half the spec format demands is never encoded, decoded or hashed: an event's `contract_hash` pairs the payload with the empty message, exactly as the Go SDK derives it. Pick any message for `output`; changing it reroutes nothing.
- Re-defining with the same spec object is a no-op; with a different spec it throws `ValidationError`.

```proto
// events.proto
syntax = "proto3";
package billing;
message ChargedEvent { string charge_id = 1; double amount = 2; }
service BillingEvents {
  // method for event.define / handle schema; the block requires a reply type, the event identity ignores it.
  rpc billing_charged (ChargedEvent) returns (ChargedEvent);
}
```

## Handle (subscribe)

```ts
sb.event.handle(
  pattern: string,
  fn: (payload: unknown, ctx: EventHandlerContext) => Promise<void> | void,
  opts?: { schema?: SchemaSpec; filter?: Record<string, unknown> },
): void

interface EventHandlerContext {
  eventId: string;
  eventName: string;              // the concrete name the publisher used
  attempt: number;
  deliveryId: string;
  leaseToken: string;
  partitionKey: string;
  headers: Record<string, string>;
  occurredAtMs: number;
  signal: AbortSignal;            // aborts when the delivery stream breaks or the bridge stops
}
```

- `pattern` is an exact name or an AMQP pattern: `*` = exactly one segment, `#` = zero or more (`billing.*`, `billing.#`). The **runtime** matches patterns; each delivery lists the patterns of this service it matched, and the SDK runs the handler of each of them.
- **One handler per pattern** per process — a duplicate throws `ValidationError`.
- `opts.schema` decodes the payload. Without it the handler receives the raw bytes (`Uint8Array`).
- `opts.filter` — `{ "$.path": literal, ... }`, all equalities, evaluated by the runtime on the JSON form of the payload before delivery. An invalid filter makes the runtime reject the registration and the bridge stops with `ValidationError`.
- Register before `start()`. Subscriptions are part of the registration: once the subscriber's `start()` resolved, new publishes match it. Events published before that are not delivered to it.
- **Handlers must be idempotent.** Delivery is at-least-once. Throwing → Nack → retry → DLQ after the runtime's max attempts. Returning normally Acks (only when every matched handler succeeded).
- Deliveries sharing a `partitionKey` are processed one at a time; others run in parallel up to `eventsMaxInFlight` (32).

```ts
sb.event.handle(
  "billing.charged",
  async (payload, ctx) => {
    const e = payload as { chargeId: string; amount: number };
    await applyOnce(e.chargeId, e.amount, ctx.eventId); // idempotent by chargeId
  },
  {
    schema: { protoFile: "./events.proto", method: "billing_charged" },
    filter: { "$.amount": 100 },
  },
);
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

- Call **after** `await sb.start()` (earlier: `StateError`). The event must be `define()`d first (otherwise `StateError`).
- A payload that does not encode with the schema rejects with `ValidationError` before anything is sent.
- Resolves `{ eventId }` (UUIDv7, monotonic in publish order) after the runtime ACK.
- While the runtime is unreachable the event waits in a bounded in-memory queue and is retried with backoff:
  - queue full (`maxPendingPublishes`, default 10000) → `ServiceBridgeError` `QUEUE_FULL` immediately (retryable);
  - no ACK within `publishTimeoutMs` (default 30000) → `TimeoutError`, message "not sent" (safe to repeat) or "outcome unknown" (repeat only with the same `idempotencyKey`).
- Events sharing a `partitionKey` reach the runtime in publish order.

```ts
sb.event.define("billing.charged", { protoFile: "./events.proto", method: "billing_charged" });
await sb.start();
const { eventId } = await sb.event.publish(
  "billing.charged",
  { chargeId: "ch-123", amount: 100 },
  { idempotencyKey: "ch-123", partitionKey: "user-42" },
);
```

### PublishOpts

```ts
interface PublishOpts {
  idempotencyKey?: string;           // runtime dedup (24h window): same key + same content → success with the ORIGINAL eventId; different content → CONFLICT
  partitionKey?: string;             // FIFO ordering per key
  fireAndForget?: boolean;           // resolve right after enqueueing; lost if the process dies first; terminal rejections are only logged
  headers?: Record<string, string>;  // reach the subscriber as ctx.headers
  occurredAtMs?: number;             // business timestamp, unix ms; default now
}
```

`fireAndForget` still throws `QUEUE_FULL` and validation errors synchronously. Use it only for events whose loss is acceptable.

`publish` is not atomic with your database transaction. When the business change and the event must commit together, store the intent in the same transaction and publish from that table with `idempotencyKey` = the row id.

## Delivery semantics (what to rely on)

- **At-least-once**: a handler may see the same event more than once → make it idempotent (dedup on a business key or `ctx.eventId`).
- **Fan-out**: every subscriber whose subscription matches (and whose filter passes) gets its own delivery.
- **Ordering**: only guaranteed per `partitionKey`, per consumer.
- **Retries + DLQ**: a throwing handler is retried by the runtime; after max attempts the delivery goes to the DLQ. Replay/purge the DLQ from the dashboard — the SDK has no DLQ API.

## Errors

| Error | When |
|---|---|
| `InvalidEventNameError` (`INVALID_EVENT_NAME`) | `publish()` with a name failing the regex. |
| `StateError` (`STATE`) | `publish()` before `start()` or for a name never `define()`d. |
| `ValidationError` (`VALIDATION`) | Payload does not match the schema; duplicate `handle()` pattern. |
| `ServiceBridgeError` `QUEUE_FULL` | `maxPendingPublishes` events are already waiting for the runtime. |
| `TimeoutError` (`TIMEOUT`) | No ACK within `publishTimeoutMs`. |
| `ServiceBridgeError` `CONFLICT` | Same `idempotencyKey`, different content. |
| `AccessDeniedError` (`ACCESS_DENIED`) | Access policy forbids publishing this event (also emitted as `policy_violation`). |
| `ServiceBridgeError` `CONNECTION` | `stop()` ran before the runtime acknowledged the event. |

## Capacity knobs (constructor options)

`publishTimeoutMs` (30000), `maxPendingPublishes` (10000), `eventsMaxInFlight` (32 concurrent inbound deliveries). See [configuration.md](configuration.md).
