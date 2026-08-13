# Testing — unit-test handlers without a live runtime

`service-bridge/testing` is an in-memory double for `sb.rpc` and `sb.event`. No network, no SQLite, no runtime process. Use it to unit-test the handlers you register with `rpc.handle` / `event.handle`, including their outbound side effects (`rpc.call`, `event.publish`).

```ts
import { createTestHarness } from "service-bridge/testing";
```

## RPC handler: register and invoke

```ts
harness.rpc.handle<Req, Res>(name: string, fn: (req: Req) => Promise<Res> | Res): void
harness.rpc.invoke<Req, Res>(name: string, req: Req): Promise<Res>
```

`fn` is the exact `RpcHandlerFn` type `sb.rpc.handle(name, fn, opts)` accepts in production — `opts.schema` is not needed here, the harness skips wire encode/decode and calls `fn(req)` with the typed object directly. `invoke()` propagates the handler's own thrown error unchanged (no `errorCode`/`errorMessage` wire mapping — that's `CallServer`'s job, not the handler's).

```ts
const harness = createTestHarness();
harness.rpc.handle("Charge", async (req: { userId: string; amount: number }) => {
  if (req.amount <= 0) throw new Error("amount must be positive");
  return { transactionId: `tx-${req.userId}`, ok: true };
});

const res = await harness.rpc.invoke("Charge", { userId: "u-1", amount: 42 });
// { transactionId: "tx-u-1", ok: true }
```

`invoke()` throws `no RPC handler registered for "..."` if nothing was registered under that name.

## Outbound RPC calls: mock + record

```ts
harness.rpc.mockResponse(serviceName: string, methodName: string, responder: Res | ((payload, opts?) => Res | Promise<Res>)): void
harness.rpc.calls(): readonly { serviceName; methodName; payload; opts? }[]
```

```ts
harness.rpc.mockResponse("fraud-svc", "Check", { blocked: false });
// inside the handler under test: await deps.rpc.call("fraud-svc", "Check", { userId });

expect(harness.rpc.calls()).toEqual([
  { serviceName: "fraud-svc", methodName: "Check", payload: { userId: "u-1" } },
]);
```

Calling `call()` for a `(serviceName, methodName)` pair with no configured mock throws — a forgotten mock fails the test immediately instead of resolving to `undefined`.

## Event handler: deliver + ack/nack

```ts
harness.event.handle(pattern: string, fn: (payload: unknown) => Promise<void> | void): void
harness.event.deliver(name: string, payload: unknown): Promise<{ outcome: "ack" } | { outcome: "nack"; reason: string }>
```

`deliver()` reproduces the exact ack/nack contract of `Subscriber.handleDelivery` in production: no handler registered for the exact name → `ack` (routing is server-side); a handler throws → `nack` with `String(error)`, remaining handlers for that delivery are skipped; every handler succeeds → `ack`. Multiple handlers on the same pattern run in registration order.

```ts
harness.event.handle("payment.charged", async (payload) => {
  const { transactionId } = payload as { transactionId: string };
  await sendReceipt(transactionId); // must be idempotent — delivery is at-least-once
});

await harness.event.deliver("payment.charged", { transactionId: "tx-1" });
// { outcome: "ack" }
```

There is no `attempt` number in the real `EventHandlerFn` contract (`sb.event.handle`'s `fn` only receives the decoded payload) — a retry attempt is simulated by calling `deliver()` again with the same handler and asserting the outcome of each call:

```ts
let dbDown = true;
harness.event.handle("payment.charged", async () => {
  if (dbDown) throw new Error("db unavailable");
});

await harness.event.deliver("payment.charged", {}); // { outcome: "nack", reason: "Error: db unavailable" }
dbDown = false;
await harness.event.deliver("payment.charged", {}); // { outcome: "ack" }
```

## Outbound event publishing

```ts
harness.event.publish<T>(name: string, payload: T, opts?: PublishOpts): Promise<{ eventId: string }>
harness.event.published(): readonly { name; payload; opts? }[]
```

```ts
// inside the handler under test: await deps.event.publish("payment.charged", { transactionId, amount });

expect(harness.event.published()).toEqual([
  { name: "payment.charged", payload: { transactionId: "tx-u-1", amount: 42 } },
]);
```

`publish()` only records the call and returns a freshly generated `eventId` — it does not validate the event name or encode the payload. It's an observation point for "what did the handler publish", not a `Publisher` replacement.

## Pattern: testable handler factory

Write handlers that need an outbound channel as a factory over a narrow dependency, not a closure over a global `sb`:

```ts
import type { EventDomain, RpcDomain } from "service-bridge";

function makeChargeHandler(deps: {
  rpc: Pick<RpcDomain, "call">;
  event: Pick<EventDomain, "publish">;
}) {
  return async (req: { userId: string; amount: number }) => {
    const fraud = await deps.rpc.call<{ userId: string }, { blocked: boolean }>(
      "fraud-svc", "Check", { userId: req.userId },
    );
    if (fraud.blocked) throw new Error(`user ${req.userId} blocked`);

    const transactionId = `tx-${req.userId}`;
    await deps.event.publish("payment.charged", { transactionId, amount: req.amount });
    return { transactionId, ok: true };
  };
}

// production: sb.rpc.handle("Charge", makeChargeHandler(sb), { schema: ... });
// test:       harness.rpc.handle("Charge", makeChargeHandler(harness));
```

`Pick<RpcDomain, "call">` / `Pick<EventDomain, "publish">` are structural types — `harness.rpc` / `harness.event` satisfy them without a cast, because `TestRpcDomain.call` / `TestEventDomain.publish` share the exact same signature as the production methods.

## Scope — what this harness does not do

| Not covered | Why |
|---|---|
| Protobuf encode/decode | The handler receives and returns typed objects directly, matching what its own business logic sees post-decode on the real path. |
| Wire error mapping (`errorCode`/`errorMessage`) | `invoke()` rethrows the handler's own error so `rejects.toThrow(...)` checks the business message, not the transport envelope. |
| Streaming RPC (`handleStream`) | Out of scope for now — `handle`/`invoke` are unary only. |
| Workflow steps | The runner checkpoints step state against the runtime (persist/resume/replay); without a runtime a step can't be honestly committed or replayed. |
| Event name validation, idempotency, partitioning | `TestEventDomain` is a recorder for outbound publishes, not a `Publisher` replacement. |
| Live gRPC, SQLite outbox | The harness runs entirely in the test process's memory. |

See [userDocs/testing.md](../../userDocs/testing.md) for the full guide and [src/testing/README.md](../../src/testing/README.md) for the module contract.
