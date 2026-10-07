# Testing — unit-test handlers without a live runtime

`service-bridge/testing` gives `createTestHarness()`: a real `ServiceBridge` started against an in-memory runtime. Only the network is replaced. Handlers register through the normal API (`sb.rpc.handle`, `sb.rpc.handleStream`, `sb.event.handle`, `sb.event.define`, `sb.client`, `sb.useSchema`), and every call goes through production code — schema encode/decode, error mapping, Publisher, Subscriber. A schema mistake fails the unit test instead of e2e.

```ts
import { createTestHarness } from "service-bridge/testing";
```

## API

```ts
createTestHarness(opts?: { callDefaults?: CallOpts; publishTimeoutMs?: number }): TestHarness

interface TestHarness {
  sb: ServiceBridge;                                   // register handlers/deps on it before start()
  start(): Promise<void>;                              // loads schemas; no network
  invoke<Req, Res>(method: string, req: Req, opts?: InvokeOpts): Promise<Res>;
  invokeStream<Req, Chunk>(method: string, req: Req, opts?: InvokeOpts): Promise<Chunk[]>;
  respond<Req, Res>(service: string, method: string, fn: (req: Req, call: CallRecord) => Res | Promise<Res>): void;
  respondStream<Req, Chunk>(service: string, method: string, fn: (req: Req, call: CallRecord) => AsyncIterable<Chunk> | Iterable<Chunk>): void;
  calls(): readonly CallRecord[];                      // outbound calls: { service, method, payload, opts }
  published(): readonly PublishedRecord[];             // { id, name, payload, payloadJson, partitionKey, idempotencyKey, headers, occurredAtMs }
  deliver(name: string, payload: unknown, opts?: DeliverOpts): Promise<DeliveryResult>;
  reset(): void;                                       // forget calls and publishes; registrations stay
  stop(): Promise<void>;
}

interface InvokeOpts { caller?: { serviceId: string; instanceId: string }; requestId?: string; idempotencyKey?: string; signal?: AbortSignal; deadline?: number }
interface DeliverOpts { matchedPatterns?: string[]; attempt?: number; partitionKey?: string; headers?: Record<string, string> }
interface DeliveryResult { acked: boolean; reason: string; matchedPatterns: string[] }
```

Also exported: `matchPattern(pattern, name)` (the runtime's routing rule) and `TEST_IDENTITY`.

## Behaviour to rely on

- `invoke` encodes `req` with the handler's schema, dispatches it for real, decodes the answer. `InvokeOpts` become the handler's `ctx`.
- Errors arrive as the caller would see them: `HandlerError` with the handler's `handlerCode` (or `"INTERNAL"` for any other throw); a refusal before the handler is a `ServiceBridgeError` with a status code (`NOT_FOUND` for an unknown method, `VALIDATION` for an undecodable request).
- `respond`/`respondStream` answer outbound `sb.rpc.call` / typed client / `sb.stream`. The caller schema is required (`sb.client` or `sb.useSchema`), otherwise `ConfigurationError` — same as production. A call without a responder is recorded and fails with `NO_LIVE_INSTANCE`.
- `deliver` goes through the real Subscriber. Matched patterns follow the runtime rules (`*` one segment, `#` zero or more); subscription filters are not evaluated. The payload is encoded with the first matched subscription's schema (or pass a `Uint8Array`). `acked: false` with the error text in `reason` when a handler throws; "no handler for matched patterns" when nothing matched.
- `published()` records events that went through the real Publisher (name check, `define` required, schema encode); the in-memory runtime acknowledges each one.

## Example (from `src/testing/example.test.ts`)

```ts
import { join } from "node:path";
import { HandlerError } from "service-bridge";
import { createTestHarness } from "service-bridge/testing";

const SHOP = join(import.meta.dir, "testdata", "shop.proto");

async function setup() {
  const h = createTestHarness();
  const { sb } = h;
  // The production wiring, unchanged.
  await sb.client("fraud-svc", SHOP, { methods: ["Check"] });
  sb.event.define("payment.charged", { protoFile: SHOP, input: "PaymentCharged", output: "PaymentCharged" });
  sb.rpc.handle(
    "Charge",
    async (req: { userId: string; amount: number }) => {
      const verdict = await sb.rpc.call<{ userId: string }, { blocked: boolean }>(
        "fraud-svc", "Check", { userId: req.userId },
      );
      if (verdict.blocked) throw new HandlerError("BLOCKED", `user ${req.userId} is blocked`);
      const transactionId = `tx-${req.userId}`;
      await sb.event.publish("payment.charged", { transactionId, amount: req.amount });
      return { transactionId, ok: true };
    },
    { schema: { protoFile: SHOP, method: "Charge" } },
  );
  await h.start();
  return h;
}

it("checks fraud, publishes payment.charged and returns the transaction", async () => {
  const h = await setup();
  h.respond("fraud-svc", "Check", () => ({ blocked: false }));

  const res = await h.invoke("Charge", { userId: "u-1", amount: 42 });

  expect(res).toEqual({ transactionId: "tx-u-1", ok: true });
  expect(h.calls().map((c) => [c.service, c.method, c.payload])).toEqual([
    ["fraud-svc", "Check", { userId: "u-1" }],
  ]);
  expect(h.published().map((p) => [p.name, p.payload])).toEqual([
    ["payment.charged", { transactionId: "tx-u-1", amount: 42 }],
  ]);
  await h.stop();
});

it("answers with the business code when fraud blocks the user", async () => {
  const h = await setup();
  h.respond("fraud-svc", "Check", () => ({ blocked: true }));

  const err = await h.invoke("Charge", { userId: "u-2", amount: 1 }).catch((e) => e);

  expect(err).toBeInstanceOf(HandlerError);
  expect((err as HandlerError).handlerCode).toBe("BLOCKED");
  expect(h.published()).toHaveLength(0);
  await h.stop();
});
```

## Event handler

```ts
const h = createTestHarness();
h.sb.event.handle("payment.*", async (payload, ctx) => {
  await sendReceipt(payload, ctx.eventId); // must be idempotent — delivery is at-least-once
}, { schema: { protoFile: SHOP, input: "PaymentCharged", output: "PaymentCharged" } });
await h.start();

const r = await h.deliver("payment.charged", { transactionId: "tx-1", amount: 1 });
// { acked: true, reason: "", matchedPatterns: ["payment.*"] }
```

Retry behaviour is simulated by calling `deliver()` again (optionally with `{ attempt: 2 }`) and asserting each result.

## Scope — what this harness does not do

| Not covered | Why |
|---|---|
| Access policy, subscription filters | Runtime behaviour; cover it with e2e against a real runtime. |
| Delivery retries, leases, DLQ | Also runtime; `deliver` returns the Ack/Nack the subscriber would send. |
| Jobs and workflows | Scheduling, leases and step checkpoints live in the runtime. |
| Network, mTLS, reconnect | The harness runs entirely in the test process's memory. |

The full guide is `node/userDocs/testing.md` and the module contract is `node/src/testing/README.md` in the [SDK repository](https://github.com/service-bridge/sdk).
