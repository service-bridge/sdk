# RPC — request/response

Direct, typed request/response between services. The runtime resolves routing by service name; you never hardcode host/port.

## Handle incoming calls

```ts
sb.rpc.handle<Req, Res>(
  name: string,
  fn: (req: Req, ctx: RpcHandlerContext) => Promise<Res> | Res,
  opts: { schema: SchemaSpec; captureMode?: "all" | "errors" | "none" },
): void

interface RpcHandlerContext {
  signal: AbortSignal;       // aborts when the caller cancels or the deadline passes
  deadline: number | null;   // absolute deadline, unix ms
  requestId: string;
  idempotencyKey: string;    // "" when the caller set none
  caller: { serviceId: string; instanceId: string } | null;  // verified peer; instanceId "" via the runtime proxy
}
```

- `schema` is **required**. See [Schemas](#schemas). A second handler for the same name throws `ValidationError`.
- Register before `await sb.start()`.
- Business failure: `throw new HandlerError(code, message)` — the caller receives a `HandlerError` with the same `handlerCode`. Any other thrown error reaches the caller as `HandlerError` with `handlerCode: "INTERNAL"` (including a `HandlerError` rethrown from a nested call).
- Pass `ctx.signal` to downstream I/O so a cancelled call stops work.

```ts
import { HandlerError } from "service-bridge";

sb.rpc.handle(
  "Charge",
  async (req: { userId: string; amount: number }, ctx) => {
    if (req.amount <= 0) throw new HandlerError("INVALID_AMOUNT", "amount must be positive");
    await ledger.reserve(req.userId, req.amount, { signal: ctx.signal });
    return { transactionId: `tx-${req.userId}`, ok: true };
  },
  { schema: { protoFile: "./payment.proto", input: "ChargeRequest", output: "ChargeReply" } },
);
```

## Call another service

Two ways. Prefer the typed client for ergonomics; use `rpc.call` for dynamic/low-level calls.

### Typed client (recommended)

```ts
const payment = await sb.client(
  serviceName: string,
  protoFile: string,
  opts?: { methods?: string[]; callDefaults?: CallOpts },
); // returns a proxy with one method per rpc in the .proto service block
```

```ts
const payment = await sb.client("payment-svc", "./payment.proto");
await sb.start();
const res = await payment.Charge({ userId: "u-1", amount: 100 });
```

`client()` reads the `.proto` once, declares every method in its `service` block as an outgoing dependency, and loads schemas. Call `client()` **before** `start()` so the dependency rides along in the first registration. Calls succeed once `start()` has connected.

### Low-level call

```ts
await sb.rpc.call<Req, Res>(
  serviceName: string,
  methodName: string,
  payload: Req,
  opts?: CallOpts,
): Promise<Res>
```

When using `rpc.call` for a method whose schema the SDK doesn't yet know, do **both** before `start()`: declare the dependency with `sb.service(serviceName, { rpc: ["Method"] })` and register the schema with `sb.useSchema(serviceName, methodName, spec)`. They are separate: `service()` only tells the runtime you call the method, and without `useSchema` the call throws `ConfigurationError` ("rpc: no schema for ...") because there is nothing to encode with. `sb.client(service, protoFile)` does both in one step and is the better default.

```ts
sb.service("payment-svc", { rpc: ["Charge"] });
await sb.useSchema("payment-svc", "Charge", {
  protoFile: "./payment.proto", input: "ChargeRequest", output: "ChargeReply",
});
await sb.start();
const res = await sb.rpc.call("payment-svc", "Charge", { userId: "u-1", amount: 100 }, { timeout: "15s" });
```

### CallOpts

```ts
interface CallOpts {
  signal?: AbortSignal;                      // cancel; rejects with code CANCELLED
  timeout?: string;                          // "10s", "500ms", "2m" — default "30s", covers all attempts
  requestId?: string;                        // auto UUID if omitted; reaches ctx.requestId
  transport?: "direct" | "proxy" | "auto";   // default "auto"
  idempotencyKey?: string;                   // reaches ctx.idempotencyKey and the runtime proxy's dedup
  retry?: Partial<RetryOpts>;                // pre-dispatch failures only; defaults below
}

interface RetryOpts {
  maxAttempts: number;   // default 3
  baseDelayMs: number;   // default 200
  factor: number;        // default 2
  maxDelayMs: number;    // default 5000
  jitter: number;        // [0,1], default 0.3
}
```

Per-call opts override `client(..., { callDefaults })`, which overrides `callDefaults` from the constructor.

- **Transport.** `auto`: direct to the picked instance; if that attempt fails before the request was sent, the next attempt goes through the runtime proxy. `direct`: never through the runtime. `proxy`: always through the runtime. The callee needs an `advertise`d endpoint in every mode.
- **Retries.** Only failures proven to happen before the handler ran are retried: no live candidate, channel not ready within the deadline, or a callee/runtime refusal marked "not dispatched" (callee not ready, draining, overloaded). Everything else — including a bare `UNAVAILABLE` and `TIMEOUT` — is returned to you.
- **Idempotency key.** Does **not** make a dispatched call retryable. It lets the callee dedup (`ctx.idempotencyKey`) and, with `transport: "proxy"`, lets the runtime return its stored answer for a repeat. Use it when *your* code repeats a call after `TIMEOUT`.

## Streaming

Server-streaming: handler returns an async iterable; caller consumes one.

```ts
// provider
sb.rpc.handleStream<Req, Chunk>(
  name: string,
  fn: (req: Req, ctx: RpcHandlerContext) => AsyncIterable<Chunk>,
  opts: { schema: SchemaSpec },
): void

// caller — typed client method returns an async iterable, or use sb.stream:
sb.stream<Req, Chunk>(serviceName, methodName, payload, opts?): AsyncIterable<Chunk>
```

```ts
sb.rpc.handleStream("Ticks", async function* (req: { n: number }) {
  for (let i = 0; i < req.n; i++) yield { i };
}, { schema: { protoFile: "./ticks.proto", input: "TicksRequest", output: "Tick" } });

// caller
for await (const chunk of sb.stream("tick-svc", "Ticks", { n: 5 })) {
  console.log(chunk);
}
```

Breaking out of the loop cancels the stream; the handler's `ctx.signal` aborts. Streams are never retried.

## Schemas

Every RPC handler and every declared call needs a schema. Two forms:

```ts
type SchemaSpec =
  | { protoFile: string; input?: string; output?: string; method?: string }
  | { schemaFile: string };   // .schema.json with explicit fieldNumber per property
```

- With `protoFile` and no `input`/`output`, the SDK finds the rpc in the `.proto` `service` block whose name matches the method and uses its request/response messages. Without a matching rpc, pass **both** `input` and `output`.
- Paths are relative to `process.cwd()` unless absolute.

## Errors

Every failure is a `ServiceBridgeError` (`code`, `retryable`):

| Class / `code` | Meaning | retryable |
|---|---|---|
| `HandlerError` / `HANDLER` | The callee handler answered with an error; `handlerCode` is its business code or `"INTERNAL"`. | no |
| `NoLiveInstanceError` / `NO_LIVE_INSTANCE` | No instance can serve: offline, contract hash mismatch, no endpoint, all circuit-open. | yes |
| `AccessDeniedError` / `ACCESS_DENIED` | Access policy denied the call (also emitted as `policy_violation`). | no |
| `TimeoutError` / `TIMEOUT` | Deadline passed; the handler may have run. | no |
| `CONNECTION` | Channel or runtime unavailable. | yes |
| `OVERLOADED` | Callee or runtime shedding load. | yes |
| `CANCELLED` | `opts.signal` aborted. | no |
| `VALIDATION` | Undecodable request, `call` on a streaming method (or the reverse). | no |
| `NOT_FOUND` | The callee has no such method. | no |
| `ConfigurationError` / `CONFIG` | No caller schema, bad `timeout`. | no |

```ts
import { HandlerError, ServiceBridgeError } from "service-bridge";
try {
  await sb.rpc.call("payment-svc", "Charge", payload);
} catch (e) {
  if (e instanceof HandlerError && e.handlerCode === "INVALID_AMOUNT") return badRequest(e.message);
  if (e instanceof ServiceBridgeError && e.code === "ACCESS_DENIED") { /* fix the access policy */ }
  throw e;
}
```

Connection/auth failures at startup make `start()` throw; later unrecoverable ones arrive as the `disconnected` event (see [configuration.md](configuration.md)).
