# RPC — request/response

Direct, typed request/response between services. The runtime resolves routing by service name; you never hardcode host/port.

## Handle incoming calls

```ts
sb.rpc.handle<Req, Res>(
  name: string,
  fn: (req: Req) => Promise<Res> | Res,
  opts: { schema: SchemaSpec; captureMode?: "all" | "errors" | "none" },
): void
```

- `schema` is **required**. See [Schemas](#schemas).
- Register before `await sb.start()`.
- Throw from the handler to signal failure to the caller; the runtime maps it to an error response.

```ts
sb.rpc.handle(
  "Charge",
  async (req: { userId: string; amount: number }) => {
    if (req.amount <= 0) throw new Error("amount must be positive");
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

When using `rpc.call` for a method whose schema the SDK doesn't yet know, do **both** before `start()`: declare the dependency with `sb.service(serviceName, { rpc: ["Method"] })` and register the schema with `sb.useSchema(serviceName, methodName, spec)`. They are separate: `service()` only tells the runtime you call the method, and without `useSchema` the call throws at dispatch time because there is nothing to encode with. `sb.client(service, protoFile)` does both in one step and is the better default.

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
  timeout?: string;                          // "10s", "500ms" — default "30s"
  requestId?: string;                        // auto UUID v4 if omitted
  transport?: "direct" | "proxy" | "auto";   // default "auto" (direct when endpoint known, else proxy)
  idempotencyKey?: string;                   // opt-in runtime-side dedup; empty = no dedup
  retry?: Partial<RetryOpts>;                // defaults below
}

interface RetryOpts {
  maxAttempts: number;   // default 3
  baseDelayMs: number;   // default 200
  factor: number;        // default 2
  maxDelayMs: number;    // default 5000
  jitter: number;        // [0,1], default 0.3
}
```

Per-call opts override `callDefaults` from the constructor.

## Streaming

Server-streaming: handler returns an async iterable; caller consumes one.

```ts
// provider
sb.rpc.handleStream<Req, Chunk>(
  name: string,
  fn: (req: Req) => AsyncIterable<Chunk>,
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

## Schemas

Every RPC handler and every declared call needs a schema. Two forms:

```ts
type SchemaSpec =
  | { protoFile: string; input?: string; output?: string; method?: string }
  | { schemaFile: string; method?: string };   // .schema.json with explicit fieldNumber per property
```

- With `protoFile` and no `input`/`output`, the SDK finds the rpc in the `.proto` `service` block whose name matches the method and uses its request/response messages. Provide `input`/`output` explicitly when the message names differ from the auto-resolution or there's ambiguity.
- Paths are relative to `process.cwd()` unless absolute.

## Errors

- `RpcAccessDeniedError` (`serviceName`, `methodName`, `reason`) — thrown from `rpc.call` when the runtime's bilateral access policy denies the call. Not retryable.
- Handler exceptions surface to the caller as an error response.
- Connection/auth failures surface via the `disconnected` event with a `ConnectionError` (see [configuration.md](configuration.md)).

```ts
import { RpcAccessDeniedError } from "service-bridge";
try {
  await sb.rpc.call("payment-svc", "Charge", payload);
} catch (e) {
  if (e instanceof RpcAccessDeniedError) { /* policy denial — fix the access policy */ }
  else throw e;
}
```
