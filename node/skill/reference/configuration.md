# Configuration, lifecycle & errors

## Constructor

```ts
new ServiceBridge(url: string, key: string, options?: ServiceBridgeOptions)
```

- `url` — runtime gRPC control plane as `host:port`, e.g. `"localhost:14445"` (no scheme).
- `key` — the `sb.…` bootstrap key (dashboard → Services → Create service).
- The SDK reads **no env vars** for `url`/`key`. Pass them yourself: `new ServiceBridge(process.env.SERVICEBRIDGE_URL!, process.env.MY_SERVICE_KEY!)`.
- A malformed `url`, key or numeric option throws `ConfigurationError` right in the constructor.

## ServiceBridgeOptions

```ts
interface ServiceBridgeOptions {
  advertise?: { host: string; port: number } | false;  // see below
  callDefaults?: CallOpts;            // base opts for rpc.call, sb.stream and typed clients; default {}
  failOnPolicyViolation?: boolean;    // default false (warn only)
  publishTimeoutMs?: number;          // how long publish() waits for the runtime ACK; default 30000
  maxPendingPublishes?: number;       // publishes waiting for the runtime before QUEUE_FULL; default 10000
  eventsMaxInFlight?: number;         // concurrently handled event deliveries; default 32
  rpcMaxConcurrentCalls?: number;     // inbound handlers running at once; default 256
  rpcMaxQueuedCalls?: number;         // inbound queue before OVERLOADED; default = rpcMaxConcurrentCalls
  reconnectAttempts?: number;         // consecutive failures before giving up; default 0 = never give up
  reconnectIntervalMs?: number;       // fixed delay; unset = ladder [1s,5s,15s,30s,60s] ±20%
  startTimeoutMs?: number;            // start() deadline (Welcome + first registry snapshot); default 30000
  stopTimeoutMs?: number;             // stop() drain deadline; default 10000
  logger?: Logger;                    // SDK diagnostics; default warn/error to the console
  telemetry?: {
    onDrop?: (info: { serverDrops: number; ringDrops: number; backpressureLevel: number }) => void;
  };
}

interface Logger {
  debug(message: string, attrs?: Record<string, unknown>): void;
  info(message: string, attrs?: Record<string, unknown>): void;
  warn(message: string, attrs?: Record<string, unknown>): void;
  error(message: string, attrs?: Record<string, unknown>): void;
}
```

Telemetry on/off, the per-channel payload capture mode (default `errors`) and the payload cap are pushed by the runtime and edited in its dashboard; read the current verdict with `sb.telemetry.enabled()` and `sb.telemetry.captureModeForChannel(channel)`. The SDK sends payloads as they are; the runtime masks secrets on ingest. `telemetry.onDrop` reports telemetry lost in the local ring or by the runtime (deltas since the previous call).

`logger` receives the SDK's own diagnostics (reconnects, policy warnings, publish failures). `sb.logger` is different: structured application logs shipped to the runtime.

### advertise

Controls the inbound Call RPC server (needed if this service **handles** RPC or workflows):

- `{ host, port }` — explicit; use in production (e.g. `{ host: process.env.POD_IP!, port: 7777 }`, `port: 0` lets the OS pick).
- omitted — binds `127.0.0.1` on a free port and logs a warning (local dev only; not reachable cross-host).
- `false` — caller-only; bind no inbound server. Such an instance cannot be called.

## Lifecycle

```ts
await sb.start();   // connect, authenticate, register; resolves after Welcome + first registry snapshot
await sb.ready();   // resolves when the current session is live (immediately if it already is)
await sb.stop();    // graceful shutdown (there is no close())
```

- Declare handlers/dependencies/clients/HTTP plugins **before** `start()`. Make outgoing calls (`rpc.call`, `event.publish`, `workflow.start`) **after** `start()` — earlier they throw `StateError`.
- `start()` failure stops the bridge and throws (`TimeoutError`, `ConnectionError`, `ConfigurationError`, `ValidationError`, `AccessDeniedError`). A second `start()` throws `StateError`; create a new `ServiceBridge` instead.
- Lost sessions reconnect on their own, without limit by default. Only unrecoverable answers stop the bridge: `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT` (e.g. an invalid subscription filter), `FAILED_PRECONDITION` (incompatible protocol).
- Certificate renewal swaps TLS material in place: no reconnect, no new `connected` event, `instanceId` stays the same.
- `stop()` runs in order within `stopTimeoutMs`: withdraw the inbound endpoint, refuse new calls and deliveries, wait for in-flight calls, event handlers and jobs, flush queued publishes (leftovers reject with `CONNECTION`), flush telemetry, close everything.

```ts
process.on("SIGTERM", async () => { await sb.stop(); process.exit(0); });
```

## Declaring dependencies

```ts
sb.service(serviceName: string, deps: { rpc?: string[]; workflows?: string[]; http?: string[] }): void
```

Declares what this service calls, so the runtime can wire the graph and enforce policy. `client()` does this for you for RPC; use `service()` for explicit/low-level `rpc.call`.

## Introspection & events

```ts
sb.identity(): { sessionId; serviceId; serviceName; instanceId } | null   // null until connected
sb.serviceMap(): ReadonlyMap<string, ServiceMapEntry>                     // live discovery snapshot
sb.on("connected" | "reconnecting" | "draining" | "disconnected" | "policy_violation", handler): this
```

- `connected` — `{ sessionId, serviceId, serviceName, runtimeVersion }` on every new session.
- `reconnecting` — `{ attempt, delayMs, reason }`.
- `draining` — `{ reason }`: the runtime announced a shutdown; the bridge reconnects afterwards by itself.
- `disconnected` — `{ reason, error }`: the bridge stopped for good. `sb.stop()` does not emit it.
- `policy_violation` — `{ declaration, value, denySide, reason }`.

A throwing listener is logged and does not affect the bridge.

```ts
sb.on("disconnected", (e) => {
  console.error("disconnected:", e.reason, e.error?.code);
  process.exit(1);
});
```

## Errors

Every error the SDK throws is a `ServiceBridgeError` with `code: ErrorCode` and `retryable: boolean`. `retryable` is true only for `CONNECTION`, `NO_LIVE_INSTANCE`, `OVERLOADED`, `QUEUE_FULL` — a repeat may succeed and the previous attempt had no effect. `TIMEOUT` is not retryable: the outcome is unknown; repeat only with an idempotency key.

```ts
import {
  ServiceBridgeError,        // base: .code, .retryable
  ConfigurationError,        // CONFIG — bad option, key, URL, missing caller schema
  StateError,                // STATE — wrong lifecycle phase (call before start, ...)
  ValidationError,           // VALIDATION — invalid declaration or payload
  AccessDeniedError,         // ACCESS_DENIED — access policy denial or revoked peer
  TimeoutError,              // TIMEOUT — deadline passed, outcome unknown
  NoLiveInstanceError,       // NO_LIVE_INSTANCE — nothing can serve the call
  HandlerError,              // HANDLER — the callee handler's answer; .handlerCode
  ConnectionError,           // CONNECTION — control-plane failure; .grpcCode
  InvalidEventNameError,     // INVALID_EVENT_NAME
  WorkflowAccessDeniedError, // workflow.start denied by policy
  WorkflowNotFoundError,     // workflow.start on unknown name
  WorkflowTerminalError,     // signal/cancel on a terminal run
  WorkflowValidationError,   // workflow.handle on an invalid graph
  JsonPathError,             // bad $. expression
} from "service-bridge";
```

Codes without their own class (`CANCELLED`, `NOT_FOUND`, `CONFLICT`, `OVERLOADED`, `QUEUE_FULL`, `INTERNAL`, `TERMINAL`) arrive as a plain `ServiceBridgeError` — switch on `err.code`.

```ts
try {
  await sb.rpc.call("payment-svc", "Charge", payload);
} catch (e) {
  if (e instanceof HandlerError && e.handlerCode === "INSUFFICIENT_FUNDS") return declined();
  if (e instanceof ServiceBridgeError && e.retryable) return retryLater();
  throw e;
}
```

## Production checklist

- Set `advertise: { host: <reachable host>, port }` on any service that handles RPC/workflows.
- Pass `url`/`key` from your own config/secrets; key from the dashboard.
- Route SDK diagnostics into your logger with `logger`.
- Make event and job handlers idempotent.
- Size `maxPendingPublishes`/`publishTimeoutMs` for how long publishers must ride out a runtime outage, and `rpcMaxConcurrentCalls` for inbound RPC load; payload capture is set in the runtime dashboard, not here.
