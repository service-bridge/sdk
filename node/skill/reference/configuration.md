# Configuration, lifecycle & errors

## Constructor

```ts
new ServiceBridge(url: string, key: string, options?: ServiceBridgeOptions)
```

- `url` — runtime gRPC control plane, e.g. `"localhost:14445"`.
- `key` — the `sb.…` bootstrap key (dashboard → Services → Create service).
- The SDK reads **no env vars** for `url`/`key`. Pass them yourself: `new ServiceBridge(process.env.SERVICEBRIDGE_URL!, process.env.MY_SERVICE_KEY!)`.

## ServiceBridgeOptions

```ts
interface ServiceBridgeOptions {
  advertise?: { host: string; port: number } | false;  // see below
  callDefaults?: CallOpts;            // base opts for every rpc.call; default {}
  failOnPolicyViolation?: boolean;    // default false (warn only)
  dataDir?: string;                   // event outbox dir; default "./.servicebridge"
  maxOutboxRows?: number;             // default 100000 (publish backpressures at cap)
  eventsDrainerBatch?: number;        // default 50
  eventsMaxInFlight?: number;         // in-flight window advertised on subscribe; default 32
  rpcMaxConcurrentCalls?: number;     // inbound handlers running at once; default 256
  rpcMaxQueuedCalls?: number;         // admission queue depth; default = concurrency
  reconnectAttempts?: number;         // default 3 (0 = unlimited)
  reconnectIntervalMs?: number;       // fixed delay; unset = ladder [1s,5s,15s,30s,60s] ±20%
}
```

Telemetry has no constructor options. Whether it runs, the per-channel payload capture mode and the payload cap are pushed by the runtime and edited in its dashboard; read the current verdict with `sb.telemetry.enabled()` and `sb.telemetry.captureModeForChannel(channel)`.

### advertise

Controls the inbound Call RPC server (needed if this service **handles** RPC or workflows):

- `{ host, port }` — explicit; use in production (e.g. `{ host: process.env.POD_IP!, port: 7777 }`, `port: 0` lets the OS pick).
- omitted — binds `127.0.0.1` on a free port and logs a warning (local dev only; not reachable cross-host).
- `false` — caller-only; bind no inbound server. Use when the instance only makes outgoing calls.

## Lifecycle

```ts
await sb.start();   // connect, authenticate, register everything declared so far
await sb.stop();    // graceful shutdown (there is no close())
```

Declare handlers/dependencies/clients/HTTP plugins **before** `start()`. Make outgoing calls (`rpc.call`, `event.publish`, `workflow.start`) **after** `start()`.

## Declaring dependencies

```ts
sb.service(serviceName: string, deps: { rpc?: string[]; workflows?: string[]; http?: string[] }): void
```

Declares what this service calls, so the runtime can wire the graph and enforce policy. `client()` does this for you for RPC; use `service()` for explicit/low-level `rpc.call`.

## Introspection & events

```ts
sb.identity(): { sessionId; serviceId; serviceName; instanceId } | null   // null until connected
sb.serviceMap(): ReadonlyMap<string, ServiceMapEntry>                     // live discovery snapshot
sb.on("connected" | "reconnecting" | "disconnected" | "policy_violation", handler): this
```

```ts
sb.on("disconnected", (e) => {
  console.error("disconnected:", e.reason, e.error?.code); // e.error is a ConnectionError (gRPC code)
});
```

## Error types

```ts
import {
  ServiceBridgeError,        // base of every error below — catch this to catch them all
  ConnectionError,           // connect/provision failure; .code = gRPC status
  RpcAccessDeniedError,      // rpc.call denied by policy (serviceName, methodName, reason)
  NoLiveInstanceError,       // no callee instance matches the caller's contract
  WorkflowAccessDeniedError, // workflow.start denied by policy
  WorkflowNotFoundError,     // workflow.start on unknown name
  WorkflowTerminalError,     // signal/cancel on a terminal run
  WorkflowValidationError,   // workflow.handle on an invalid graph
  JsonPathError,             // bad $. expression during a run
  InvalidEventNameError,     // bad event name
  OutboxFullError,           // event outbox at cap
} from "service-bridge";
```

`catch (e) { if (e instanceof ServiceBridgeError) … }` catches any of them; each subclass carries its own fields.

Connection/auth problems arrive via the `disconnected` event carrying a `ConnectionError`. Codes `UNAUTHENTICATED (16)`, `PERMISSION_DENIED (7)`, `NOT_FOUND (5)`, `INVALID_ARGUMENT (3)` are fatal (no reconnect); others trigger reconnect per `reconnectAttempts`/`reconnectIntervalMs`.

## Production checklist

- Set `advertise: { host: <reachable host>, port }` on any service that handles RPC/workflows.
- Pass `url`/`key` from your own config/secrets; key from the dashboard.
- Consider `reconnectAttempts: 0` (unlimited) for long-lived services.
- Make event and job handlers idempotent.
- Tune `maxOutboxRows` for high event throughput and `rpcMaxConcurrentCalls` for inbound RPC load; payload capture is set in the runtime dashboard, not here.
