---
name: servicebridge-node
description: Build backend services with the ServiceBridge Node SDK (service-bridge on npm) — RPC, durable events, workflows, scheduled jobs, and Express/Fastify/Hono integration against a self-hosted ServiceBridge runtime. Use when writing TypeScript/JavaScript that calls or handles RPC, publishes or consumes events, defines workflows or jobs, or wires an HTTP framework into the service mesh.
---

# ServiceBridge Node SDK

ServiceBridge is a self-hosted runtime ("one container + PostgreSQL") that replaces a service mesh, a message broker, and a workflow engine. Your service declares its handlers and dependencies, then `start()`s — the runtime owns transport, delivery, orchestration, policy, and observability. No sidecars.

This SDK is the **backend** client for that runtime. The npm package is **`service-bridge`** (with a hyphen). Everything in this skill is the real, current API — do not invent methods or options.

## The one mental model that prevents most mistakes

A `ServiceBridge` instance has two phases. Get this wrong and nothing connects.

1. **Before `start()` — declare.** Register incoming handlers (`rpc.handle`, `event.handle`, `workflow.handle`, `job.handle`), declare outgoing dependencies (`service()`, `client()`, `useSchema()`), attach HTTP frameworks. These ride along in the first registration to the runtime.
2. **`await sb.start()`** — connect, authenticate (mTLS from the bootstrap key), register everything atomically.
3. **After `start()` — act.** Make outgoing calls: `rpc.call`, `event.publish`, `workflow.start`. These need a live connection.

```ts
import { ServiceBridge } from "service-bridge";

// 1. construct — url + bootstrap key, nothing else is read from env
const sb = new ServiceBridge("localhost:14445", process.env.PAYMENT_KEY!);

// 2. declare (before start)
sb.rpc.handle(
  "Charge",
  async (req: { userId: string; amount: number }) => ({ ok: req.amount > 0 }),
  { schema: { protoFile: "./payment.proto", input: "ChargeRequest", output: "ChargeReply" } },
);

// 3. connect
await sb.start();

// 4. act (after start)
// const res = await sb.rpc.call("other-svc", "Method", { ... });

// teardown
// await sb.stop();
```

## Golden rules

- **Package name is `service-bridge`.** `npm i service-bridge` / `import { ServiceBridge } from "service-bridge"`. Never `servicebridge`.
- **The SDK reads NO env vars.** You pass `url`, `key`, and the advertise host explicitly (e.g. `process.env.PAYMENT_KEY!` in your own code). There is no env-var fallback inside the SDK.
- **Get the bootstrap key from the dashboard.** Open the runtime dashboard (`http://localhost:14444`) → **Services → Create service** → copy the `sb.…` string. That opaque value is the second constructor arg.
- **Every RPC handler needs a `schema`.** `rpc.handle(name, fn, { schema })` — schema is required. Without it, registration fails.
- **`event.handle` matches the EXACT event name**, not a wildcard. Wildcard routing is configured server-side, not in the handler string.
- **Event and job handlers must be idempotent.** Delivery is at-least-once. For jobs, dedup on `ctx.idempotencyKey`, never on `ctx.attempt`.
- **Declare before `start()`, call after `start()`.** Outgoing `rpc.call`/`event.publish`/`workflow.start` before `start()` throw "not ready".
- **Teardown is `await sb.stop()`.** There is no `close()`.
- **Set `advertise` in production.** If a service handles RPC, pass `{ advertise: { host, port } }` (e.g. the pod IP). Omitting it falls back to `127.0.0.1` (local-only) with a warning. A pure caller can pass `advertise: false`.

## Install & connect

```sh
npm i service-bridge      # or: bun add service-bridge
```

The runtime must be running. One-line install of the runtime:

```sh
bash <(curl -fsSL https://servicebridge.dev/install.sh)
```

Dashboard at `http://localhost:14444` (create your admin account on first open, then create services to get keys); SDK connects to the gRPC control plane at `localhost:14445`.

## What each domain is for

| You want to… | Use | Reference |
|---|---|---|
| Request/response between services (incl. streaming) | `sb.rpc` | [reference/rpc.md](reference/rpc.md) |
| Fire-and-forget / pub-sub with durable at-least-once delivery | `sb.event` | [reference/events.md](reference/events.md) |
| Multi-step orchestration (DAG, compensation, signals, replay) | `sb.workflow` | [reference/workflows.md](reference/workflows.md) |
| Cron / delayed / interval scheduled work | `sb.job` | [reference/jobs.md](reference/jobs.md) |
| Expose your Express/Fastify/Hono app to Service Map + discovery | `service-bridge/{express,fastify,hono}` | [reference/http-integrations.md](reference/http-integrations.md) |
| Constructor options, error types, capacity tuning | `ServiceBridgeOptions` | [reference/configuration.md](reference/configuration.md) |

Read the matching reference file before writing code for a domain — each has exact signatures, defaults, and a runnable recipe. When a task spans domains, the lifecycle rule above still holds: one `ServiceBridge` per process, declare everything, then `start()`.

## Minimal two-service RPC (the canonical smoke test)

```ts
// provider.ts
import { ServiceBridge } from "service-bridge";
const sb = new ServiceBridge("localhost:14445", process.env.PROVIDER_KEY!);
sb.rpc.handle(
  "Charge",
  async (req: { userId: string; amount: number }) => ({ transactionId: `tx-${req.userId}`, ok: req.amount > 0 }),
  { schema: { protoFile: "./payment.proto", input: "ChargeRequest", output: "ChargeReply" } },
);
await sb.start();
```

```ts
// caller.ts
import { ServiceBridge } from "service-bridge";
const sb = new ServiceBridge("localhost:14445", process.env.CALLER_KEY!);
const payment = await sb.client("payment-svc", "./payment.proto"); // payment-svc = provider's service name
await sb.start();
const res = await payment.Charge({ userId: "u-1", amount: 100 }); // { transactionId: "tx-u-1", ok: true }
```

```proto
// payment.proto — the shared contract
syntax = "proto3";
package demo;
message ChargeRequest { string user_id = 1; double amount = 2; }
message ChargeReply   { string transaction_id = 1; bool ok = 2; }
service Payment { rpc Charge(ChargeRequest) returns (ChargeReply); }
```

The caller targets the provider by its **service name** (the name used when the service was created in the dashboard), not by host/port — the runtime resolves routing.
