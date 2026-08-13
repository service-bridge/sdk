<!--
Keywords: service-bridge, ServiceBridge, microservices, Node.js SDK, TypeScript SDK, Bun,
gRPC, mTLS, RPC framework, durable events, pub/sub, message broker alternative, RabbitMQ alternative,
workflow engine, saga, orchestration, Temporal alternative, job scheduler, cron, distributed tracing,
observability, OpenTelemetry alternative, Jaeger alternative, service mesh alternative, Istio alternative,
self-hosted, PostgreSQL, Express, Fastify, Hono, circuit breaker, idempotency, retries, load balancing.
-->

# service-bridge

[![npm version](https://img.shields.io/npm/v/service-bridge?color=cb3837&label=npm)](https://www.npmjs.com/package/service-bridge)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![TypeScript](https://img.shields.io/badge/types-included-3178c6.svg)](https://www.typescriptlang.org/)
[![Node](https://img.shields.io/badge/node-%E2%89%A518-339933.svg)](https://nodejs.org/)

**The Node.js / Bun SDK for [ServiceBridge](https://servicebridge.dev) — RPC, durable events, workflows, jobs, streaming and full observability over one self-hosted runtime. No broker. No sidecar. No tracing stack. Just one Go binary plus PostgreSQL.**

You declare what your service handles and what it calls. ServiceBridge does the rest: provisions an mTLS identity, opens the connection, registers your handlers, and routes every RPC, event, job and workflow step — with tracing, metrics and access policy built in.

```
        BEFORE                                       AFTER

  ┌─────────────────────┐
  │  Istio + Envoy      │  ← mesh / mTLS
  │  RabbitMQ / Kafka   │  ← events                 ┌──────────────────────┐
  │  Temporal           │  ← workflows              │                      │
  │  a cron scheduler   │  ← jobs                   │   ServiceBridge      │
  │  gRPC plumbing      │  ← RPC          ═══►       │   runtime (1 binary) │
  │  Jaeger / Tempo     │  ← tracing                │          +           │
  │  Prometheus wiring  │  ← metrics                │      PostgreSQL      │
  │  Loki               │  ← logs                   │                      │
  │  a load balancer    │  ← LB / retries           └──────────────────────┘
  │  service registry   │  ← discovery
  └─────────────────────┘
     10+ moving parts                                  2 things to run
```

---

## Table of contents

- [Install](#install)
- [AI coding skill](#ai-coding-skill)
- [CLI](#cli)
- [Why ServiceBridge](#why-servicebridge)
- [Use cases](#use-cases)
- [Quick start](#quick-start)
- [Runtime setup](#runtime-setup)
- [End-to-end example](#end-to-end-example)
- [Platform features](#platform-features)
- [How it compares](#how-it-compares)
- [API reference](#api-reference)
  - [RPC](#rpc)
  - [Events](#events)
  - [Jobs](#jobs)
  - [Workflows](#workflows)
  - [Streaming](#streaming)
  - [Telemetry](#telemetry)
  - [HTTP](#http)
- [HTTP plugins](#http-plugins)
- [Configuration](#configuration)
- [Error handling](#error-handling)
- [FAQ](#faq)
- [Community](#community)
- [License](#license)

---

## Install

```sh
npm i service-bridge
# or
bun add service-bridge
```

- **Runtime:** Node.js 18+ or any current Bun.
- **Types:** included, written in TypeScript 5.
- **Backend:** a running ServiceBridge runtime (gRPC control plane on `:14445`) backed by PostgreSQL 18+. See [Runtime setup](#runtime-setup).

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(
  "localhost:14445", // runtime control-plane address
  "sb_key_...",      // bootstrap service key from the runtime
);
```

The third constructor argument is an [options](#configuration) object. The SDK reads **no environment variables** — every knob is a constructor option, so you stay in control of where config comes from.

---

## AI coding skill

This package ships an official skill so your AI coding agent (Claude Code, etc.) writes correct ServiceBridge code on the first try — RPC, events, workflows, jobs and Express/Fastify/Hono integration, grounded in this exact SDK rather than guessed.

It comes with the install. Copy it into your agent's skills directory — `.claude/skills/` for Claude Code, or `~/.claude/skills/` for all projects:

```sh
cp -r node_modules/service-bridge/skill .claude/skills/servicebridge-node
```

Not installed yet? Pull it straight from the repo with degit:

```sh
npx degit service-bridge/sdk/node/skill .claude/skills/servicebridge-node
```

Restart the agent so it loads the skill. Source and contents: [`node/skill/`](https://github.com/service-bridge/sdk/tree/main/node/skill).

---

## CLI

The runtime ships with `sb`, a command-line client for managing services, traces, events, jobs, workflows, alerts and settings. It comes with the runtime image — there is no separate install — so a running runtime already has it at `/usr/local/bin/sb`. Use `docker exec <container> sb …` or the host binary, and pass `-o json` to get machine-readable output for AI agents. Check it with:

```sh
sb version
```

Full command reference: **[servicebridge.dev/#docs/cli](https://servicebridge.dev/#docs/cli)**.

---

## Why ServiceBridge

Microservices rarely fail because of business logic. They fail in the gaps *between* services — the broker that dropped a message, the workflow engine nobody fully understands, the trace that stops at a service boundary, the mesh config that takes a week to debug. Each gap is another system to run, secure and correlate.

ServiceBridge collapses those gaps into one runtime. Your service talks to a single gRPC endpoint over mTLS; the runtime is the single source of truth for routing, delivery and state.

| Problem | Without ServiceBridge | With ServiceBridge |
|---|---|---|
| Service-to-service calls | gRPC/HTTP plumbing + a mesh for mTLS + retries | `sb.rpc.call("svc", "Method", req)` — mTLS, LB, retries, breakers built in |
| Reliable async messaging | Stand up and operate a broker | `sb.event.publish(...)` — durable outbox, at-least-once, fan-out, DLQ |
| Multi-step business processes | A separate workflow engine to learn and host | `sb.workflow.handle(...)` — durable DAGs with compensation and replay |
| Scheduled work | A cron box or a job scheduler service | `sb.job.handle(...)` — cron / interval / delay, leased and retried |
| Knowing what happened | Wire up tracing + metrics + logs across N tools | Every hop is traced, measured and logged automatically |
| Identity & access | Certificates, a mesh policy layer | mTLS from a service key + granular access policy, on by default |

One binary, one database, one place to look when something breaks.

---

## Use cases

- **Replace a broker** — durable, at-least-once events with fan-out and a dead-letter queue, without operating Kafka or RabbitMQ.
- **Run sagas / orchestration** — checkout, onboarding, fulfilment as durable workflows with automatic compensation on failure.
- **Internal RPC backbone** — typed service-to-service calls with load balancing, retries and circuit breakers, secured by mTLS.
- **Scheduled & delayed work** — nightly rollups, reminders, periodic syncs as leased, retried jobs.
- **Streaming responses** — token-by-token LLM output or progress feeds over server-side streaming RPC.
- **Observability for free** — get a full distributed trace across RPC → event → workflow → job without instrumenting by hand.

---

## Quick start

Schemas are **file-based**: point the SDK at a `.proto` file (it resolves request/response types from the `service` block) or a `.schema.json` with explicit field numbers. There is no inline schema.

```proto
// payment.proto
syntax = "proto3";
message ChargeRequest { string user_id = 1; int64 amount = 2; }
message ChargeReply   { bool ok = 1; }
service Payment {
  rpc Charge(ChargeRequest) returns (ChargeReply);
}
```

**Worker** — register the handler. One argument in, one value out.

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge("localhost:14445", process.env.PAYMENT_KEY!);

sb.rpc.handle(
  "Charge",
  async (req: { userId: string; amount: number }) => {
    return { ok: req.amount > 0 };
  },
  { schema: { protoFile: "./payment.proto" } },
);

await sb.start();
```

**Caller** — in another process, build a typed client and call it. `sb.client()` reads the `.proto` once, declares every method in its `service` block as an outgoing dependency, loads the schemas, and returns a typed proxy.

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge("localhost:14445", process.env.ORDERS_KEY!);
const payment = await sb.client("payment-svc", "./payment.proto");

await sb.start();

const res = await payment.Charge({ userId: "u-1", amount: 100 });
// res.ok === true
```

Declare dependencies and build typed clients **before** `start()` — they ride along in the first registration. Calls succeed once `start()` has connected.

---

## Runtime setup

The SDK needs a running ServiceBridge runtime. Spin one up with the one-line installer:

```sh
bash <(curl -fsSL https://servicebridge.dev/install.sh)
```

It pulls the runtime container, wires it to PostgreSQL 18+, and exposes the gRPC control plane on `:14445` and the dashboard on `:14444`. Open the dashboard, create a service, and copy its **bootstrap service key** — that opaque string is the second argument to `new ServiceBridge(url, key)`.

Each instance authenticates with its key: the SDK calls `Bootstrap.Provision`, receives a short-lived leaf certificate, opens an mTLS gRPC channel and registers. Certificates rotate automatically with overlap (the new session is live before the old one closes), so long-running instances never drop traffic at renewal.

Full self-hosting docs live at **[servicebridge.dev/docs](https://servicebridge.dev/docs)**.

---

## End-to-end example

A small order flow: an HTTP request triggers a workflow that charges a payment, then publishes an event another service consumes — all traced as one tree.

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge("localhost:14445", process.env.ORDERS_KEY!);

// Outgoing dependencies — declared before start().
sb.service("payment-svc", { rpc: ["Charge"] });
sb.event.define("order.placed", { protoFile: "./events.proto", input: "OrderPlaced" });

// A durable workflow: charge, then announce. Steps run by dependency level.
sb.workflow.handle("checkout", {
  input: { type: "object", properties: { orderId: { type: "string" } } },
  steps: [
    { id: "charge", type: "call", service: "payment-svc", method: "Charge",
      input: "$.input" },
    { id: "announce", type: "publish", event: "order.placed",
      input: "$.input", waitFor: ["charge"] },
  ],
});

sb.on("connected", ({ serviceName }) => console.log(`up as ${serviceName}`));

await sb.start();

// Kick off a run and wait for the final state.
const { runId } = await sb.workflow.start("checkout", { orderId: "o-1" });
const state = await sb.workflow.await(runId);
console.log("done", state);
```

The consuming service just subscribes:

```ts
sb.event.handle("order.placed", async (payload) => {
  await sendReceipt(payload);
});
await sb.start();
```

In the dashboard you see one trace spanning the workflow run, the `Charge` RPC, the `order.placed` publish, and its delivery to the subscriber.

---

## Platform features

| Area | What you get |
|---|---|
| **Communication** | Direct RPC, server-side streaming, durable events, service discovery, full-mesh routing, a live service map |
| **Orchestration** | Workflows (DAG steps with compensation), sub-workflows, jobs (cron / interval / delayed), bidirectional replay |
| **Reliability** | At-least-once delivery, retries, DLQ, idempotency, fan-out, session resilience, multi-instance failover, circuit breakers |
| **Traffic control** | Load balancing, rate limiting, per-definition limits, filter expressions, adaptive performance |
| **Security** | TLS by default, mTLS identity, auto-provisioned certs from a service key, granular access policy |
| **Observability** | Unified tracing with propagation, Prometheus-compatible metrics, structured logs, smart alerts |

Designed to run up to 1000 services against a single runtime.

---

## How it compares

| You'd otherwise reach for | ServiceBridge gives you |
|---|---|
| Istio / Linkerd (mesh, mTLS) | mTLS identity + routing + policy, no sidecars |
| RabbitMQ / Kafka / NATS | Durable events with outbox, fan-out, retries, DLQ |
| Temporal / Cadence | Durable workflows with compensation, signals, replay |
| A cron service / Quartz | Leased, retried scheduled jobs |
| Jaeger / Tempo + Prometheus + Loki | Tracing, metrics and logs, correlated out of the box |
| gRPC + a service registry | Typed RPC with discovery, LB and breakers |

The point isn't that ServiceBridge beats each tool at its own game — it's that you stop running and correlating ten of them.

---

## API reference

The bridge exposes four domains (`sb.rpc`, `sb.event`, `sb.job`, `sb.workflow`) plus `sb.stream()` and `sb.telemetry`. Register handlers and declare dependencies **before** `start()`.

### RPC

`sb.rpc` is request/response: register handlers, call other services.

```ts
// Unary handler: (req) => res
sb.rpc.handle<ChargeRequest, ChargeReply>(
  "Charge",
  async (req) => ({ ok: req.amount > 0 }),
  { schema: { protoFile: "./payment.proto" } },
);

// Server-side streaming handler: (req) => AsyncIterable<chunk>
sb.rpc.handleStream<GenRequest, Token>(
  "Generate",
  async function* (req) {
    for (const word of req.prompt.split(" ")) yield { token: word };
  },
  { schema: { protoFile: "./gen.proto" } },
);
```

Calling — the typed proxy from `sb.client()` (preferred), or the lower-level `sb.rpc.call()`:

```ts
const res = await payment.Charge({ userId: "u-1", amount: 100 });

const res2 = await sb.rpc.call("payment-svc", "Charge",
  { userId: "u-1", amount: 100 },
  { timeout: "5s", idempotencyKey: "order-42" },
);
```

`CallOpts` apply per call, layered over `callDefaults` from the constructor:

| `CallOpts` | Type | Default | Description |
|---|---|---|---|
| `timeout` | `string` | `"30s"` | Deadline, e.g. `"500ms"`, `"10s"`, `"2m"`. |
| `requestId` | `string` | random UUID v4 | Correlation id carried to the callee. |
| `transport` | `"direct" \| "proxy" \| "auto"` | `"auto"` | `direct` = caller→callee mTLS; `proxy` = via the runtime; `auto` = direct when an endpoint is known. |
| `idempotencyKey` | `string` | none | Opts into runtime-side dedup; replays within the TTL return the cached response. |
| `retry` | `Partial<RetryOpts>` | exp. backoff | `{ maxAttempts: 3, baseDelayMs: 200, factor: 2, maxDelayMs: 5000, jitter: 0.3 }`. Set `maxAttempts: 1` to disable. |

Without an `idempotencyKey`, ambiguous failures (`INTERNAL` / `ABORTED` / `UNKNOWN`) are treated as non-retryable so a non-idempotent call is never silently repeated. Schema-version mismatches are filtered at routing time, so blue-green deploys route `v1→v1` and `v2→v2` automatically.

### Events

Durable, at-least-once publish/subscribe. Events hit a local SQLite outbox first, then drain to the runtime, so a publish survives a transient disconnect.

```ts
// Declare what you publish (same file-based SchemaSpec as RPC).
sb.event.define("order.placed", { protoFile: "./events.proto", input: "OrderPlaced" });

// Subscribe — exact name or wildcard ("order.*", "order.#").
sb.event.handle("order.placed", async (payload) => {
  await fulfil(payload);
});

await sb.start();

const { eventId } = await sb.event.publish("order.placed", { orderId: "o-1", total: 4200 });
```

Event names must match `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$` (invalid → `InvalidEventNameError`). A full outbox throws `OutboxFullError`.

| `PublishOpts` | Type | Description |
|---|---|---|
| `idempotencyKey` | `string` | Dedup key for at-least-once delivery. |
| `partitionKey` | `string` | Orders delivery within a partition. |
| `fireAndForget` | `boolean` | Skip the durable wait for the publish ack. |
| `headers` | `Record<string, string>` | Custom envelope headers. |
| `occurredAtMs` | `number` | Event time (unix-ms); defaults to now. |

The runtime delivers at-least-once, retries failures, fans out to every matching subscriber, and dead-letters exhausted messages. The DLQ is operated from the dashboard — the SDK has no DLQ API; make handlers idempotent and throw to signal "retry me".

### Jobs

Scheduled work: cron, fixed interval, or one-shot delay. The runtime owns the schedule, leasing and retries.

```ts
sb.job.handle("nightly-rollup",
  { trigger: { cron: "0 3 * * *", tz: "UTC" } },     // 5-field cron, no seconds
  async (ctx) => { await rollup(ctx.scheduledAt); },
);

sb.job.handle("heartbeat", { trigger: { interval: 30_000 } }, async () => { await ping(); });

sb.job.handle("send-reminder",
  { trigger: { delayed: { at: Date.now() + 60_000 } } }, // Date | number | ISO string
  async (ctx) => { await remind(ctx.idempotencyKey); },
);
```

The handler receives a `JobHandlerCtx`: `{ jobName, executionId, scheduledAt, localScheduledAt, attempt, idempotencyKey, signal }`.

| `JobOpts` | Type | Default | Description |
|---|---|---|---|
| `trigger` | `{cron, tz?} \| {delayed:{at}} \| {interval}` | required | Exactly one trigger; `interval` is in ms. |
| `catchup` | `"skip" \| "fire_once" \| "fire_all"` | `skip` | What to do for fire times missed during downtime. |
| `overlap` | `"skip" \| "allow" \| "buffer_one"` | `allow` | Behaviour when a previous run is still in flight. |
| `deps` | `DeclaredDep[]` | none | Outgoing deps: `{ rpc }`, `{ event }`, `{ workflow }`. |
| `maxAttempts` / `leaseTtlMs` / `maxConcurrent` / `retry` | — | runtime default | Execution limits and `{ initialMs, maxMs, multiplier, jitter }` retry. |

### Workflows

Durable DAGs. Declare the graph once; the runtime executes it, persists state between steps, survives restarts, and compensates on failure or cancel.

```ts
sb.workflow.handle("checkout", {
  input: { type: "object", properties: { orderId: { type: "string" } } },
  steps: [
    { id: "reserve", type: "call", service: "inventory-svc", method: "Reserve",
      input: "$.input",
      compensate: { service: "inventory-svc", method: "Release", input: "$.reserve" } },
    { id: "charge", type: "call", service: "payment-svc", method: "Charge",
      input: "$.input", waitFor: ["reserve"] },
    { id: "notify", type: "publish", event: "order.placed",
      input: "$.input", waitFor: ["charge"] },
  ],
});
```

Top-level steps run in parallel by default; `waitFor` declares dependencies and defines the execution levels. Step types: `call`, `publish`, `sleep`, `wait_event`, `wait_signal`, `workflow` (sub-workflow), `parallel`, `sequence`, `local`. Inputs are JSON-path expressions (`"$.input"`, `"$.reserve.id"`) over the accumulated run state.

Driving a run:

```ts
const { runId } = await sb.workflow.start("checkout", { orderId: "o-1" });

const state = await sb.workflow.await(runId);          // block until terminal
const snap  = await sb.workflow.query(runId);          // { status, state, steps: [...] }
await sb.workflow.signal(runId, "approval", { ok: 1 }); // resume a wait_signal step
await sb.workflow.cancel(runId);                        // compensate in reverse
const { runId: forked } = await sb.workflow.replay(runId, { fromStepId: "charge" });
```

Use `sb.workflow.query()` for the snapshot — there is no `getStatus`. `start()` with no permission throws `WorkflowAccessDeniedError`; an unknown name throws `WorkflowNotFoundError`; signalling/cancelling a finished run throws `WorkflowTerminalError`.

### Streaming

Server-side streaming is a first-class RPC shape. Register with `sb.rpc.handleStream`, consume with `sb.stream()` (or the typed proxy, which auto-detects `returns (stream T)` methods).

```ts
for await (const chunk of sb.stream("gen-svc", "Generate", { prompt: "write a haiku" })) {
  process.stdout.write(chunk.token);
}
```

Breaking the loop (`break`/`return`) tears down the gRPC stream end to end. Streams are single-pick — never retried — by design.

### Telemetry

Telemetry flows automatically: every RPC, event, job, workflow step and HTTP request emits an operation span and propagates the trace across hops. Add your own through `sb.telemetry`; anything emitted inside a handler nests under that handler's trace.

```ts
import { Channel, UserSubOp } from "service-bridge";

const op = sb.telemetry.startOp({
  channel: Channel.USER, kind: UserSubOp, subject: "reprice-cart", businessKey: cartId,
});
try {
  await reprice(cartId);
  op.end(/* Status.SUCCESS */);
} catch (err) {
  op.end(/* Status.ERROR */, String(err));
  throw err;
}

sb.telemetry.log.info("cart repriced", { cartId, items: 7 }); // also sb.logger
sb.telemetry.counter("carts_repriced_total").inc();
sb.telemetry.gauge("queue_depth").set(42);
sb.telemetry.histogram("reprice_ms", "ms").observe(12.5);
```

`startOp()` returns a handle whose `.end(status, message?)` closes the span. Anything emitted before `start()` buffers in an in-memory ring and drains once connected.

### HTTP

ServiceBridge does **not** proxy your business HTTP. You run your own server; the integration discovers your routes, publishes them to the Service Map, and wraps each request in a trace span so HTTP stitches into the same trace as the RPCs and events it triggers. See [HTTP plugins](#http-plugins).

Useful read accessors after `start()`: `sb.identity()` (current session identity or `null`), `sb.serviceMap()` (live registry: visible methods, instances, endpoints), `sb.policyEvaluation()` (the runtime's current access-policy verdict).

---

## HTTP plugins

Each integration is a subpath import with an optional peer dependency.

**Express** — `service-bridge/express`:

```ts
import express from "express";
import { ServiceBridge } from "service-bridge";
import { attachExpress } from "service-bridge/express";

const app = express();
app.post("/orders", (req, res) => res.json({ ok: true }));

const sb = new ServiceBridge("localhost:14445", KEY);
await sb.start();

app.listen(3000, () => attachExpress(app, sb, { port: 3000 }));
```

**Fastify** — `service-bridge/fastify`:

```ts
import Fastify from "fastify";
import { ServiceBridge } from "service-bridge";
import { sbFastify } from "service-bridge/fastify";

const app = Fastify();
const sb = new ServiceBridge("localhost:14445", KEY);

app.post("/orders", async () => ({ ok: true }));
await app.register(sbFastify, { sb }); // discovers routes + endpoint in onListen

await sb.start();
await app.listen({ port: 3000 });
```

**Hono** — `service-bridge/hono`:

```ts
import { Hono } from "hono";
import { ServiceBridge } from "service-bridge";
import { attachHono } from "service-bridge/hono";

const app = new Hono();
app.post("/orders", (c) => c.json({ ok: true }));

const sb = new ServiceBridge("localhost:14445", KEY);
await sb.start();

attachHono(app, sb, { port: 3000 }); // Hono doesn't own the socket — pass the port
Bun.serve({ port: 3000, fetch: app.fetch });
```

`attachExpress`/`attachHono` take `{ port, host? }`; `sbFastify` reads the bound address itself. Host defaults to the bound socket, falling back to `127.0.0.1`. Attaching before `start()` is safe — the endpoint rides along in the first registration.

---

## Configuration

All configuration lives on the `ServiceBridge` constructor — `new ServiceBridge(url, key, options)`. The SDK reads no environment variables; you decide where `url`, `key` and options come from. Every option is optional.

| Option | Type | Default | Description |
|---|---|---|---|
| `advertise` | `{ host, port } \| false` | `127.0.0.1` on a free port (with a warning) | Inbound RPC server address. Pass `{ host, port }` in containers / k8s; `false` for caller-only instances that never serve RPC. |
| `callDefaults` | `CallOpts` | `{}` | Default `CallOpts` merged under every `sb.rpc.call()` / `sb.stream()`. |
| `failOnPolicyViolation` | `boolean` | `false` | When `true`, any policy warning at registration makes `start()` surface a `disconnected` event and stop. Otherwise warnings are logged and emitted as `policy_violation`. |
| `dataDir` | `string` | `"./.servicebridge"` | Directory for the local SQLite event outbox. |
| `maxOutboxRows` | `number` | `100000` | Outbox rows before `publish` back-pressures with `OutboxFullError`. |
| `eventsDrainerBatch` | `number` | `50` | Outbox rows drained to the runtime per tick. |
| `eventsMaxInFlight` | `number` | `32` | In-flight window advertised to the runtime on subscribe. |
| `rpcMaxConcurrentCalls` | `number` | `256` | Inbound RPC handlers running at once. |
| `rpcMaxQueuedCalls` | `number` | = concurrency | Admission queue depth; past it callers get `RESOURCE_EXHAUSTED` rather than piling up. |
| `reconnectIntervalMs` | `number` | jittered ladder | Fixed delay between reconnect attempts. Unset means the ladder `[1s, 5s, 15s, 30s, 60s]` ±20%. |
| `reconnectAttempts` | `number` | `3` | Reconnect attempts before giving up. `0` = unlimited. |

Telemetry is not configured here. Whether it runs at all, the payload capture mode per channel and the payload size cap are pushed by the runtime and changed from its dashboard — read the current verdict with `sb.telemetry.enabled()` and `sb.telemetry.captureModeForChannel(...)`.

```ts
const sb = new ServiceBridge("localhost:14445", KEY, {
  advertise: { host: process.env.POD_IP!, port: 50051 },
  callDefaults: { timeout: "10s" },
  reconnectAttempts: 0,
  dataDir: "/var/lib/myservice/sb",
});
```

### Lifecycle

```ts
const sb = new ServiceBridge("localhost:14445", KEY);

sb.service("payment-svc", { rpc: ["Charge"] });               // what you call
sb.rpc.handle("Ship", shipHandler, { schema: { protoFile: "./ship.proto" } }); // what you serve

sb.on("connected",      ({ serviceName }) => console.log(`connected as ${serviceName}`));
sb.on("reconnecting",   ({ attempt, reason }) => console.warn(`reconnecting #${attempt}: ${reason}`));
sb.on("disconnected",   ({ reason }) => console.error(`disconnected: ${reason}`));
sb.on("policy_violation", (v) => console.warn(`policy: ${v.declaration} ${v.value} — ${v.reason}`));

await sb.start();

process.on("SIGTERM", async () => { await sb.stop(); process.exit(0); });
```

---

## Error handling

Typed errors are exported from the package root, so you can `catch` precisely:

```ts
import {
  RpcAccessDeniedError,
  WorkflowAccessDeniedError,
  InvalidEventNameError,
  OutboxFullError,
  ServiceBridgeError,
} from "service-bridge";

try {
  await payment.Charge({ userId: "u-1", amount: 100 });
} catch (err) {
  if (err instanceof RpcAccessDeniedError) {
    // denied by access policy: { serviceName, methodName, reason }
  } else if (err instanceof ServiceBridgeError) {
    // any other failure raised by the SDK
  }
}
```

Every error below extends `ServiceBridgeError`, so one `instanceof` separates an SDK failure from an application one — and an error added in a later release will not slip past that check.

| Error | Thrown when |
|---|---|
| `ConnectionError` | Connection / provisioning failure; carries a typed `.code` (retryable ones drive auto-reconnect). |
| `RpcAccessDeniedError` | An RPC call is denied by access policy. Also fires a `policy_violation` event. |
| `NoLiveInstanceError` | No callee instance matches the caller's contract hash, or every one is shed by the breaker. |
| `WorkflowAccessDeniedError` | A workflow `start()` is denied by access policy. |
| `WorkflowNotFoundError` | Starting a workflow name the runtime doesn't know. |
| `WorkflowTerminalError` | Signalling/cancelling a run that already finished. |
| `WorkflowValidationError` | `workflow.handle()` is given a graph that fails validation. |
| `JsonPathError` | A `$.` expression in a workflow step is malformed. |
| `InvalidEventNameError` | Publishing/defining an event whose name fails the naming rule. |
| `OutboxFullError` | The local event outbox is at `maxOutboxRows` (back-pressure). |

---

## FAQ

**Do I have to use Protobuf?** You point handlers at a `.proto` file or a `.schema.json` with explicit field numbers. Both are file-based; there is no inline schema.

**Does ServiceBridge proxy my HTTP traffic?** No. You run your own Express / Fastify / Hono server. The integration only discovers your routes for the Service Map and adds trace spans — your HTTP path is untouched.

**How do I scale horizontally?** Run as many SDK instances as you like; the runtime load-balances RPC across live instances and fails over automatically. The runtime itself is a single source of truth backed by PostgreSQL.

**What happens on a transient disconnect?** Published events sit in the local SQLite outbox and drain when the connection returns. The SDK auto-reconnects (configurable) and rotates certs with overlap so live instances don't drop traffic.

**Where do I see traces, metrics and the DLQ?** In the runtime dashboard on `:14444`. Tracing, metrics and the dead-letter queue are operated there.

**Node or Bun?** Both. Node 18+ or any current Bun. Bun-native APIs are used where available.

---

## Community

- **Website & docs:** [servicebridge.dev](https://servicebridge.dev) · [servicebridge.dev/docs](https://servicebridge.dev/docs)
- **SDK umbrella repo (all languages):** [github.com/service-bridge/sdk](https://github.com/service-bridge/sdk)
- **Runtime:** [github.com/servicebridge2/runtime](https://github.com/servicebridge2/runtime)

This is an alpha release (`2.0.0-alpha`). The API is stabilising — issues and feedback are welcome.

---

## License

Licensed under the **MIT License** — see [LICENSE](./LICENSE). Free for any use, including commercial; you only need to keep the copyright and license notice (attribution to esurkov1 <esurkovv@yandex.ru>).
