<!--
Keywords: ServiceBridge, microservices runtime, self-hosted, service mesh alternative,
gRPC RPC, durable events, message broker alternative, RabbitMQ alternative, Kafka alternative,
workflow engine, saga orchestration, Temporal alternative, job scheduler, cron,
distributed tracing, observability, Jaeger alternative, mTLS, PostgreSQL,
Node SDK, TypeScript SDK, Go SDK, Python SDK, Istio alternative, Consul alternative.
-->

# ServiceBridge SDKs

[![npm](https://img.shields.io/npm/v/service-bridge?color=cb3837&label=service-bridge%40npm)](https://www.npmjs.com/package/service-bridge)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Website](https://img.shields.io/badge/site-servicebridge.dev-0b0b0b.svg)](https://servicebridge.dev)

**One self-hosted Go runtime plus PostgreSQL that replaces a whole microservices stack.** Service mesh, message broker, workflow engine, job scheduler, tracing backend, mTLS PKI — collapsed into a single binary. RPC, durable events, workflows, jobs and streaming over mTLS gRPC, with observability built in. Zero sidecars.

Your services declare what they handle and what they call. The runtime takes over transport, delivery, orchestration, policy and observability — no proxy on the data path, no separate infrastructure to run, secure and correlate.

This repo holds the official SDKs. **Pick your language in the [table below](#sdks-by-language).**

## Ten tools in. One runtime out.

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

## What you get

One runtime, every inter-service primitive:

- **Direct RPC** — request/response and server-side streaming over mTLS gRPC, caller-to-callee with no proxy hop. Load balancing, retries of calls that never reached a handler, idempotency keys and circuit breakers built in.
- **Durable events** — at-least-once publish/subscribe: `publish` returns once the event is stored in PostgreSQL. Wildcard topics, filters evaluated by the runtime, fan-out delivery, retries and a dead-letter queue. No broker.
- **Workflows** — durable DAGs with compensation (sagas), signals and replay. State persists in PostgreSQL and survives restarts.
- **Jobs** — cron, interval and one-shot scheduled work with leasing, catchup and retries. No external scheduler.
- **Streaming** — server-side streaming RPC for LLM token output, progress feeds and live logs. Break the loop and the stream tears down end to end.
- **mTLS by default** — identity auto-provisioned from a service key, short-lived leaf certs rotated before expiry. No cert-manager, no Vault PKI.
- **Observability** — every hop traced end to end, Prometheus-compatible metrics, structured logs, smart alerts. No Jaeger, no exporter sidecar.
- **Dashboard** — a live service map, run details, queue and DLQ state, and per-entity stats in a built-in web UI.

Designed to run up to 1000 services against a single runtime.

## Why

Microservices rarely fail in the business logic. They fail in the gaps between services — the broker that dropped a message, the workflow engine nobody fully understands, the trace that stops at a boundary, the mesh config that took a week to debug. Each gap is another system to run, secure and correlate. ServiceBridge collapses them into one place to look when something breaks.

| You'd otherwise run | ServiceBridge gives you |
|---|---|
| Istio / Linkerd / Envoy | mTLS identity, routing and policy, zero sidecars |
| RabbitMQ / Kafka / NATS | Durable events with filters, fan-out, retries, DLQ |
| Temporal / Cadence / Step Functions | Durable workflows with compensation, signals, replay |
| A cron service / Quartz / Bull | Leased, retried cron and one-shot jobs |
| Jaeger / Tempo + Prometheus + Loki | Tracing, metrics and logs, correlated out of the box |
| Consul / etcd | Service discovery with P2C load balancing |
| cert-manager / Vault PKI | Auto-provisioned certs from a service key |

The point isn't beating each tool at its own game. It's that you stop running and correlating ten of them.

## SDKs by language

Every SDK speaks the same runtime over the same gRPC control plane, so the API surface is intentionally identical across languages.

| Language | Status | Package | Directory |
|---|---|---|---|
| **Node.js / Bun** (TypeScript) | **Live** | [![npm](https://img.shields.io/npm/v/service-bridge?label=npm)](https://www.npmjs.com/package/service-bridge) `service-bridge` | [`./node`](./node) |
| **Go** | **Live** | [![Go module](https://img.shields.io/github/v/tag/service-bridge/sdk?filter=go%2Fv*&label=go%20module&color=00ADD8)](https://pkg.go.dev/github.com/service-bridge/sdk/go) `go get github.com/service-bridge/sdk/go` | [`./go`](./go) |
| **Python** | Coming soon | — | `./python` |

The Go SDK covers the same surface as the Node one — RPC and server streams, durable events, workflows, jobs, telemetry, HTTP integrations. Its end-to-end suite runs against the runtime and calls across to the Node SDK in both directions. It ships on the `v0` line, so the API can still change between minor versions — pin the version you build against.

The gin integration is a second module, which keeps gin out of the dependency graph of everyone who does not use it:

```sh
go get github.com/service-bridge/sdk/go/sbgin
```

Both modules live under `go/` rather than at the repository root, so their release tags carry that path prefix — `go/v0.2.0` and `go/sbgin/v0.2.0`.

Each SDK directory holds its own README with install instructions, a quick start and the full API reference.

## Behaviour parity

The Node and Go SDKs behave the same way on everything below. Only the syntax differs between languages. Shared conformance scenarios ([`conformance/`](./conformance)) run every pairing (Go→Go, Go→Node, Node→Go, Node→Node) against a live runtime. Shared vectors pin the bytes both SDKs hash: [`contract-hash-vectors.json`](./contract-hash-vectors.json) and [`job-canonical-vectors.json`](./job-canonical-vectors.json).

| Area | Behaviour | Node | Go |
|---|---|---|---|
| Errors | One error type with a `code`. `CONNECTION`, `NO_LIVE_INSTANCE`, `OVERLOADED` and `QUEUE_FULL` are retryable. `TIMEOUT` is not: the outcome is unknown. | `ServiceBridgeError` `.code` `.retryable` | `*sb.Error` `.Code` `.Retryable()` |
| Business errors | The handler's code and message reach the caller. Any other failure reaches the caller as `INTERNAL`, and so does a rethrown error from a nested call. | `throw new HandlerError(code, msg)` | `return &sb.HandlerError{Code, Message}` |
| Call defaults | Timeout 30 s. Transport `auto` (direct, falling back to the runtime proxy before dispatch). 3 attempts, backoff 200 ms ×2 up to 5 s, jitter 0.3. | `callDefaults` | `WithCallDefaults`, `DefaultCallTimeout` |
| Retries | Only failures proven to happen before dispatch: no candidate, a channel that never became ready, or a callee answer marked not-dispatched. Streams are never retried. | same | same |
| Handler context | Deadline, caller service and instance, request id, idempotency key, cancellation. | `(req, ctx)` | `sb.CallInfoFromContext(ctx)` |
| Inbound limits | 256 concurrent calls and 256 queued per instance. | `rpcMaxConcurrentCalls`, `rpcMaxQueuedCalls` | `WithInboundLimits` |
| Publish | Returns once the runtime stored the event. A bounded in-memory queue (10 000) holds events for up to 30 s while the runtime is unreachable. A duplicate is a success with the original id. `fireAndForget` returns after enqueueing. | `maxPendingPublishes`, `publishTimeoutMs` | `WithMaxPendingPublishes`, `WithPublishTimeout` |
| Subscribe | One handler per pattern. Handlers run for the patterns the runtime matched. The filter `{"$.path": literal}` is evaluated by the runtime. At most 32 deliveries in flight, serial per partition key. | `sb.event.handle(p, fn, {schema, filter})` | `SubscribeEvent(c, p, fn, WithFilter(...))` |
| Lifecycle | `start` waits for Welcome and the first registry snapshot (30 s). Reconnect is unlimited, on a 1/5/15/30/60 s ladder ±20 %. `Drain` triggers a reconnect. Revoked services and instances are cut off. | `start`, `ready`, `on(...)` | `Start`, `Ready`, `On*` |
| Stop | Withdraw the endpoint and stop taking work, wait for in-flight work (10 s), flush publishes, flush telemetry and wait for the ack (2 s), close. | `stopTimeoutMs` | `DefaultStopTimeout` |
| Certificates | Renewed 30 min before expiry (±5 min) without reopening streams or channels. The instance id does not change. | same | same |
| Telemetry | Drops are reported to the runtime as `sb_sdk_telemetry_dropped_total` and passed to a callback. The runtime captures payloads on `errors` by default and masks them on ingest. | `telemetry.onDrop` | `WithTelemetryDropHandler` |
| HTTP | No perimeter protection in the integrations. Subjects use route templates. A client's `X-SB-Trace` is ignored unless trusted explicitly. | `trustTraceHeader` | `sbhttp.WithTrustTraceHeader` |
| Testing | An in-memory harness drives a real client: invoke handlers, answer outbound calls, record publishes, deliver events. | `createTestHarness()` | `sbtest.New(t)` |

## Protocol contract

The gRPC contract between every SDK and the runtime lives here, in [`proto/servicebridge/v1`](./proto/servicebridge/v1) — the single source of truth. The runtime generates its server stubs from this directory at a pinned commit; each SDK generates its own client stubs. `bash scripts/gen-proto.sh` regenerates the Go (`go/internal/pb`) and Node (`node/src/pb`) stubs with pinned plugin versions; CI fails when committed stubs drift from the `.proto` sources.

## AI coding skill

Building with an AI agent like Claude Code? Each language SDK ships its own skill so the agent writes correct code on the first try — the real RPC, events, workflows, jobs and HTTP-integration API, grounded in the shipped SDK rather than guessed. Copy the one for your language into the agent's skills directory:

```sh
# Node — the skill ships inside the npm package
cp -r node_modules/service-bridge/skill .claude/skills/servicebridge-node

# Go — the skill ships inside the module
cp -r "$(go env GOMODCACHE)"/github.com/service-bridge/sdk/go@*/skill .claude/skills/servicebridge-go
```

Or pull either from the repo without installing: `npx degit service-bridge/sdk/node/skill .claude/skills/servicebridge-node`, `npx degit service-bridge/sdk/go/skill .claude/skills/servicebridge-go`. Restart the agent to pick it up. Sources: [`node/skill/`](./node/skill), [`go/skill/`](./go/skill).

## Links

- **Full feature tour, docs & quickstart:** [servicebridge.dev](https://servicebridge.dev) · [servicebridge.dev/docs](https://servicebridge.dev/docs)
- **Node SDK — install, examples, API reference:** [`./node`](./node)
- **Go SDK — install, examples, API reference:** [`./go`](./go)
- **AI coding skills:** [`./node/skill`](./node/skill) · [`./go/skill`](./go/skill)

## License

Licensed under the **MIT License** — see [LICENSE](./LICENSE). Free for any use, including commercial; you only need to keep the copyright and license notice (attribution to esurkov1 <esurkovv@yandex.ru>).

For `scripts/bootstrap-e2e-keys.sh`, Docker database access uses `PG_CONTAINER`, `PG_USER` (default `servicebridge`) and `PG_DATABASE` (default `service-bridge`). Direct mode uses `POSTGRES_DSN`. Go E2E uses corresponding `SB_E2E_PG_CONTAINER`, `SB_E2E_PG_USER`, `SB_E2E_PG_DATABASE` and `SB_E2E_PG_PASSWORD`; direct mode uses `SB_E2E_PG_DSN` or `TEST_DATABASE_URL`. The local CI runner supplies these values for its isolated database.

The script override template is `scripts/e2e.env.example`; the local CI runner supplies these values automatically.
