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

**One self-hosted Go runtime plus PostgreSQL that replaces a whole microservices stack.** Service mesh, message broker, workflow engine, job scheduler, tracing backend, mTLS PKIcollapsed into a single binary. RPC, durable events, workflows, jobs and streaming over mTLS gRPC, with observability built in. Zero sidecars.

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

- **Direct RPC** — request/response and server-side streaming over mTLS gRPC, caller-to-callee with no proxy hop. Load balancing, retries, idempotency and circuit breakers built in.
- **Durable events** — at-least-once publish/subscribe with a local outbox, wildcard topics, fan-out delivery, retries and a dead-letter queue. No broker.
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
| RabbitMQ / Kafka / NATS | Durable events with outbox, fan-out, retries, DLQ |
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
| **Go** | Unreleased | `go get github.com/service-bridge/sdk/go` | [`./go`](./go) |
| **Python** | Coming soon | — | `./python` |

The Go SDK covers the same surface as the Node one — RPC and server streams, durable events, workflows, jobs, telemetry, HTTP integrations. It carries no tagged version yet, so `go get` resolves a pseudo-version off the default branch. Its end-to-end suite runs against the runtime and calls across to the Node SDK in both directions.

Each SDK directory holds its own README with install instructions, a quick start and the full API reference.

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
