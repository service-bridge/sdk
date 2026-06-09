# Introduction

← [Назад к индексу](./index.md) · Дальше: [Quickstart](./quickstart.md) →

## Что такое ServiceBridge

ServiceBridge — единый self-hosted рантайм для микросервисов: **один Go-бинарь + PostgreSQL** вместо связки Istio/Envoy/RabbitMQ/Temporal/Jaeger и т. п.

Сервисы декларируют входящие хендлеры (RPC, события, workflow, HTTP) и исходящие зависимости **до старта**. Runtime сразу строит граф сервисов и берёт на себя транспорт, доставку, политики и observability — без sidecar-прокси и отдельной инфраструктуры.

Node.js SDK — backend-SDK (Bun + TypeScript), подключается к рантайму по gRPC (порт `14445`).

## Чем НЕ является

- **Не sidecar.** SDK живёт в процессе вашего сервиса, нет отдельного daemon.
- **Не service mesh.** Нет data-plane прокси: транспорт либо direct (caller → callee mTLS), либо через runtime `Invoke`.
- **Не SaaS.** Self-hosted, в одном Docker-контейнере.
- **Не frontend SDK.** Bun/Node only.

## Архитектура на одну страницу

```
┌──────────────────────────────────────────────────────────────────┐
│                       Ваш сервис                                 │
│ ┌─────────────────────────────────────────────────────────────┐  │
│ │  new ServiceBridge(url, key, options)                       │  │
│ │                                                             │  │
│ │  sb.rpc.handle("Charge", fn, { schema })   ← регистрация    │  │
│ │  sb.service("other-svc", { rpc: ["..."] }) ← deps           │  │
│ │  await sb.start()                                           │  │
│ │                                                             │  │
│ │  await sb.rpc.call("other-svc", "method", payload)          │  │
│ │  for await (chunk of sb.stream(...)) { ... }                │  │
│ │                                                             │  │
│ │  payment = await sb.client("payment-svc", "./pay.proto")    │  │
│ │  await payment.Charge({...})              ← typed proxy     │  │
│ └─────────────────────────────────────────────────────────────┘  │
│           │                              ▲                       │
│           │ mTLS gRPC                    │ inbound CallServer    │
│           │ (control plane)              │ (если advertise)      │
│           ▼                              │                       │
└──────────────────────────────────────────────────────────────────┘
            │                              │
            ▼                              │
    ┌───────────────────────┐              │
    │  ServiceBridge runtime│              │  direct mTLS
    │  Go binary + Postgres │ ─────────────┴───── другой инстанс
    │  • registry           │                     этого сервиса или
    │  • Invoke proxy       │                     другого сервиса
    │  • idempotency cache  │
    │  • events / workflows │
    └───────────────────────┘
```

Один SDK-инстанс может быть **и callee, и caller одновременно**.

## Что умеет SDK

### Connectivity
- Bootstrap + provisioning mTLS-сертификата
- Reconnect с overlap-rotation сертификатов
- Live `serviceMap()` через `RegisterAndWatch`

### Communication
- **Unary RPC** (`sb.rpc.call`) и **server-side streaming** (`sb.stream`) — см. [RPC](./rpc.md)
- **Direct mode** (caller → callee mTLS) + **Proxy mode** (через runtime `Invoke`) + `transport: "auto"`
- **Typed client** `sb.client(svc, .proto)` — авторегистрация deps + schemas
- **Events** (durable pub/sub): `sb.event.define` / `sb.event.handle` / `sb.event.publish` — см. [Events](./events.md)
- **Workflows** (durable steps) + **Jobs** (cron) — см. [Workflows](./workflows.md)
- **HTTP integrations** (Express / Fastify / Hono) — авто-публикация роутов в Service Map; HTTP-сервер у пользователя — см. [Integrations](./integrations.md)

### Reliability
- Load Balancer (power-of-two-choices по in-flight), Circuit Breaker (per-instance), Retry (exp+jitter)
- Idempotency cache в proxy mode
- Contract-version routing (blue-green: v1 caller → v1 callee, v2 → v2)

### Observability
- `sb.telemetry` — ops/traces, structured logs (`sb.logger`), `counter` / `gauge` / `histogram`
- Авто-flush в runtime (off-switch — env `SB_TELEMETRY=off`); trace-контекст наследуется вложенными вызовами

### Schemas
- Источники: `.proto` (с/без service block) и `.schema.json` с явными `fieldNumber`
- Auto-resolve `input/output` из `service` блока `.proto`

## Что НЕ входит в SDK

- Bidirectional streaming RPC (только server-side: `sb.stream`)
- Прокси бизнес-HTTP. Свой HTTP-сервер (Express / Fastify / Hono) поднимаете вы; рантайм только видит роуты в Service Map (ADR 0001)

→ Дальше: [Quickstart](./quickstart.md)
