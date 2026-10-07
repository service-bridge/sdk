# ServiceBridge Node.js SDK — документация

TypeScript SDK для [ServiceBridge runtime](https://github.com/servicebridge2/runtime).

```sh
npm i service-bridge    # или bun add service-bridge
```

## Документация — по доменам

Каждый файл — самодостаточный гайд по одной фиче от регистрации до production-edge-cases. Читается линейно.

| Документ | О чём |
|----------|-------|
| [Introduction](./introduction.md) | Что такое SB, чем не sidecar, фичи SDK |
| [Quickstart](./quickstart.md) | 5-минутный E2E |
| [**RPC**](./rpc.md) | `rpc.handle/stream` + schemas + `sb.rpc.call`/`sb.stream` + typed client + transport + resilience + idempotency + contract routing + ошибки |
| [Events](./events.md) | `event.handle` + `event.define` + `event.publish` + delivery semantics |
| [Workflows](./workflows.md) | `sb.workflow.handle` — durable DAG-шаги с persistent state, compensation, replay |
| [Jobs](./jobs.md) | `sb.job.handle` — cron / delayed / interval с at-least-once + heartbeat + DST |
| [Тестирование](./testing.md) | `service-bridge/testing` — настоящий `ServiceBridge` на in-memory runtime: юнит-тест RPC/event-хендлеров без сети |
| [Integrations](./integrations.md) | HTTP-фреймворки: Express / Fastify / Hono. Service Map для существующих REST API |
| [Access Policy](./access-policy.md) | Гранулярные политики: capabilities, egress, acceptance. Default-allow |
| [Operations](./operations.md) | опции, lifecycle (start / ready / stop), события, logger, identity, advertise, mTLS, ротация, troubleshooting |
| [API reference](./api-reference.md) | Компактный справочник публичных типов |
| [References](./references.md) | ADR и internal docs |

## Где что искать

**«Как зарегистрировать обработчик X?»** → файл соответствующего домена ([RPC](./rpc.md) / [Events](./events.md) / [Workflows](./workflows.md) / [Jobs](./jobs.md) / [Integrations](./integrations.md)).

**«Как вызвать чужой RPC?»** → [RPC §3](./rpc.md#3-исходящие-вызовы).

**«Почему я получаю ошибку X?»** → [Operations §8 Troubleshooting](./operations.md#8-troubleshooting) + [RPC §9 Ошибки](./rpc.md#9-ошибки). `AccessDeniedError` при старте → [Access Policy](./access-policy.md). Все коды ошибок — [API reference](./api-reference.md#ошибки).

**«Какие env-переменные?»** → [Operations §7](./operations.md#7-environment-variables).

**«Как сгенерировать bootstrap-ключ?»** → [Operations §6](./operations.md#6-security-bootstrap-key-mtls-ротация).

**«Какая сигнатура у `X`?»** → [API reference](./api-reference.md).

**«Как юнит-тестировать хендлер без живого рантайма?»** → [Тестирование](./testing.md).
