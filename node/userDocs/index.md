# ServiceBridge Node.js SDK — документация

TypeScript SDK для [ServiceBridge runtime](https://github.com/servicebridge2/runtime).

```sh
bun add service-bridge    # или npm install / yarn add
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
| [Тестирование](./testing.md) | `service-bridge/testing` — юнит-тест RPC/event-хендлеров без сети и без живого рантайма |
| [Integrations](./integrations.md) | HTTP-фреймворки: Express / Fastify / Hono. Service Map для существующих REST API |
| [Access Policy](./access-policy.md) | Гранулярные политики: capabilities, egress, acceptance. Default-allow |
| [Operations](./operations.md) | lifecycle, identity, advertise, mTLS, ротация, env, troubleshooting |
| [API reference](./api-reference.md) | Компактный справочник публичных типов |
| [References](./references.md) | ADR и internal docs |

## Где что искать

**«Как зарегистрировать обработчик X?»** → файл соответствующего домена ([RPC](./rpc.md) / [Events](./events.md) / [Workflows](./workflows.md) / [Jobs](./jobs.md) / [Integrations](./integrations.md)).

**«Как вызвать чужой RPC?»** → [RPC §3](./rpc.md#3-исходящие-вызовы).

**«Почему я получаю ошибку X?»** → [Operations §8 Troubleshooting](./operations.md#8-troubleshooting) + [RPC §9 Ошибки](./rpc.md#9-ошибки). `PermissionDenied` при старте → [Access Policy](./access-policy.md).

**«Какие env-переменные?»** → [Operations §7](./operations.md#7-environment-variables).

**«Как сгенерировать bootstrap-ключ?»** → [Operations §6](./operations.md#6-security-bootstrap-key-mtls-ротация).

**«Какая сигнатура у `X`?»** → [API reference](./api-reference.md).

**«Как юнит-тестировать хендлер без живого рантайма?»** → [Тестирование](./testing.md).
