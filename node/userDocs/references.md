# References

← [API reference](./api-reference.md) · [Назад к индексу](./index.md)

Внешние и внутренние документы, на которые опирается SDK.

## Архитектурные ADR (runtime)

В `runtime/docs/adr/`:

- [0001 — RPC](../../../runtime/docs/adr/0001-rpc.md) — транспорт (proxy + direct), resilience (LB / CB / retry), contract-version routing, single-row trace
- [0002 — Durable Events](../../../runtime/docs/adr/0002-events.md) — at-least-once доставка, server-only routing, opaque-payload bytes
- [0003 — Workflows](../../../runtime/docs/adr/0003-workflows.md) — DAG-исполнение шагов, runtime-observable trace
- [0004 — Access policy, identity, TLS](../../../runtime/docs/adr/0004-access-security-tls.md) — capabilities + двусторонние allow-list, mTLS, CA в Postgres
- [0005 — Reliability](../../../runtime/docs/adr/0005-reliability.md) — liveness, единый ключ идемпотентности, ABANDONED-sweep
- [0006 — Time & identity](../../../runtime/docs/adr/0006-time-identity.md) — `int64 unix-ms`, UUIDv7, `X-SB-Trace`
- [0007 — Telemetry](../../../runtime/docs/adr/0007-telemetry-tracing.md) — примитив `operations`, схема, единый Report-канал

## Internal module READMEs

Описывают внутренние контракты SDK-модулей (для разработчиков SDK):

- [`src/connection/README.md`](../src/connection/README.md) — connection lifecycle, session, cert rotation
- [`src/registry/README.md`](../src/registry/README.md) — WatchStream, registry cache
- [`src/rpc/README.md`](../src/rpc/README.md) — CallServer, RpcClient, transports
- [`src/serde/README.md`](../src/serde/README.md) — protobufjs-based codec

## Runtime

- [`runtime/README.md`](../../../runtime/README.md) — как запустить runtime; env только для подключения к Postgres, остальная конфигурация (порты, таймауты, квоты) живёт в БД и правится в UI
- [`runtime/proto/servicebridge/v1/`](../../../runtime/proto/servicebridge/v1/) — proto-контракты gRPC между SDK и runtime

Порты по умолчанию: gRPC control-plane (mTLS, для SDK) — `14445`, UI-gateway (h2c, админ-консоль) — `14444`.

## Требования

| | Минимум |
|--|--|
| Bun | 1.x |
| Node.js | 18+ (альтернатива Bun) |
| TypeScript | 5 |
| Postgres | 18+ (нужен рантайму, не SDK) |

## Внешние ресурсы

- [Protobuf 3 language guide](https://protobuf.dev/programming-guides/proto3/)
- [gRPC status codes](https://grpc.io/docs/guides/status-codes/)
- [SPIFFE / SPIRE concepts](https://spiffe.io/docs/latest/spiffe-about/spiffe-concepts/)
- [Circuit Breaker pattern (Fowler)](https://martinfowler.com/bliki/CircuitBreaker.html)

[Назад к индексу](./index.md)
