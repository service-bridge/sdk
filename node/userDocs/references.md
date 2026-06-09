# References

← [API reference](./api-reference.md) · [Назад к индексу](./index.md)

Внешние и внутренние документы, на которые опирается SDK.

## Архитектурные ADR (runtime)

В `runtime/docs/adr/`:

- [0001 — RPC architecture](../../../runtime/docs/adr/0001-rpc-architecture.md) — passthrough proxy, per-pod CB, explicit advertise
- [0002 — Direct mode](../../../runtime/docs/adr/0002-direct-mode.md) — SPIFFE verification, TTL eviction
- [0003 — Resilience](../../../runtime/docs/adr/0003-resilience.md) — LB + CB + retry + idempotency
- [0004 — Server-side streaming](../../../runtime/docs/adr/0004-streaming.md)
- [0005 — Contract-version routing](../../../runtime/docs/adr/0005-contract-version-routing.md) — SDK as hash source of truth
- [0006 — Durable Events](../../../runtime/docs/adr/0006-durable-events.md) — at-least-once доставка, retries, DLQ
- [0007 — Idempotency unified](../../../runtime/docs/adr/0007-idempotency-unified.md) — единый ключ дедупликации
- [0012 — RPC contract hash + idempotency](../../../runtime/docs/adr/0012-rpc-contract-hash-and-idempotency.md) — enforcement `contract_hash` на proxy-пути
- [0013 — Events Protobuf payload](../../../runtime/docs/adr/0013-events-protobuf-payload.md) — runtime трактует payload как opaque bytes
- [0014 — Access policy](../../../runtime/docs/adr/0014-access-policy.md) — capabilities + двусторонние allow-list

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
