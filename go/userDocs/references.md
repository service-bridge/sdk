# References

[← API reference](./api-reference.md) · [к индексу](./index.md)

## README пакетов SDK

Публичные пакеты — контракт, архитектурные решения и обоснования:

- [`../README.md`](../README.md) — обзор SDK целиком
- [`../job/README.md`](../job/README.md) — объявление задач
- [`../workflow/README.md`](../workflow/README.md) — словарь графа workflow
- [`../sbhttp/README.md`](../sbhttp/README.md) — интеграция `net/http` и chi
- [`../sbgin/README.md`](../sbgin/README.md) — интеграция gin (отдельный модуль)
- [`../sbtest/README.md`](../sbtest/README.md) — in-memory двойник

Внутренние пакеты — для тех, кто читает или правит сам SDK. Прикладной код на них не опирается:

- [`../internal/connection/README.md`](../internal/connection/README.md) — жизненный цикл соединения, провижининг, ротация сертификатов
- [`../internal/registry/README.md`](../internal/registry/README.md) — объявления и поток реестра
- [`../internal/rpc/README.md`](../internal/rpc/README.md) — входящий сервер, клиент, транспорты, балансировка, размыкатель, ретраи
- [`../internal/events/README.md`](../internal/events/README.md) — публикация, дренаж, подписка
- [`../internal/outbox/README.md`](../internal/outbox/README.md) — локальное хранилище буфера
- [`../internal/job/README.md`](../internal/job/README.md) — каноническая форма задачи и подписчик исполнений
- [`../internal/workflow/README.md`](../internal/workflow/README.md) — заморозка графа, валидация, раннер
- [`../internal/telemetry/README.md`](../internal/telemetry/README.md) — операции, метрики, кольцевые буферы, мост `slog`
- [`../internal/stream/README.md`](../internal/stream/README.md) — супервизор стрима и лестница переподключения

## Рантайм

- Репозиторий рантайма: [github.com/servicebridge2/runtime](https://github.com/servicebridge2/runtime)
- Архитектурные ADR лежат там же, в `docs/adr/`: RPC, durable events, workflow, политика доступа и TLS, надёжность, время и идентичность, телеметрия. В этом репозитории их нет — SDK и рантайм живут раздельно.
- Установка рантайма одной строкой: `bash <(curl -fsSL https://servicebridge.dev/install.sh)`

Порты по умолчанию: gRPC control plane для SDK — `14445`, дашборд — `14444`.

## Требования

| | Минимум |
|---|---|
| Go | 1.24 |
| PostgreSQL | 18+ (нужен рантайму, не SDK) |
| cgo | не требуется |

## Внешние ресурсы

- [Protobuf 3 language guide](https://protobuf.dev/programming-guides/proto3/)
- [gRPC status codes](https://grpc.io/docs/guides/status-codes/)
- [SPIFFE concepts](https://spiffe.io/docs/latest/spiffe-about/spiffe-concepts/)
- [Circuit Breaker (Fowler)](https://martinfowler.com/bliki/CircuitBreaker.html)
- [`iter.Seq2` в стандартной библиотеке Go](https://pkg.go.dev/iter)
- [Документация пакета на pkg.go.dev](https://pkg.go.dev/github.com/service-bridge/sdk/go)

## Лицензия

MIT — см. [LICENSE](../../LICENSE).

[к индексу](./index.md)
