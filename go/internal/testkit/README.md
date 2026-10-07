# testkit

## Зона ответственности

Шов между клиентом и двойником `sbtest`: двойник передаёт клиенту in-memory транспорт, клиент отдаёт двойнику входы своей диспетчеризации. Логики не содержит.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Memory.Call` · `Memory.Stream` | функции | — (ставит двойник) | Исходящие вызовы клиента вместо `rpc.Client`. |
| `Memory.Publish` | `events.PublishFunc` | — (ставит двойник) | Транспорт настоящего publisher'а клиента. |
| `Memory.Unary` · `Memory.ServeStream` | функции | — (ставит клиент) | `Dispatcher.Unary` / `Dispatcher.Stream` клиента. |
| `Memory.Deliver` · `Memory.Subscriptions` | функции | — (ставит клиент) | `Subscriber.Handle` / `Subscriber.Subscriptions` клиента. |
| `Memory.Wrap` | `func(op string, err error) error` | — (ставит клиент) | Классификация ошибки публичным API клиента. |
| `NewOption` | `func(*Memory) any` | ставит корневой пакет в `init` | Опция `servicebridge.Option`, включающая in-memory режим. |

## Приватный контракт

Нет приватного контракта.

## Архитектурные решения и почему

**Отдельный internal-пакет.** `sbtest` и корневой пакет оба его импортируют, а друг друга в нужную сторону — нет: корневой пакет не может знать о `sbtest`, а опция клиента — неэкспортируемый конфиг. `NewOption` возвращает `any`, поэтому в публичном API клиента не появляется ни одной тестовой опции.

## Зависимости

Опирается на: `internal/rpc`, `internal/events`, `internal/pb`. На него опираются: корневой пакет `servicebridge` (in-memory режим `Start`/`Stop`), `sbtest`.
