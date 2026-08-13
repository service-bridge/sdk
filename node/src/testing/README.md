# testing

## Зона ответственности

In-memory test double для RPC- и event-хендлеров SDK: `TestRpcDomain` двойник `sb.rpc`, `TestEventDomain` двойник `sb.event`, `createTestHarness()` собирает оба под одной точкой доступа. Позволяет юнит-тестировать зарегистрированный хендлер и его исходящие эффекты (`rpc.call`, `event.publish`) без сети, без SQLite, без живого рантайма. Не работает с workflow (см. «Архитектурные решения»), не подменяет wire-кодирование (Protobuf encode/decode) — хендлеры вызываются напрямую с типизированными объектами, как их вызывает `Registry.asDispatchPort()` после декодирования payload.

## Публичный контракт

Реэкспортируется через `service-bridge/testing`.

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `createTestHarness()` | функция | — | Собирает свежий `TestHarness` — независимый `rpc` + `event`, без общего состояния с другими вызовами. |
| `TestHarness` | interface | — | `{ rpc: TestRpcDomain; event: TestEventDomain; reset(): void }`. `reset()` делегирует в `rpc.reset()` + `event.reset()`. |
| `TestRpcDomain` | class | — | Двойник `sb.rpc`. Методы ниже. |
| `TestRpcDomain.handle(name, fn)` | метод | — | Регистрирует хендлер. `fn` — тот же `RpcHandlerFn<Req,Res>` (`registry/registry.ts`), что принимает `sb.rpc.handle`. |
| `TestRpcDomain.invoke(name, req)` | `async (...) => Res` | — | Вызывает зарегистрированный хендлер напрямую с `req`, возвращает его результат. Ошибка хендлера пробрасывается как есть (без wire-обёртки `errorCode`/`errorMessage`, которую делает `CallServer`). Бросает, если хендлер под `name` не зарегистрирован. |
| `TestRpcDomain.call(serviceName, methodName, payload, opts?)` | `async (...) => Res` | — | Двойник исходящего `sb.rpc.call`. Записывает вызов в `calls()` и возвращает ответ, заданный `mockResponse()`. Без настроенного мока — бросает (чтобы забытый мок падал тестом, а не тихо резолвился `undefined`). |
| `TestRpcDomain.mockResponse(serviceName, methodName, responder)` | метод | — | Задаёт ответ для `call()` на пару `(serviceName, methodName)`. `responder` — значение или `RpcMockResponder` (функция от `payload, opts`). |
| `TestRpcDomain.calls()` | `() => readonly RpcCallRecord[]` | — | Все вызовы `call()` в порядке, как они произошли. |
| `TestRpcDomain.reset()` | метод | — | Очищает зарегистрированные хендлеры, моки и `calls()`. |
| `RpcCallRecord` | interface | — | `{ serviceName, methodName, payload, opts? }` — одна запись `calls()`. `opts` присутствует только если был передан. |
| `RpcMockResponder<Req,Res>` | type | — | `(payload: Req, opts?: CallOpts) => Res \| Promise<Res>`. |
| `TestEventDomain` | class | — | Двойник `sb.event`. Методы ниже. |
| `TestEventDomain.handle(pattern, fn)` | метод | — | Регистрирует subscription. `fn` — тот же `(payload: unknown) => Promise<void> \| void`, что принимает `sb.event.handle` (`events/domain.ts`). |
| `TestEventDomain.deliver(name, payload)` | `async (...) => EventDeliveryResult` | — | Симулирует одну доставку. Воспроизводит контракт `Subscriber.handleDelivery` (`events/subscriber.ts`): нет хендлера под точным `name` → `{ outcome: "ack" }` (routing — на сервере, ADR-0002); хендлер бросает → `{ outcome: "nack", reason: String(error) }`, остальные хендлеры этой доставки не вызываются; все хендлеры отработали → `{ outcome: "ack" }`. Несколько хендлеров под одним `pattern` вызываются последовательно в порядке регистрации. |
| `EventDeliveryResult` | type | — | `{ outcome: "ack" } \| { outcome: "nack"; reason: string }`. |
| `EventHandlerFn` | type | — | `(payload: unknown) => Promise<void> \| void`. |
| `TestEventDomain.publish(name, payload, opts?)` | `async (...) => { eventId }` | — | Двойник исходящего `sb.event.publish`. Записывает вызов в `published()`, возвращает свежесгенерированный `eventId` (`crypto.randomUUID()`). Не валидирует имя события и не кодирует payload — это recorder, не замена `Publisher`. |
| `TestEventDomain.published()` | `() => readonly PublishedEventRecord[]` | — | Все вызовы `publish()` в порядке, как они произошли. |
| `TestEventDomain.reset()` | метод | — | Очищает зарегистрированные хендлеры и `published()`. |
| `PublishedEventRecord` | interface | — | `{ name, payload, opts? }` — одна запись `published()`. `opts` присутствует только если был передан. |

## Приватный контракт

Нет приватного контракта — модуль состоит из одного публичного фасада, внутренних не-экспортируемых деталей нет (`responderKey` в `rpc-harness.ts` — приватная функция модуля, не тип и не hook, отдельной записи не требует).

## Архитектурные решения и почему

**Хендлер вызывается напрямую с декодированным объектом, не с wire-байтами.** Продакшен-путь для unary RPC — `Registry.asDispatchPort().dispatchUnary(method, payloadBytes)`: decode → `fn(request)` → encode (`registry/registry.ts`). Harness пропускает decode/encode и вызывает `fn(req)` с уже типизированным объектом — ровно то, что видит сама бизнес-логика хендлера. Wire-кодирование (Protobuf) — забота serde/CallServer, не юнит-теста хендлера; требовать от теста реальный `.proto`-файл и `SchemaPair` ради unit-теста было бы лишней связанностью.

**Ошибка хендлера пробрасывается как есть, без wire-маппинга.** `dispatchUnary` ловит throw и превращает его в `{ errorCode: "INTERNAL", errorMessage }` — это протокол ошибок CallServer↔caller. Harness этот слой не воспроизводит: `invoke()` даёт тесту сырую ошибку хендлера, чтобы `expect(...).rejects.toThrow(...)` проверял бизнес-сообщение, а не транспортную обёртку.

**Регистрация происходит НЕ через настоящий `ServiceBridge`/`Registry`.** `ServiceBridge.rpc`/`.event` — конкретные классы (`RpcDomain`/`EventDomain`) с приватными полями конструктора; TypeScript не даёт подменить их структурно совместимым объектом без `as unknown as`. Сам `Registry._handle` помечен `@internal` и не экспортируется наружу пакета. Вместо reach-into-internals или полноценной подмены `ServiceBridge` (тяжёлая — TLS-креды, gRPC-каналы, SQLite outbox, LB, circuit breaker) harness — минимальная, независимая пара классов с идентичными публичными сигнатурами (`RpcHandlerFn`, `RpcHandlerOpts`-совместимый `handle`, `CallOpts`-совместимый `call`, `PublishOpts`-совместимый `publish`), типы импортированы напрямую из `registry/registry.ts`, `rpc/client.ts`, `events/publisher.ts` — ни один тип не продублирован. Никакой правки production-кода эта схема не потребовала.

**Рекомендуемый паттерн — узкая зависимость в фабрике хендлера.** Хендлеры, которым нужен исходящий канал, пишутся как `(deps: { rpc: Pick<RpcDomain,"call">; event: Pick<EventDomain,"publish"> }) => handlerFn`, а не как замыкание над глобальным `sb`. `Pick<...>` — чисто структурный тип, поэтому `TestRpcDomain`/`TestEventDomain` подходят под него без каста: то же самое «интерфейсы объявляются у потребителя», что уже требует `CLAUDE.md` проекта. Пример — `example.test.ts`.

**`TestRpcDomain.call()` без настроенного мока — бросает, а не резолвится в `undefined`.** Забытый `mockResponse(...)` в реальном коде означает баг в тесте (не в хендлере); тихий `undefined` замаскировал бы это до assertion где-то ниже по цепочке. Явный throw с подсказкой конкретной команды — fail fast.

**`TestEventDomain.deliver()` не декодирует payload и не проверяет схему.** В отличие от `Subscriber.handleDelivery`, которому нужен `SchemaIndex` для decode wire-bytes (`no_schema` → `Nack`), harness работает на уровне уже-декодированного объекта — тот же уровень, что видит сам handler. Это осознанное сужение контракта: harness проверяет ack/nack-логику хендлера, а не наличие схемы у события.

**`TestEventDomain.publish()` — recorder, не Publisher.** Настоящий `Publisher.publish` (`events/publisher.ts`) валидирует имя по regex, кодирует payload через `SchemaPair`, пишет в SQLite outbox, шлёт gRPC. Ничего из этого harness не воспроизводит — цель not «подделать Publisher», а дать тесту точку наблюдения «что хендлер опубликовал», не требуя реальной схемы/БД/сети ради unit-теста.

**Streaming RPC (`RpcStreamHandlerFn`) и Workflow — вне охвата.** RPC ограничен unary (`handle`/`invoke`) — задача явно требовала «получить результат или ошибку», что соответствует unary-контракту; добавление `handleStream`/`invokeStream` не требует смены архитектуры и может появиться отдельным приростом, если понадобится. Workflow-раннер намеренно не тронут: `WorkflowRunner` (`workflow/runner.ts`) исполняет замороженный план через `SbDomains` (тот же `rpc.call`/`event.publish`/`workflow.start`) **и** `RuntimeOps` — чекпоинтинг состояния шага в рантайме (persist/resume/replay, ADR-W-018). Без реального рантайма шаг workflow нельзя ни закоммитить, ни реплеить: честная имитация потребовала бы переписывать сам движок персистентности, что явно вне рамок «harness без сети/БД/рантайма» и вне зоны этой задачи.

## Зависимости

- Использует (только чтение типов, без изменений): `sdk/node/src/registry/registry.ts` (`RpcHandlerFn`), `sdk/node/src/rpc/client.ts` (`CallOpts`), `sdk/node/src/events/publisher.ts` (`PublishOpts`), `node:crypto` (`randomUUID`).
- Используется: пользовательским тестовым кодом через `service-bridge/testing` (см. `userDocs/testing.md`, `skill/reference/testing.md`). Никакой производственный модуль SDK его не импортирует — `testing/` не участвует в рантайм-пути `sb.start()`.
