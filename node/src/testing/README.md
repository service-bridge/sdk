# testing

## Зона ответственности

`service-bridge/testing` — юнит-тесты хендлеров сервиса без runtime и без сети. Harness держит настоящий `ServiceBridge`, запущенный против in-memory runtime: регистрация идёт обычным API (`sb.rpc.handle`, `sb.rpc.handleStream`, `sb.event.handle`, `sb.event.define`, `sb.client`/`sb.useSchema`), а вызовы проходят через продакшен-код — кодирование/декодирование схемами, маппинг ошибок, `Publisher`, `Subscriber`. Не воспроизводит политику доступа, фильтры подписок, ретраи/лизы доставки, jobs и workflow.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `createTestHarness(opts?)` | `(Pick<ServiceBridgeOptions, "callDefaults" \| "publishTimeoutMs">) => TestHarness` | `{}` | Новый harness со своим `ServiceBridge`. |
| `TestHarness.sb` | `ServiceBridge` | — | Бридж под тестом; хендлеры и зависимости регистрируются до `start()`. |
| `TestHarness.start()` | `Promise<void>` | — | Запечатывает декларации и грузит схемы; сеть не открывается. |
| `TestHarness.invoke(method, req, opts?)` | `Promise<Res>` | — | Входящий unary-вызов: `req` кодируется схемой хендлера → реальный dispatch → ответ декодируется. Ошибки в форме вызывающего: `HandlerError` (код хендлера или `"INTERNAL"`), отказ до хендлера — `ServiceBridgeError` с кодом статуса (`NOT_FOUND`, `VALIDATION`). |
| `TestHarness.invokeStream(method, req, opts?)` | `Promise<Chunk[]>` | — | То же для server-streaming; собирает чанки. |
| `InvokeOpts` | interface | — | `{ caller?, requestId?, idempotencyKey?, signal?, deadline? }` → `ctx` хендлера. `requestId` по умолчанию — UUID. |
| `TestHarness.respond(service, method, fn)` | метод | — | Ответ на исходящий `sb.rpc.call`/typed client. `fn(req, call)` получает декодированный запрос; `HandlerError` → бизнес-код у вызывающего, иная ошибка → `"INTERNAL"`. Нужна объявленная схема (`sb.client`/`sb.useSchema`), иначе `ConfigurationError`, как в продакшене. |
| `TestHarness.respondStream(service, method, fn)` | метод | — | То же для исходящего `sb.stream`. |
| `TestHarness.calls()` | `readonly CallRecord[]` | — | Исходящие вызовы по порядку: `{ service, method, payload, opts }`. Вызов без ответа записывается и падает `NO_LIVE_INSTANCE`. |
| `TestHarness.published()` | `readonly PublishedRecord[]` | — | Опубликованные события (через настоящий Publisher, in-memory runtime отвечает ACCEPTED): `{ id, name, payload, payloadJson, partitionKey, idempotencyKey, headers, occurredAtMs }`, `payload` декодирован схемой `define`. |
| `TestHarness.deliver(name, payload, opts?)` | `Promise<DeliveryResult>` | — | Доставка через настоящий Subscriber. `matched_patterns` вычисляются правилами runtime (`*` — сегмент, `#` — ноль и более) по подпискам сервиса; фильтры не вычисляются. Payload кодируется схемой первой совпавшей подписки или передаётся как `Uint8Array`. |
| `DeliverOpts` | interface | — | `{ matchedPatterns?, attempt?, partitionKey?, headers? }`. |
| `DeliveryResult` | interface | — | `{ acked, reason, matchedPatterns }`; ни одного своего шаблона → nack «no handler for matched patterns». |
| `TestHarness.reset()` / `stop()` | метод | — | Забыть записанные вызовы и публикации / остановить бридж. |
| `matchPattern(pattern, name)` | function | — | Правило маршрутизации runtime. |
| `TEST_IDENTITY` | const | — | Идентичность in-memory сессии. |

### Паритет с Go `sbtest`

| Сценарий | Node | Go |
|----------|------|----|
| Создание | `createTestHarness()` → `h.sb` | `sbtest.New(t)` → `h.Client` |
| Запуск | `await h.start()` | `h.Start(ctx)` |
| Входящий unary / stream | `h.invoke`, `h.invokeStream` | `sbtest.Invoke`, `sbtest.InvokeStream` |
| ctx хендлера | `InvokeOpts` → `RpcHandlerContext` | `WithCaller`, `WithRequestID`, `WithIdempotencyKey` → `CallInfoFromContext` |
| Ошибка хендлера | `HandlerError{handlerCode}` / `"INTERNAL"` | `*Error{Code: HANDLER}` + `HandlerError{Code}` / `"INTERNAL"` |
| Исходящий вызов | `h.respond`, `h.respondStream`, `h.calls()` | `sbtest.Respond`, `RespondStream`, `h.Calls()` |
| Без ответа | `NO_LIVE_INSTANCE` | `ErrNoResponse` |
| Публикация | `h.published()` | `h.Published()` |
| Доставка | `h.deliver()` → `{acked, reason, matchedPatterns}` | `h.Deliver()` → `DeliveryResult` |
| Матчинг шаблонов | `matchPattern` | `sbtest.MatchPattern` |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `ServiceBridge._startInMemory(rt)` | метод (`@internal`) | — | Запуск бриджа на in-memory runtime: `rt.rpc` (исходящие), `rt.events` (Publish/Subscribe), `rt.identity`. Возвращает handle реестра и реестр схем вызывающего. Используется только этим модулем. |

## Архитектурные решения и почему

**Настоящий клиент, подменённый транспорт.** Harness подменяет только сеть, всё остальное идёт продакшен-путём. Вызов хендлера напрямую проверял бы одну бизнес-логику, а ошибки схемы, маппинг ошибок на проводе и маршрутизация событий всплывали бы только в e2e. Сценарии совпадают с Go `sbtest`: оба SDK ведут себя одинаково.

**Ответы на исходящие вызовы обязательны.** Забытый `respond(...)` — ошибка теста, а не тихий `undefined`.

**Политика, фильтры, ретраи доставки не эмулируются.** Это поведение runtime; его место — e2e против настоящего runtime.

## Зависимости

- Использует: `connection/service-bridge` (`ServiceBridge`, `_startInMemory`), `events/subscriber`, `rpc/client` (`RpcCaller`, `SchemaRegistry`), `rpc/dispatch-port`, `registry/registry`, `errors`, `logger`, `pb/servicebridge/v1/events`.
- Используется: прикладными тестами через `service-bridge/testing` (см. `userDocs/testing.md`, `skill/reference/testing.md`).
