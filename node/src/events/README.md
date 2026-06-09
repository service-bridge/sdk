# events

## Зона ответственности

SDK-сторона Durable Events: domain namespace (`EventDomain`), публикация событий в локальный SQLite outbox (Publisher), фоновая доставка в runtime (Drainer), приём входящих событий через bidi Subscribe stream (Subscriber). Не работает напрямую с сетью — только через gRPC-клиент `EventsClient`. Wire format — Protobuf binary через `serde/` (ADR-0002); runtime трактует payload как opaque bytes. Routing и dedup живут на сервере (ADR-0002); handlers обязаны быть идемпотентны.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `EventDomain` | class | — | Domain namespace для событий. Доступен через `sb.event`. Реэкспортируется как type. |
| `EventDomain.define(name, spec?)` | метод | — | Декларирует published event. `spec` — `SchemaSpec` (`.proto` файл или `.schema.json` c явными `fieldNumber`), тот же что у `sb.rpc.handle`. Повторный `define` с тем же объектом `spec` (или оба без spec) — no-op; с другим spec — throws. Без spec регистрируется только имя (schema-less): `publish` такого события бросает «no schema registered», а subscriber отвечает `Nack` `no_schema` — реальная публикация/доставка требует spec. |
| `EventDomain.handle(pattern, fn)` | метод | — | Регистрирует subscription (subscriber-side). `pattern` — точное имя или AMQP wildcard; матчинг выполняет сервер. |
| `EventDomain.publish(name, payload, opts?)` | `async (...) => { eventId }` | — | Публикует event. Требует `sb.start()` (иначе throws — publisher не готов). Encode идёт через Protobuf serde — `type.verify()` бросает на невалидный payload до записи в outbox. |
| `PublishOpts` | interface | — | Опции `publish`. Поля ниже. |
| `PublishOpts.idempotencyKey` | `string?` | `""` | Ключ идемпотентности; runtime дедупит по нему. |
| `PublishOpts.partitionKey` | `string?` | `""` | Ключ партиции; гарантирует FIFO-доставку внутри ключа. Пустой ключ — параллельная доставка. |
| `PublishOpts.fireAndForget` | `boolean?` | `false` | `true` — отправка напрямую в runtime, минуя outbox (без durability, без ретраев). |
| `PublishOpts.headers` | `Record<string,string>?` | `{}` | Произвольные заголовки envelope. |
| `PublishOpts.occurredAtMs` | `number?` | `Date.now()` | Время события, unix-ms. Уходит в `EventEnvelope.occurred_at_unix_ms`. |
| `Publisher` | class | — | Публикует события; конструктор принимает `PublisherDeps`. |
| `Publisher.publish(name, payload, opts?)` | `async (...) => { eventId }` | — | Валидирует имя по `EVENT_NAME_RE`, кодирует payload, пишет в outbox (durable) либо шлёт напрямую (`fireAndForget`). Бросает `InvalidEventNameError`, `OutboxFullError`, либо ошибку при отсутствии схемы. |
| `Drainer` | class | — | Фоновый loop; конструктор принимает `DrainerDeps`. |
| `Drainer.start()` | `() => void` | — | Запускает фоновый drain-loop (идемпотентен). |
| `Drainer.kick()` | `() => void` | — | Будит drainer из ожидания (edge-triggered). |
| `Drainer.stop()` | `async () => void` | — | Сигналит loop выйти; ждёт завершения текущей итерации. |
| `DrainerDeps` | interface | — | Зависимости Drainer. Публичные поля ниже; `clockFn`/`sleepFn` — test-only (см. приватный контракт). |
| `DrainerDeps.batchSize` | `number` | `50` (`ServiceBridgeOptions.eventsDrainerBatch`) | Сколько строк outbox забирать за итерацию. |
| `DrainerDeps.onPolicyViolation` | `((v) => void)?` | `undefined` | Вызывается при `PUBLISH_STATUS_REJECTED_FORBIDDEN` (запрет `event.publish` на стороне runtime). Forbidden — терминальный статус: строка outbox помечается `failed` (`last_error='forbidden:<reason>'`), без ретраев. Публикация в outbox асинхронна, поэтому ошибку нельзя бросить в `publish()` — она всплывает через этот callback + `logger.warn`. Owner подключает его к эмиту `policy_violation`. |
| `Subscriber` | class | — | Открывает long-lived bidi Subscribe stream; конструктор принимает `SubscriberDeps`. |
| `Subscriber.start()` | `() => void` | — | Запускает connect (no-op после `stop()`). |
| `Subscriber.stop()` | `async () => void` | — | Отменяет reconnect-таймер, закрывает stream. |
| `SubscriberDeps` | interface | — | Зависимости Subscriber. Поля: `rpcClient`, `schemaIndex`, `identity`, `handlers`, `maxInFlight?`, `logger?`, `sb?`, `runWithTrace`. |
| `SubscriberDeps.maxInFlight` | `number?` | `32` (`ServiceBridgeOptions.eventsMaxInFlight`) | Макс. параллельных доставок (объявляется серверу в `SubscribeInit.max_in_flight`). |
| `SubscriberDeps.logger` | `Logger?` | `{ warn: console.warn, error: console.error }` | Логгер для ошибок stream/ack/nack. |
| `SubscriberDeps.sb` | `ServiceBridge?` | `undefined` | Reserved. EVENT.DELIVER op пишет runtime (T-015); SDK только ack/nack обратно. |
| `SubscriberDeps.runWithTrace` | `(xSbTrace, fn) => Promise<void>` | — (обязателен) | Оборачивает handler в ALS trace scope из `envelope.x_sb_trace`, чтобы вложенные RPC/event-публикации наследовали trace. |
| `uuidv7()` | `() => string` | — | Реэкспорт `uuidv7()` из npm-пакета `uuidv7` (монотонна в пределах ms через внутренний counter; работает под Node и Bun). |
| `validEventName(name)` | `(string) => boolean` | — | Валидирует имя по regex `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`. |
| `InvalidEventNameError` | class | — | Бросается при невалидном имени события. |
| `OutboxFullError` | class | — | Бросается при превышении `maxOutboxRows`. |
| `Logger` | interface | — | `{ warn, error }` — единый logger contract для Publisher / Drainer / Subscriber. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `PublisherDeps` | interface (`@internal`) | — | Зависимости Publisher: `storage`, `rpcClient`, `schemaIndex`, `drainer`, `identity`, `maxOutboxRows`, `logger`, `sb?`, `xSbTraceFn`. Граф собирается в `connection/service-bridge.ts`. |
| `PublisherDeps.maxOutboxRows` | `number` | `100000` (`ServiceBridgeOptions.maxOutboxRows`) | Cap строк в outbox; при достижении — `OutboxFullError`. |
| `PublisherDeps.sb` | `ServiceBridge?` | `undefined` | Reserved; событийные ops пишет сам runtime (T-015). |
| `PublisherDeps.xSbTraceFn` | `() => string` | — (обязателен) | Возвращает текущий X-SB-Trace header (из ALS) для каждого publish. Пустая строка → runtime минтит свежий root trace на ingest (T-017). |
| `SchemaIndex` | interface (`@internal`) | — | `{ get(name): { contractHash, pair } \| undefined }` — schema-lookup для Publisher. |
| `DrainerHandle` | interface (`@internal`) | — | `{ kick() }` — edge-triggered wakeup, который Publisher дёргает после INSERT в outbox. |
| `SubscriberDeps.runWithTrace` | callback | — | (описан в публичном контракте; реализация — `@internal` hook composition root'а.) |
| `SubscriberSchemaIndex` | interface (`@internal`) | — | `{ get(name): { contractHash, pair } \| undefined }` — schema-lookup для Subscriber (decode входящих). |
| `EventHandler` | interface (`@internal`) | — | `{ pattern, fn }` — зарегистрированный обработчик; dispatch по exact `name`. |
| `SubscriberIdentity` | interface (`@internal`) | — | `{ serviceId, instanceId }` — идентичность подписчика для `SubscribeInit`. |
| `DrainerDeps.clockFn` | `(() => number)?` | `Date.now` | Test-only hook: источник текущего времени unix-ms. |
| `DrainerDeps.sleepFn` | `((ms) => Promise<void>)?` | `setTimeout` | Test-only hook: задержка в loop. |
| `OutboxRow` | interface (`@internal`) | — | Строка `event_outbox`, селектится drainer'ом. |
| `EVENT_NAME_RE` | `RegExp` | — | `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$` (в `event-name.ts` и `publisher.ts`). |
| `BACKOFF_MS` | `number[]` | `[1000, 5000, 30000, 120000, 600000]` | Drainer retry-лестница, ±25% jitter. |
| `MAX_ATTEMPTS` | `number` | `5` | Максимум попыток drainer'а перед `status='failed'`. |
| `RECONNECT_LADDER_MS` | `number[]` | `[1000, 5000, 15000, 30000, 60000]` | Subscriber reconnect-лестница (inline в `subscriber.ts`). |
| `DEFAULT_MAX_IN_FLIGHT` | `number` | `32` | Дефолт `Subscriber.maxInFlight` при отсутствии явного значения. |

## Архитектурные решения и почему

**Wire format — Protobuf через serde/ (ADR-0002).** `EventDomain.define(name, spec)` принимает тот же `SchemaSpec`, что и `sb.rpc.handle`. `buildSchemaPair(spec)` строит Protobuf encoder через `protobufjs`; `pair.input.encode()` валидирует payload (`type.verify()`) и кодирует в binary. Runtime не декодит payload — это passthrough bytes. Inline JSON Schema не поддерживается.

**payload_json рядом с canonical payload.** Publisher кладёт JSON-вид того же payload в `EventEnvelope.payload_json` (через `JSON.stringify`, пустые байты если payload не сериализуем). Runtime использует его только для JSON-path `wait_event` фильтров в workflow-роутере, не декодя protobuf-форму.

**Schema loading async, finalize() ждёт.** `Registry._handle.publishEvent(name, spec)` синхронно регистрирует декларацию и кладёт promise в общий `pending[]`. `finalize()` (из `sb.start()`) await'ит все pending до построения `RegisterRequest`. Это унифицирует загрузку event- и rpc-схем.

**SchemaIndex backed by `getPublishedEvent`.** Publisher и Subscriber получают адаптер, читающий `Handle.getPublishedEvent(name)`. Локальная декларация — единственный источник правды для encode/decode pair; schemaIndex не делится между процессами. Подписчик чужого события сам объявляет `define(name, spec)` с той же схемой.

**UUID v7 — npm-пакет `uuidv7`.** Пакет хранит монотонный counter внутри процесса (sequential id'ы в пределах одной ms), даёт ту же защиту от clock skew, что требует ADR-0006, и работает под чистым Node (`node:crypto`) и под Bun одинаково. `ids.ts` реэкспортирует `uuidv7` как единую точку входа SDK; реализация не дублируется.

**Subscriber dispatch по exact `event.name`** — никакого client-side AMQP matcher'а, никакого Seen dedup (ADR-0002). Routing — `registry.TopicMatch` на сервере. Handler contract: at-least-once + idempotency required. События с непустым `partition_key` сериализуются в FIFO через per-partition promise-цепочку; пустой ключ обрабатывается параллельно.

**Ack/Nack семантика.** Успешный handler → `Ack`. Отсутствие envelope/схемы, decode-ошибка, throw из handler → `Nack` с причиной; ретраи и DLQ — на стороне runtime (events = статус доставки, не клиентские ретраи). После успешной доставки subscriber сбрасывает reconnect-счётчик.

**Drainer статусы (PublishStatus).** `ACCEPTED` и `REJECTED_DUPLICATE` → строка удаляется из outbox (успех/идемпотентный дубль). `REJECTED_INVALID_NAME` → `failed` (терминально). `REJECTED_FORBIDDEN` → `failed` + `onPolicyViolation` (терминально). `UNSPECIFIED` и сетевые ошибки → retry с backoff до `MAX_ATTEMPTS`, затем `failed`. Outbox-колонка `status` принимает значения `pending`/`inflight`/`failed` — это локальные SQLite-состояния, не wire-статусы.

**Drainer edge-triggered kick.** При `pendingKick=true` в момент `kick()` wakeResolve вызывается немедленно; если kick приходит во время активной итерации — флаг сохраняется, следующий wait пропускается. Предотвращает потерю сигнала.

**cap check + INSERT в одной `storage.transaction()`** — нативный SQLite-драйвер сериализует транзакции, исключая гонку при конкурентных publish.

**fireAndForget bypass** — напрямую в `rpcClient.publish`, без записи в outbox и без kick. Для use-case без требования durability.

**Reconnect по фиксированной лестнице** (без рандомизации) — детерминированно для тестов: 1s, 5s, 15s, 30s, 60s, затем удержание максимума. Inline в `subscriber.ts`.

**Trace propagation (T-015, T-017).** Publisher кладёт текущий X-SB-Trace в `EventEnvelope.x_sb_trace` ("traceID-parentOpID"); runtime связывает EVENT.PUBLISH op в существующее trace-дерево, либо минтит свежий root при пустом trace. Subscriber читает `envelope.x_sb_trace` (DELIVER-level header от runtime) и оборачивает handler в `runWithTrace`, чтобы вложенные `sb.rpc.call` / `sb.event.publish` / `sb.workflow.start` наследовали trace. EVENT.DELIVER op-строку пишет runtime.

## Зависимости

- Использует: `sdk/node/src/sqlite/` (`Storage`), `sdk/node/src/pb/servicebridge/v1/events` (`EventsClient`, `PublishStatus`, `SubscribeClientMessage`, `Ack`, `Nack`, `SubscribeInit`), `sdk/node/src/connection/service-bridge` (`Identity`, `ServiceBridge`), `sdk/node/src/registry/registry` (`Registry`), `sdk/node/src/serde/serializer` (`SchemaSpec`, `SchemaPair`), npm-пакет `uuidv7`.
- Используется: `sdk/node/src/connection/service-bridge.ts` (собирает Publisher, Drainer, Subscriber, EventDomain), `sdk/node/index.ts` (реэкспортирует `EventDomain`, `PublishOpts`, `InvalidEventNameError`, `OutboxFullError`).
