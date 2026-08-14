# events

## Зона ответственности

SDK-сторона Durable Events: domain namespace (`EventDomain`), публикация событий в локальный SQLite outbox (Publisher), фоновая доставка в runtime (Drainer), приём входящих событий через bidi Subscribe stream (Subscriber). Не работает напрямую с сетью — только через gRPC-клиент `EventsClient`. Wire format — Protobuf binary через `serde/` (ADR-0002); runtime трактует payload как opaque bytes. Routing и dedup живут на сервере (ADR-0002); handlers обязаны быть идемпотентны.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `EventDomain` | class | — | Domain namespace для событий. Доступен через `sb.event`. Реэкспортируется как type. |
| `EventDomain.define(name, spec?)` | метод | — | Декларирует published event. `spec` — `SchemaSpec` (`.proto` файл или `.schema.json` c явными `fieldNumber`), тот же что у `sb.rpc.handle`; формат требует пару input/output, но событие использует только input — output не кодируется, не декодируется и не входит в `contract_hash`. Повторный `define` с тем же объектом `spec` (или оба без spec) — no-op; с другим spec — throws. Без spec регистрируется только имя (schema-less): `publish` такого события бросает «no schema registered», а subscriber отвечает `Nack` `no_schema` — реальная публикация/доставка требует spec. |
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
| `DrainerDeps.onPolicyViolation` | `((v) => void)?` | `undefined` | Вызывается при `PUBLISH_STATUS_REJECTED_FORBIDDEN` (запрет `event.publish` на стороне runtime). Forbidden — терминальный статус: строка outbox помечается `failed` (`last_error='forbidden:<reason>'`), без ретраев. Публикация в outbox асинхронна, поэтому ошибку нельзя бросить в `publish()` — она всплывает через этот callback + `logger.warn`. Вызывается ПОСЛЕ коммита батч-транзакции: колбэк сам публикует событие и иначе реентерил бы `storage.transaction()`. Owner подключает его к эмиту `policy_violation`. |
| `Subscriber` | class | — | Открывает long-lived bidi Subscribe stream; конструктор принимает `SubscriberDeps`. Жизненный цикл стрима держит `registry/StreamSupervisor`. |
| `Subscriber.start()` | `() => void` | — | Запускает supervisor: открывает стрим и шлёт `SubscribeInit` первым фреймом. |
| `Subscriber.stop()` | `async () => void` | — | Отменяет reconnect-таймер, `cancel()` стрима, глушит дальнейшие реконнекты. |
| `SubscriberDeps` | interface | — | Зависимости Subscriber. Поля: `rpcClient`, `schemaIndex`, `identity`, `handlers`, `maxInFlight?`, `logger?`, `sb?`, `reconnectOpts?`, `onSchedule?`, `runWithTrace`. |
| `SubscriberDeps.maxInFlight` | `number?` | `32` (`ServiceBridgeOptions.eventsMaxInFlight`) | Макс. параллельных доставок (объявляется серверу в `SubscribeInit.max_in_flight`). |
| `SubscriberDeps.logger` | `Logger?` | `{ warn: console.warn, error: console.error }` | Логгер для ошибок stream/ack/nack. |
| `SubscriberDeps.sb` | `ServiceBridge?` | `undefined` | Reserved. EVENT.DELIVER op пишет runtime (ADR 0007 §5); SDK только ack/nack обратно. |
| `SubscriberDeps.runWithTrace` | `(xSbTrace, fn) => Promise<void>` | — (обязателен) | Оборачивает handler в ALS trace scope из `envelope.x_sb_trace`, чтобы вложенные RPC/event-публикации наследовали trace. |
| `uuidv7()` | `() => string` | — | Реэкспорт `uuidv7()` из npm-пакета `uuidv7` (монотонна в пределах ms через внутренний counter; работает под Node и Bun). |
| `InvalidEventNameError` | class | — | Бросается при невалидном имени события. |
| `OutboxFullError` | class | — | Бросается при превышении `maxOutboxRows`. |
| `Logger` | interface | — | `{ warn, error }` — единый logger contract для Publisher / Drainer / Subscriber. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `PublisherDeps` | interface (`@internal`) | — | Зависимости Publisher: `storage`, `rpcClient`, `schemaIndex`, `drainer`, `identity`, `maxOutboxRows`, `logger`, `sb?`, `xSbTraceFn`. Граф собирается в `connection/service-bridge.ts`. |
| `PublisherDeps.maxOutboxRows` | `number` | `100000` (`ServiceBridgeOptions.maxOutboxRows`) | Cap строк в outbox; при достижении — `OutboxFullError`. |
| `PublisherDeps.sb` | `ServiceBridge?` | `undefined` | Reserved; событийные ops пишет сам runtime (ADR 0007 §5). |
| `PublisherDeps.xSbTraceFn` | `() => string` | — (обязателен) | Возвращает текущий X-SB-Trace header (из ALS) для каждого publish. Пустая строка → runtime минтит свежий root trace на ingest (ADR 0006 §3). |
| `SchemaIndex` | interface (`@internal`) | — | `{ get(name): { contractHash, pair } \| undefined }` — schema-lookup для Publisher. |
| `DrainerHandle` | interface (`@internal`) | — | `{ kick() }` — edge-triggered wakeup, который Publisher дёргает после INSERT в outbox. |
| `SubscriberDeps.runWithTrace` | callback | — | (описан в публичном контракте; реализация — `@internal` hook composition root'а.) |
| `SubscriberDeps.reconnectOpts` | `ReconnectDelayOptions?` (`@internal`) | общая лестница + ±20% jitter | Тестовый hook: пиннит лестницу/jitter, чтобы reconnect-поведение наблюдалось за миллисекунды. |
| `SubscriberDeps.onSchedule` | `((delayMs: number) => void)?` (`@internal`) | нет | Тестовый hook: наблюдает каждую задержку reconnect. См. `registry/README.md`. |
| `SubscriberSchemaIndex` | interface (`@internal`) | — | `{ get(name): { contractHash, pair } \| undefined }` — schema-lookup для Subscriber (decode входящих). |
| `SubscriberDeps.handlers` | `(pattern: string) => readonly EventHandlerFn[]` | — (обязателен) | Fan-out set для одного точного имени события. Композиционный корень отдаёт сюда `Handle.eventHandlers(pattern)` — индекс по pattern, поддерживаемый на регистрации. |
| `SubscriberIdentity` | interface (`@internal`) | — | `{ serviceId, instanceId }` — идентичность подписчика для `SubscribeInit`. |
| `DrainerDeps.clockFn` | `(() => number)?` | `Date.now` | Test-only hook: источник текущего времени unix-ms. |
| `DrainerDeps.sleepFn` | `((ms: number, signal: AbortSignal) => Promise<void>)?` | `setTimeout` | Test-only hook: задержка ожидания, отменяемая через `signal` при `kick()`. |
| `OutboxRow` | interface (`@internal`) | — | Строка `event_outbox`, селектится drainer'ом. |
| `EVENT_NAME_RE` | `RegExp` | — | `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$` (единственный источник — `publisher.ts`). |
| `BACKOFF_MS` | `number[]` | `[1000, 5000, 30000, 120000, 600000]` | Drainer retry-лестница, ±25% jitter. Последняя ступень насыщается — транзиентные отказы ретраятся бесконечно. |
| `MAX_BACKOFF_MS` | `number` | `600000` | Задержка, которую переиспользует любая попытка за пределами лестницы. |
| `SELECT_DUE_SQL` | `string` (`@internal`, экспортирован) | — | Запрос дренажа: `status='pending' AND next_attempt_at_ms <= ?`, `ORDER BY enqueued_at_ms, id`, `LIMIT ?`. Порядок колонок совпадает с индексом `event_outbox_pending_order_idx`, поэтому SQLite идёт по индексу и `LIMIT` обрывает скан — без temp b-tree сортировки всего бэклога. Экспортирован ради теста плана запроса. |
| `DEFAULT_MAX_IN_FLIGHT` | `number` | `32` | Дефолт `Subscriber.maxInFlight` при отсутствии явного значения. |

## Архитектурные решения и почему

**Wire format — Protobuf через serde/ (ADR-0002).** `EventDomain.define(name, spec)` принимает тот же `SchemaSpec`, что и `sb.rpc.handle`. `buildSchemaPair(spec)` строит Protobuf encoder через `protobufjs`; `pair.input.encode()` валидирует payload (`type.verify()`) и кодирует в binary. Runtime не декодит payload — это passthrough bytes. Inline JSON Schema не поддерживается.

**payload_json рядом с canonical payload.** Publisher кладёт JSON-вид того же payload в `EventEnvelope.payload_json` (через `JSON.stringify`, пустые байты если payload не сериализуем). Runtime использует его только для JSON-path `wait_event` фильтров в workflow-роутере, не декодя protobuf-форму.

**Schema loading async, finalize() ждёт.** `Registry._handle.publishEvent(name, spec)` синхронно регистрирует декларацию и кладёт promise в общий `pending[]`. `finalize()` (из `sb.start()`) await'ит все pending до построения `RegisterRequest`. Это унифицирует загрузку event- и rpc-схем.

**Идентичность события считается только по payload.** `SchemaSpec` описывает пару input/output, но событию отвечать некому: encode и decode идут через `pair.input`, в `RegisterRequest` едет только `input_schema_json`, а объявленный output не участвует ни в чём. Поэтому `contract_hash` события — `computeEventContractHash(pair.input)`: payload против пустого message. Так же считает Go SDK (`serde.EventContractHash`, вторая половина — `google.protobuf.Empty`), поэтому одна и та же схема даёт один и тот же хеш в обоих SDK; общий golden-вектор — `event_payload` в `sdk/contract-hash-vectors.json`.

**SchemaIndex backed by `getPublishedEvent`.** Publisher и Subscriber получают адаптер, читающий `Handle.getPublishedEvent(name)`. Локальная декларация — единственный источник правды для encode/decode pair; schemaIndex не делится между процессами. Подписчик чужого события сам объявляет `define(name, spec)` с той же схемой.

**UUID v7 — npm-пакет `uuidv7`.** Пакет хранит монотонный counter внутри процесса (sequential id'ы в пределах одной ms), даёт ту же защиту от clock skew, что требует ADR-0006, и работает под чистым Node (`node:crypto`) и под Bun одинаково. `ids.ts` реэкспортирует `uuidv7` как единую точку входа SDK; реализация не дублируется.

**Subscriber dispatch по exact `event.name`** — никакого client-side AMQP matcher'а, никакого Seen dedup (ADR-0002). Routing — `registry.TopicMatch` на сервере. Fan-out set берётся одним `Handle.eventHandlers(name)` — Map-индекс, поддерживаемый на регистрации. Раньше на КАЖДУЮ доставку пересобирался весь список: фильтр по `_entries`, `map` со свежим объектом `{pattern, fn}` на каждый зарегистрированный обработчик и третий фильтр по имени — 60 аллокаций и три массива, чтобы найти один. Handler contract: at-least-once + idempotency required. События с непустым `partition_key` сериализуются в FIFO через per-partition promise-цепочку; пустой ключ обрабатывается параллельно.

**Ack/Nack семантика.** Успешный handler → `Ack`. Отсутствие envelope/схемы, decode-ошибка, throw из handler → `Nack` с причиной; ретраи и DLQ — на стороне runtime (events = статус доставки, не клиентские ретраи). Reconnect-счётчик сбрасывается синхронно при получении фрейма на стриме (доказательство, что стрим жив), а не из async-пути handler→ack — иначе сброс гонялся бы с инкрементом счётчика в обработчиках `error`/`end`. Чистое закрытие стрима счётчик НЕ сбрасывает.

**Drainer статусы (PublishStatus).** `ACCEPTED` и `REJECTED_DUPLICATE` → строка удаляется из outbox (успех/идемпотентный дубль). `REJECTED_INVALID_NAME` → `failed` (терминально). `REJECTED_FORBIDDEN` → `failed` + `onPolicyViolation` (терминально). `UNSPECIFIED` и сетевые ошибки → бесконечный retry с backoff (лестница насыщается на 10 мин), `failed` не выставляется никогда; `failed` бывает только терминальным — `REJECTED_INVALID_NAME` и `REJECTED_FORBIDDEN`. Outbox-колонка `status` принимает значения `pending`/`inflight`/`failed` — это локальные SQLite-состояния, не wire-статусы.

**Транспортный отказ не тратит бюджет попыток.** Раньше пять неудачных попыток помечали строку `failed` навсегда, а лестница `[1s,5s,30s,2m,10m]` исчерпывалась за 2 минуты 40 секунд: после трёх минут недоступности рантайма все буферизованные события молча умирали в локальном SQLite — при том что `publish()` уже вернул пользователю успех и пообещал durability. Outbox существует ровно для того, чтобы пережить даунтайм, поэтому лестница теперь только ограничивает частоту (последняя ступень повторяется бесконечно), а бюджет попыток тратят исключительно терминальные отказы — те, где ретрай ничего не изменит.

**Батч результатов — одна транзакция.** Результаты всего батча применяются в одной `storage.transaction()`: построчный autocommit давал бы по WAL-фрейму и commit-записи на событие. Успехи собираются в список и удаляются одним `DELETE ... WHERE id IN (...)`. Наблюдатели (`logger.warn`, `onPolicyViolation`) вызываются ПОСЛЕ коммита — `onPolicyViolation` сам публикует событие и реентерил бы транзакцию.

**Drainer edge-triggered kick.** При `pendingKick=true` в момент `kick()` wakeResolve вызывается немедленно; если kick приходит во время активной итерации — флаг сохраняется, следующий wait пропускается. Предотвращает потерю сигнала.

**Ожидание в простое — до ближайшего `next_attempt_at_ms`.** Drainer спрашивает у outbox минимальный `next_attempt_at_ms` среди `pending` и спит ровно до него, а не поллит по фиксированному интервалу. При пустом outbox ждёт только `kick()` — таймера нет вообще, простаивающий SDK не держит event loop. Таймер отменяется через `AbortSignal`, когда `kick()` выигрывает гонку.

**cap check + INSERT в одной `storage.transaction()`** — нативный SQLite-драйвер сериализует транзакции, исключая гонку при конкурентных publish.

**Файл outbox не мигрируется.** Версия схемы штампуется в файл через `PRAGMA user_version` (владелец — `sqlite/`); файл с любой другой версией не открывается, `Storage.open` бросает с инструкцией остановить сервис и удалить каталог. Совместимости старых файлов нет: события, оставшиеся в старом outbox, теряются. Громкая ошибка вместо тихого `ALTER TABLE` — молчаливая миграция дала бы схему, о которой ни одна из сторон не знает точно.

**fireAndForget bypass** — напрямую в `rpcClient.publish`, без записи в outbox и без kick. Для use-case без требования durability.

**Жизненный цикл стрима — `registry/StreamSupervisor`.** Subscriber даёт супервизору только `open()` (subscribe + `SubscribeInit`) и `onData()` (обработка delivery); stop-флаг, единственный pending-таймер, identity-guard стрима и счётчик попыток на лестнице `utils/reconnect-ladder` (1s, 5s, 15s, 30s, 60s + удержание максимума, ±20% jitter) живут там же и одинаково у job- и workflow-подписчиков. `identity()` без значения → `open()` возвращает `null`, супервизор ждёт следующую ступень.

**Trace propagation (ADR 0007 §5, ADR 0006 §3).** Publisher кладёт текущий X-SB-Trace в `EventEnvelope.x_sb_trace` ("traceID-parentOpID"); runtime связывает EVENT.PUBLISH op в существующее trace-дерево, либо минтит свежий root при пустом trace. Subscriber читает `envelope.x_sb_trace` (DELIVER-level header от runtime) и оборачивает handler в `runWithTrace`, чтобы вложенные `sb.rpc.call` / `sb.event.publish` / `sb.workflow.start` наследовали trace. EVENT.DELIVER op-строку пишет runtime.

## Зависимости

- Использует: `sdk/node/src/sqlite/` (`Storage`), `sdk/node/src/pb/servicebridge/v1/events` (`EventsClient`, `PublishStatus`, `SubscribeClientMessage`, `Ack`, `Nack`, `SubscribeInit`), `sdk/node/src/connection/service-bridge` (`Identity`, `ServiceBridge`), `sdk/node/src/registry/registry` (`Registry`, `EventHandlerFn`), `sdk/node/src/registry/stream-supervisor` (`StreamSupervisor`), `sdk/node/src/serde/serializer` (`SchemaSpec`, `SchemaPair`), `sdk/node/src/utils/reconnect-ladder` (тип `ReconnectDelayOptions`; сама лестница крутится внутри `StreamSupervisor`), npm-пакет `uuidv7`.
- Используется: `sdk/node/src/connection/service-bridge.ts` (собирает Publisher, Drainer, Subscriber, EventDomain), `sdk/node/index.ts` (реэкспортирует `EventDomain`, `PublishOpts`, `InvalidEventNameError`, `OutboxFullError`).
