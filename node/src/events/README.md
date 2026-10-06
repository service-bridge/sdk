# events

## Зона ответственности

SDK-сторона Durable Events: namespace `sb.event` (`EventDomain`), отправка событий в runtime с ожиданием подтверждения (`Publisher`) и приём доставок через bidi-стрим `Events.Subscribe` (`Subscriber`). Payload — Protobuf через `serde/` (ADR-0002); маршрутизация, фильтры, ретраи доставки и DLQ — на runtime. Не хранит события на диске: долговечность начинается с ACK runtime (событие в Postgres).

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `EventDomain.define(name, spec)` | метод | — | Объявляет публикуемое событие и его схему (`SchemaSpec`: `.proto` или `.schema.json`). Повтор с тем же объектом `spec` — no-op, с другим — `ValidationError`. Только для издателя. |
| `EventDomain.handle(pattern, fn, opts?)` | метод | — | Подписка на имя или AMQP-шаблон (`*` — сегмент, `#` — ноль и более). Один handler на шаблон в процессе (дубль — `ValidationError`). Подписка не попадает в `published`. |
| `EventHandlerOpts.schema` | `SchemaSpec?` | нет | Схема подписчика для декодирования payload. Без неё handler получает сырые байты (`Uint8Array`). |
| `EventHandlerOpts.filter` | `Record<string, unknown>?` | нет | Filter Expression: объект `{"$.path": literal}`, все условия — равенства; вычисляется runtime по JSON-виду payload до доставки (решение 7). Уходит строкой в `EventSubscription.filter`. Невалидный фильтр → runtime отклоняет регистрацию `INVALID_ARGUMENT` → bridge останавливается с `ValidationError`. |
| `EventHandlerFn` | `(payload, ctx) => void \| Promise<void>` | — | Handler. Бросок → Nack с текстом ошибки; ретраи/DLQ — на runtime. |
| `EventHandlerContext` | interface | — | `{ eventId, eventName, attempt, deliveryId, leaseToken, partitionKey, headers, occurredAtMs, signal }`. `signal` прерывается при обрыве стрима или остановке. |
| `EventDomain.publish(name, payload, opts?)` | `Promise<{ eventId }>` | — | Публикует и ждёт ACK runtime. До `start()` — `StateError`. |
| `PublishOpts.idempotencyKey` | `string?` | `""` | Дедуп на runtime: повтор с тем же содержимым — успех с id исходного события, с другим — `CONFLICT`. |
| `PublishOpts.partitionKey` | `string?` | `""` | FIFO-полоса: подписчик видит события одного ключа в порядке публикации. |
| `PublishOpts.headers` | `Record<string,string>?` | `{}` | Заголовки envelope. |
| `PublishOpts.occurredAtMs` | `number?` | `Date.now()` | Время события, unix-ms. |
| `PublishOpts.fireAndForget` | `boolean?` | `false` | Резолвится сразу после постановки в очередь, не ждёт ACK. Событие всё равно отправляется с ретраями, но теряется при падении процесса; терминальный отказ только логируется. |
| `InvalidEventNameError` | class | — | `code: "INVALID_EVENT_NAME"`. Имя вне `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`. |
| `ServiceBridgeOptions.publishTimeoutMs` | `number` | `30000` | Сколько publish ждёт ACK. Истёк до отправки — `TimeoutError` «not sent»; после — «outcome unknown» (повтор с тем же `idempotencyKey` безопасен). |
| `ServiceBridgeOptions.maxPendingPublishes` | `number` | `10000` | Предел очереди ожидающих ACK; сверх — `ServiceBridgeError` `QUEUE_FULL` (retryable). |
| `ServiceBridgeOptions.eventsMaxInFlight` | `number` | `32` | Параллельно обрабатываемые доставки (`SubscribeInit.max_in_flight`). |

Коды отказа publish: `REJECTED_DUPLICATE` → успех с `results[i].event_id` (id исходного события); `REJECTED_CONFLICT` → `CONFLICT`; `REJECTED_INVALID_NAME` → `InvalidEventNameError`; `REJECTED_FORBIDDEN` → `AccessDeniedError` + событие `policy_violation`; `UNSPECIFIED` или транспортная ошибка → повтор того же envelope (тот же id) с backoff 100/250/500/1000/2000/5000 мс до `publishTimeoutMs`.

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Publisher` | class (`@internal`) | — | Очередь + отправитель. `publish`, `kick()` (сразу повторить, вызывается на Welcome), `close(deadlineMs)` (дослать, остаток — `CONNECTION` «client stopped»), `pending()`. |
| `PublisherDeps` | interface (`@internal`) | — | `client()`, `schemaIndex`, `logger`, `timeoutMs`, `maxPending`, `xSbTraceFn()`, `onPolicyViolation`, `now?` (тест). |
| `SchemaIndex` | interface (`@internal`) | — | `get(name) → { contractHash, pair }` — схемы объявленных событий. |
| `Subscriber` | class (`@internal`) | — | `start`, `restart`, `drain(timeoutMs)` (новые доставки — Nack, ждать запущенные), `stop`. |
| `SubscriberDeps` | interface (`@internal`) | — | `client()`, `identity()`, `subscription(pattern)`, `maxInFlight`, `logger`, `runWithTrace`, `reconnectOpts?`, `onSchedule?` (тест). |
| `DEFAULT_PUBLISH_TIMEOUT_MS` / `DEFAULT_MAX_PENDING_PUBLISHES` / `DEFAULT_EVENTS_MAX_IN_FLIGHT` | const | `30000` / `10000` / `32` | Дефолты, те же в Go SDK. |
| `uuidv7()` | function | — | Реэкспорт пакета `uuidv7`: монотонные id в порядке publish (runtime упорядочивает партицию по id). |

## Архитектурные решения и почему

**ACK вместо локального outbox (решение 4).** Сервисы stateless: локальный диск в контейнере эфемерен, общий SQLite-файл нарушал FIFO, тянул нативную зависимость и всё равно не решал dual-write. Поэтому `publish` резолвится только после того, как runtime записал событие в Postgres; на время обрыва — ограниченная очередь в памяти с повтором и таймаутом, при переполнении/таймауте — типизированная ошибка вызывающему. Никаких «тихих» буферов, где publish возвращает успех до ACK, кроме явного `fireAndForget`.

**Порядок по partition key.** В полёте один запрос `Publish`, в запросе не больше одного события на непустой ключ: если событие ключа получило временный отказ, следующие события того же ключа ждут за ним и не могут его обогнать. События без ключа идут пачкой до 100. Старт отправки откладывается на микротаск — публикации одного тика уходят одним запросом.

**Ответы сопоставляются по позиции.** `results[i]` отвечает `events[i]` (гарантия runtime): для дубля runtime возвращает id исходного события, поэтому id не может быть ключом сопоставления.

**Подписчик со своей схемой, маршрутизация по `matched_patterns` (NSDK-08).** Подписчик не объявляет чужое событие (раньше это давало ложное ребро «публикует» в Service Map) и не матчит шаблоны сам (ADR-0002): runtime присылает в доставке список совпавших шаблонов этого сервиса, прошедших фильтр, и SDK вызывает handler каждого, который есть у процесса. Ни одного — Nack (rolling deploy: доставка уйдёт на повтор, вероятно, к другому инстансу). Ack — только если все вызванные handler'ы успешны.

**payload_json всегда.** JSON-вид payload заполняется при каждой публикации: по нему runtime вычисляет фильтры подписок и `wait_event` workflow.

**Drain при остановке.** `Subscriber.drain` перестаёт брать новые доставки (Nack, runtime передоставит другому инстансу), а стрим остаётся открытым, пока запущенные handler'ы не отправят свои Ack.

**Жизненный цикл стрима — `registry/StreamSupervisor`.** Лестница переподключения `utils/reconnect-ladder` (1s, 5s, 15s, 30s, 60s ±20%), identity-guard стрима, один таймер; общая с jobs и workflow.

**Trace propagation (ADR 0006 §3).** Publisher кладёт X-SB-Trace текущего ALS-контекста в envelope; Subscriber запускает handler в trace-контексте доставки.

## Зависимости

- Использует: `pb/servicebridge/v1/events`, `registry/registry` (`SubscriptionEntry`, `EventHandlerFn`), `registry/stream-supervisor`, `serde/serializer`, `utils/semaphore`, `errors`, `logger`, npm-пакет `uuidv7`.
- Используется: `connection/service-bridge.ts` (собирает Publisher, Subscriber, EventDomain), `index.ts` (`EventDomain`, `PublishOpts`, `InvalidEventNameError`, типы handler'а).
