# events

## Зона ответственности

События Go SDK: публикация через очередь в памяти и единственного отправителя, который держит в полёте одну пачку `Events.Publish` до подтверждения рантаймом; подписка через двунаправленный стрим `Events.Subscribe` с маршрутизацией по `matched_patterns`, FIFO по ключу партиции и реальным ограничением конкурентности.

Не делает: не хранит события на диске (ничего не переживает падение процесса), не открывает соединение и не держит mTLS (`internal/connection`), не решает, когда переоткрывать стрим (`internal/stream`), не объявляет подписки рантайму (`internal/registry`), не сопоставляет шаблоны с именами (это делает рантайм), не эмитит операции телеметрии — `EVENT.PUBLISH` и `EVENT.DELIVER` принадлежат рантайму (ADR-0001).

## Публичный контракт

Публичный для других пакетов SDK; наружу через корневой `servicebridge` не реэкспортируется.

### Общее

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `ValidEventName(name) bool` | функция | — | `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$` — правило рантайма. |
| `ValidEventPattern(pattern) bool` | функция | — | То же плюс сегменты `*` (ровно один) и `#` (ноль или более). |
| `Identity` | struct | — | `ServiceID`, `InstanceID` живой сессии для кадра инициализации подписки. |
| `Encoded` | struct | — | `Proto`, `JSON` (зеркало для фильтров рантайма), `ContractHash`. |
| `Codec` | интерфейс | — | `Encode(name, payload any) (Encoded, error)` · `Decode(name, payload, out) error`. |
| `PublishFunc` | `func(ctx, *pb.PublishRequest) (*pb.PublishResponse, error)` | — | Единственный унарный вызов пакета. |
| `ErrInvalidName` · `ErrInvalidConfig` · `ErrAlreadyStarted` | `error` | — | Общие отказы. |

### Публикация (`publisher.go`)

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `NewPublisher(PublisherConfig) (*Publisher, error)` | функция | — | Проверяет конфиг, подставляет дефолты. |
| `(*Publisher) Publish(ctx, name, payload, ...PublishOption) (string, error)` | метод | — | Ставит событие в очередь и ждёт подтверждения рантайма; возвращает id, который хранит рантайм. |
| `(*Publisher) Start(ctx) error` | метод | — | Запускает отправителя. |
| `(*Publisher) Kick()` | метод | — | Отправитель, ждущий ступень повтора, пробует сразу (вызывается на каждом переподключении). |
| `(*Publisher) Close(ctx)` | метод | — | Прекращает приём, досылает очередь до конца `ctx`, остаток завершает `ErrStopped`. |
| `(*Publisher) Pending() int` | метод | — | Сколько событий ждут подтверждения. |
| `cfg.Codec` · `cfg.Publish` | — | — (обязательные) | Кодек и транспорт. |
| `cfg.MaxPending` | `int` | `10000` | Предел очереди; сверх него — `ErrQueueFull` сразу. |
| `cfg.Timeout` | `time.Duration` | `30s` | Предел одной публикации от постановки до подтверждения. |
| `cfg.BatchSize` | `int` | `100` | Событий в одном запросе. |
| `cfg.RetryLadder` | `[]time.Duration` | `100 · 250 · 500 · 1000 · 2000 · 5000 мс` | Паузы между повторами неподтверждённой пачки; последняя повторяется, успех сбрасывает. |
| `cfg.OnPolicyViolation` | `func(PolicyViolation)` | `nil` | Отказ политики наружу. |
| `cfg.Now` · `cfg.NewID` · `cfg.Logger` | — | часы · UUIDv7 · `slog.Default()` | Источники времени, id и лог. |
| `WithIdempotencyKey` · `WithPartitionKey` · `WithHeaders` · `WithOccurredAt` | `PublishOption` | — | Поля конверта. |
| `WithFireAndForget()` | `PublishOption` | `false` | Вернуть id сразу после постановки. Принимает потерю при падении процесса; сбой доставки только логируется. |
| `PolicyViolation` | struct | — | `EventID`, `EventName`, `Reason`. |
| `ErrQueueFull` | `error` | — | Очередь полна. |
| `ErrNotSent` · `ErrOutcomeUnknown` | `error` | — | Таймаут до первой отправки / после неё. |
| `ErrConflict` · `ErrForbidden` | `error` | — | Вердикты `REJECTED_CONFLICT` / `REJECTED_FORBIDDEN`. |
| `ErrStopped` | `error` | — | Событие осталось в очереди при остановке клиента. |
| `DefaultMaxPending` · `DefaultPublishTimeout` · `DefaultBatchSize` | константы | `10000` · `30s` · `100` | Дефолты. |

Вердикты рантайма: `ACCEPTED` — успех; `REJECTED_DUPLICATE` — успех, id = `results[].event_id` (исходный); `REJECTED_CONFLICT` → `ErrConflict`; `REJECTED_INVALID_NAME` → `ErrInvalidName`; `REJECTED_FORBIDDEN` → `ErrForbidden` + `OnPolicyViolation`; `UNSPECIFIED`, отсутствие вердикта и транспортная ошибка → повтор того же конверта с тем же id.

### Подписка (`subscriber.go`)

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `NewSubscriber(SubscriberConfig) (*Subscriber, error)` | функция | — | Подписчик поверх одного `stream.Supervisor`. |
| `Subscribe[T any](s, pattern, filter string, fn Handler[T]) error` | функция | — | Обработчик шаблона. Один шаблон — один обработчик (`ErrDuplicatePattern`). `filter` — JSON фильтр-выражение или пусто. |
| `Handler[T any]` | `func(ctx, event T) error` | — | Обработчик одного декодированного события. |
| `Subscription` | `struct{Pattern, Filter string}` | — | Объявленная подписка. |
| `(*Subscriber) Subscriptions() []Subscription` | метод | — | Все подписки, отсортированные, — для `RegisterRequest`. |
| `(*Subscriber) Handle(ctx, *pb.EventDelivery) (acked bool, reason string)` | метод | — | Контракт одной доставки без стрима: обработчики всех совпавших шаблонов; ack, только если все успешны. Им пользуется `sbtest`. |
| `(*Subscriber) Start(ctx)` · `Stop()` | методы | — | Стрим и его остановка с ожиданием обработчиков. |
| `(*Subscriber) Drain()` · `Wait(ctx) error` | методы | — | Перестать брать новые доставки; дождаться работающих. |
| `DeliveryInfo` | struct | — | `EventID`, `EventName`, `Attempt`, `DeliveryID`, `LeaseToken`, `PartitionKey`, `Headers`, `OccurredAtMs`. |
| `DeliveryFromContext(ctx) (DeliveryInfo, bool)` | функция | — | Метаданные доставки в обработчике. |
| `NoHandlerReason` | `string` | `no handler for matched patterns` | Причина nack, когда ни один совпавший шаблон не обслуживается. |
| `ErrDuplicatePattern` | `error` | — | Второй обработчик того же шаблона. |
| `SubscribeStream` | интерфейс | — | Клиент bidi-стрима. |
| `cfg.Open` · `cfg.Codec` · `cfg.Identity` | — | — (обязательные) | Стрим, кодек, идентичность. |
| `cfg.MaxInFlight` | `int` | `32` (макс. 1024) | Предел одновременных доставок. |
| `cfg.Backoff` · `cfg.OnError` · `cfg.Logger` | — | общий · `nil` · `slog.Default()` | Переподключение и диагностика. |
| `DefaultMaxInFlight` | `int` | `32` | Дефолт. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `entry` | struct | — | Одна публикация в очереди: конверт, флаги `inflight`/`sent`/`abandoned`/`resolved`, результат. |
| `(*Publisher) run` · `take` · `settle` · `release` | методы | — | Цикл отправителя, выбор пачки, применение вердиктов, возврат пачки без вердикта. |
| `(*Publisher) abandon` · `resolveLocked` · `unflightLocked` · `removeLocked` | методы | — | Отказ ожидающего, завершение, возврат в очередь, удаление. |
| `requestTimeout` | `time.Duration` | `10s` | Предел одного RPC `Publish`. |
| `defaultRetryLadder()` · `signal(ch)` · `traceHeader` · `newEventID` | функции | — | Лестница, неблокирующий сигнал, заголовок трейса, UUIDv7. |
| `rawHandler` · `invokeHandler` | тип, функция | — | Обработчик на байтах; паника → nack. |
| `(*Subscriber) handlersFor(matched)` | метод | — | Обработчики совпавших шаблонов, каждый один раз. |
| `(*Subscriber) open` · `onData` · `process` · `acquire` · `release` · `chains` · `retireChain` · `traceContext` · `ack` · `nack` · `send` | — | — | Стрим, конкурентность, FIFO по ключу, запись. |

## Архитектурные решения и почему

**Публикация завершается подтверждением рантайма.** Когда `Publish` вернул id, событие лежит в Postgres рантайма. Локального outbox нет: durability на стороне SDK ничего не гарантировала, а ошибки скрывала. Пока рантайм недоступен, событие ждёт в очереди памяти — с пределом размера (`ErrQueueFull`) и пределом ожидания (`ErrNotSent`, если ни разу не отправлено; `ErrOutcomeUnknown`, если отправлено и не подтверждено).

**Одна пачка в полёте, одно событие на ключ партиции в пачке.** Следующее событие того же ключа ждёт следующей пачки, поэтому порядок каждого ключа сохраняется и при повторах. Пустой ключ порядка не требует.

**Повтор — тем же конвертом.** `UNSPECIFIED` (включая квоту публикаций рантайма) и транспортная ошибка возвращают событие в очередь под тем же id; рантайм дедуплицирует повтор. Лестница короткая (до 5 с) и сбрасывается на первом успехе и на переподключении.

**id — UUIDv7, монотонный в порядке публикации.** Выдаётся под тем же замком, что ставит событие в очередь.

**`payload_json` заполняется всегда.** По нему рантайм считает фильтры подписок и `wait_event` workflow.

**Fire-and-forget честно назван.** Возвращает id после постановки в очередь; событие живёт только в памяти процесса до подтверждения. Полная очередь всё равно отказывает.

**Маршрутизация — только по `matched_patterns`.** Рантайм перечисляет шаблоны подписчика, с которыми совпало событие; запускаются обработчики именно этих шаблонов, каждый один раз. Локального сопоставления шаблонов нет — оно расходилось бы с рантаймом. Ни один шаблон не обслуживается — nack с `NoHandlerReason`.

**Один обработчик на шаблон.** Рантайм хранит одну строку подписки на шаблон и отвергает дубль; несколько обработчиков на одно событие получаются разными шаблонами.

**Drain не тратит попытки.** При остановке новые доставки не берутся и не подтверждаются: рантайм отдаст их снова, когда стрим закроется. Nack сжёг бы попытку.

**Идентичность читается на каждое открытие стрима; переподключение — общий супервизор; FIFO по ключу и предел конкурентности — локально, предел реально останавливает чтение стрима; паника обработчика — nack; запись в стрим сериализована.**

## Зависимости

Опирается на: `internal/stream`, `internal/telemetry` (формат `X-SB-Trace`), `internal/pb`, `github.com/google/uuid`.

На него опираются: корневой пакет `servicebridge` (публикация, подписки, остановка), `sbtest` (`Subscriber.Handle`).
