# sbtest

## Зона ответственности

Юнит-тесты хендлеров без рантайма: `sbtest.New` строит настоящий `*servicebridge.Client`, у которого сетевые края заменены памятью (`internal/testkit`). Хендлеры, подписки и зависимости объявляются через обычный API клиента; вызов хендлера, доставка события, исходящий вызов и публикация идут через тот же код SDK, что и в проде, — кодирование proto в обе стороны, обёртку хендлера, маппинг ошибок, очередь публикаций, маршрутизацию по `matched_patterns`.

Не воспроизводит рантайм: политику доступа, фильтры подписок, лизы, ретраи, размыкатели, DLQ, workflow, jobs.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `New(t TB, opts ...servicebridge.Option) *Harness` | функция | — | Клиент с in-memory транспортом; останавливается в `t.Cleanup`. `opts` — обычные опции клиента. |
| `TB` | интерфейс | — | `Helper`, `Fatalf`, `Cleanup` — его удовлетворяет `*testing.T`. |
| `Harness.Client` | `*servicebridge.Client` | — | Настоящий клиент: `servicebridge.Handle`, `HandleStream`, `SubscribeEvent`, `NewMethod`, `DefineEvent` и т. д. |
| `Harness.Start(ctx) error` | метод | — | Запечатывает объявления и делает клиент готовым (как `Client.Start`, без сети). |
| `Harness.Reset()` | метод | — | Забывает ответы и записи. |
| `Invoke[Req, Resp](ctx, h, method, req, ...InvokeOption) (Resp, error)` | функция | — | Вызов унарного хендлера как от пира: `req` кодируется, диспетчер клиента декодирует, вызывает, кодирует ответ, ответ декодируется в `Resp`. Ошибка — как у вызывающего: `*HandlerError` хендлера → `*servicebridge.Error{Code: CodeHandler}` с `HandlerError` внутри (`errors.As`); другая ошибка или паника → `CodeHandler` с кодом `INTERNAL`; неизвестный метод → `CodeNotFound`; не декодируемый запрос → `CodeValidation`. |
| `InvokeStream[Req, Chunk](ctx, h, method, req, ...InvokeOption) ([]Chunk, error)` | функция | — | То же для `HandleStream`; чанки до сбоя возвращаются вместе с ошибкой. |
| `WithCaller(serviceID, instanceID)` · `WithRequestID(id)` · `WithIdempotencyKey(key)` | `InvokeOption` | `RequestID` — новый UUID | Что хендлер увидит в `servicebridge.CallInfoFromContext`. `Deadline` — дедлайн `ctx`; отмена `ctx` отменяет хендлер. |
| `Respond[Req, Resp](h, service, method, fn) error` | функция | — | Ответ на любой исходящий вызов `service/method` (`servicebridge.Call`, `Method.Call`, шаг workflow `call`). Запрос декодируется в `Req`, ответ кодируется из `Resp`. `*HandlerError` из `fn` — бизнес-код ответа, другая ошибка — `INTERNAL`. Повторный `Respond` заменяет ответ. |
| `RespondStream[Req, Chunk](h, service, method, fn) error` | функция | — | То же для исходящего `servicebridge.Stream`; `fn` возвращает чанки. |
| `Harness.Calls() []CallRecord` · `DecodeCall[T](CallRecord)` | метод, функция | — | Исходящие вызовы по порядку: `Service`, `Method`, `Payload`, `IdempotencyKey`, `BusinessKey`, `Transport`. Вызов без `Respond` тоже записывается и падает с `ErrNoResponse`. |
| `Harness.Published() []PublishedEvent` · `DecodePublished[T](PublishedEvent)` | метод, функция | — | События, прошедшие настоящую очередь публикаций и принятые in-memory рантаймом: `ID`, `Name`, `Payload`, `PayloadJSON`, `PartitionKey`, `IdempotencyKey`, `Headers`, `OccurredAtMs`. |
| `Harness.Deliver(ctx, name, payload, ...DeliverOption) (DeliveryResult, error)` | метод | — | Доставка как от рантайма: `matched_patterns` — шаблоны подписок клиента, совпавшие с `name` по правилам рантайма (`*` — сегмент, `#` — ноль и более); подписчик запускает обработчики именно этих шаблонов. `DeliveryFromContext` работает. |
| `DeliveryResult` | struct | — | `Acked`, `Reason` (причина nack; `no handler for matched patterns`, если совпадений нет), `MatchedPatterns`. |
| `WithMatchedPatterns(...)` · `WithAttempt(n)` · `WithDeliveryPartitionKey(k)` · `WithDeliveryHeaders(h)` | `DeliverOption` | attempt `1` | Явные шаблоны (так воспроизводится решение фильтра рантайма) и поля доставки. |
| `MatchPattern(pattern, name) bool` | функция | — | Правило маршрутизации рантайма — замена рантайма в тестах. |
| `ErrNoResponse` · `ErrInvalidArg` | `error` | — | Отказы самого двойника. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `internal/testkit.Memory` | struct | — | Шов: двойник даёт `Call`, `Stream`, `Publish`; клиент отдаёт `Unary`, `ServeStream`, `Deliver`, `Subscriptions`, `Wrap`. |
| `internal/testkit.NewOption` | `func(*Memory) any` | ставит корневой пакет в `init` | Опция клиента с in-memory транспортом; `any`, потому что `testkit` не может импортировать корневой пакет. |
| `testKey()` | функция | — | Синтаксически валидный bootstrap-ключ: in-memory клиент ничего не провижинит. |
| `Harness.call` · `stream` · `publish` · `record` · `wireError` · `remoteFailure` | методы, функции | — | Ответы, записи и маппинг на форму ошибки вызывающего. |

## Паритет с Node (`service-bridge/testing`)

| Сценарий | Go (`sbtest`) | Node |
|----------|---------------|------|
| Двойник строится вокруг | настоящего `*servicebridge.Client` с in-memory транспортом | настоящего клиента с in-memory транспортом |
| Регистрация хендлеров, подписок, зависимостей | обычный API клиента (`servicebridge.Handle`, `SubscribeEvent`, `NewMethod`) | обычный API клиента (`sb.rpc.handle`, `sb.event.handle` со схемой) |
| Вызов хендлера | `sbtest.Invoke` / `InvokeStream`, encode/decode proto в обе стороны | `invoke` / `invokeStream` через реальный dispatch |
| Ошибка хендлера | `CodeHandler` + `HandlerError{Code}` (свой код или `INTERNAL`); неизвестный метод `NOT_FOUND`; битый запрос `VALIDATION` | то же |
| Контекст хендлера | `CallInfoFromContext`: `RequestID`, `IdempotencyKey`, `CallerServiceID/InstanceID`, `Deadline`; отмена `ctx` | `ctx`: `requestId`, `idempotencyKey`, `caller`, `deadline`, `signal` |
| Исходящий вызов | `Respond` / `RespondStream` по `service/method`; без ответа — `ErrNoResponse`; записи `Calls()` | `mockResponse`; без мока — ошибка; `calls()` |
| Публикация | через настоящую очередь; `Published()` + `DecodePublished` | через настоящий publisher; `published()` |
| Доставка | `Deliver`, `matched_patterns` по правилам рантайма, `WithMatchedPatterns` | `deliver`, те же правила, явные шаблоны |
| Результат доставки | `{Acked, Reason, MatchedPatterns}`; нет совпадений — nack `no handler for matched patterns` | то же |
| Не покрыто | политика, фильтры, ретраи, лизы, workflow, jobs | то же |

## Архитектурные решения и почему

**Двойник — настоящий клиент, а не параллельный API.** Отдельный набор `Handle`/`Invoke` с типостёртыми функциями проверял бы код двойника, а не SDK: маппинг ошибок, кодирование, маршрутизация событий и очередь публикаций существовали бы в двух копиях, и тест зеленел бы там, где прод падает. Здесь подменены только сетевые края.

**Сериализация в обе стороны.** Хендлер получает то, что получил бы от пира, и вызывающий видит то, что увидел бы по сети, включая отказ от несовпадения типов.

**Ошибка — в форме вызывающего.** `Invoke` возвращает `*servicebridge.Error`, как `servicebridge.Call`. Бизнес-код достаётся через `errors.As` к `*HandlerError`; ошибка вложенного вызова, пробрасываемая хендлером как есть, отвечает `INTERNAL`, а не чужим бизнес-кодом.

**Маршрутизация событий — правилами рантайма.** Клиент шаблоны не сопоставляет; двойник играет рантайм и вычисляет `matched_patterns`. Фильтры не вычисляются — их решение задаётся `WithMatchedPatterns`.

**Забытый ответ падает.** `ErrNoResponse` вместо нулевого значения: молчаливый ноль прячет ошибку теста до утверждения, которое уже не называет причину.

## Зависимости

Опирается на: корневой пакет `servicebridge`, `internal/testkit` (шов), `internal/rpc` (`CallInfo`, `HandlerError`, `NewStream`), `internal/serde`, `internal/pb`, `github.com/google/uuid`.

На него опираются: тесты прикладного кода.
