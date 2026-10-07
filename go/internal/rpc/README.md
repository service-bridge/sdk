# rpc

## Зона ответственности

Обе стороны Direct RPC. Входящая: mTLS-сервер `servicebridge.v1.Call` инстанса SDK (`server.go`), проверка приёма (`acceptance.go`), индекс хендлеров (`dispatch.go`), `CallInfo` хендлера (`callinfo.go`). Исходящая: выбор инстанса (`lb.go`), размыкатель (`breaker.go`), повторы (`retry.go`), прямой транспорт (`direct.go`), прокси через рантайм (`proxy.go`) и цикл одного логического вызова (`client.go`).

Не делает: не сериализует бизнес-типы (сырые байты в обе стороны), не эмитит операцию телеметрии на стороне callee, не владеет mTLS-материалом (получает его через `connection.CredentialConsumer`).

## Публичный контракт

### Сервер (`server.go`, `callinfo.go`)

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `DefaultMaxConcurrentCalls` | `int` | `256` | Хендлеров одновременно. |
| `DefaultMaxQueuedCalls` | `int` | `256` | Вызовов, ждущих свободного слота. |
| `ServerLimits` | `struct{MaxConcurrentCalls, MaxQueuedCalls int}` | — | Границы входящей нагрузки. `MaxConcurrentCalls > 0`, `MaxQueuedCalls >= 0`; HTTP/2-стримов на соединение — сумма. |
| `DefaultServerLimits() ServerLimits` | функция | — | Границы по умолчанию. |
| `AdmissionSource` | интерфейс `Ready() bool; Policy() *pb.PolicyEvaluation; Revoked(serviceID, instanceID string) bool` | — | Что сервер читает из живого вида меша. Его удовлетворяет `registry.Cache`. |
| `ServerConfig.Host` | `string` | — (обязательный) | Анонсируемый адрес. |
| `ServerConfig.Port` | `int` | `0` | Порт; `0` — выбирает ОС. |
| `ServerConfig.Limits` | `ServerLimits` | — (обязательный) | Границы. |
| `ServerConfig.Dispatcher` | `*Dispatcher` | — (обязательный) | Индекс хендлеров. |
| `ServerConfig.Admission` | `AdmissionSource` | — (обязательный) | Готовность, правила приёма, отзывы. |
| `ServerConfig.Logger` | `*slog.Logger` | `slog.Default()` | Логгер. |
| `NewServer(cfg) (*Server, error)` | функция | — | Валидирует, ничего не биндит. |
| `Server.UseCredentials(ctx, connection.Credentials) error` | метод | — | Ставит текущий лист; следующий handshake отдаёт его. Слушатель не пересоздаётся. |
| `Server.Start(ctx) (string, error)` | метод | — | Биндит и обслуживает; идемпотентен; запечатывает `Dispatcher`. |
| `Server.Endpoint() (string, error)` | метод | — | Анонсированный адрес. |
| `Server.Drain()` | метод | — | Новые вызовы получают `UNAVAILABLE "draining"` с трейлером not-dispatched. |
| `Server.Wait(ctx) error` | метод | — | Ждёт завершения принятых вызовов или конца `ctx`. |
| `Server.Close(ctx) error` | метод | — | GracefulStop до конца `ctx`, затем Stop. |
| `CallInfo` | `struct{RequestID, IdempotencyKey, CallerServiceID, CallerInstanceID string; Deadline time.Time}` | — | Входящий вызов глазами хендлера. На прокси-пути `CallerServiceID` = `caller_service` запроса, `CallerInstanceID` пуст. |
| `CallInfoFromContext(ctx) (CallInfo, bool)` · `WithCallInfo(ctx, CallInfo)` | функции | — | Чтение/запись `CallInfo` в контексте. |
| `NotDispatchedKey` | `string` | `x-sb-not-dispatched` | Трейлер временного отказа до хендлера. |
| `ErrServerConfig` · `ErrServerNotStarted` · `ErrServerClosed` · `ErrNoCredentials` · `ErrOverloaded` | `error` | — | Отказы сервера. |

Отказы на проводе (одинаковы с Node SDK):

| Ситуация | Код | Трейлер `x-sb-not-dispatched: 1` |
|----------|-----|-------------------------|
| Сервер дренируется | `UNAVAILABLE "draining"` | да |
| Нет первого снапшота реестра | `UNAVAILABLE "not ready"` | да |
| Все слоты и вся очередь заняты | `RESOURCE_EXHAUSTED` | да |
| Личность не установлена | `UNAUTHENTICATED` | нет |
| Сервис или инстанс вызывающего отозван | `PERMISSION_DENIED` | нет |
| Правила приёма не пускают | `PERMISSION_DENIED` | нет |
| Метод не зарегистрирован | `NOT_FOUND` | нет |
| Не тот вид вызова | `FAILED_PRECONDITION` | нет |
| Payload не разобрался | `INVALID_ARGUMENT` | нет |
| Хендлер вернул `*HandlerError` | `OK`, `error_code = HandlerError.Code` | — |
| Хендлер вернул другую ошибку или паникнул | `OK`, `error_code = INTERNAL` | — |

Стрим отказывает статусом, не error-чанком.

### Приём (`acceptance.go`)

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `PeerKind` | `uint8` | `PeerUnknown` | `PeerUnknown` / `PeerService` / `PeerRuntime`. |
| `Peer` | `struct{Kind; ServiceID, InstanceID string}` | — | Опознанный вызывающий. |
| `IdentifyPeer(*x509.Certificate) (Peer, error)` · `PeerFromContext(ctx) (Peer, error)` | функции | — | Личность по единственному SPIFFE URI SAN проверенного листа. |
| `Allow(peer, method, *pb.PolicyEvaluation) error` | функция | — | Решение по правилам `rpc.handle`. Рантайм-прокси проходит. |
| `ErrPeerUnidentified` · `ErrAcceptanceDenied` | `error` | — | Отказы приёма. |

### Диспетчеризация (`dispatch.go`)

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `UnaryFunc` · `StreamFunc` · `Sender` | типы | — | Хендлеры на сырых байтах. |
| `Outcome` | struct | — | Форма ответа: `Status` (транспорт) и `ErrorCode` (ответ хендлера) — разные оси. |
| `NewDispatcher(log)` · `RegisterUnary` · `RegisterStream` · `Seal` · `Sealed` · `Methods` · `Unary` · `Stream` | — | — | Индекс хендлеров. |
| `ErrDecode` · `ErrNoHandler` · `ErrWrongKind` · `ErrPanic` · `ErrSealed` · `ErrDuplicate` · `ErrEmptyMethod` · `ErrNoFunc` | `error` | — | Отказы. |

### Исходящая сторона (`client.go`, `direct.go`, `proxy.go`, `lb.go`, `breaker.go`, `retry.go`)

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Transport` | `uint8` | `TransportAuto` | `TransportAuto` — прямой путь, переход на прокси после доказанного pre-dispatch отказа прямого; `TransportDirect` — только прямой; `TransportProxy` — только `Invoke` рантайма. |
| `CandidateSource` | интерфейс `Candidates`, `Instance`, `Revoked` | — | Чтение индекса реестра. |
| `ClientConfig` | struct | — | `Registry`, `Direct`, `Proxy` обязательны; `Balancer`, `Breaker`, `Retry`, `HealthHintTTLMs`, `Now`, `Logger` с умолчаниями. |
| `NewClient(ClientConfig) (*Client, error)` | функция | — | Исходящий путь. |
| `Request` | struct | — | `Service`, `Method`, `Payload`, `ContractHash`, `IdempotencyKey`, `BusinessKey`, `Transport`. |
| `Client.Unary(ctx, Request) ([]byte, error)` | метод | — | Один логический вызов, одна операция телеметрии; повторы только при доказанном pre-dispatch. |
| `Client.Stream(ctx, Request) (*Stream, error)` | метод | — | Server-stream. Не повторяется. |
| `Stream` · `NewStream(recv, stop)` | тип, функция | — | Поток чанков, ресурсы отдаются ровно раз. `NewStream` нужен и in-memory транспорту `sbtest`. |
| `HandlerError` | `struct{Code, Message string}` | — | Бизнес-ответ хендлера; на проводе `error_code`/`error_message`. Публичный `servicebridge.HandlerError` — алиас. |
| `SelectionError` | struct | — | Почему не выбран инстанс: `ErrNoCandidates` / `ErrNoEndpoint` / `ErrAllUnavailable`. |
| `NotDispatchedError` | `struct{Err error; Direct bool}` | — | Доказанный отказ до хендлера: канал к callee не стал ready или ответ с трейлером. `Direct` — доказательство от прямого пути. |
| `PreDispatch(err) bool` | функция | — | Можно ли повторить: `SelectionError` или `NotDispatchedError`. |
| `ErrPeerUnreachable` | `error` | — | Канал к callee не стал ready до отправки. |
| `RetryPolicy` · `DefaultRetryPolicy()` | struct, функция | 3 попытки, 200 мс × 2, потолок 5 с, джиттер 0.3 | Лестница повторов. |
| `Balancer` · `NewBalancer` · `Candidate` · `PickStats` · `Eligible` · `Healthy` · `DefaultHealthHintTTLMs` | — | TTL подсказки 60 с | P2C по in-flight. |
| `Breaker` · `NewBreaker` · `BreakerConfig` · `DefaultBreakerConfig` · `BreakerFailure` · `BreakerKey` · `BreakerTicket` | — | окно 10 с, 10 вызовов, 50 %, open 30 с | Размыкатель на инстанс. |
| `Direct` · `NewDirect` · `DirectConfig` · `DefaultIdleTTLMs` · `PeerDialer` · `GRPCPeerDialer` | — | idle 5 мин | Кеш mTLS-каналов к пирам с SPIFFE-пином. |
| `Direct.DropRevoked(services, instances)` · `Direct.RetainInstances(live)` | методы | — | Закрыть каналы отозванных / ушедших инстансов. |
| `Proxy` · `NewProxy` · `InvokeClientSource` · `EncodeContractHash` | — | — | Вызов через `Invoke` рантайма. `InvokeClientSource.WaitReady(ctx)` — готовность канала к рантайму; её отказ — pre-dispatch доказательство. |
| `ErrNoLease` · `ErrDirectClosed` · `ErrPeerIdentity` · `ErrInvalidConfig` | `error` | — | Отказы транспорта. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `keepaliveMinTime` | `time.Duration` | `20s` | `EnforcementPolicy.MinTime` сервера, `PermitWithoutStream: true`. |
| `Server.enter(ctx, req)` | метод | — | Все проверки до хендлера в порядке: drain, готовность, личность, отзыв, приём, допуск; кладёт `CallInfo` и трейс. |
| `Server.admit` · `Server.overloaded` · `notDispatched` | — | — | Опознание и отказы; `notDispatched` ставит трейлер. |
| `Server.admitted` · `Server.inflight` | счётчик, WaitGroup | — | Граница `calls+queued` и то, чего ждёт `Wait`. |
| `markNotDispatched(err, trailer, direct)` | функция | — | Оборачивает статус с трейлером в `NotDispatchedError`. |
| `directPreDispatch(err)` | функция | — | Повод для auto перейти на прокси. |
| `Direct.ready(ctx, lease)` | метод | — | Ждёт `READY` канала в пределах дедлайна; `TRANSIENT_FAILURE` → `NotDispatchedError` с причиной handshake. |
| `pooledPeer.handshakeErr` | `atomic.Pointer[error]` | — | Последняя ошибка TLS-проверки канала — называет причину сбоя подключения. |
| `Client.reserve` · `Client.wait` · `Client.dispatch` · `Client.openStream` | методы | — | Выбор с fail-open, решение о повторе, отправка. |
| `callCode(err)` · `sleepMs(ctx, ms)` | функции | — | Код статуса ошибки; сон с учётом `ctx`. |
| `errorCodeInternal` · `actionRPCHandle` · `wildcardTarget` | константы | — | `INTERNAL`, `rpc.handle`, `*`. |

## Архитектурные решения и почему

**Повтор — только при доказательстве, что хендлер не запускался.** Три доказательства: локальный выбор не нашёл кандидата; канал к callee не стал ready до записи запроса (`waitForReady`, ошибка подключения — включая сбой TLS-пина); ответ со статусом и трейлером `x-sb-not-dispatched: 1`. Статус без трейлера, дедлайн, ответ хендлера и наличие idempotency key повтор не разрешают: эффект мог уже случиться. Стримы не повторяются никогда.

**Трейлер только у временных отказов.** `UNAVAILABLE` (draining / not ready) и `RESOURCE_EXHAUSTED` — другой инстанс может ответить иначе. `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`, `FAILED_PRECONDITION` идут без трейлера: другой инстанс ответит так же.

**Прокси повторяется только по доказательству.** Канал к рантайму не стал ready — ничего не отправлено; ответ рантайма с трейлером not-dispatched (его отказы «не отправлено» и проброшенный трейлер callee) — тоже. Голый `UNAVAILABLE` от прокси означает неизвестный исход и не повторяется.

**Transport auto.** Прямой путь к инстансу с endpoint; доказанный pre-dispatch отказ прямого пути переключает следующую попытку на прокси без backoff — отказал путь, а не callee; недостижимые напрямую инстансы уходят в `InvokeRequest.exclude_instance_ids`, и рантайм пробует их последними. Прокси-доказательство (трейлер от рантайма) транспорт не меняет.

**Очередь с границей, а не сброс сразу.** До `MaxConcurrentCalls` хендлеров работают, ещё до `MaxQueuedCalls` ждут слот; дальше `RESOURCE_EXHAUSTED` с трейлером, и вызывающий уходит на другой инстанс. Поток HTTP/2 на соединение ограничен суммой — транспорт не пропускает больше, чем сервер готов принять. Значения по умолчанию совпадают с Node.

**До первого снапшота не пускаем никого.** Без снапшота нет политики приёма, и «пустить всех» означало бы выдумать политику. `UNAVAILABLE "not ready"` с трейлером — вызывающий повторит на готовом инстансе.

**Отзыв действует сразу.** Отозванный сервис/инстанс не выбирается, его прямые каналы закрываются (`DropRevoked`), его входящие вызовы получают `PERMISSION_DENIED` — включая прокси-путь, где вызывающий назван в `caller_service`.

**Размыкатель считает только сбои инстанса.** Отказ — CONNECTION / TIMEOUT / OVERLOADED / INTERNAL-статус (`Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, `Internal`, `Unknown`, `DataLoss`, `Aborted`) и канал, не ставший ready. Ответ хендлера и любой другой код — успех: инстанс ответил.

**Подсказка здоровья не опустошает выбор.** Если подсказка исключила всех кандидатов, которых пропускает размыкатель, выбор идёт среди них — устаревшая подсказка не должна выключать callee.

**Ротация не трогает каналы.** TLS-конфигурации читают текущий лист на каждом handshake (`connection.RotatingTLSConfig`, `GetConfigForClient` сервера), поэтому продление сертификата не рвёт ни один вызов. Каналы к пирам сбрасываются только при смене инстанса (переподготовка истёкшего листа): их соединения говорят от имени ушедшего инстанса.

**Keepalive согласован.** Клиенты пингуют раз в 30 с (`connection.ClientKeepalive`), сервер разрешает пинги не чаще 20 с и без вызовов — ни одна сторона не получает `GOAWAY too_many_pings`.

**Сервер не эмитит операцию телеметрии.** Одна логическая операция — одна строка у вызывающего (ADR-0001); хендлер работает в его трейс-контексте.

**Проверки до занятия слота.** Неавторизованный вызывающий не конкурирует за ёмкость.

**Паника стоит своего вызова.** Перехватывается, логируется со стеком, уходит как `INTERNAL`.

## Зависимости

Опирается на: `internal/connection` (SPIFFE, `Credentials`, `ClientKeepalive`, `VerifyServerChain`), `internal/telemetry` (трейс, операции), `internal/pb`, `golang.org/x/sync/semaphore`, `google.golang.org/grpc`.

Опираются на него: корневой пакет SDK (`Handle`, `Call`, `Stream`, `CallInfoFromContext`, `HandlerError`, workflow call steps), `internal/connection` через интерфейсы `InboundServer` и `CredentialConsumer`, `registry.Cache` как `AdmissionSource`/`CandidateSource`, `sbtest` (`NewStream`, `Dispatcher`).
