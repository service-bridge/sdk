# rpc

## Зона ответственности

RPC в SDK: namespace `sb.rpc` (`RpcDomain`), typed-клиент, входящий Call-сервер (callee), исходящий клиент (caller) с прямым и proxy-транспортом, кэш инстансов, балансировщик, circuit breaker, проверка acceptance. Кодек (`serde`) — отдельный модуль. Логический вызов в телеметрии — ровно одна операция `RPC.CALL`, её пишет caller (ADR-0001).

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `RpcDomain.handle(name, fn, opts)` | метод | — | Unary-handler `fn(req, ctx)`. `opts.schema` обязателен; второй handler на то же имя — `ValidationError`. |
| `RpcDomain.handleStream(name, fn, opts)` | метод | — | Server-streaming handler `async function* (req, ctx)`. |
| `RpcHandlerContext` | interface | — | `{ signal, deadline, requestId, idempotencyKey, caller }`. `signal` прерывается отменой вызывающего и дедлайном; `deadline` — unix-ms или `null`; `caller` — проверенная идентичность пира `{ serviceId, instanceId }` (`instanceId` пуст, если вызов пришёл через proxy runtime) или `null`. |
| `HandlerError` | class (`../errors`) | — | `throw new HandlerError("OUT_OF_STOCK", "…")` в handler'е → `error_code` на проводе → у вызывающего тот же класс с тем же `handlerCode`. Любая другая ошибка → `handlerCode: "INTERNAL"`. |
| `RpcDomain.call(service, method, payload, opts?)` | `Promise<Res>` | — | Исходящий вызов. До `start()` — `StateError`. `AccessDeniedError` дополнительно публикуется как `policy_violation`. |
| `CallOpts.timeout` | `string` | `"30s"` | Дедлайн всего логического вызова (`"500ms"`, `"10s"`, `"2m"`); неверная строка — `ConfigurationError`. |
| `CallOpts.transport` | `"auto" \| "direct" \| "proxy"` | `"auto"` | `auto`: прямо к выбранному инстансу, после pre-dispatch отказа — через proxy runtime. `direct`: никогда не через runtime. `proxy`: всегда через runtime. |
| `CallOpts.idempotencyKey` | `string` | `""` | Передаётся callee (`ctx.idempotencyKey`) и в дедуп proxy. Не делает вызов повторяемым. |
| `CallOpts.requestId` | `string` | UUID | Корреляция. |
| `CallOpts.signal` | `AbortSignal` | — | Отмена: `ServiceBridgeError` `CANCELLED`. |
| `CallOpts.retry` | `Partial<RetryOpts>` | `{maxAttempts:3, baseDelayMs:200, factor:2, maxDelayMs:5000, jitter:0.3}` | Повторы pre-dispatch отказов в пределах дедлайна. |
| `TypedClient` | type | — | Proxy из `sb.client(svc, proto)`: unary → `Promise`, stream → `AsyncIterable`. |
| `AdvertiseConfig` | type | — | `{ host, port }` Call-сервера; `port: 0` — порт выбирает ОС. |

Ошибки вызова — `ServiceBridgeError` с кодом по статусу (`ACCESS_DENIED`, `NOT_FOUND`, `VALIDATION`, `TIMEOUT`, `CONNECTION`, `OVERLOADED`, `CANCELLED`, `INTERNAL`), `HandlerError` (ответ handler'а), `NoLiveInstanceError` (некуда отправить; retryable), `ConfigurationError` (нет схемы вызывающего).

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `RpcClient` | class | — | `call`/`stream`: кандидаты из `InstanceCache` (контракт по хешу, ADR-0005), P2C, транспорт, одна op `RPC.CALL`, повторы только pre-dispatch. |
| `RpcCaller` | type | — | `Pick<RpcClient, "call" \| "stream">` — то, что нужно домену (in-memory реализация у `testing`). |
| `RpcClientDeps` | interface | — | `proxy, direct, instances, resolveSchema, cb, lb, callDefaults(), sb`. |
| `timeoutMs(s)` | function | `30000` | Разбор `CallOpts.timeout`. |
| `SchemaRegistry` / `CallerSchema` / `SchemaResolver` | class / type | — | Схемы вызывающего с precomputed хешем и его UTF-8 формой для proxy. |
| `CallServer` | class | — | Входящий `Call.Unary/Stream`: `start(advertise)`, `rotate()` (перебинд на тот же порт со свежими кредами, in-flight дорабатывают до 30 с), `beginDrain()`, `waitIdle(ms)`, `stop(ms)`. Op не пишет. |
| `CallServerDeps` | interface | — | `dispatch, store, policy(), isRevoked(), limits?, logger`. |
| `DEFAULT_MAX_CONCURRENT_CALLS` | const | `256` | Лимит handler'ов; очередь по умолчанию равна ему. |
| `DirectTransport` | class | — | Канал на инстанс (кэш `endpoint\|serviceId\|instanceId`), SPIFFE-пиннинг, креды от `CertificateStore`; `retain(keep)` закрывает каналы ушедших/отозванных; idle-sweep 5 мин; `waitForReady` до отправки. |
| `ProxyTransport` | class | — | `Invoke.Unary/Stream` к runtime с тем же `waitForReady`. |
| `WireCall` / `DirectTarget` | interface | — | Параметры одного вызова / цель прямого канала. |
| `wire.ts` | module | — | `X_SB_TRACE_HEADER`, `NOT_DISPATCHED_TRAILER` (`x-sb-not-dispatched`), `CallFailure{error, preDispatch, transport}`, `grpcFailure`, `handlerFailure`, `traceMetadata`, `asBuffer`. |
| `InstanceCache` | class | — | Индекс кандидатов `service/method/hash`; отозванные инстансы исключаются, пересборка по изменениям реестра и отзывам. |
| `LoadBalancer` / `Candidate` / `cbKey` / `HEALTH_HINT_TTL_MS` | class / … | `60000` | P2C по inflight; фильтр по endpoint, breaker, hint нездоровья; fail-open, если hint исключил всех. |
| `CircuitBreakerRegistry` | class | — | Окно 10 с; OPEN при ≥10 запросах и >50% ошибок, на 30 с; HALF_OPEN — один probe. |
| `DispatchPort` / `UnaryResult` / `StreamItem` | interface | — | Граница реестр ↔ сервер: `payload` или `status` (отказ до handler'а) или `errorCode` (ответ handler'а). |
| `acceptance.ts` | module | — | `classifyPeer`, `peerOfCall`, `evaluatePeerAcceptance(policy, peer, method)`, `checkAcceptance`; SAN из DER через `node:crypto.X509Certificate`. |
| `extractServiceMethods(protoFile)` | function | — | Методы `service`-блоков `.proto` для typed-клиента. |
| `makeStubSb(opts?)` (`test-helpers.ts`) | function | — | Стаб бриджа с настоящим ring для unit-тестов. |

## Архитектурные решения и почему

- **Форма отказа на проводе (одинакова в Go).** Отказ до handler'а — gRPC-статус: неизвестный метод `NOT_FOUND`, не тот вид `FAILED_PRECONDITION`, недекодируемый запрос `INVALID_ARGUMENT`, политика или отозванный вызывающий `PERMISSION_DENIED` (и для стримов — статус, не чанк), нет политики `UNAVAILABLE` «not ready», дренаж `UNAVAILABLE`, перегрузка `RESOURCE_EXHAUSTED`. Временные отказы (`UNAVAILABLE`, `RESOURCE_EXHAUSTED`) несут трейлер `x-sb-not-dispatched: 1`. Ответ handler'а — `OK` с `error_code`/`error_message`.
- **Повтор — только доказанный pre-dispatch (GSDK-02/NSDK).** Нет кандидата, канал к callee/runtime не стал готов в пределах дедлайна (`waitForReady`), статус с трейлером `x-sb-not-dispatched`. Всё остальное, включая голый `UNAVAILABLE` и истёкший дедлайн, — исход неизвестен, возвращается вызывающему. В `auto` pre-dispatch отказ прямого пути переводит следующую попытку на proxy: в локальном снимке может ещё значиться мёртвый под, runtime уже знает живой.
- **Breaker считает только транспорт (NSDK-02).** Отказ `CONNECTION`/`TIMEOUT`/`OVERLOADED`/`INTERNAL`-статус — неудача инстанса; ответ handler'а, отказ политики или валидации — инстанс жив и ответил.
- **Fail-open балансировщика (OBS-02).** Если hint нездоровья от runtime исключил всех иначе пригодных кандидатов, выбор идёт среди них: ошибочный hint вероятнее, чем мёртвый весь флот.
- **Отзыв (решение 12).** `RegistryUpdate.revoked_services/instances`: отозванные не выбираются целями, их прямые каналы закрываются, их входящие вызовы отклоняются сразу. Отзыв инстанса — навсегда; отзыв сервиса снимается, когда появляется его инстанс, которого не было в момент отзыва.
- **Нет входящих вызовов до первой политики (GSDK-07).** Без политики правила acceptance неизвестны, а default-allow пустил бы всех.
- **ctx и отмена (NSDK-09).** Сигнал handler'а прерывается событием `cancelled` вызова и таймером дедлайна; streaming-генератору вызывается `return()`.
- **Ротация сертификата без обрыва.** Каналы прямого транспорта следуют за `CertificateStore`; кэш каналов больше не сбрасывается и не живёт по TTL сертификата.
- **Стримы не ретраятся** ни на одной фазе: повтор переотправил бы уже прочитанные чанки.
- **Одна op `RPC.CALL` на логический вызов**, повторы — счётчик `attempt` на той же строке; X-SB-Trace идёт и в metadata, и в теле запроса.

## Зависимости

- Опирается на: `@grpc/grpc-js`, `protobufjs` (typed-клиент), `node:crypto`; `../connection/{tls-material,spiffe}`, `../serde`, `../telemetry`, `../registry`, `../errors`, `../logger`, pb `call`/`invoke`/`registry`.
- Используется: `../connection/service-bridge.ts`, `../registry/registry.ts` (dispatch port), `../testing`.
