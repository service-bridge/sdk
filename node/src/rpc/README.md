# rpc

## Зона ответственности

Единый модуль для RPC-функциональности SDK: domain namespace (`RpcDomain`), typed-client фасад, inbound сервер (callee), outbound клиент (caller), proxy-транспорт к runtime, direct-транспорт к callee, instance cache, load balancer, circuit breaker, acceptance-проверка, dispatch port. Codec (`serde`) живёт отдельно как переиспользуемый компонент.

Логический RPC-вызов в telemetry-модели — ровно одна `operations`-строка **RPC.CALL**, владелец = caller-SDK (ADR-0001). Callee НЕ эмиттит op, runtime НЕ эмиттит op. Streaming, ретраи и proxy-факт укладываются в ту же единственную строку.

## Публичный контракт

Реэкспортируется через корневой `index.ts` SDK. Всё остальное в модуле — `@internal` (см. «Приватный контракт») и доступно только через `ServiceBridge`-фасад (`sb.rpc`, `sb.client`, `sb.stream`).

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `RpcDomain` | class | — | Domain namespace для RPC. Доступен через `sb.rpc`. Реэкспортируется как `type`. |
| `RpcDomain.handle(name, fn, opts)` | метод | — | Регистрирует входящий unary RPC-хендлер. `opts.schema` (`SchemaSpec`) обязателен. |
| `RpcDomain.handleStream(name, fn, opts)` | метод | — | Регистрирует входящий server-side streaming RPC-хендлер. `opts.schema` обязателен. |
| `RpcDomain.call(target, method, payload, opts?)` | async метод | — | Исходящий unary RPC-вызов. Требует поднятого rpc-клиента (после `sb.start()`); иначе бросает `Error`. Gate #3 denial (runtime `PERMISSION_DENIED`, gRPC code 7) → бросает `RpcAccessDeniedError` и зовёт `onPolicyViolation` (`declaration:"rpc.call"`, `denySide:"self_egress"`). |
| `RpcAccessDeniedError` | class | — | Бросается из `RpcDomain.call()` при call-time запрете политики (`rpc.call`/`rpc.handle`, ADR-0004). Поля: `serviceName`, `methodName`, `reason`. |
| `TypedClient` | type | — | Динамический proxy из `sb.client(svc, protoFile)`: методы из `.proto`-блока `service` вызываются напрямую. Unary → `Promise<Res>`, streaming (`responseStream=true`) → `AsyncIterable<Chunk>`. Вторым аргументом любого метода принимается `CallOpts`. |
| `CallOpts` | type | — | Per-call опции `RpcDomain.call` / методов `TypedClient`; см. таблицу ниже. Реэкспортируется через `ServiceBridge`. |
| `RetryOpts` | type | — | Полная форма retry-политики; `CallOpts.retry` принимает `Partial<RetryOpts>`. См. таблицу ниже. |
| `AdvertiseConfig` | type | — | Advertise-адрес inbound call-сервера: `{ host: string; port: number }`. `host` обязателен (без авто-детекта), `port=0` → OS выбирает свободный. Реэкспортируется через `ServiceBridge`. |

### CallOpts

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `timeout` | `string` | `"30s"` | Deadline на вызов. Формат `^\d+(ms\|s\|m)$`: `"10s"`, `"500ms"`, `"2m"`. Невалидная строка → `throw`. |
| `requestId` | `string` | auto UUID v4 | Идентификатор запроса; прокидывается в транспорт. |
| `transport` | `"direct" \| "proxy" \| "auto"` | `"auto"` | `"direct"` — caller → callee mTLS (ошибка, если у callee нет `call_endpoint`); `"proxy"` — через runtime `Invoke`; `"auto"` — direct если endpoint известен, иначе proxy. |
| `idempotencyKey` | `string` | `""` (нет ключа) | Opt-in runtime-side dedup (ADR-0001). Пустая строка — wire-сигнал «нет ключа», runtime пропускает Claim/Save. Без ключа коды `INTERNAL`/`ABORTED`/`UNKNOWN` non-retryable. |
| `retry` | `Partial<RetryOpts>` | см. RetryOpts | Переопределение retry-политики для этого вызова. Не применяется к streaming. |

### RetryOpts

Дефолты берутся из `DEFAULT_RETRY` и мёржатся per-call.

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `maxAttempts` | `number` | `3` | Максимум попыток. `1` отключает retry. |
| `baseDelayMs` | `number` | `200` | Базовая задержка backoff, мс. |
| `factor` | `number` | `2` | Экспоненциальный множитель: `baseDelayMs * factor^attempt`. |
| `maxDelayMs` | `number` | `5000` | Потолок задержки, мс. |
| `jitter` | `number` | `0.3` | Доля случайного jitter в `[0, 1]`: `delay * (1 - jitter + random*2*jitter)`. |

Retryable-коды: `UNAVAILABLE(14)`, `RESOURCE_EXHAUSTED(8)`, `DEADLINE_EXCEEDED(4)` — всегда; `INTERNAL(13)`, `ABORTED(10)`, `UNKNOWN(2)` — только при заданном `idempotencyKey` (ADR-0001). Ошибки без числового `.code` (application errors хендлера, schema errors) — non-retryable.

**Streaming (`sb.stream` / `TypedClient` методы с `responseStream=true`) ретраи не применяет вообще** — включая сбой установки соединения на самом первом пике кандидата. Стрим выбирает инстанс один раз; любая ошибка (connect-failure, mid-stream разрыв) пробрасывается наружу без повторной попытки. Mid-stream replay ре-доставил бы уже полученные чанки (ADR-0001), а ретраить только connect-фазу — значит держать скрытую дву­режимность «до первого чанка ретраим, после — нет»; этого нет. Нужен retry на стриме — caller переоткрывает стрим сам.

## Приватный контракт

Не реэкспортируется через корневой `index.ts`; используется только wire-up'ом в `connection/service-bridge.ts` либо в `*.test.ts`. В коде помечено `@internal`.

| Имя | Тип | Что делает |
|-----|-----|------------|
| `RpcClient` | class | Entry point исходящих вызовов. `call()`/`stream()` берут descriptor и готовый список кандидатов из `InstanceCache` (contract-hash уже учтён ключом индекса, ADR-0005), пикают через `LoadBalancer`, кодируют payload `SchemaPair`, выбирают транспорт и эмиттят одну RPC.CALL op. `callerService` — `() => string`, резолвится на каждом вызове. |
| `SchemaResolver` | type | `(service, method) => CallerSchema \| undefined` — резолвер схемы вызова для `RpcClient`. |
| `CallerSchema` | interface | `{ pair: SchemaPair; contractHash: string; contractHashBytes: Buffer }` — пара с precomputed hash для version-routing фильтра и его UTF-8 wire-формой для `ProxyTransport`. |
| `SchemaRegistry` | class | Caller-side map `(service, method) → CallerSchema`. `set`/`get`/`asResolver()`; `contractHash` считается через `computeContractHash`, `contractHashBytes` — один раз рядом с ним. |
| `CallServer` | class | Inbound gRPC `Call.Unary`/`Call.Stream` сервер. `start(cfg)` биндится на `host:port` с mTLS (`createSsl`, `checkClientCertificate=true`), `endpoint()`, `stop()`. Op НЕ эмиттит. Acceptance-check на каждый incoming call. Хендлер исполняется в trace-контексте вызова. |
| `CallServerCredentials` | interface | `{ caChainDer, leafCertDer, privateKeyDer }` (DER) для call-сервера. |
| `AdvertiseConfig` (как тип сервера) | interface | Объявлен здесь, публикуется наружу через `ServiceBridge` (см. публичный контракт). |
| `ProxyTransport` | class | gRPC client к runtime `Invoke.Unary`/`Invoke.Stream`. Принимает `contractHash: Buffer` (precomputed в `CallerSchema`) и прокидывает X-SB-Trace; `close()`. |
| `DirectTransport` | class | mTLS client к callee `Call.Unary`/`Call.Stream` с SPIFFE-валидацией SAN. Кеш каналов по `targetKey` с TTL-, error- и idle-eviction; `updateCredentials` сбрасывает кеш при ротации cert; `close()`; test-only `cacheSize()`. |
| `DirectCredentials` / `DirectTarget` | interface | DER-креды (+`notAfterUnix` для TTL) и резолвнутая пара `(endpoint, serviceId, instanceId)` для SPIFFE-канала. |
| `targetKey(target)` | function | `"${endpoint}\|${serviceId}\|${instanceId}"` — ключ кеша каналов. |
| `expectedSpiffeUri(target)` | function | `spiffe://<trust-domain>/service/<serviceId>/instance/<instanceId>` — SAN URI, который обязан нести leaf-cert callee. |
| `channelTtlMs(notAfterUnix, nowMs)` | function | Время жизни канала: `notAfter - now - 5min`, пол `60s`. |
| `makeSpiffeCheck(expectedUri)` | function | `checkServerIdentity`-колбэк: отвергает peer-cert, у которого среди URI-SAN нет `expectedUri`. |
| `InstanceCache` | class | Join `MethodDescriptor` × `ServiceInstanceInfo` из `WatchStream` + материализация индекса кандидатов. `bind(watch, breakers)` / `dispose`, `candidatesFor(service, method, contractHash)`, `descriptorFor(service, method)`. |
| `InstanceRetainer` | interface | `{ retain(liveKeys: ReadonlySet<string>): void }` — узкий контракт per-instance-состояния, которое обязано вычищаться вместе с ушедшими подами. Реализуется `CircuitBreakerRegistry`; `InstanceCache` зовёт его на каждом refresh и с пустым набором на `dispose`. |
| `Instance` | interface | `ServiceInstanceInfo` + `isUnhealthyAt: Date \| null` (health-hint из watch snapshot) + `cbKey: string` (готовый ключ брейкера, посчитанный один раз на снапшот). |
| `LoadBalancer` | class | Power-of-Two-Choices (ADR-0001) по inflight. `pick` исключает CB-OPEN и instances с health-hint моложе `HEALTH_HINT_TTL_MS`; для HALF_OPEN-инстанса кандидат допускается только если свободен probe-слот (`cb.probeAvailable`), и на выбранном победителе `pick` атомарно занимает probe через `cb.canCall` — освобождает его парный `cb.recordSuccess`/`recordFailure` после диспатча, поэтому в HALF_OPEN летит ровно один probe. Один проход без промежуточного массива: резервуарная выборка двух кандидатов плюс жеребьёвка порядка пары — без неё второй кандидат никогда не выигрывает при равном inflight. `acquire(id)` → release-closure для `finally`; `inflightOf`. |
| `Candidate` | interface | `{ descriptor, instance, isUnhealthyAt }` — кандидат для P2C. |
| `NoLiveInstanceError` | class | Бросается `LoadBalancer.pick`, когда нет живых кандидатов. `RpcClient` не разворачивает тип: добавляет координаты вызова в message и вешает `code: 14` на тот же экземпляр, поэтому «флот callee пуст» отличается от «callee вернул UNAVAILABLE» по `instanceof`, а не по регэкспу на тексте. |
| `HEALTH_HINT_TTL_MS` | const | `60_000` — TTL доверия runtime-хинту нездоровья (2× окно HealthTracker). |
| `cbKey(instance)` | function | `"${serviceId}:${instanceId}"` — ключ CB/LB на инстанс. Считается один раз на снапшот реестра в `InstanceCache.refresh()` и лежит на инстансе полем `Instance.cbKey`; функция мемоизирована `WeakMap`'ом по объекту инстанса для вызовов, которым типизированный `Instance` недоступен. |
| `CircuitBreakerRegistry` | class | Sliding-window CB per `(serviceId, instanceId)` (ADR-0001). Окно 10s × 10 buckets; OPEN при `total ≥ 10 && errorRate > 0.5` на 30s; HALF_OPEN допускает один probe. `canCall` (атомарно занимает probe-слот в HALF_OPEN — зовётся LB только на выбранном инстансе), `probeAvailable` (read-only eligibility-проверка для фильтра LB, без захвата слота), `recordSuccess`/`recordFailure` (освобождают probe), `state`, `evict(key)` (удалить одну запись), `retain(liveKeys)` (оставить только записи живых инстансов — зовётся владельцем снапшота реестра, `InstanceCache`, при смене набора инстансов), `size()` (test/metrics). |
| `IDLE_TTL_MS` (`circuit-breaker.ts`) | const | `60_000` — ленивое вытеснение записей, которых не касались дольше TTL; страховка на случай, если `retain` не вызывается. Намеренно больше `OPEN_DURATION_MS`, иначе OPEN-запись сбросилась бы в CLOSED и трафик вернулся бы на сломанный инстанс. |
| `DispatchPort` | interface | Boundary Registry/Handle ↔ CallServer: `dispatchUnary`, `dispatchStream`, `captureMode`. Изолирует gRPC-слой от `Handle._entries`. |
| `UnaryResult` / `StreamItem` | interface | Результат диспатча: `payload` + опциональные `errorCode`/`errorMessage`. |
| `RpcDomain._declareForTests(name, streaming=false)` | метод | Регистрирует RPC-запись без схемы. Только для e2e-тестов; прод-код использует `handle()` с явной схемой. |
| `extractServiceMethods(protoFile)` | function | Грузит `.proto`, возвращает `MethodSpec[]` из любого блока `service`. Throws, если service-блока нет. |
| `MethodSpec` | interface | `{ name, requestType, responseType, responseStream }` — метод из proto для typed-proxy. |
| acceptance: `evaluatePeerAcceptance` / `checkAcceptance` / `extractSpiffeServiceId` / `parsePeerSpiffeUri` / `getPeerCertFromCall` | function | Direct-acceptance (ADR-0004): извлечение SPIFFE service UUID из peer-cert и проверка против `PolicyEvaluation.acceptance` (`action == "rpc.handle"`, default-allow, fail-closed при сломанном accessor). |
| `SpiffeIdentity` | interface | `{ serviceId, instanceId }` — распарсенный SPIFFE-URI. |
| `inboundTraceContext(req)` | function | `parseXSbTrace(req.xSbTrace)` или свежий root-context, если поле пустое/битое. |
| `currentTraceHeader()` (`proxy-transport.ts`, `direct-transport.ts`) | function | X-SB-Trace из ALS через `formatXSbTrace`; пустая строка, если контекста нет (runtime минтит свежий trace). |
| `makeStubSb(opts?)` (`test-helpers.ts`) | function | Минимальный `ServiceBridge`-стаб с реальным `TelemetryRing` для unit-тестов `RpcClient`. |

## Telemetry-модель (ADR-0001, ADR-0006, ADR-0007)

Логический RPC-вызов — **ровно одна `operations`-строка RPC.CALL**, владелец = caller-SDK. Кода `RPC.HANDLE`/`RPC.FORWARD` нет: runtime ничего не пишет, callee не эмиттит op.

**Caller side (`RpcClient.call` / `.stream`):** SDK эмиттит ОДНУ RPC.CALL op:

- Subject — `rpc.call:<svc>/<method>` (зеркалит Go `FormatSubject`).
- Op стартует лениво на первом успешном пике кандидата, поэтому `peer_service_id` и `meta.via_proxy` отражают фактический транспорт.
- Ретраи — счётчик `attempt` на ТОЙ ЖЕ строке (один `op_id`), не op-на-попытку.
- Streaming — один CALL op на весь lifetime стрима; захватывается только request-payload (IN), OUT для стримов не пишется.
- Статусы только из единого словаря (`Status` из proto): `SUCCESS` на успехе, `ERROR` + `status_message` на ошибке. На проводе `success` — строка, не `completed`.
- `childCtx = { traceId, parentOpId: callOp.opId }` оборачивает `invoke()` → downstream/callee читают `callOp.opId` как `parentOpId`.

X-SB-Trace прокидывается двумя путями:
1. gRPC metadata `x-sb-trace` (lowercase) — runtime читает его на Invoke-пути.
2. `CallRequest.xSbTrace` body-field — callee-SDK на Direct-пути читает body, не metadata.

**Callee side (`CallServer.handleUnary` / `handleStream`):** парсит `req.xSbTrace` и оборачивает handler в `runWithTrace(inboundTraceContext(req), fn)`. **Op НЕ эмиттится.** Вложенные `sb.rpc.call` / `sb.event.publish` / `sb.workflow.*` внутри user-handler'а наследуют trace через ALS с `parent_op_id = CALL.op_id`. Ошибки хендлера уходят в `CallResponse.error_code/error_message` (gRPC status остаётся OK); caller записывает их на CALL-строку.

## Архитектурные решения и почему

- **Один модуль вместо четырёх.** Domain over layer (CLAUDE.md). Все компоненты RPC меняются вместе при изменении proto-контракта — split по техническим ролям создавал бы лишние границы.
- **SchemaRegistry на caller-стороне.** Caller владеет `.proto` target-сервиса. Несовпадение contract hash → fail-fast через `contractHash`-routing (ADR-0001).
- **CallServer mTLS reuse.** Cert от runtime Bootstrap переиспользуется как server-cert call-сервера; `caChainDer` — тот же CA, что у runtime.
- **Auto-port `port=0`.** SDK биндится на свободный порт, advertise = `${host}:${bound}`. `host` обязателен явный — не угадываем hostname.
- **Application errors через `error_code/error_message`, не gRPC status.** Handler `throw` → `error_code: "INTERNAL"`; транспортные ошибки — через gRPC status. Чёткое разделение для retry-решений.
- **Retry не применяется к streaming — ни к одной фазе.** Mid-stream replay ре-доставил бы уже полученные чанки (ADR-0001). Connect-фазу тоже не ретраим: иначе появилась бы скрытая двухрежимность по фазе вызова. Стрим — single-pick; повтор на стороне caller'а.
- **HALF_OPEN single-probe enforced в LB, не только в CB.** `CircuitBreakerRegistry.canCall` всегда умел отдавать probe-слот ровно одному вызывающему, но `LoadBalancer.pick` фильтровал лишь `state() !== "OPEN"`, пропуская в HALF_OPEN все конкурентные вызовы. Теперь `pick` фильтрует кандидатов через read-only `probeAvailable`, а на выбранном победителе занимает слот через `canCall`; парный `recordSuccess`/`recordFailure` после диспатча освобождает его. `pick` синхронен, поэтому конкурентный `pick` видит занятый слот и уходит на здоровый peer или падает в `NoLiveInstanceError` — инвариант «один probe в HALF_OPEN» реально выполняется per-pod (cross-pod дубли допускает ADR-0001).
- **Identity by UUID, имена display-only.** Идентификаторы внутри SDK и на проводе — `service_id` (UUID). `service_name` — только DX-поверхность и UI label (ADR-0004).
- **Direct-transport acceptance enforcement (ADR-0004).** `CallServer` извлекает SPIFFE-URI из verified peer-cert и проверяет локально по `PolicyEvaluation.acceptance`. Сломан accessor → fail-closed (`PERMISSION_DENIED`); peer без SPIFFE-URI = runtime-proxy (caller-gate уже отработал) → пропуск.
- **Одна RPC.CALL строка, владелец — caller-SDK (ADR-0001).** Caller-SDK один видит и direct-, и proxy-путь, поэтому эмиттит единственную op. Ретраи — счётчик `attempt`; proxy-факт — `meta.via_proxy`.
- **X-SB-Trace через body-field как defense-in-depth.** gRPC metadata может теряться в transport-pipeline; `CallRequest.xSbTrace` дублирует header в body. Runtime/callee принимают любой источник.
- **Кандидаты индексируются, а не сканируются.** Реестр ключует дескрипторы по `(instance, type, method, published)`, поэтому длина набора = инстансы × методы (200 подов × 30 методов = 6000 элементов). Линейный скан по нему стоил бы тем дороже, чем шире отмасштабирован callee — ровно наоборот к смыслу масштабирования. `InstanceCache` материализует список кандидатов по ключу `service/method/contractHash` на refresh: вызов стоит один lookup в `Map`, перестройка платится на событиях реестра (масштабирование, шаг роллинг-деплоя). Фильтр по contract hash тоже уходит в ключ индекса, а не в per-call `.filter()`.
- **Ключ брейкера — поле инстанса.** `cbKey` читается на каждого кандидата в `pick()` и ещё дважды на завершение вызова. Строка считается один раз там, где объект `Instance` и так строится.
- **`retain` из владельца снапшота.** Map брейкера растёт по одной записи на под, и без вычистки роллинг-деплой даёт неограниченную утечку. Снапшот реестра — единственный источник правды о том, какие инстансы ещё существуют, поэтому `InstanceCache` отдаёт живые ключи в `CircuitBreakerRegistry.retain` на каждом refresh и пустой набор на `dispose`. Зависимость объявлена узким интерфейсом `InstanceRetainer` у потребителя — `InstanceCache` не знает про брейкер.
- **Кеш direct-каналов ключуется идентичностью пира, не только эндпоинтом.** Ожидаемый SPIFFE-URI вшит в креды канала, а k8s переиспользует IP: ключ по одному эндпоинту отдавал бы новому инстансу канал, запиннный на SPIFFE-URI прежнего, и рукопожатие падало бы на каждом вызове до вытеснения по ошибке. Ключ — `endpoint|serviceId|instanceId`.
- **Idle-sweep каналов вместо подписки на реестр.** Удаление инстанса из реестра с транспортом ничем не связано, поэтому иначе канал к мёртвому пиру жил бы до истечения TTL сертификата (часы). Ушедший под по определению перестаёт получать вызовы — канал закрывается по `IDLE_TTL_MS` (5 мин без использования); сканирование запускается не чаще раза за TTL и остаётся вне per-call пути. Тот же приём, что backstop-sweep в `circuit-breaker.ts`, и он не тянет `DirectTransport` (создаётся лениво в `ensureRpcReady`) в граф подписок на `WatchStream`.
- **Никаких копий буферов на горячем пути.** `payload` из `SchemaPair.encode()` уже owned, ответ из декодера — тоже, поэтому `Buffer.from(view)` / `new Uint8Array(payload)` дали бы вторую полную копию каждого запроса и ответа. Вместо них — `Buffer`-view без копии на запросе и проброс декодированного буфера как есть на ответе. `contractHash` неизменен для метода и кодируется в UTF-8 один раз в `SchemaRegistry.set`.
- **`caller_service` резолвится замыканием.** `RpcClient` строится из `openSession` до первого `Welcome`, а guard на пересоздание клиента держит его живым через реконнекты — значение, снятое в конструкторе, осталось бы пустым навсегда, и на direct-пути `CallRequest.caller_service` всегда был бы пуст (на proxy-пути его проставляет runtime, поэтому расхождение незаметно). Конструктор принимает `() => string`.

## Зависимости

Опирается на:

- `@grpc/grpc-js` — server + client gRPC, mTLS credentials, статус-коды.
- `@peculiar/x509` + `reflect-metadata` — парсинг DER-сертификата для SPIFFE SAN, когда `subjectaltname` пуст (acceptance).
- `protobufjs` — загрузка `.proto` для `extractServiceMethods`.
- `node:crypto` (`randomUUID`), `node:tls` (`PeerCertificate`).
- `../serde/serializer` (`SchemaPair`), `../serde/contract-hash` (`computeContractHash`).
- `../pb/servicebridge/v1/call` (`CallService`/`CallClient`), `.../invoke` (`InvokeClient`), `.../registry` (`MethodDescriptor`/`ServiceInstanceInfo`/`PolicyEvaluation`), `.../telemetry` (`Channel`/`Status`).
- `../registry/registry` (`Registry`, handler-типы), `../registry/watch` (`WatchStream`).
- `../telemetry/context` (`runWithTrace`, `streamWithContext`, `currentTraceContext`), `../telemetry/ops` (`Channel.RPC`, `RpcCall`, `OpHandle`, `Status`), `../telemetry/wire-trace` (`formatXSbTrace`/`parseXSbTrace`), `../telemetry/trace-context`, `../telemetry/payload-capture`, `../telemetry/ring` (test-helpers).
- `../connection/pem` (`derToPem`), `../connection/spiffe` (`SPIFFE_TRUST_DOMAIN`), `../connection/service-bridge` (тип `ServiceBridge` для эмиссии RPC.CALL).

Используется: `sdk/node/src/connection/service-bridge.ts` (wire-up всех `@internal` классов в lifecycle) и `sdk/node/src/registry/registry.ts` (через `dispatch-port`).
