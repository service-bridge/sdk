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

## Приватный контракт

Не реэкспортируется через корневой `index.ts`; используется только wire-up'ом в `connection/service-bridge.ts` либо в `*.test.ts`. В коде помечено `@internal`.

| Имя | Тип | Что делает |
|-----|-----|------------|
| `RpcClient` | class | Entry point исходящих вызовов. `call()`/`stream()` резолвят descriptor через `InstanceCache`, фильтруют кандидатов по `contractHash` (ADR-0001), пикают через `LoadBalancer`, кодируют payload `SchemaPair`, выбирают транспорт и эмиттят одну RPC.CALL op. |
| `SchemaResolver` | type | `(service, method) => CallerSchema \| undefined` — резолвер схемы вызова для `RpcClient`. |
| `CallerSchema` | interface | `{ pair: SchemaPair; contractHash: string }` — пара со precomputed hash для version-routing фильтра. |
| `SchemaRegistry` | class | Caller-side map `(service, method) → CallerSchema`. `set`/`get`/`asResolver()`; `contractHash` считается через `computeContractHash`. |
| `CallServer` | class | Inbound gRPC `Call.Unary`/`Call.Stream` сервер. `start(cfg)` биндится на `host:port` с mTLS (`createSsl`, `checkClientCertificate=true`), `endpoint()`, `stop()`. Op НЕ эмиттит. Acceptance-check на каждый incoming call. Хендлер исполняется в trace-контексте вызова. |
| `CallServerCredentials` | interface | `{ caChainDer, leafCertDer, privateKeyDer }` (DER) для call-сервера. |
| `AdvertiseConfig` (как тип сервера) | interface | Объявлен здесь, публикуется наружу через `ServiceBridge` (см. публичный контракт). |
| `ProxyTransport` | class | gRPC client к runtime `Invoke.Unary`/`Invoke.Stream`. Прокидывает `contractHash` и X-SB-Trace; `close()`. |
| `DirectTransport` | class | mTLS client к callee `Call.Unary`/`Call.Stream` с SPIFFE-валидацией SAN. Per-endpoint кеш с TTL-eviction; `updateCredentials` сбрасывает кеш при ротации cert; `close()`; test-only `cacheSize()`. |
| `DirectCredentials` / `DirectTarget` | interface | DER-креды (+`notAfterUnix` для TTL) и резолвнутая пара `(endpoint, serviceId, instanceId)` для SPIFFE-канала. |
| `InstanceCache` | class | Join `MethodDescriptor` × `ServiceInstanceInfo` из `WatchStream`. `bind`/`dispose`, `pickAll`, `descriptorFor`. |
| `Instance` | interface | `ServiceInstanceInfo` + `isUnhealthyAt: Date \| null` (health-hint из watch snapshot). |
| `LoadBalancer` | class | Power-of-Two-Choices (ADR-0001) по inflight. `pick` исключает CB-OPEN и instances с health-hint моложе `HEALTH_HINT_TTL_MS`; `acquire(id)` → release-closure для `finally`; `inflightOf`. |
| `Candidate` | interface | `{ descriptor, instance, isUnhealthyAt }` — кандидат для P2C. |
| `NoLiveInstanceError` | class | Бросается `LoadBalancer.pick`, когда нет живых кандидатов. |
| `HEALTH_HINT_TTL_MS` | const | `60_000` — TTL доверия runtime-хинту нездоровья (2× окно HealthTracker). |
| `cbKey(instance)` | function | `"${serviceId}:${instanceId}"` — ключ CB/LB на инстанс. |
| `CircuitBreakerRegistry` | class | Sliding-window CB per `(serviceId, instanceId)` (ADR-0001). Окно 10s × 10 buckets; OPEN при `total ≥ 10 && errorRate > 0.5` на 30s; HALF_OPEN допускает один probe. `canCall`/`recordSuccess`/`recordFailure`/`state`/`reset`. |
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
- **Retry не применяется к streaming.** Mid-stream replay ре-доставил бы уже полученные чанки (ADR-0001).
- **Identity by UUID, имена display-only.** Идентификаторы внутри SDK и на проводе — `service_id` (UUID). `service_name` — только DX-поверхность и UI label (ADR-0004).
- **Direct-transport acceptance enforcement (ADR-0004).** `CallServer` извлекает SPIFFE-URI из verified peer-cert и проверяет локально по `PolicyEvaluation.acceptance`. Сломан accessor → fail-closed (`PERMISSION_DENIED`); peer без SPIFFE-URI = runtime-proxy (caller-gate уже отработал) → пропуск.
- **Одна RPC.CALL строка, владелец — caller-SDK (ADR-0001).** Caller-SDK один видит и direct-, и proxy-путь, поэтому эмиттит единственную op. Ретраи — счётчик `attempt`; proxy-факт — `meta.via_proxy`.
- **X-SB-Trace через body-field как defense-in-depth.** gRPC metadata может теряться в transport-pipeline; `CallRequest.xSbTrace` дублирует header в body. Runtime/callee принимают любой источник.

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
