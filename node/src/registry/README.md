# registry

## Зона ответственности

Клиентская сторона Registry: хранит декларации входящих хендлеров (rpc, stream, event, workflow, job) и исходящих зависимостей, строит `RegisterRequest` для gRPC, управляет стримом `RegisterAndWatch`, кешем `MethodDescriptor` и snapshot'ами enrichment'а (instances, event-subscriptions, outgoing-calls, policy, per-channel capture modes). Не содержит логики маршрутизации, не диспетчит вызовы сам (отдаёт `DispatchPort`) и не знает о транспорте.

## Публичный контракт

Реэкспортируется наружу через `ServiceBridge` (top-level `index.ts`). Сами классы `Registry`, `Handle`, `WatchStream` наружу НЕ экспортируются — они встроены в `ServiceBridge` и доступны прикладному коду только через domain namespaces (`sb.rpc`, `sb.event`, `sb.workflow`, `sb.job`).

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `MethodType` | enum (re-export из pb) | — | Тип метода: RPC / EVENT / WORKFLOW / JOB / HTTP |
| `MethodDescriptor` | type (re-export из pb) | — | Дескриптор видимого метода в snapshot'е `sb.serviceMap()` |
| `RpcHandlerOpts` | interface | — | Опции RPC/stream-хендлера (см. ниже) |
| `WorkflowHandlerOpts` | interface | — | Опции workflow-хендлера (см. ниже) |
| `ServiceDeps` | interface | — | Декларация исходящих зависимостей для `sb.service(name, deps)` |

`RpcHandlerOpts`:

| Поле | Тип | По умолчанию | Что делает |
|------|-----|--------------|------------|
| `schema` | `SchemaSpec` | — (обязательно) | `.proto`-файл + имена сообщений (или `.schema.json`) для input и output; нужен для decode/encode и contract-hash (ADR 0001) |
| `captureMode?` | `CaptureMode` (`"all"\|"errors"\|"none"`) | runtime-pushed режим RPC-канала | Per-handler override payload-capture. Может только СУЖАТЬ runtime-режим (`none < errors < all`), не расширять |

`WorkflowHandlerOpts`:

| Поле | Тип | По умолчанию | Что делает |
|------|-----|--------------|------------|
| `input?` | `Record<string, unknown>` | нет | JSON-схема стартового state. Финального output у workflow нет: каждый step возвращает обновлённый state |

`ServiceDeps`:

| Поле | Тип | По умолчанию | Что делает |
|------|-----|--------------|------------|
| `rpc?` | `string[]` | `[]` | Имена RPC-методов целевого сервиса (outgoing-декларация) |
| `workflows?` | `string[]` | `[]` | Имена workflow-методов целевого сервиса |
| `http?` | `string[]` | `[]` | HTTP-паттерны (outgoing-декларация для Service Map; ADR 0001 — runtime НЕ проксирует HTTP, вызов делает пользователь сам) |

## Приватный контракт

`@internal` — не реэкспортируется через top-level `index.ts`. Доступ только из `src/connection`, domain-классов и rpc-instance-cache.

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Registry` | class @internal | — | Хранит outgoing-deps, HTTP-роуты и единственный `Handle`; строит `RegisterRequest` |
| `Registry` (конструктор) | `(onRestart?: () => void)` | `() => {}` | `onRestart` вызывается из `routes.publishHttp(...)` после записи нового endpoint'а — рестарт watch-стрима. До `sb.start()` — no-op |
| `Registry._handle` | `Handle` | — | Единственный `Handle` экземпляра. Владеет всеми async-loadable декларациями |
| `Registry.routes` | `RouteCollector` | — | Public-but-undocumented; пишут только интеграции `servicebridge/{express,fastify,hono}` (ADR 0001) |
| `Registry.service(name, deps)` | `(string, ServiceDeps) => void` | — | Объявляет исходящие зависимости; каждый метод → `OutgoingDep` с типом RPC/WORKFLOW/HTTP |
| `Registry.setCallEndpoint(endpoint)` | `(string) => void` | — | Задаёт `call_endpoint` для `RegisterRequest` |
| `Registry.buildRegisterRequest()` | `() => RegisterRequest` | — | Собирает proto: incoming (+ HTTP-роуты как METHOD_TYPE_HTTP), published, outgoing, call/http endpoints, event_subscriptions (dedup по pattern) |
| `Handle` | class @internal | — | Хранилище incoming-хендлеров и published-events. Доступ — через domain-классы |
| `Handle.rpc(name, fn, opts)` | — | — | Регистрирует unary RPC-хендлер; `opts.schema` async-грузит `SchemaPair` |
| `Handle.stream(name, fn, opts)` | — | — | Регистрирует server-streaming RPC; `fn` — `AsyncIterable`/async-generator |
| `Handle.event(pattern, fn)` | — | — | Регистрирует обработчик Durable Event; `pattern` — точное имя или AMQP wildcard. Схема payload-а живёт у publisher'а (`publishEvent`) |
| `Handle.publishEvent(name, spec?)` | — | `spec` undefined | Объявляет published event (publisher-side). `spec` — `SchemaSpec`; async-загрузка в общий `pending[]`. Idempotent re-define по reference identity `spec`; иной spec — throws (ADR-0002) |
| `Handle.getPublishedEvent(name)` | `{contractHash, pair} \| undefined` | — | Schema-lookup для Publisher / Subscriber. undefined для schema-less event или до finalize() |
| `Handle.workflow(name, steps, opts?, graphJson?, contractHash?)` | — | — | Регистрирует workflow. `graphJson` (ADR-W-002) перекрывает schema-derived input; `contractHash` едет как `IncomingMethod.contract_hash` |
| `Handle.job(name, contractHash, specJson, fn)` | — | — | Регистрирует scheduled job. `specJson` — canonical spec (CanonicalJobSpec); `fn` хранится локально, не едет по wire |
| `Handle.finalize()` | `Promise<void>` | — | Ожидает async-загрузок `SchemaPair` (rpc/stream + publishEvent). Вызывается `ServiceBridge.start()` до `buildRegisterRequest()` |
| `Handle.incomingMethods()` | `PbIncomingMethod[]` | — | Все типы КРОМЕ EVENT (события едут только через `event_subscriptions`); contract_hash из override или из `SchemaPair` |
| `Handle.publishedEvents()` | `PbPublishedEvent[]` | — | Published events для `RegisterRequest`; каждый несёт `contractHash` (или "" для schema-less) |
| `Handle.asDispatchPort()` | `DispatchPort` | — | Boundary для `CallServer`: decode/encode по `SchemaPair`, маппинг ошибок в `errorCode` (NOT_FOUND, FAILED_PRECONDITION, INTERNAL, INVALID_ARGUMENT); `captureMode(method)` — per-handler override |
| `Handle._declareForTests(name, streaming?)` | — | `streaming=false` | Регистрирует RPC-запись без схемы. Только unit-тесты |
| `Handle._declarePublishedEventForTests(name)` | — | — | Регистрирует published event без `.proto`-загрузки. Только unit-тесты |
| `WatchStream` | class @internal | — | Управляет gRPC-стримом `RegisterAndWatch`; держит кеш дескрипторов, instances, event-subs, outgoing-calls, policy, per-channel capture modes. Конструктор принимает опциональный `ReconnectDelayOptions` (тестовый hook лестницы) |
| `WatchStream.start(req, client, onError?)` | — | `onError=() => {}` | Открывает стрим, запоминает `(req, client)` для авторестартов, сбрасывает retry-состояние. Упавший стрим (`"error"`/`"end"`) сам рестартует по общей лестнице `utils/reconnect-ladder`; один pending-таймер на инстанс (error+end одного обрыва не множат циклы), события устаревших стримов отбрасываются по identity-guard. Пришедший `"data"` сбрасывает лестницу. `onError` — нотификация, не управление рестартом |
| `WatchStream.stop()` | — | — | Гасит retry-таймер, `stream.cancel()`; не закрывает gRPC-канал |
| `WatchStream.restart(req, client, onError?)` | — | — | `stop()` + `start()`; ротация сессии подменяет client этим путём |
| `WatchStream.snapshot()` | `ReadonlyMap<string, MethodDescriptor>` | — | Копия кеша; ключ `"${instanceId}:${type}:${name}:${published}"` |
| `WatchStream.instancesSnapshot()` | `Map<string, ServiceInstanceInfo>` | — | Живой map инстансов по `instanceId` |
| `WatchStream.eventSubscriptionsSnapshot()` | `Map<string, EventSubscriptionDescriptor>` | — | Копия event-subs; ключ `"${serviceId}\|${pattern}"` (ADR-0004) |
| `WatchStream.outgoingCallsSnapshot()` | `Map<string, OutgoingCallDescriptor>` | — | Копия outgoing-calls; ключ `(caller, target, method, type)` (ADR-0004) |
| `WatchStream.policyEvaluation()` | `PolicyEvaluation \| null` | `null` | Последняя `PolicyEvaluation` (ADR-0004) |
| `WatchStream.captureModeForChannel(channel)` | `CaptureMode` | `"none"` | Runtime-pushed эффективный payload-режим для op-канала из `capture_modes` (rpc/http/event/workflow). `"none"` до первого snapshot и для непереданных каналов (fail-safe) |
| `WatchStream.onCaptureModes(fn)` | `() => void` | — | Подписка на изменение per-channel capture modes; возвращает unsubscribe |
| `WatchStream.pushedTelemetryConfig()` | `PushedTelemetryConfig` | `{enabled:true,payloadMaxBytes:65536}` | Runtime-pushed глобальные телеметрические настройки: `enabled` из `telemetry.enable`, `payloadMaxBytes` из `telemetry.payload_max_bytes`. Fail-safe до первого snapshot: включён, cap=65536 |
| `WatchStream.onTelemetryConfig(fn)` | `() => void` | — | Подписка на изменение телеметрических настроек; возвращает unsubscribe |
| `PushedTelemetryConfig` | interface | — | `{ enabled: boolean; payloadMaxBytes: number }` |
| `WatchStream.onInstancesChange(fn)` | `() => void` | — | Подписка на add/remove инстансов; возвращает unsubscribe |
| `WatchStream.onPolicyEvaluation(fn)` | `() => void` | — | Подписка на свежую `PolicyEvaluation` (snapshot + live policy update); возвращает unsubscribe |
| `WatchStream.onPeersChange(fn)` | `() => void` | — | Подписка на `added_peers`/`removed_peers` из update; возвращает unsubscribe |

## Архитектурные решения и почему

- **`"error"` handler в WatchStream**: gRPC-стрим эмитит `"error"` при CANCELLED/UNIMPLEMENTED. Без handler'а в Node.js это unhandled error и краш. `onError` callback позволяет caller'у логировать без пробрасывания.
- **Per-channel capture modes**: runtime — единственный источник истины по payload-capture и пушит весь набор `CaptureModes` (rpc/http/event/workflow) на каждый snapshot/update. SDK берёт режим для канала операции как эффективный. Job-канала нет (jobs не несут payload — поле 5 в proto reserved). Per-handler `captureMode` может только сужать runtime-режим.
- **Кеш — `Map<string, MethodDescriptor>`**: snapshot полностью заменяет кеш; update делает точечные set/delete. Удаление несуществующего ключа — no-op (гонка дисконнект-до-снепшота). `removed_peers` чистит все кеши (methods/instances/event-subs/outgoing), привязанные к выпавшему из scope peer'у.
- **`Handle` внутри `Registry`**: все декларации (RPC, stream, события, workflow, jobs, HTTP-роуты) едут единым `RegisterRequest` через `Registry.RegisterAndWatch`. `Handle` владеет всеми async-loadable декларациями (incoming + published) в одном `pending[]`; `finalize()` ждёт их перед сериализацией. `Registry` хранит outgoing-deps (`service()`), роуты (`routes`) и endpoints.
- **EVENT не в `incomingMethods()`**: подписки едут только через `RegisterRequest.event_subscriptions`, dedup по pattern — серверный `event_subscriptions` имеет PRIMARY KEY `(subscriber_id, pattern)`. In-process fan-out на несколько хендлеров одного pattern делает SDK сам (ADR 0002).

## Зависимости

Зависит от:
- `src/pb/servicebridge/v1/registry`, `src/pb/servicebridge/v1/telemetry` — proto-типы (`MethodType`, `MethodDescriptor`, `RegisterRequest`, `RegistryEvent`, `CaptureModes`, `Channel`, …)
- `src/http/route` — `RouteCollector` для HTTP-интеграций
- `src/serde/serializer`, `src/serde/contract-hash` — async-загрузка `SchemaPair`, contract-hash
- `src/rpc/dispatch-port`, `src/telemetry/payload-capture` — типы `DispatchPort` / `CaptureMode`

Используется в:
- `src/connection/service-bridge.ts` — `Registry`, `WatchStream`, `Handle` встроены в `ServiceBridge`
- `src/connection/session.ts`, `src/rpc/instance-cache.ts` — типы `WatchStream`
- `src/{rpc,events,workflow,job}/domain.ts` — типы `Registry`, `RpcHandlerFn`, `RpcStreamHandlerFn`, `WorkflowHandlerOpts`
- top-level `index.ts` (через `ServiceBridge`) — re-export `MethodType`, `MethodDescriptor`, `RpcHandlerOpts`, `WorkflowHandlerOpts`, `ServiceDeps`
