# src/connection

## Зона ответственности

Bounded context жизненного цикла соединения SDK↔runtime: парсинг bootstrap-ключа, обмен ключа на mTLS-сертификат через `Bootstrap.Provision`, ведение server-стрима `Control.Open` (Welcome/Drain), reconnect, overlap-ротация cert'а через `Control.RefreshCert`. `ServiceBridge` — корневой объект SDK: владеет реестром, RPC/event/workflow/job-доменами и telemetry-инфраструктурой, собирает их в один граф зависимостей и управляет их lifecycle вместе с сессией.

Не делает: не хранит креды на диск, не запускает прикладную бизнес-логику, не реализует сам транспорт Direct RPC / доставку событий (это домены `rpc`, `events`, `workflow`, `job`, `telemetry`).

## Публичный контракт

Реэкспортируется через `sdk/node/index.ts`: `ServiceBridge`, `ServiceBridgeError`, и типы `AdvertiseConfig`, `CallOpts`, `ConnectedEvent`, `DisconnectedEvent`, `Identity`, `MethodDescriptor`, `MethodType`, `PolicyViolationEvent`, `ReconnectingEvent`, `RpcHandlerOpts`, `SchemaSpec`, `ServiceBridgeOptions`, `ServiceDeps`, `WorkflowHandlerOpts`.

### `class ServiceBridge`

```ts
new ServiceBridge(url: string, key: string, options?: ServiceBridgeOptions)
```

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `url` | `string` | — (обязательный) | Адрес рантайма `host:port`. |
| `key` | `string` | — (обязательный) | Bootstrap-ключ формата `sb.<base64url(proto.Marshal(BootstrapKeyPayload{key_id, secret, ca_cert_der}))>`. CA-cert встроен в ключ как доверенный якорь. |
| `options` | `ServiceBridgeOptions` | `{}` | Tuning, см. ниже. |

Методы и свойства:

| Член | Тип / возвращает | Эффект |
|------|------------------|--------|
| `.service(name, deps)` | `void` | Декларирует исходящие зависимости (`rpc`/`workflows`/`http`). Вызывается до `start()`. |
| `.on(event, handler)` | `this` | Подписка на `connected` / `reconnecting` / `disconnected` / `policy_violation`. Цепляется. |
| `.start()` | `Promise<void>` | Финализирует pending schema-loads, открывает SQLite outbox, провижионит cert, открывает mTLS-канал, поднимает inbound CallServer (если `advertise`), запускает RegisterAndWatch-стрим и `Control.Open`. |
| `.stop()` | `Promise<void>` | Гасит refresh-таймер, закрывает session, CallServer, транспорты, telemetry, subscriber'ы, drainer, storage. Идемпотентно. |
| `.identity()` | `Identity \| null` | Идентификатор текущей живой сессии (`sessionId`/`serviceId`/`serviceName`/`instanceId`); `null` до первого Welcome / во время reconnect / после `stop()`. |
| `.instanceIdString()` | `string` | `instance_id` текущей сессии (12-симв. Crockford-base32). Пустая строка до первого Welcome. Используется HTTP-плагинами для автотрейсинга. |
| `.serviceMap()` | `ReadonlyMap<string, ServiceMapEntry>` | Живой снепшот реестра по `serviceName`. Пустой до первого снепшота от runtime. |
| `.policyEvaluation()` | `PolicyEvaluation \| null` | Последний `PolicyEvaluation`, пушнутый runtime в снепшоте реестра. `null` до первого снепшота. |
| `.useSchema(service, method, spec)` | `Promise<void>` | Регистрирует `SchemaPair` для конкретного outgoing RPC. Вызывается до `sb.rpc.call()` этого метода. |
| `.client(service, protoFile, opts?)` | `Promise<TypedClient>` | High-level: читает `.proto`, декларирует методы service-блока как зависимости, грузит схемы, возвращает proxy с типизированными вызовами. Вызывается до `start()`. `opts.methods` ограничивает подмножество; `opts.callDefaults` — дефолты per-client. |
| `.stream(service, method, payload, opts?)` | `AsyncIterable<Chunk>` | Server-streaming RPC. Отмена for-await закрывает gRPC-стрим. Бросает, если rpc-клиент ещё не готов (до `connected`). |
| `.rpc` | `RpcDomain` | RPC-домен: `.handle()`, `.handleStream()`, `.call()`. |
| `.event` | `EventDomain` | Event-домен: `.define()`, `.handle()`, `.publish()`. |
| `.workflow` | `WorkflowDomain` | Workflow-домен: `.handle()`, `.start()`, `.signal()`, `.cancel()`. |
| `.job` | `JobDomain` | Job-домен: `.handle(name, opts, fn)`. См. [job/README.md](../job/README.md). |
| `.telemetry` | `TelemetryAPI` | Surface эмита: `.startOp(params)`, `.captureModeForChannel(channel)`, `.log.{debug,info,warn,error}(msg, fields?)`, `.counter/gauge/histogram(name, ...)`. До `start()` ops/logs/metrics буферизуются в ring; transport их flush'нет после connect. См. [telemetry/README.md](../telemetry/README.md). |
| `.logger` | `ReturnType<typeof makeLogger>` | Эквивалент `sb.telemetry.log` как top-level convenience. |

`ServiceMapEntry`, `ServiceInstanceInfo`, `TelemetryAPI` экспортируются из `service-bridge.ts`, но **не** реэкспортируются через `index.ts`; это типы возвращаемых значений публичных методов. `ServiceMapEntry = { methods: MethodDescriptor[]; instances: ServiceInstanceInfo[]; eventSubscriptions: EventSubscriptionDescriptor[]; outgoingCalls: OutgoingCallDescriptor[] }`. Последние два массива заполняются для своего сервиса и сервисов в outgoing-dep scope (ADR-0014), иначе пустые.

### `interface ServiceBridgeOptions`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `reconnectIntervalMs` | `number` | `3000` | Задержка между попытками reconnect. |
| `reconnectAttempts` | `number` | `3` | Кол-во попыток до `disconnected{reason:'exhausted'}` + auto-stop. `0` = без лимита. |
| `advertise` | `AdvertiseConfig \| false \| undefined` | `undefined` | `{ host, port }` — поднять inbound Call gRPC сервер (`port=0` → ОС выбирает). `undefined` — `127.0.0.1:0` с одноразовым warn. `false` — caller-only, inbound сервер не поднимается. |
| `callDefaults` | `CallOpts \| undefined` | `{}` | Дефолтные опции для каждого `sb.rpc.call()` / `sb.stream()` (`timeout` default `"30s"`, `requestId` авто). Перебиваются per-call аргументом. |
| `failOnPolicyViolation` | `boolean` | `false` | `true` — любой policy-warning в снепшоте реестра роняет `start()` через `disconnected{reason:'policy'}` + `stop()`. `false` — только warn + `policy_violation`-события (ADR-0014). |
| `telemetry` | `boolean` | `true` | `false` полностью отключает telemetry-transport (ops/logs/metrics не уходят; ring буферизует, но не дренится). |
| `telemetryRingSize` | `number` | `262144` | Байтовый budget ops-ring'а (kind под burst-давлением dense workflow step-span emission). Маленький budget молча drop'ает step spans. |
| `dataDir` | `string` | `"./.servicebridge"` | Каталог local SQLite outbox (`sdk.db`); прокидывается в `../sqlite/storage.ts`. |
| `maxOutboxRows` | `number` | `100000` | Максимум строк в local SQLite outbox до `OutboxFullError`. |
| `eventsDrainerBatch` | `number` | `50` | Размер batch'а drainer'а событий. |
| `eventsMaxInFlight` | `number` | `32` | Максимум in-flight event-доставок на subscriber-стрим. |
| `payloadMaxBytes` | `number` | `65536` | Per-direction cap байтов захватываемого payload'а; прокидывается в op'ы через telemetry API. |

### `class ServiceBridgeError`

```ts
new ServiceBridgeError(scope: string, cause: unknown)
```

| Имя | Тип | Что делает |
|-----|-----|------------|
| `.code` | `number` | gRPC Status код (`16` = UNAUTHENTICATED, `-1` = не gRPC). |
| `.cause` | `unknown` | Оригинальный объект ошибки (через `Error.cause`). |

### События (`sb.on(...)`)

| Имя | Payload | Когда |
|-----|---------|-------|
| `connected` | `{ sessionId, serviceId, serviceName }` | Сервер прислал `Welcome` на `Control.Open` (на старте и после каждой успешной overlap-ротации). |
| `reconnecting` | `{ attempt, delayMs, reason }` | Соединение упало, запланирован retry. `attempt` — номер следующей попытки. |
| `disconnected` | `{ reason, error? }` | `reason='exhausted'` — попытки исчерпаны, SDK остановлен. `reason='drain: ...'` — сервер прислал `Drain`. `reason='policy violations...'` — при `failOnPolicyViolation`. `error` (`ServiceBridgeError`) — если причина non-retryable gRPC (без retry). |
| `policy_violation` | `PolicyViolationEvent` (`declaration`/`value`/`denySide`/`reason`) | На каждый warning из `PolicyEvaluation.warnings` снепшота реестра И на call-time запретах (`rpc.call`/`workflow.run`/`event.publish`) — единый канал; дублируется в `console.warn`. |

## Приватный контракт

Module-level экспорты, **не** реэкспортируемые через `index.ts`. Используются другими доменами SDK и тестами этого пакета (`@internal`).

| Имя | Файл | Тип | Что делает |
|-----|------|-----|------------|
| `parseBootstrapKey(raw)` | `key.ts` | `(string) => BootstrapKey` | Декодит `sb.<base64url>` → `BootstrapKeyPayload` → `{keyID, secret, caCertDer}`. Бросает на кривом prefix/base64/пустых полях. |
| `BootstrapKey` | `key.ts` | `interface` | `{ keyID, secret, caCertDer: Buffer }`. |
| `provision(url, key)` | `provision.ts` | `(string, BootstrapKey) => Promise<ProvisionResult>` | Генерит keypair+CSR, делает `Bootstrap.Provision`, возвращает leaf cert + chain. |
| `refresh(client, previous)` | `provision.ts` | `(ControlClient, ProvisionResult) => Promise<ProvisionResult>` | Перевыпуск cert через `Control.RefreshCert` на живом mTLS-канале (без argon2). Новый `instance_id`; `serviceId`/`serviceName` переносятся. |
| `ProvisionResult` | `provision.ts` | `interface` | `{ certDer, caChainDer, serviceId, serviceName, instanceId, notAfterUnix: bigint, privateKey, privateKeyDer }`. `privateKeyDer` (PKCS#8) материализуется один раз для синхронной сборки mTLS-кредов. |
| `Keypair` | `provision.ts` | `interface` | `{ privateKey, publicKey, csrDer }`. |
| `generateKeypairAndCSR()` | `provision.ts` | `() => Promise<Keypair>` | EC P-256 + PKCS#10 CSR через `@peculiar/x509`. |
| `buildPinnedCredentials(caCertDer)` | `provision.ts` | `(Buffer) => grpc.ChannelCredentials` | `createSsl` с встроенной CA + отключённой hostname-проверкой. |
| `newBootstrapClient(url, caCertDer)` | `provision.ts` | `(string, Buffer) => BootstrapClient` | Конструирует pinned gRPC-клиент Bootstrap. |
| `parseURL(url)` | `provision.ts` | `(string) => { host, port }` | Парсит и валидирует `host:port`. |
| `Session` | `session.ts` | `class` | Обёртка над live-соединением: владеет `Control.Open` (Welcome/Drain) и `Registry.RegisterAndWatch`. `close()`, `isClosed()`, `updateRegistration(req)` (рестарт watch с новым `RegisterRequest`). Флаг `expectedClose` подавляет reconnect при намеренном закрытии. |
| `SessionCallbacks` | `session.ts` | `interface` | `{ onWelcome, onDrain, onError, onEnd }`. |
| `ServerStream` | `session.ts` | `type` | `ReturnType<ControlClient["open"]>`. |
| `openControlStream(client)` | `session.ts` | `(ControlClient) => ServerStream` | Конструирует `Control.Open` server-stream; выделен для тестового стаба. |
| `derToPem(der, label?)` | `pem.ts` | `(Buffer, string) => Buffer` | DER → PEM (label по умолчанию `"CERTIFICATE"`). |
| `isRetryable(code)` | `service-bridge-error.ts` | `(number) => boolean` | `false` для UNAUTHENTICATED / PERMISSION_DENIED / NOT_FOUND / INVALID_ARGUMENT; `true` для всех остальных (включая `-1`). |
| `SPIFFE_TRUST_DOMAIN` | `spiffe.ts` | `const string` | `"servicebridge"`; синхронен с `connection.SPIFFETrustDomain` в Go-runtime. |

### `interface ServiceBridgeInternalHooks extends ServiceBridgeOptions` (`@internal`, не экспортируется)

Конструктор `ServiceBridge` принимает `options` как `ServiceBridgeOptions | ServiceBridgeInternalHooks`; hooks подменяют I/O для тестов.

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `certRefreshLeadMs` | `number` | `1_800_000` (30 мин) | За сколько ms до `notAfter` запускать ротацию cert. |
| `certRefreshJitterMs` | `number` | `300_000` (5 мин) | Случайный сдвиг refresh-задержки для размазывания herd'а клиентов. |
| `rotationHandshakeTimeoutMs` | `number` | `10_000` | Сколько ждать `Welcome` на новой сессии при overlap-rotation (heartbeat'ов в Control нет — Welcome единственный liveness-сигнал). |
| `provisionFn` | `(url, key) => Promise<ProvisionResult>` | реальный RPC | Подмена `Bootstrap.Provision`. |
| `refreshFn` | `(client, prev) => Promise<ProvisionResult>` | реальный RPC | Подмена `Control.RefreshCert`. |
| `clientFactory` | `(url, creds) => ControlClient` | `new ControlClient` | Подмена конструктора Control-клиента. |
| `registryClientFactory` | `(url, creds) => RegistryClient` | `new RegistryClient` | Подмена конструктора Registry-клиента. |

## Архитектурные решения и почему

- **`ServiceBridge` — корневой граф зависимостей SDK.** Здесь явно собираются Registry, RPC/event/workflow/job-домены, transports, circuit breaker, load balancer и telemetry. Lifecycle telemetry, subscriber'ов, drainer'а и storage совпадает с lifecycle сессии и управляется из `start()`/`stop()`.
- **CA cert встроен в bootstrap-ключ** как доверенный якорь: `@grpc/grpc-js` валидирует chain до `checkServerIdentity`; встроенный cert — единственный надёжный путь (Bun's `getPeerCertificate` не отдаёт полный chain).
- **Hostname-проверка выключена** через `checkServerIdentity: () => undefined`. У серверного cert'а нет SAN; доверие — chain-валидацией к встроенной CA.
- **Heartbeat'ов в Control нет.** `Control.Open` — read-only server-stream (Welcome/Drain); liveness держит telemetry-стрим. Cert refresh — отдельный unary `Control.RefreshCert`.
- **Overlap rotation ждёт `Welcome`** на новой сессии перед закрытием старой: иначе окно гонки, когда новая сессия в БД, а `Control.Open` ещё не установлен. При неуспехе — rollback на старую сессию + `reconnecting`.
- **`Session.close()` ставит `expectedClose=true`** — иначе при overlap-rotation `onEnd` старой session порождает фантомный `reconnecting`.
- **Compensation marker wire**: `maybeStartWorkflowSubscriber()` собирает `wrapStep`, оборачивающий каждый исполняемый unit (шаг / fanout-группа / ветка / компенсация) в `USER.SUBOP`-op через `sb.telemetry.startOp`, строя дерево run → step → op. Для компенсаций `meta` несёт `is_compensation: true` + `compensates_for_step_id` + `workflow_run_id`; wrapper не дублирует wire-вызов — настоящий `rpc.call`/`event.publish` живёт внутри и эмитит свои каноничные op'ы.
- **`@peculiar/x509`** для CSR — даёт `Pkcs10CertificateRequestGenerator` через Web Crypto без нативной openssl.
- **`privateKeyDer` материализуется один раз** при provision (async export), хранится в `ProvisionResult`, чтобы `buildMTLSCredentials` собирал PEM синхронно.
- **`certRefreshLeadMs` / `certRefreshJitterMs` / `rotationHandshakeTimeoutMs` — приватные hooks**, не публичные опции: связаны с серверной корректностью протокола, их перенастройка без понимания контракта его ломает.
- **Конфигурация только через `ServiceBridgeOptions`** — SDK не читает `process.env`. Все настройки (`advertise`, `telemetry`, `telemetryRingSize`, `dataDir`, `maxOutboxRows`, `eventsDrainerBatch`, `eventsMaxInFlight`, `payloadMaxBytes`) задаются явными опциями конструктора и прокидываются вниз в storage / drainer / subscriber / telemetry ring.

## Зависимости

Зависит на: `@grpc/grpc-js`, `@peculiar/x509`, `reflect-metadata`; внутренние домены `../registry`, `../rpc`, `../events`, `../workflow`, `../job`, `../telemetry`, `../serde`, `../sqlite`; pb-стабы `../pb/servicebridge/v1/{bootstrap,control,events,jobs,registry,telemetry,workflows}`.

Зависят: `sdk/node/index.ts` (реэкспорт публичного API), HTTP-интеграции (`../http/*` через `sb.routes`), `sdk/node/tests/e2e/`.
