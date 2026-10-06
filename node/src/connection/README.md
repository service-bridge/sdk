# src/connection

## Зона ответственности

Жизненный цикл соединения SDK↔runtime: разбор bootstrap-ключа, обмен ключа на mTLS-сертификат (`Bootstrap.Provision`), стрим `Control.Open` (Welcome/Drain), стрим реестра, reconnect, продление сертификата (`Control.RefreshCert`) и упорядоченная остановка. `ServiceBridge` — корневой объект SDK: собирает граф зависимостей (реестр, RPC/event/workflow/job-домены, транспорты, телеметрию) и владеет их ресурсами.

Не делает: не хранит креды на диске, не реализует транспорт RPC, доставку событий и workflow (домены `rpc`, `events`, `workflow`, `job`, `telemetry`).

## Публичный контракт

Реэкспортируется через `sdk/node/index.ts`: `ServiceBridge`, `ConnectionError`, типы событий и опций.

### `class ServiceBridge`

```ts
new ServiceBridge(url: string, key: string, options?: ServiceBridgeOptions)
```

| Член | Тип / возвращает | Что делает |
|------|------------------|------------|
| `url` | `string` | Адрес runtime `host:port`. |
| `key` | `string` | Bootstrap-ключ `sb.<base64url(BootstrapKeyPayload)>`; CA внутри ключа — доверенный якорь. |
| `.start()` | `Promise<void>` | Провижининг, Call-сервер, `Control.Open`, реестр. Резолвится после Welcome **и** первого snapshot реестра в пределах `startTimeoutMs`; иначе бридж останавливается и ошибка бросается (`TimeoutError`, `ConnectionError`, `ConfigurationError`, `ValidationError`). Повтор — `StateError`. |
| `.ready()` | `Promise<void>` | Резолвится, когда текущая сессия жива и её snapshot применён (сразу, если уже). Отклоняется после остановки. |
| `.stop()` | `Promise<void>` | Упорядоченная остановка (см. «Архитектурные решения»). Идемпотентна. |
| `.on(event, handler)` | `this` | `connected` / `reconnecting` / `disconnected` / `draining` / `policy_violation`. Исключение в handler'е логируется и не мешает остальным. |
| `.service(name, deps)` | `void` | Исходящие зависимости (`rpc`/`workflows`/`http`) до `start()`. |
| `.client(service, protoFile, opts?)` | `Promise<TypedClient>` | Typed-клиент по `.proto`: объявляет методы зависимостями, грузит схемы. До `start()`. `opts.callDefaults` — между `ServiceBridgeOptions.callDefaults` и опциями вызова. |
| `.useSchema(service, method, spec)` | `Promise<void>` | Схема вызывающего для одного метода. |
| `.stream(service, method, payload, opts?)` | `AsyncIterable<Chunk>` | Server-streaming RPC; до `start()` — `StateError`. |
| `.identity()` | `Identity \| null` | `{ sessionId, serviceId, serviceName, instanceId }` живой сессии. `instanceId` стабилен на весь процесс. |
| `.instanceIdString()` | `string` | `instance_id` (пусто до первого Welcome). |
| `.serviceMap()` | `ReadonlyMap<string, ServiceMapEntry>` | Живой вид реестра по имени сервиса. |
| `.policyEvaluation()` | `PolicyEvaluation \| null` | Последняя политика от runtime. |
| `.rpc` / `.event` / `.workflow` / `.job` | домены | См. README доменов. |
| `.telemetry` / `.logger` | `TelemetryAPI` | Ops, логи (`sb.logger` — структурные логи в runtime), метрики. |

### `interface ServiceBridgeOptions`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `reconnectIntervalMs` | `number?` | лестница 1s/5s/15s/30s/60s ±20% | Плоская задержка reconnect вместо лестницы. |
| `reconnectAttempts` | `number` | `0` (без лимита) | Сколько **подряд идущих** неудач допустимо; счётчик сбрасывается на Welcome. Превышение → `disconnected` + остановка. |
| `advertise` | `AdvertiseConfig \| false` | `127.0.0.1:0` + warn | Адрес Call-сервера; `false` — только вызывающий. |
| `callDefaults` | `CallOpts` | `{}` | Дефолты всех исходящих вызовов: `sb.rpc.call`, `sb.stream`, typed-клиенты. |
| `failOnPolicyViolation` | `boolean` | `false` | Остановиться при warning'е политики. |
| `publishTimeoutMs` | `number` | `30000` | Ожидание ACK publish. |
| `maxPendingPublishes` | `number` | `10000` | Очередь publish до `QUEUE_FULL`. |
| `eventsMaxInFlight` | `number` | `32` | Параллельные доставки. |
| `rpcMaxConcurrentCalls` | `number` | `256` | Одновременные входящие handler'ы. |
| `rpcMaxQueuedCalls` | `number` | = `rpcMaxConcurrentCalls` | Очередь входящих до `RESOURCE_EXHAUSTED`. |
| `startTimeoutMs` | `number` | `30000` | Дедлайн `start()`. |
| `stopTimeoutMs` | `number` | `10000` | Дедлайн дренажа в `stop()`. |
| `logger` | `Logger` | warn/error в консоль | Диагностика SDK (`{debug,info,warn,error}(message, attrs?)`). |
| `telemetry.onDrop` | `DropObserver?` | нет | Сообщение о потерянной телеметрии (ring/runtime). |

Неверное значение опции → `ConfigurationError` в конструкторе.

### События (`sb.on(...)`)

| Имя | Payload | Когда |
|-----|---------|-------|
| `connected` | `{ sessionId, serviceId, serviceName, runtimeVersion }` | Welcome новой сессии (старт и каждый reconnect). |
| `reconnecting` | `{ attempt, delayMs, reason }` | Сессия потеряна или попытка не удалась; `attempt` — номер подряд идущей неудачи. |
| `draining` | `{ reason }` | Runtime объявил остановку (`Drain`); reconnect последует после закрытия стрима. |
| `disconnected` | `{ reason, error }` | Бридж остановился окончательно: неустранимая ошибка или исчерпан `reconnectAttempts`. |
| `policy_violation` | `{ declaration, value, denySide, reason }` | Warning политики из snapshot и call-time запреты (`rpc.call`, `event.publish`). |

### `class ConnectionError extends ServiceBridgeError`

`code: "CONNECTION"`, `grpcCode: number` — статус runtime (`-1`, если его не было).

## Приватный контракт

| Имя | Файл | Тип | Что делает |
|-----|------|-----|------------|
| `parseBootstrapKey(raw)` | `key.ts` | function | `sb.<base64url>` → `{ keyID, secret, caCertDer }`. |
| `provision(url, key, clientFactory?)` | `provision.ts` | function | Ключ + CSR → leaf. Bootstrap-канал закрывается в `finally`. |
| `refresh(client, previous)` | `provision.ts` | function | `Control.RefreshCert`: новый leaf, тот же `instance_id`. |
| `ProvisionResult` | `provision.ts` | interface | `{ certDer, caChainDer, serviceId, serviceName, instanceId, notAfterUnixMs, privateKey, privateKeyDer }`; срок — unix-ms (ADR-0006). |
| `generateKeypairAndCSR()` | `provision.ts` | function | P-256 ключ (WebCrypto) + PKCS#10 из `csr.ts`. |
| `buildCsr(keys, cn?)` | `csr.ts` | function | DER PKCS#10 на WebCrypto, без внешних библиотек. |
| `CertificateStore` | `tls-material.ts` | class | In-memory `CertificateProvider` grpc-js: `channelCredentials(check)`, `serverCredentials()`, `update(material)`, `pem()`. |
| `CLIENT_CHANNEL_OPTIONS` | `tls-material.ts` | const | Keepalive клиентских каналов: 30 s / 10 s, `permit_without_calls`. |
| `Session` / `openControlStream` | `session.ts` | class / function | Стрим `Control.Open` с handshake (`protocol_version`, `sdk_language`, `sdk_version`); `close()` гасит колбэки. |
| `PROTOCOL_VERSION` / `SDK_LANGUAGE` / `SDK_VERSION` | `handshake.ts` | const | `1`, `"node"`, версия пакета (тест сверяет с `package.json`). |
| `isTerminal(grpcCode)` | `service-bridge-error.ts` | function | `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`, `FAILED_PRECONDITION` — остановка без reconnect. |
| `ServiceBridge.diagnostics` | `service-bridge.ts` | getter | Логгер SDK для HTTP-интеграций. |
| `ServiceBridge._startInMemory(rt)` | `service-bridge.ts` | method | Запуск на in-memory runtime для `service-bridge/testing`. |
| `ServiceBridgeInternalHooks` | `service-bridge.ts` | interface | Тестовые подмены: `certRefreshLeadMs`, `certRefreshJitterMs`, `provisionFn`, `refreshFn`, `controlClientFactory`, `registryClientFactory`, `_disableTelemetryTransport`. |

## Архитектурные решения и почему

- **start() ждёт Welcome и первый snapshot (NSDK-10).** До snapshot у SDK нет ни вида на mesh (вызов упал бы «no descriptor»), ни политики доступа (Call-сервер не знает, кого пускать). Поэтому `start()` резолвится только когда оба есть; `ready()` даёт то же после reconnect.
- **Reconnect считает подряд идущие неудачи (NSDK-01).** Счётчик сбрасывается на Welcome, по умолчанию лимита нет: сервис, сдавшийся посреди rolling restart runtime, требует человека. Остановка — только на неустранимых кодах (`isTerminal`), несовместимом протоколе (Welcome или `FAILED_PRECONDITION`) и `INVALID_ARGUMENT` стрима реестра (невалидный фильтр подписки).
- **Каналы строятся один раз; ротация меняет только TLS-материал (NSDK-03, решение 1).** `instance_id` стабилен, поэтому продление сертификата не должно трогать ни один стрим. Все клиентские каналы получают креды от одного `CertificateStore` (grpc-js `CertificateProvider`): `update()` меняет то, что предъявит следующее рукопожатие, установленные соединения и стримы (jobs, доставки, входящие RPC) продолжают работать. `Control.Open` не переоткрывается — runtime считает второй Open того же инстанса заменой сессии. Call-сервер перебиндится на тот же порт со свежими кредами и даёт своим in-flight вызовам доработать: in-place смена secure context поддерживается не всеми runtime (Bun).
- **Reconnect переиспользует leaf.** Пока до `notAfter` больше `certRefreshLeadMs`, Provision не вызывается; RefreshCert с `RESOURCE_EXHAUSTED` (лимит частоты) повторяется через 60 с.
- **Keepalive (NSDK-11).** 30 s / 10 s на всех клиентских каналах — совместимо с политикой runtime (MinTime 10 s). grpc-js на сервере политику пингов не применяет.
- **Drain.** `Drain{reason}` → событие `draining` и info-лог; reconnect — после закрытия стрима runtime, без error-логов.
- **Упорядоченный stop.** 1) снять анонс (перерегистрация с пустым `call_endpoint`, чтобы пиры перестали выбирать инстанс); 2) Call-сервер отвечает `UNAVAILABLE` + `x-sb-not-dispatched`, подписчики перестают брать работу; 3) дождаться in-flight вызовов, доставок и jobs; 4) дослать очередь publish; 5) финальный flush телеметрии с ожиданием ACK (≤ 2 с); 6) закрыть стримы, каналы и сервер. Всё в пределах `stopTimeoutMs`.
- **Logger вместо console.** Диагностика SDK идёт в `options.logger`; по умолчанию только warn/error в консоль.
- **Изоляция слушателей (NSDK-13).** Исключение в `sb.on(...)` логируется и не ломает ни бридж, ни другие слушатели.
- **CSR без `@peculiar/x509`.** PKCS#10 для P-256 — несколько десятков строк DER поверх WebCrypto; библиотека тянула `reflect-metadata` глобально (NSDK-15).
- **Identity читается по требованию.** Логгер, метрики, подписчики и транспорты получают геттеры: идентичность появляется на Welcome (и меняется только при свежем Provision после долгого простоя).

## Зависимости

Зависит на: `@grpc/grpc-js`; домены `../registry`, `../rpc`, `../events`, `../workflow`, `../job`, `../telemetry`, `../serde`; `../errors`, `../logger`; pb-стабы `../pb/servicebridge/v1/*`.

Зависят: `sdk/node/index.ts`, HTTP-интеграции (`sb.routes`, `sb.diagnostics`), `../testing`, `tests/e2e`.

Каналы к runtime требуют ровно один URI SAN `spiffe://service-bridge/runtime` поверх проверки цепочки и EKU.
