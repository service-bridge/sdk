# http

## Зона ответственности

Общий доменный код для HTTP-интеграций (ADR 0001): сбор и дедуп собранных роутов, публикация HTTP-endpoint инстанса в `Registry`, resolver advertise-host для HTTP-плейна, старт и завершение `HTTP.HANDLE` op'а (шаблон роута в subject и meta, businessKey, trace-scope, маппинг кода в статус) и сериализация тел запроса/ответа в байты для payload-capture. Через эти хелперы integrations (`./express`, `./fastify`, `./hono`) пишут роуты и endpoint в `Registry` и эмиттят `HTTP.HANDLE` op'ы.

Не делает: ничего фреймворк-специфичного (это в подпапках), ничего I/O (не лезет в DB, не открывает сокеты), не защищает периметр (rate limit, фильтр сканеров — ingress/WAF приложения), **не нормализует** route-паттерны — pattern хранится и сериализуется как есть (косметика на UI Service Map).

## Публичный контракт

Корень `src/http/` ничего не реэкспортирует — нет `index.ts`. Весь его код помечен `@internal` и потребляется только интеграциями. Публичный API прикладного кода живёт в subpath-пакетах и описан в их README: `./express` (`attachExpress`, `ExpressEndpoint`), `./fastify` (`sbFastify`, `SbFastifyOptions`), `./hono` (`attachHono`, `HonoEndpoint`).

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| (нет публичных опций) | — | — | Корень `http/` не экспортирует ничего наружу. Прикладной код пишет роуты в свой фреймворк и подключает его через subpath-интеграцию. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Route` | `{ method: string; pattern: string; source: "express" \| "fastify" \| "hono" }` | — | Канонический роут: метод UPPERCASE, `pattern` — raw framework-паттерн без нормализации. |
| `RouteSink` | `{ setEndpoint(endpoint: string): void; triggerRestart(): void }` | — | Контракт на стороне потребителя; реализуется `Registry` (+ `ServiceBridge` через `onRestart`). |
| `RouteCollector` | `new (sink: RouteSink)` | — | Аккумулирует роуты по ключу `${method} ${pattern}` (дедуп) и публикует HTTP-endpoint. |
| `RouteCollector.add` | `(route: Route) => void` | — | Добавляет роут; дубликат по `${method} ${pattern}` затирает предыдущий (last write wins). |
| `RouteCollector.size` | `() => number` | — | Кол-во уникальных `${method} ${pattern}`; для диагностики тестов. |
| `RouteCollector.publishHttp` | `(endpoint: { host: string; port: number }) => void` | — | `sink.setEndpoint("host:port")` + `sink.triggerRestart()`. До `sb.start()` `triggerRestart` no-op — endpoint оседает и попадёт в первый `RegisterRequest`. |
| `RouteCollector.snapshot` | `() => readonly Route[]` | — | Snapshot собранных роутов в insertion order. |
| `resolveHttpAdvertiseHost` | `(explicit: string \| undefined, log) => string` | `"127.0.0.1"` | `explicit` (если непустой) → иначе `"127.0.0.1"` с одноразовым warn в logger SDK (`sb.diagnostics`). Env не читается. |
| `_resetHostWarn` | `() => void` | — | Test-only: сбрасывает one-shot warn-флаг `resolveHttpAdvertiseHost`. Без него `endpoint.test.ts` зависел бы от порядка тестовых файлов — флаг модульный, а Bun держит один module registry на процесс. |
| `firstHeader` | `(value: string \| string[] \| null \| undefined) => string \| undefined` | — | Первое значение заголовка: Node отдаёт повторяющиеся массивом, Fetch API — строкой или `null`. |
| `bodyToBytes` | `(body: unknown) => Uint8Array \| null` | — | Best-effort сериализация тела в байты; `null` если захватывать нечего (`undefined`/`null`/`{}`/`"null"`/пустое/несериализуемое). |
| `RAW_JSON_CONTRACT` | `string` | `"raw/json"` | Contract-hash маркер already-JSON payload; равен runtime `telemetry.ContractRawJSON` — рантайм отдаёт байты verbatim. |
| `makeSbStub` | `(captureMode?: CaptureMode) => SbStub` | `"none"` | Test-only: `ServiceBridge`-заглушка с настоящим `RouteCollector` и записью `started` / `endCalls` / `captures`; выданный ей op-handle отдаёт `capturing` по этому режиму. Импортируется только из `*.test.ts`. |

## Архитектурные решения и почему

- **Без нормализации паттернов.** SDK хранит `pattern` как есть. Канон на wire — `${method} ${pattern}` в `name` IncomingMethod (`METHOD_TYPE_HTTP`), без input/output-схемы и contract-hash: HTTP-роуты декларируются, не транспортируются рантаймом (ADR 0001). Косметика паттерна — на UI Service Map.
- **Один публичный метод endpoint-жизненного цикла: `publishHttp`.** Hono узнаёт port до `sb.start()` через явный аргумент в `attachHono`; Express — из переданного `ExpressEndpoint.port`; Fastify — в `onListen` после `listen()` через `server.address()`. Во всех случаях `publishHttp` пишет endpoint в `Registry` и дёргает `triggerRestart`. `ServiceBridge` транслирует `triggerRestart` в рестарт Registry-watch стрима — runtime моментально видит endpoint, без ожидания natural reconnect.
- **`publishHttp` до start — безопасный.** Если интеграция дёрнет `publishHttp` до `sb.start()`, `triggerRestart` — no-op; endpoint оседает в `Registry` и попадёт в первый `RegisterRequest`.
- **Domain-aware код в `src/http/`, не в `src/utils/`.** CLAUDE.md project override запрещает `utils/` для domain-aware кода. HTTP-роутинг — domain.
- **`HTTP.HANDLE` живёт в `_common/http-op.ts`, а не в трёх плагинах.** Плагины дают только фреймворк-специфичное: сбор роутов, шаблон роута до маршрутизации, доступ к телу, хуки. Единый subject `http.handle:<METHOD>/<route>` и meta `{method, route, status}` во всех трёх (NSDK-04/16): раньше Express и Hono писали сырой путь (кардинальность и PII в subject), Fastify — шаблон. businessKey без query по той же причине. Downstream chain выполняется в trace-scope op'а (Express/Hono — `runWithTrace`, Fastify — `als.enterWith`).
- **`X-SB-Trace` не принимается по умолчанию (NSDK-17).** Публичный endpoint позволял любому клиенту вписать свой запрос в произвольное дерево trace. Для внутренних HTTP-серверов, к которым ходят только сервисы ServiceBridge, включается `trustTraceHeader`. То же в Go (`sbhttp.WithTrustTraceHeader`).
- **Нет защиты периметра в плагинах (NSDK-04).** Встроенный фильтр «сканеров» резал легитимные пути (`/api/v1/test`, `/users/42/info`), а лимит 300 rpm на IP за балансировщиком душил весь трафик. Это работа ingress/WAF приложения, не SDK наблюдаемости.
- **Маскирование секретов — на runtime.** SDK передаёт тела как есть; runtime маскирует payload при приёме (решение 3).
- **Захват тел гейтится ДО сериализации, а не внутри `OpHandle`.** Режим канала HTTP по умолчанию `"none"` (fail-safe до первого registry-snapshot), и `OpHandle.capture` в этом режиме выбрасывает переданные байты первой же строкой. Пока решение о захвате принималось только там, каждый запрос платил полный `JSON.stringify` тела запроса И ответа в мусор (у Hono — ещё и клон стрима ответа). Поэтому `startHttpOp` возвращает `capturing` — геттер `OpHandle.capturing`, — и плагины по нему гейтят всё: чтение тела, `bodyToBytes`, обёртку `res.json`/`res.send` у Express, `req.clone()` / `res.clone()` у Hono, хук `onSend` у Fastify. Признак берётся с самого хендла, а не спросом у канала (`captureModeForChannel`): `OpHandle` резолвит режим один раз в `start()`, учитывая per-handler сужение (`resolveCaptureMode`) — вопрос к каналу этого сужения не увидел бы, и хендлер с явно суженным режимом всё равно платил бы за сериализацию.
- **Один `AsyncLocalStorage` на весь пакет.** `als` (trace-scope) живёт в `telemetry/context`. Плагины импортируются через subpath-экспорты (`service-bridge/fastify` и т.д.), ядро — через `service-bridge`. Если сборка инлайнит копию `telemetry/context` в каждый entry-бандл (tsup `splitting:false`), `als` дублируется: плагин ставит контекст на один инстанс, `rpc.call` ядра читает другой → контекст не виден → trace расщепляется на два `traceId`. Инвариант: общий код сводится в один чанк (`tsup splitting:true`) — ровно один `new AsyncLocalStorage` на весь `dist`. Защищено `tests/build/single-als-instance.test.ts`.
- **Захват тел (Input/Output Data).** Плагины капчат request body (IN) и response body (OUT) через `OpHandle.captureIn/captureOut(bytes, RAW_JSON_CONTRACT)`; `bodyToBytes` даёт `null` для bodyless-запросов (GET), так что пустые IN-payload'ы не эмиттятся. HTTP-тела не имеют proto-схемы — рантайм хранит и отдаёт их verbatim (`"raw/json"`). Источники тела: Express — `req.body` (нужен `express.json()`) + обёртка `res.json/res.send`; Fastify — `req.body` в `preHandler` + `payload` в `onSend`; Hono — клон `Request`/`Response` (стримы одноразовы). Читаются они только при `HttpOp.capturing`; выбор между `all` и `errors` (немедленная отправка против буфера до ошибки) остаётся за `OpHandle`.

## Зависимости

Зависит от:
- `../telemetry/trace-context`, `../telemetry/wire-trace` — разбор `X-SB-Trace`, root-контекст.
- `../telemetry/context`, `../telemetry/ops` — trace-scope и `HTTP.HANDLE` op.
- `../telemetry/payload-capture` — тип `CaptureMode` (гейт захвата тел).
- `../connection/service-bridge` — type-only `ServiceBridge` (`sb.routes`, `sb.telemetry`).
- `../pb/servicebridge/v1/registry` — индирект, через `Registry` (`METHOD_TYPE_HTTP`).

Используется в:
- `../registry/registry.ts` — `Registry` владеет `RouteCollector` (`Registry.routes`) и реализует `RouteSink`; `snapshot()` сериализуется в `RegisterRequest`.
- `../connection/service-bridge.ts` — пробрасывает `onRestart` в `Registry`-конструктор; callback рестартует Registry-watch стрим.
- `./express/`, `./fastify/`, `./hono/` — integrations потребляют `RouteCollector`, `resolveHttpAdvertiseHost`, `startHttpOp`, `bodyToBytes`, `RAW_JSON_CONTRACT`.
