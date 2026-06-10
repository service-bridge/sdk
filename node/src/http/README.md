# http

## Зона ответственности

Общий доменный код для HTTP-интеграций (ADR 0001): сбор и дедуп собранных роутов, публикация HTTP-endpoint инстанса в `Registry`, resolver advertise-host для HTTP-плейна, парсинг входящего `X-SB-Trace` и сериализация тел запроса/ответа в байты для payload-capture. Через эти хелперы integrations (`./express`, `./fastify`, `./hono`) пишут роуты и endpoint в `Registry` и эмиттят `HTTP.HANDLE` op'ы.

Не делает: ничего фреймворк-специфичного (это в подпапках), ничего I/O (не лезет в DB, не открывает сокеты), **не нормализует** route-паттерны — pattern хранится и сериализуется как есть (косметика на UI Service Map).

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
| `resolveHttpAdvertiseHost` | `(explicit?: string) => string` | `"127.0.0.1"` | Resolver: `explicit` (если непустой) → `"127.0.0.1"` (с одноразовым `console.warn`). Хост задаётся явной опцией `host` плагина — env не читается. |
| `_resetHostWarn` | `() => void` | — | Test-only: сбрасывает one-shot warn-флаг `resolveHttpAdvertiseHost`. |
| `contextFromXSbTrace` | `(header: string \| null \| undefined) => TraceContext` | — | Парсит `X-SB-Trace`; на miss/невалидный — `mintRootContext()`. Никогда не throw'ит. |
| `bodyToBytes` | `(body: unknown) => Uint8Array \| null` | — | Best-effort сериализация тела в байты; `null` если захватывать нечего (`undefined`/`null`/`{}`/`"null"`/пустое/несериализуемое). |
| `RAW_JSON_CONTRACT` | `string` | `"raw/json"` | Contract-hash маркер already-JSON payload; равен runtime `telemetry.ContractRawJSON` — рантайм отдаёт байты verbatim. |

## Архитектурные решения и почему

- **Без нормализации паттернов.** SDK хранит `pattern` как есть. Канон на wire — `${method} ${pattern}` в `name` IncomingMethod (`METHOD_TYPE_HTTP`), без input/output-схемы и contract-hash: HTTP-роуты декларируются, не транспортируются рантаймом (ADR 0001). Косметика паттерна — на UI Service Map.
- **Один публичный метод endpoint-жизненного цикла: `publishHttp`.** Hono узнаёт port до `sb.start()` через явный аргумент в `attachHono`; Express — из переданного `ExpressEndpoint.port`; Fastify — в `onListen` после `listen()` через `server.address()`. Во всех случаях `publishHttp` пишет endpoint в `Registry` и дёргает `triggerRestart`. `ServiceBridge` транслирует `triggerRestart` в рестарт Registry-watch стрима — runtime моментально видит endpoint, без ожидания natural reconnect.
- **`publishHttp` до start — безопасный.** Если интеграция дёрнет `publishHttp` до `sb.start()`, `triggerRestart` — no-op; endpoint оседает в `Registry` и попадёт в первый `RegisterRequest`.
- **Domain-aware код в `src/http/`, не в `src/utils/`.** CLAUDE.md project override запрещает `utils/` для domain-aware кода. HTTP-роутинг — domain.
- **`HTTP.HANDLE` op emission.** Каждый incoming HTTP request парсит `X-SB-Trace` через `contextFromXSbTrace`; на miss/malformed — `mintRootContext()`. Plugin эмиттит `HTTP.HANDLE` через `sb.telemetry.startOp({ channel: Channel.HTTP, kind: HttpHandle, subject: "http.handle:<METHOD>/<path>", businessKey })` и оборачивает downstream chain в trace-scope `childContext(ctx, handle.opId)` (Express/Hono — `runWithTrace`, Fastify — `als.enterWith` в `preHandler`). `OpHandle.end(...)` ставит `Status.SUCCESS` (`<400`), `Status.ERROR` (`>=400`), `Status.TIMEOUT` (client abort). `businessKey` — заголовок `Idempotency-Key`, иначе `"<METHOD> <path>"`. parent_op_id самого HTTP.HANDLE — из parsed `X-SB-Trace` (root request — без parent); downstream-операции (`sb.rpc.call` / `event.publish`) наследуют `traceId` HTTP.HANDLE и встают под него как parent — единый trace, без split'а.
- **Один `AsyncLocalStorage` на весь пакет.** `als` (trace-scope) живёт в `telemetry/context`. Плагины импортируются через subpath-экспорты (`service-bridge/fastify` и т.д.), ядро — через `service-bridge`. Если сборка инлайнит копию `telemetry/context` в каждый entry-бандл (tsup `splitting:false`), `als` дублируется: плагин ставит контекст на один инстанс, `rpc.call` ядра читает другой → контекст не виден → trace расщепляется на два `traceId`. Инвариант: общий код сводится в один чанк (`tsup splitting:true`) — ровно один `new AsyncLocalStorage` на весь `dist`. Защищено `tests/build/single-als-instance.test.ts`.
- **Захват тел (Input/Output Data).** Плагины капчат request body (IN) и response body (OUT) через `OpHandle.captureIn/captureOut(bytes, RAW_JSON_CONTRACT)`; `bodyToBytes` даёт `null` для bodyless-запросов (GET), так что пустые IN-payload'ы не эмиттятся. HTTP-тела не имеют proto-схемы — рантайм хранит и отдаёт их verbatim (`"raw/json"`). Источники тела: Express — `req.body` (нужен `express.json()`) + обёртка `res.json/res.send`; Fastify — `req.body` в `preHandler` + `payload` в `onSend`; Hono — клон `Request`/`Response` (стримы одноразовы). Запись гейтит сам `OpHandle` по runtime-pushed режиму HTTP-канала (none/all/errors).

## Зависимости

Зависит от:
- `../telemetry/trace-context`, `../telemetry/wire-trace` — `contextFromXSbTrace`, `childContext`.
- `../telemetry/context`, `../telemetry/ops` — trace-scope и `HTTP.HANDLE` op (используется в интеграциях подпапок).
- `../pb/servicebridge/v1/registry` — индирект, через `Registry` (`METHOD_TYPE_HTTP`).

Используется в:
- `../registry/registry.ts` — `Registry` владеет `RouteCollector` (`Registry.routes`) и реализует `RouteSink`; `snapshot()` сериализуется в `RegisterRequest`.
- `../connection/service-bridge.ts` — пробрасывает `onRestart` в `Registry`-конструктор; callback рестартует Registry-watch стрим.
- `./express/`, `./fastify/`, `./hono/` — integrations потребляют `RouteCollector`, `resolveHttpAdvertiseHost`, `contextFromXSbTrace`, `bodyToBytes`, `RAW_JSON_CONTRACT`.
