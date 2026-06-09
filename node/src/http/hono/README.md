# http/hono

## Зона ответственности

Интеграция SDK с Hono. `attachHono` собирает зарегистрированные роуты из `app.routes`, кладёт их в `sb.routes` (как `source: "hono"`), публикует HTTP-endpoint (`host:port`) через `RouteCollector.publishHttp`, и оборачивает `app.fetch` для трейсинга: парсит входящий `X-SB-Trace`, прогоняет downstream-цепочку в `runWithTrace`, эмитит HTTP.HANDLE-операцию и захватывает тела запроса/ответа. ADR 0001.

Не делает: не запускает сервер (Hono server-agnostic — пользователь поднимает `Bun.serve` / `@hono/node-server` / Deno вручную), не нормализует паттерны роутов (pattern идёт в registry as-is — косметика на UI Service Map).

## Публичный контракт

Импорт: `import { attachHono, collectHonoRoutes, type HonoEndpoint } from "servicebridge/hono";`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `attachHono` | `(app: Hono, sb: ServiceBridge, endpoint: HonoEndpoint) => void` | — | Собирает роуты из `app.routes` в `sb.routes`, резолвит `host` через `resolveHttpAdvertiseHost(endpoint.host)`, публикует endpoint `host:port` через `sb.routes.publishHttp`, оборачивает `app.fetch` для трейсинга (idempotent — повторный вызов на том же `app` не дублирует обёртку). Если вызван до `sb.start()` — endpoint попадёт в первый `RegisterRequest` без restart; после `start()` — `publishHttp` рестартует Registry-watch стрим. |
| `collectHonoRoutes` | `(app: Hono, sb: ServiceBridge) => void` | — | Только сбор роутов в `sb.routes`, без публикации endpoint и без обёртки `fetch`. Низкоуровневый слой для тестов. Метод приводится к верхнему регистру; `method === "ALL"` (от `app.all(...)`) пропускается. |
| `HonoEndpoint` | `interface { host?: string; port: number }` | — | Адрес, на котором фактически слушает Hono-сервер. `port` обязателен (Hono агностичен к серверу, не открывает сокет сам — должен совпадать с тем, что передан в `Bun.serve`/`serve`). `host` опционален. |
| `HonoEndpoint.host` | `string \| undefined` | `127.0.0.1` (с одноразовым warn) | Advertise-host для HTTP-плоскости (ADR 0001). Если опущен — `resolveHttpAdvertiseHost()` → `127.0.0.1`. |
| `HonoEndpoint.port` | `number` | — (обязателен) | Порт HTTP-сервера. |

### Трейсинг и захват тел (поведение обёртки `app.fetch`)

`attachHono` оборачивает `app.fetch` один раз (флаг `Symbol.for("servicebridge.hono.trace")` на инстансе `app`). На каждый запрос:

- `X-SB-Trace` парсится в `TraceContext`; при отсутствии/невалидности минтится свежий root.
- `businessKey` = заголовок `Idempotency-Key`, иначе `"<METHOD> <pathname>"`.
- downstream-цепочка выполняется внутри `runWithTrace`, чтобы handler и пользовательский код видели активный контекст через ALS.
- эмитится HTTP.HANDLE-операция (`Channel.HTTP`, `kind: HttpHandle`).
- тело запроса (через клон, чтобы не «съесть» стрим для handler) и тело ответа захватываются best-effort как raw-JSON (`contract: "raw/json"`); пустые/`{}`/`null` тела не пишутся.
- статус операции: HTTP `>= 400` (включая `>= 500`) → `Status.ERROR` (с текстом `HTTP <code>`); исключение из цепочки → `Status.ERROR` с сообщением и ре-throw; иначе → `Status.SUCCESS`. На wire это единый словарь статусов (`success`/`error`).

### Пример использования (Bun)

```ts
import { Hono } from "hono";
import { ServiceBridge } from "servicebridge";
import { attachHono } from "servicebridge/hono";

const sb = new ServiceBridge(URL, KEY);
const app = new Hono();
app.get("/api/orders/:id", (c) => c.json({ id: c.req.param("id") }));
app.post("/api/orders", async (c) => c.json({ created: true }));

attachHono(app, sb, { port: 3000 });   // host -> resolveHttpAdvertiseHost()
await sb.start();                       // endpoint попадёт в initial register
Bun.serve({ fetch: app.fetch, port: 3000 });
```

### Пример (Node + @hono/node-server)

```ts
import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { attachHono } from "servicebridge/hono";

const sb = new ServiceBridge(URL, KEY);
const app = new Hono();
app.get("/ping", (c) => c.text("pong"));
attachHono(app, sb, { port: 8080 });
await sb.start();
serve({ fetch: app.fetch, port: 8080 });
```

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `installHonoTracing` | `(app: Hono, sb: ServiceBridge) => void` | — | Оборачивает `app.fetch` логикой трейсинга и захвата тел. Идемпотентен через `Symbol.for("servicebridge.hono.trace")` на инстансе. Не реэкспортируется. |
| `TRACE_FLAG` | `unique symbol` (`Symbol.for("servicebridge.hono.trace")`) | — | Маркер на инстансе `app`, защищающий от двойной обёртки `fetch`. |
| фильтр `method === "ALL"` | — | — | Внутри `collectHonoRoutes`: Hono ставит method `"ALL"` для `app.all(...)`; SDK не раскладывает его в конкретные методы и пропускает такой роут. |

## Архитектурные решения и почему

- **Не pure middleware.** Hono server-agnostic, middleware не имеет надёжного способа узнать host:port (нет `server.address()` из контекста). Поэтому явный `attachHono(app, sb, endpoint)`.
- **`port` обязателен.** Hono не открывает сокет, и `c.req.url` — это URL запроса, не bind-адрес. Чтобы не угадывать, порт передаёт пользователь.
- **Обёртка `fetch`, а не `app.use`.** `Hono.use(...)` после регистрации роутов не догоняет уже сматченные роуты — порядок объявления матчит. Чтобы трейсинг и захват тел работали для всех роутов, оборачивается сам `app.fetch`.
- **Pattern не нормализуется.** Сырой Hono-паттерн (`:id{[0-9]+}` и т. п.) уходит в registry as-is; нормализация — косметика на UI Service Map, а не задача SDK (`Route.pattern`).
- **Вызов до `sb.start()` — рекомендованный flow.** Тогда endpoint попадает в первый `RegisterRequest` без restart. `publishHttp` безопасен до старта — `triggerRestart` тогда no-op.
- **Тела захватываются через клон запроса/ответа.** Чтение тела для capture не должно «съедать» стрим, который прочитает handler или вернёт клиент.

## Зависимости

Зависит от:
- `hono` (peerDependency, type-only) — тип `Hono`.
- `../../connection/service-bridge` — тип `ServiceBridge` (`sb.routes`, `sb.telemetry`).
- `../../telemetry/context` — `runWithTrace`.
- `../../telemetry/ops` — `Channel`, `HttpHandle`, `Status`.
- `../_common/body-capture` — `bodyToBytes`, `RAW_JSON_CONTRACT`.
- `../_common/trace-wrap` — `contextFromXSbTrace`.
- `../endpoint` — `resolveHttpAdvertiseHost`.

Используется в:
- `sdk/node/tests/e2e/http-hono.test.ts`.
- `sdk/node/src/http/hono/plugin.test.ts`.
- Прикладной код через subpath `servicebridge/hono`.
