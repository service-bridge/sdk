# http/express

## Зона ответственности

Интеграция SDK с Express 4 / 5. Обходит router stack пользовательского `app` (включая sub-routers), кладёт собранные роуты в `sb.routes`, публикует HTTP-endpoint (`host:port`) в registry для Service Map (ADR 0001) и ставит на `app` один trace+telemetry middleware (X-SB-Trace propagation + HTTP.HANDLE op с захватом тела).

Не делает: не запускает HTTP-сервер — `app.listen()` остаётся за пользователем; не вмешивается в маршрутизацию и не меняет ответы (тело только читается для capture).

## Публичный контракт

Импорт: `import { attachExpress, type ExpressEndpoint } from "service-bridge/express";`

### `attachExpress(app, sb, endpoint)`

Сигнатура `(app: Express, sb: ServiceBridge, endpoint: ExpressEndpoint) => void`. Идемпотентен (дедуп роутов в `RouteCollector`, trace-middleware ставится ровно один раз). Безопасен и до `sb.start()` — endpoint осядет в Registry и попадёт в первый `RegisterRequest`. Делает:

1. Обходит router stack, добавляет каждый роут в `sb.routes.add({ method, pattern, source: "express" })`.
2. Публикует endpoint через `sb.routes.publishHttp({ host, port })`; `host` резолвится через `resolveHttpAdvertiseHost(endpoint.host)`.
3. Ставит trace+telemetry middleware (`installTraceMiddleware`), поднимая его в начало router stack.

| Параметр | Тип | По умолчанию | Что делает |
|----------|-----|--------------|------------|
| `app` | `Express` | — | Пользовательское Express-приложение; источник роутов и носитель middleware. |
| `sb` | `ServiceBridge` | — | Клиент рантайма; приёмник роутов (`sb.routes`) и telemetry (`sb.telemetry`). |
| `endpoint` | `ExpressEndpoint` | — | Адрес, на котором слушает Express-сервер. |

### `ExpressEndpoint`

| Поле | Тип | По умолчанию | Что делает |
|------|-----|--------------|------------|
| `port` | `number` | — (обязателен) | Порт, на котором фактически слушает Express. Передаётся явно: при bind на `0` реальный порт в момент сбора роутов неизвестен. |
| `host` | `string` | `127.0.0.1` (с одноразовым warn) | Advertise-host. Если опущен — fallback `127.0.0.1` с одноразовым `console.warn`. Для cross-host передавай явный `host`. |

### Trace + telemetry middleware

Ставится автоматически внутри `attachExpress`. Отдельного публичного API нет.

- Стартует HTTP.HANDLE через общий `startHttpOp` (`../_common/http-op`): заголовок `x-sb-trace` → `TraceContext`, `subject = "http.handle:${method}/${path}"`, `businessKey` = заголовок `Idempotency-Key` или `"${method} ${path}"`. Downstream chain идёт в `runWithTrace(op.scope, () => next())`. Route handlers и их `sb.rpc.call` / `event.publish` видят через ALS контекст, где `traceId` един с HTTP.HANDLE, а `parentOpId` = `opId` этого op'а — downstream вложен под HTTP.HANDLE, а не отдельный корень.
- Захватывает тело запроса (IN) и ответа (OUT) как raw-JSON (`RAW_JSON_CONTRACT`); пустые тела не пишутся. Пока `op.capturing === false` (эффективный режим op'а — `none`), обёртка `res.json`/`res.send` не ставится и тела не сериализуются вовсе.
- Завершает op статусом из единого словаря: `statusForHttpCode` даёт `SUCCESS` (код < 400) и `ERROR` (код ≥ 400); `TIMEOUT` (`client abort`) ставится, когда соединение закрылось без `res.end`.

### Пример использования

```ts
import express from "express";
import { ServiceBridge } from "service-bridge";
import { attachExpress } from "service-bridge/express";

const sb = new ServiceBridge(URL, KEY);
const app = express();

app.get("/api/orders/:id", getOrder);
app.post("/api/orders", createOrder);

attachExpress(app, sb, { port: 3000 }); // host -> resolveHttpAdvertiseHost()
await sb.start();
app.listen(3000);
```

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `RouteLike`, `LayerLike` | `type` | — | Type narrowing над Express layer/route shape. Express 4 хранит router как `_router`, Express 5 — как `router`. |
| `getRootRouter(app)` | `(Express) => { stack } \| null` | — | Runtime fallback на оба shape: `app._router ?? app.router`. |
| `prefixFromLayer(layer)` | `(LayerLike) => string` | `""` | Best-effort извлечение префикса sub-router из `layer.regexp.source`. На Express 5 (path-to-regexp v8) `regexp` отсутствует — префикс не извлекается, sub-router routes регистрируются БЕЗ префикса. |
| `collect(router, prefix, out)` | `(router, string, out[]) => void` | — | Рекурсивный обход stack с поддержкой sub-routers; раскладывает multi-method роуты, пропускает `_all`. |
| `installTraceMiddleware(app, sb)` | `(Express, ServiceBridge) => void` | — | Ставит ровно один trace+telemetry middleware (флаг `TRACE_FLAG` на `app`) и зовёт `hoistTraceMiddleware`. |
| `hoistTraceMiddleware(app)` | `(Express) => void` | — | Поднимает только что добавленный слой в начало router stack, чтобы он отрабатывал до роутов. Бросает, если root router недоступен. |
| `TRACE_FLAG` | `const string` | `"__servicebridge_trace__"` | Маркер идемпотентности middleware на `app`. |
| `Router` (re-export) | `type` | — | Реэкспорт типа `express.Router` для тестов. |

## Архитектурные решения и почему

- **Синхронный сбор в `attachExpress`, а не patch `app.listen` и не first-request walk.** Пользователь сам знает `port` (особенно при bind на `0`) и передаёт его явно, поэтому endpoint и роуты собираются в момент вызова — детерминированно, без магии вокруг `listen` (которая ломка в Express 5). Симметрично `attachHono`.
- **Middleware поднимается в начало stack, и провал подъёма — это ошибка.** `attachExpress` обычно зовут ПОСЛЕ `app.get(...)`. Express ставит `app.use(...)` в конец stack, где роуты уже завершили запрос. Поэтому только что добавленный слой переносится в начало — иначе trace/telemetry не отработал бы. `app.use(...)` обязан материализовать root router; если после него `getRootRouter` всё равно вернул `null`, стек не той формы, которую мы умеем читать, и HTTP.HANDLE молча не эмиттился бы вообще — поэтому `hoistTraceMiddleware` бросает, а не пропускает подъём.
- **Тело только читается, ответ не меняется.** `res.json`/`res.send` оборачиваются лишь чтобы запомнить OUT-тело; оригинальный вызов сохраняется. IN-тело читается в finalize, когда body-parser уже отработал. Захват — raw-JSON (`RAW_JSON_CONTRACT`), без proto-схемы. Всё это включается только при `HttpOp.capturing`: в дефолтном режиме `none` `OpHandle` всё равно выбросил бы байты, а `JSON.stringify` ответа стоил бы дороже самой операции.
- **Express 4 vs 5.** Поддержаны оба shape (`_router`/`router`). Sub-router prefix извлекается только на 4 (документированное ограничение). Для строгой публикации с префиксом — регистрируйте роуты top-level (`app.get("/v1/users/list", ...)`).
- **Type-only import Express.** `import type { Express, ... }`. Пользователь без установленного express (`peerDependenciesMeta.optional`) не падает на resolve.

## Зависимости

Зависит от:
- `express` (peerDependency, optional) — type-only.
- `../endpoint` — `resolveHttpAdvertiseHost`.
- `../_common/body-capture` — `bodyToBytes`, `RAW_JSON_CONTRACT`.
- `../_common/http-op` — `startHttpOp`, `statusForHttpCode`.
- `../route` — тип `Route` через `sb.routes`.
- `../../connection/service-bridge` — type-only `ServiceBridge`.
- `../../telemetry/context`, `../../telemetry/ops` — `runWithTrace`, `Status` (только `TIMEOUT` на client abort).

Используется в:
- `sdk/node/tests/e2e/http-express.test.ts`.
- Прикладной код через subpath `service-bridge/express`.
