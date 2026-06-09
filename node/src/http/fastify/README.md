# http/fastify

## Зона ответственности

Интеграция SDK с Fastify 4 / 5 через нативный plugin (`fastify-plugin`). Собирает роуты через `onRoute`-хук в `sb.routes`, публикует `http_endpoint` после `fastify.listen()` через `onListen`-хук (ADR 0001). Дополнительно ведёт HTTP.HANDLE op-lifecycle на каждый request: ставит ALS trace-scope из `X-SB-Trace`, стартует/закрывает op по статус-коду и захватывает payload запроса/ответа.

Не делает: не запускает HTTP-сервер (`fastify.listen({...})` остаётся за пользователем); не вмешивается в маршрутизацию и сериализацию ответа.

## Публичный контракт

Импорт: `import { sbFastify, type SbFastifyOptions } from "service-bridge/fastify";`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `sbFastify` | `fastify-plugin`-обёрнутый `FastifyPluginAsync<SbFastifyOptions>` | — | Регистрируется через `fastify.register(sbFastify, { sb })`. Хуки: `onRoute` (собирает роут в `sb.routes` — поддерживает `method: string \| string[]`, HEAD отсекает); `onListen` (читает `fastify.server.address()` → `sb.routes.publishHttp({ host, port })`); `preHandler`/`onSend`/`onResponse`/`onRequestAbort` (HTTP.HANDLE op + payload capture). Совместимость декларирована `{ fastify: "4.x \|\| 5.x" }`. |
| `SbFastifyOptions.sb` | `ServiceBridge` | — (обязательно) | Инстанс SDK-клиента, через который публикуются роуты (`sb.routes`) и стартует телеметрия (`sb.telemetry`). |
| `SbFastifyOptions.host` | `string` | bound socket address из `fastify.server.address()` | Явный host для публикуемого `http_endpoint`. Если опущен — `resolveHttpAdvertiseHost(addr.address)`: фактический bound-address сокета (если непустой) → `127.0.0.1` (с одноразовым warn). |

Событийный API: плагин аугментирует `FastifyRequest` полями `sbTraceCtx?: TraceContext` (распарсенный/свежий root trace-контекст) и `sbHttpHandle?: OpHandle` (in-flight HTTP.HANDLE op). Заполняются в `preHandler`, доступны в пользовательских хуках/хендлерах.

### Пример использования

```ts
import Fastify from "fastify";
import { ServiceBridge } from "service-bridge";
import { sbFastify } from "service-bridge/fastify";

const sb = new ServiceBridge(URL, KEY);
const app = Fastify();
await app.register(sbFastify, { sb });

app.get("/api/orders/:id", async (req) => ({ id: (req.params as any).id }));
app.post("/api/orders", async () => ({ created: true }));

await sb.start();
await app.listen({ port: 3000 });  // onListen → publishHttp
```

С явным host для cross-host advertise:

```ts
await app.register(sbFastify, { sb, host: process.env.POD_IP });
```

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `plugin` | `FastifyPluginAsync<SbFastifyOptions>` | — | Сама функция-плагин до обёртки `fp(...)`. Регистрирует хуки `preHandler`, `onSend`, `onResponse`, `onRequestAbort`, `onRoute`, `onListen`. Не экспортируется. |
| `netAddressOf(server)` | `(server) => NetAddress \| null` | — | Type-safe чтение `server.address()` — Node возвращает либо string (Unix socket), либо `{ address, port }`. Возвращает только последнюю форму. |
| `NetAddress` | `{ address: string; port: number }` | — | Внутренний shape результата `netAddressOf`. |

## Архитектурные решения и почему

- **`fp` (fastify-plugin) обязателен.** Без обёртки `onRoute` видит только роуты этого encapsulated context — а нужны все роуты app. `fastify-plugin` снимает encapsulation, чтобы хуки добрались до root scope.
- **`onListen`, не `onReady`.** `onReady` срабатывает ДО `listen` — `fastify.server.address()` ещё null. `onListen` — после bind, port уже доступен (важно при `{ port: 0 }`). Прочитанный bound-address подаётся как explicit в `resolveHttpAdvertiseHost`, поверх него — `opts.host`. Если адрес прочитать не удалось — `fastify.log.warn`, endpoint не публикуется.
- **HTTP.HANDLE op-lifecycle через хуки.** `preHandler` ставит ALS trace-scope через `als.enterWith` (Fastify-хуки возвращают Promise, а не принимают `next()`), стартует HTTP.HANDLE op и захватывает IN-payload (тело запроса уже распарсено к этому моменту). `onSend` захватывает OUT-payload (сериализованный ответ) до закрытия op. `onResponse` закрывает op: `statusCode >= 400` (включая `>= 500`) → `Status.ERROR`, иначе `Status.SUCCESS`. `onRequestAbort` закрывает op как `Status.TIMEOUT` («client abort»), если клиент отвалился до ответа — иначе `onResponse` не сработает и START-frame (`Status.PENDING`) останется без END-frame (выравнено с express/hono). `OpHandle.end` идемпотентен — двойной end безопасен.
- **Payload capture — `raw/json`.** Тело HTTP не имеет proto-схемы, захватывается как есть с контракт-маркером `RAW_JSON_CONTRACT` (`"raw/json"`, зеркалит runtime `telemetry.ContractRawJSON`); пустые/`{}`/несериализуемые тела не эмитят payload. Фактический режим (`none`/`errors`/`all`) приходит из registry-стрима и применяется внутри `startOp`.
- **HEAD отсекается.** Fastify auto-генерирует HEAD из GET. Дублировать в Service Map бессмысленно.
- **`fastify-plugin` в `dependencies`, не `peerDependencies`.** Zero-dep утилита; пользователь не ставит её руками.
- **Совместимость с Fastify 4 и 5.** `fp(..., { fastify: "4.x || 5.x" })` явно декларирует версии; `fastify-plugin` падает с осмысленной ошибкой при major mismatch.

## Зависимости

Зависит от:
- `fastify` (peerDependency, optional) — type-only (`FastifyInstance`, `FastifyPluginAsync`, `FastifyReply`, `FastifyRequest`, `RouteOptions`).
- `fastify-plugin` (dependency) — обёртка для propagation хуков в root scope.
- `../endpoint` — `resolveHttpAdvertiseHost`.
- `../_common/body-capture` — `bodyToBytes`, `RAW_JSON_CONTRACT`.
- `../_common/trace-wrap` — `contextFromXSbTrace`.
- `../../connection/service-bridge` — type-only `ServiceBridge` (доступ к `sb.routes`, `sb.telemetry`).
- `../../telemetry/context` — `als` (AsyncLocalStorage trace-scope).
- `../../telemetry/ops` — `Channel`, `HttpHandle`, `OpHandle`, `Status`.
- `../../telemetry/trace-context` — type-only `TraceContext`.

Используется в:
- `sdk/node/tests/e2e/http-fastify.test.ts`.
- Прикладной код через subpath `service-bridge/fastify`.
