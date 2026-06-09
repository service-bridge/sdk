# Integrations

← [Jobs](./jobs.md) · Дальше: [Operations](./operations.md) →

HTTP-интеграции для Express / Fastify / Hono. SDK не запускает HTTP-сервер сам — пользователь поднимает свой Express/Fastify/Hono, а интеграция читает зарегистрированные роуты и публикует их в runtime registry для Service Map и Service Discovery (ADR 0001).

## Содержание

- [Модель](#модель)
- [Express](#express)
- [Fastify](#fastify)
- [Hono](#hono)
- [Что видно в Service Map](#что-видно-в-service-map)
- [Outgoing dep `http`](#outgoing-dep-http)
- [Трейсинг и payload capture](#трейсинг-и-payload-capture)
- [Шпаргалка](#шпаргалка)

## Модель

Runtime НЕ проксирует HTTP. Пользователь сам:
1. Поднимает Express/Fastify/Hono сервер на своём `host:port`.
2. Подключает SDK-интеграцию (subpath import).
3. SDK сообщает runtime'у: «вот мои HTTP-роуты, я живу на `host:port`».
4. Runtime отдаёт это в Service Map и другим инстансам через Service Discovery.

```
[ External client ]
        │ HTTP
        ▼
[ Your Express/Fastify/Hono — port :3000 ]
        │ gRPC control plane
        ▼
[ ServiceBridge runtime — port :14445 ]
        │
        ▼
   Service Map: [ httpEndpoint: host:3000, methods: GET /api/orders/:id, … ]
```

Фактические HTTP-вызовы между сервисами вы делаете сами (`fetch`, `undici`, `axios`, и т. п.) — runtime их не маршрутизирует.

Паттерны роутов уходят в registry **как есть**: SDK их не нормализует. Косметика паттерна (`:id{[0-9]+}`, регекс-ограничения) — задача UI Service Map, а не SDK.

## Express

```sh
bun add express
```

`attachExpress(app, sb, { port })` синхронно обходит router stack приложения, собирает роуты и публикует HTTP-endpoint. Вызывайте его **после** регистрации роутов (`app.get(...)`, `app.post(...)`), иначе stack ещё пустой.

```ts
import express from "express";
import { ServiceBridge } from "service-bridge";
import { attachExpress } from "service-bridge/express";

const sb = new ServiceBridge(URL, KEY);
const app = express();

app.get("/api/orders/:id", getOrder);
app.post("/api/orders", createOrder);

attachExpress(app, sb, { port: 3000 });   // после роутов; host -> resolveHttpAdvertiseHost()
await sb.start();
app.listen(3000);
```

`port` обязателен (Express может биндиться на `0`, и в момент сбора роутов реальный порт неизвестен). `host` опционален — это явная опция; если её не передать, берётся `127.0.0.1` с одноразовым warn (см. [Шпаргалку](#шпаргалка)).

`attachExpress` безопасен и до `sb.start()`: endpoint осядет в Registry и попадёт в первый register-запрос. Идемпотентен — повторный вызов не дублирует роуты.

Известное ограничение Express 5: префикс sub-router'ов (`app.use("/v1", subRouter)`) не извлекается из-за path-to-regexp v8 — такие роуты попадут в Service Map без префикса. Для строгого префикса регистрируйте роуты top-level: `app.get("/v1/users/:id", ...)`. На Express 4 префикс извлекается.

См. [src/http/express/README.md](../src/http/express/README.md).

## Fastify

```sh
bun add fastify
# fastify-plugin уже в dependencies SDK — ставить не нужно
```

Плагин регистрируется через `sbFastify` с опцией `{ sb }`. Роуты собираются хуком `onRoute`, endpoint публикуется в `onListen` (после bind — реальный порт уже известен, в т. ч. при `{ port: 0 }`).

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
await app.listen({ port: 3000 });          // onListen хук опубликует endpoint
```

`SbFastifyOptions`: `sb` обязателен, `host` опционален (явная опция; иначе `127.0.0.1` с одноразовым warn). HEAD-метод, который Fastify авто-генерирует к каждому GET, отсекается — не дублирует методы в Service Map. Совместимость: Fastify 4.x и 5.x.

См. [src/http/fastify/README.md](../src/http/fastify/README.md).

## Hono

```sh
bun add hono
```

Hono server-agnostic — сам сокет не открывает. `attachHono(app, sb, { port })` собирает роуты из `app.routes` и публикует endpoint; `port` всегда передаётся явно и должен совпадать с тем, что вы передаёте в `Bun.serve` / `@hono/node-server` / `Deno.serve`.

```ts
import { Hono } from "hono";
import { ServiceBridge } from "service-bridge";
import { attachHono } from "service-bridge/hono";

const sb = new ServiceBridge(URL, KEY);
const app = new Hono();
app.get("/api/orders/:id", (c) => c.json({ id: c.req.param("id") }));
app.post("/api/orders", (c) => c.json({ created: true }));

attachHono(app, sb, { port: 3000 });       // host -> resolveHttpAdvertiseHost()
await sb.start();
Bun.serve({ fetch: app.fetch, port: 3000 });
```

`HonoEndpoint`: `port` обязателен, `host` опционален (явная опция; иначе `127.0.0.1` с одноразовым warn). Роуты, объявленные через `app.all(...)`, в Service Map не попадают — метод `ALL` не раскладывается в конкретные.

См. [src/http/hono/README.md](../src/http/hono/README.md).

## Что видно в Service Map

После того как provider опубликовал роуты и endpoint, любой consumer, объявивший зависимость:

```ts
consumer.service("orders-svc", {
  http: ["GET /api/orders/:id", "POST /api/orders"],
});
```

— получит запись в `consumer.serviceMap()`:

```ts
const entry = consumer.serviceMap().get("orders-svc");
// entry.methods    — MethodDescriptor[] с type METHOD_TYPE_HTTP, name "GET /api/orders/:id", …
// entry.instances  — ServiceInstanceInfo[]:
//   { instanceId, serviceId, serviceName, callEndpoint, status, httpEndpoint: "10.0.0.5:3000", … }
```

`httpEndpoint` каждого инстанса — это `host:port`, куда слать `fetch`. Если у инстанса нет HTTP-интеграции, `httpEndpoint` — пустая строка.

После `provider.stop()` (graceful) или истечения heartbeat (force) runtime сбрасывает `httpEndpoint` в пустую строку и удаляет инстанс из snapshot подписчиков.

## Outgoing dep `http`

```ts
sb.service("orders-svc", {
  http: ["GET /api/orders/:id"],
});
```

Это объявление зависимости от HTTP-метода. В модели ADR 0001 — **только декларация для Service Map**: runtime HTTP не маршрутизирует. Фактический вызов вы делаете сами:

```ts
const entry = sb.serviceMap().get("orders-svc");
const inst = entry?.instances.find((i) => i.httpEndpoint);
if (!inst) throw new Error("no live HTTP instance");
const res = await fetch(`http://${inst.httpEndpoint}/api/orders/42`);
```

Балансировка / retry / circuit breaker для HTTP-вызовов — ваша зона ответственности (то же, что вы и так писали бы с `undici` или `axios`).

`sb.service(name, deps)` принимает три вида зависимостей: `rpc`, `workflows`, `http`. Вызывайте до `sb.start()`.

## Трейсинг и payload capture

Каждая интеграция автоматически (без отдельного API) на входящий request:

- читает заголовок `X-SB-Trace` и восстанавливает `TraceContext`, оборачивая downstream chain так, что handler и любой ваш `sb.rpc.call` / `sb.event.publish` внутри видят контекст через ALS — на miss/невалидный заголовок берётся новый root-контекст;
- эмитит op `Channel.HTTP` / `HttpHandle` с `businessKey` = заголовок `Idempotency-Key` (иначе `"<METHOD> <path>"`);
- завершает op статусом из единого словаря: `Status.SUCCESS` (код < 400), `Status.ERROR` (код ≥ 400), `Status.TIMEOUT` (клиент оборвал соединение);
- захватывает тело запроса (IN) и ответа (OUT) как raw-JSON; пустые тела (например, у GET) не пишутся. Запись гейтится runtime-режимом HTTP-канала (`none` / `all` / `errors`), который runtime пушит в SDK.

Тело ответа только читается для capture — интеграция его не меняет. Для Express тело IN читается из `req.body`, так что нужен `express.json()` (или другой body-parser) до роутов.

## Шпаргалка

```ts
// Express — вызывать ПОСЛЕ регистрации роутов
attachExpress(app, sb, { port: 3000 }); await sb.start(); app.listen(3000);

// Fastify
await app.register(sbFastify, { sb }); await sb.start(); await app.listen({ port: 3000 });

// Hono
attachHono(app, sb, { port: 3000 }); await sb.start(); Bun.serve({ fetch: app.fetch, port: 3000 });
```

Advertise-host задаётся только явной опцией `host` в опциях интеграции — env-переменных нет:
```ts
attachExpress(app, sb, { port: 3000, host: "10.0.0.5" });   // для cross-host
// без host → 127.0.0.1 (с одноразовым warn)
```

→ Дальше: [Operations](./operations.md)
