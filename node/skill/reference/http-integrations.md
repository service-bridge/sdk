# HTTP integrations — Express, Fastify, Hono

ServiceBridge does **not** proxy your business HTTP. You run your own HTTP server; these plugins register its routes into the Service Map and Service Discovery and add trace/metric capture. Your framework keeps serving traffic exactly as before.

Each plugin ships as a subpath export:

```ts
import { attachExpress } from "service-bridge/express";
import { sbFastify }    from "service-bridge/fastify";
import { attachHono }   from "service-bridge/hono";
```

All three: parse the `X-SB-Trace` header so handler-internal `sb.rpc.call`/`sb.event.publish` join the same trace, emit an `HTTP.HANDLE` op per request (status + optional body capture), and publish your routes + advertise endpoint. Safe to call before `sb.start()` — the endpoint queues into the first registration.

## Express

```ts
attachExpress(app: Express, sb: ServiceBridge, endpoint: { host?: string; port: number }): void
```

Call **after** all routes are registered. `port` is required (Express can bind `0`, so the plugin can't infer it). Mount a body parser (`express.json()`) before routes so request bodies are captured.

```ts
import express from "express";
import { ServiceBridge } from "service-bridge";
import { attachExpress } from "service-bridge/express";

const sb = new ServiceBridge("localhost:14445", process.env.API_KEY!);
const app = express();
app.use(express.json());
app.get("/api/orders/:id", (req, res) => res.json({ id: req.params.id }));
app.post("/api/orders", (req, res) => res.json({ created: true }));

attachExpress(app, sb, { host: process.env.POD_IP, port: 3000 }); // after routes
await sb.start();
app.listen(3000);
```

## Fastify

```ts
await app.register(sbFastify, { sb: ServiceBridge, host?: string });
```

Register **before** your routes (it collects them via the `onRoute` hook) — the advertise endpoint is published after `app.listen()` via the `onListen` hook, so the real port (even `0`) is known automatically. Supports Fastify 4.x and 5.x.

```ts
import Fastify from "fastify";
import { ServiceBridge } from "service-bridge";
import { sbFastify } from "service-bridge/fastify";

const sb = new ServiceBridge("localhost:14445", process.env.API_KEY!);
const app = Fastify();
await app.register(sbFastify, { sb, host: process.env.POD_IP });  // before routes
app.get("/api/orders/:id", async (req) => ({ id: (req.params as any).id }));
app.post("/api/orders", async () => ({ created: true }));

await sb.start();
await app.listen({ port: 3000 });
```

## Hono

```ts
attachHono(app: Hono, sb: ServiceBridge, endpoint: { host?: string; port: number }): void
```

Call **after** routes are registered. Hono doesn't bind a socket itself, so `port` must be passed and must match what you give `Bun.serve` / `@hono/node-server` / `Deno.serve`. Routes declared with `app.all(...)` are not collected (no concrete method).

```ts
import { Hono } from "hono";
import { ServiceBridge } from "service-bridge";
import { attachHono } from "service-bridge/hono";

const sb = new ServiceBridge("localhost:14445", process.env.API_KEY!);
const app = new Hono();
app.get("/api/orders/:id", (c) => c.json({ id: c.req.param("id") }));
app.post("/api/orders", (c) => c.json({ created: true }));

attachHono(app, sb, { host: process.env.POD_IP, port: 3000 }); // after routes
await sb.start();
export default { port: 3000, fetch: app.fetch }; // Bun
```

## Advertise host

`host` is optional on all three. If omitted, the plugin falls back to `SB_HTTP_ADVERTISE_HOST` → `SB_ADVERTISE_HOST` → `127.0.0.1` (with a one-time warning). Pass an explicit reachable host (e.g. the pod IP) in production.
