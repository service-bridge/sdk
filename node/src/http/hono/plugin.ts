import type { Hono } from "hono";
import type { ServiceBridge } from "../../connection/service-bridge";
import { runWithTrace } from "../../telemetry/context";
import { Channel, HttpHandle, Status } from "../../telemetry/ops";
import { bodyToBytes, RAW_JSON_CONTRACT } from "../_common/body-capture";
import { contextFromXSbTrace } from "../_common/trace-wrap";
import { resolveHttpAdvertiseHost } from "../endpoint";

/**
 * Endpoint, на котором фактически слушает Hono-сервер (Bun.serve / @hono/node-server / Deno).
 * `port` обязателен — Hono агностичен к серверу и не запускает сокет сам.
 * `host` опционален: дефолт через `resolveHttpAdvertiseHost()`.
 *
 * @public — см. ./README.md
 */
export interface HonoEndpoint {
	host?: string;
	port: number;
}

/**
 * Собирает роуты из `app.routes` Hono и кладёт их в `sb.routes`. Не публикует
 * endpoint — это делает `attachHono`. Полезно как нижний слой для тестов.
 *
 * @internal
 */
export function collectHonoRoutes(app: Hono, sb: ServiceBridge): void {
	// Hono.routes: { method: string, path: string, handler: Function }[]
	for (const r of app.routes) {
		if (typeof r.method !== "string" || typeof r.path !== "string") continue;
		// Hono ставит method "ALL" когда вызвали app.all(...) — раскладывать
		// в конкретные методы у нас нет (зависит от runtime), пропускаем.
		if (r.method.toUpperCase() === "ALL") continue;
		sb.routes.add({
			method: r.method.toUpperCase(),
			pattern: r.path,
			source: "hono",
		});
	}
}

/**
 * Подключает Hono-приложение к `ServiceBridge`: сразу собирает роуты из
 * `app.routes` и регистрирует HTTP-endpoint (`host:port`) через
 * `RouteCollector.publishHttp`. Если `attachHono` вызван ДО `sb.start()` —
 * triggerRestart no-op, endpoint попадёт в первый `RegisterRequest`
 * естественным путём. После `sb.start()` — restart Registry-watch стрима.
 *
 * Hono сам не запускает сервер: пользователь поднимает `Bun.serve` /
 * `@hono/node-server` / Deno вручную. `port` должен совпадать с тем, что
 * передан в сервер.
 *
 * @public — см. ./README.md
 */
export function attachHono(
	app: Hono,
	sb: ServiceBridge,
	endpoint: HonoEndpoint,
): void {
	collectHonoRoutes(app, sb);
	const host = resolveHttpAdvertiseHost(endpoint.host);
	sb.routes.publishHttp({ host, port: endpoint.port });
	installHonoTracing(app, sb);
}

const TRACE_FLAG = Symbol.for("servicebridge.hono.trace");

function installHonoTracing(app: Hono, sb: ServiceBridge): void {
	// biome-ignore lint/suspicious/noExplicitAny: app не хранит произвольные поля в типах
	const tagged = app as any;
	if (tagged[TRACE_FLAG]) return;
	tagged[TRACE_FLAG] = true;

	// Hono.use(...) после регистрации роутов не догоняет — порядок матчит.
	// Поэтому оборачиваем сам fetch: парсим X-SB-Trace + запускаем downstream
	// chain в runWithTrace, чтобы handler и downstream user-code видели ALS,
	// и эмитим HTTP.HANDLE op (start/end по response.status).
	const origFetch = app.fetch.bind(app);
	// biome-ignore lint/suspicious/noExplicitAny: env/executionCtx — рантайм-зависимы
	(app as any).fetch = async (req: Request, env?: any, executionCtx?: any) => {
		const ctx = contextFromXSbTrace(req.headers.get("x-sb-trace"));
		const url = new URL(req.url);
		const businessKey =
			req.headers.get("idempotency-key") ?? `${req.method} ${url.pathname}`;
		// Clone the request up front so reading its body for capture never
		// consumes the stream the route handler will read.
		const reqClone = req.clone();
		return runWithTrace(ctx, async () => {
			const handle = sb.telemetry.startOp({
				channel: Channel.HTTP,
				kind: HttpHandle,
				subject: `http.handle:${req.method}/${url.pathname}`,
				businessKey,
			});
			const captureBodies = async (res: Response) => {
				try {
					const inBytes = bodyToBytes(await reqClone.text());
					if (inBytes) handle.captureIn(inBytes, RAW_JSON_CONTRACT);
				} catch {
					// unreadable request body — skip IN capture
				}
				try {
					const outBytes = bodyToBytes(await res.clone().text());
					if (outBytes) handle.captureOut(outBytes, RAW_JSON_CONTRACT);
				} catch {
					// unreadable response body — skip OUT capture
				}
			};
			try {
				const res = (await origFetch(req, env, executionCtx)) as Response;
				await captureBodies(res);
				if (res.status >= 500) handle.end(Status.ERROR, `HTTP ${res.status}`);
				else if (res.status >= 400)
					handle.end(Status.ERROR, `HTTP ${res.status}`);
				else handle.end(Status.SUCCESS);
				return res;
			} catch (err) {
				handle.end(Status.ERROR, (err as Error).message);
				throw err;
			}
		});
	};
}
