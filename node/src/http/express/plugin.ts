import type { Express, NextFunction, Request, Response, Router } from "express";
import type { ServiceBridge } from "../../connection/service-bridge";
import { runWithTrace } from "../../telemetry/context";
import { Channel, HttpHandle, Status } from "../../telemetry/ops";
import { bodyToBytes, RAW_JSON_CONTRACT } from "../_common/body-capture";
import { contextFromXSbTrace } from "../_common/trace-wrap";
import { resolveHttpAdvertiseHost } from "../endpoint";

/**
 * Internal type narrowing над Express `app._router` / `app.router` стэком.
 * Express 4 хранит router как `_router`, Express 5 — как `router`.
 */
type RouteLike = {
	path?: string | RegExp;
	methods?: Record<string, boolean>;
};

type LayerLike = {
	route?: RouteLike;
	name?: string;
	handle?: { stack?: LayerLike[] };
	regexp?: RegExp;
};

function getRootRouter(app: Express): { stack: LayerLike[] } | null {
	// biome-ignore lint/suspicious/noExplicitAny: Express versions differ in shape
	const a = app as any;
	if (a._router?.stack) return a._router;
	if (a.router?.stack) return a.router;
	return null;
}

/**
 * Извлекает префикс из `app.use("/api", subRouter)`. Express превращает
 * "/api" в RegExp типа `/^\/api\/?(?=\/|$)/i`. Грубо достаём литерал.
 */
function prefixFromLayer(layer: LayerLike): string {
	const re = layer.regexp;
	if (!re) return "";
	const src = re.source;
	const m = src.match(/^\/\^(\\\/[^\\?]*)\\\/\?/);
	if (m?.[1]) return m[1].replace(/\\\//g, "/");
	return "";
}

function collect(
	router: { stack: LayerLike[] },
	prefix: string,
	out: Array<{ method: string; pattern: string }>,
): void {
	for (const layer of router.stack) {
		if (layer.route?.path !== undefined && layer.route.path !== null) {
			const pathStr = String(layer.route.path);
			const fullPath = `${prefix}${pathStr}`;
			const methods = layer.route.methods;
			if (methods) {
				for (const [m, on] of Object.entries(methods)) {
					if (!on) continue;
					if (m === "_all") continue;
					out.push({
						method: m.toUpperCase(),
						pattern: fullPath,
					});
				}
			}
			continue;
		}
		if (layer.name === "router" && layer.handle?.stack) {
			const nestedPrefix = `${prefix}${prefixFromLayer(layer)}`;
			collect({ stack: layer.handle.stack }, nestedPrefix, out);
		}
	}
}

/**
 * Endpoint, на котором фактически слушает Express-сервер. `port` обязателен:
 * Express может биндиться на 0 и в момент сбора роутов фактический порт не
 * известен — пользователь его передаёт явно. `host` опционален, fallback —
 * `resolveHttpAdvertiseHost()`.
 *
 * @public — см. ./README.md
 */
export interface ExpressEndpoint {
	host?: string;
	port: number;
}

/**
 * Подключает Express-приложение к `ServiceBridge`: обходит router stack
 * (включая sub-routers), собирает роуты в `sb.routes`, и публикует
 * HTTP-endpoint (`host:port`). Симметрично `attachHono`. Идемпотентен по
 * сбору роутов (дедуп в `RouteCollector`).
 *
 * Безопасен и до `sb.start()` — endpoint осядет в Registry и попадёт в
 * первый RegisterRequest.
 *
 * @public — см. ./README.md
 */
export function attachExpress(
	app: Express,
	sb: ServiceBridge,
	endpoint: ExpressEndpoint,
): void {
	const router = getRootRouter(app);
	if (router) {
		const acc: Array<{ method: string; pattern: string }> = [];
		collect(router, "", acc);
		for (const r of acc) {
			sb.routes.add({
				method: r.method,
				pattern: r.pattern,
				source: "express",
			});
		}
	}
	const host = resolveHttpAdvertiseHost(endpoint.host);
	sb.routes.publishHttp({ host, port: endpoint.port });

	installTraceMiddleware(app, sb);
}

const TRACE_FLAG = "__servicebridge_trace__";

/**
 * Ставит ровно один trace+emit middleware на app. Идемпотентен. Парсит X-SB-Trace,
 * оборачивает handler chain в `runWithTrace(ctx, () => next())` — downstream
 * middleware и route handlers видят TraceContext через ALS. Эмитит HTTP.HANDLE
 * op (start на запрос, end на `res.finish` / `res.close`).
 */
function installTraceMiddleware(app: Express, sb: ServiceBridge): void {
	// biome-ignore lint/suspicious/noExplicitAny: app не хранит произвольные поля в типах
	const tagged = app as any;
	if (tagged[TRACE_FLAG]) return;
	tagged[TRACE_FLAG] = true;

	app.use((req: Request, res: Response, next: NextFunction) => {
		const header = req.headers["x-sb-trace"];
		const value = Array.isArray(header) ? header[0] : header;
		const ctx = contextFromXSbTrace(value ?? null);
		runWithTrace(ctx, () => {
			const idempotencyHeader = req.headers["idempotency-key"];
			const businessKey = Array.isArray(idempotencyHeader)
				? idempotencyHeader[0]
				: idempotencyHeader;
			const handle = sb.telemetry.startOp({
				channel: Channel.HTTP,
				kind: HttpHandle,
				subject: `http.handle:${req.method}/${req.path}`,
				businessKey: businessKey ?? `${req.method} ${req.path}`,
			});
			// Capture the response body (OUT) by tapping res.json/res.send. The
			// request body (IN) is read in finalize, by when any body-parser ran.
			let outBody: unknown;
			let outSet = false;
			const origJson = res.json.bind(res);
			res.json = ((body: unknown) => {
				outBody = body;
				outSet = true;
				return origJson(body);
			}) as typeof res.json;
			const origSend = res.send.bind(res);
			res.send = ((body: unknown) => {
				if (!outSet) {
					outBody = body;
					outSet = true;
				}
				return origSend(body);
			}) as typeof res.send;
			let ended = false;
			const finalize = (status: Status, msg?: string) => {
				if (ended) return;
				ended = true;
				const inBytes = bodyToBytes((req as { body?: unknown }).body);
				if (inBytes) handle.captureIn(inBytes, RAW_JSON_CONTRACT);
				if (outSet) {
					const outBytes = bodyToBytes(outBody);
					if (outBytes) handle.captureOut(outBytes, RAW_JSON_CONTRACT);
				}
				handle.end(status, msg);
			};
			res.once("finish", () => {
				const code = res.statusCode;
				if (code >= 500) finalize(Status.ERROR, `HTTP ${code}`);
				else if (code >= 400) finalize(Status.ERROR, `HTTP ${code}`);
				else finalize(Status.SUCCESS);
			});
			res.once("close", () => {
				if (!res.writableEnded) finalize(Status.TIMEOUT, "client abort");
			});
			next();
		});
	});
	// `attachExpress` обычно зовут ПОСЛЕ `app.get(...)`. Express middleware,
	// добавленный через `app.use(...)` после роутов, идёт в конец router stack
	// и не вызывается, потому что роуты завершают запрос раньше. Поднимаем
	// последний слой (только что добавленный middleware) в начало.
	const router = getRootRouter(app);
	if (router && router.stack.length > 1) {
		const last = router.stack[router.stack.length - 1];
		if (last) {
			router.stack.splice(router.stack.length - 1, 1);
			router.stack.unshift(last);
		}
	}
}

// Re-export Router-related type so tests can build fake apps cleanly.
export type { Router };
