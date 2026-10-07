import type { Express, NextFunction, Request, Response } from "express";
import type { ServiceBridge } from "../../connection/service-bridge";
import { runWithTrace } from "../../telemetry/context";
import { bodyToBytes, RAW_JSON_CONTRACT } from "../_common/body-capture";
import {
	type HttpIntegrationOptions,
	startHttpOp,
	UNMATCHED_ROUTE,
} from "../_common/http-op";
import { resolveHttpAdvertiseHost } from "../endpoint";

/**
 * Internal type narrowing над Express `app._router` / `app.router` стэком.
 * Express 4 хранит router как `_router`, Express 5 — как `router`.
 */
type RouteLike = {
	path?: string | RegExp;
	methods?: Record<string, boolean>;
	// Express 5 / Express 4 spelling of the same check.
	_handlesMethod?: (method: string) => boolean;
	_handles_method?: (method: string) => boolean;
};

type LayerLike = {
	route?: RouteLike;
	name?: string;
	handle?: { stack?: LayerLike[] };
	regexp?: RegExp;
	path?: string;
	match?: (path: string) => boolean;
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
export interface ExpressEndpoint extends HttpIntegrationOptions {
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
	const host = resolveHttpAdvertiseHost(endpoint.host, sb.diagnostics);
	sb.routes.publishHttp({ host, port: endpoint.port });

	installTraceMiddleware(app, sb, endpoint);
}

/**
 * Finds the template of the route Express will dispatch the request to, by
 * asking each layer the way Express's own router does (layer.match). Runs
 * before routing, so the op can carry the template from its first frame.
 * A mount prefix contributes the path it matched.
 */
export function matchExpressRoute(
	stack: LayerLike[],
	path: string,
	method: string,
	prefix = "",
): string | null {
	for (const layer of stack) {
		if (typeof layer.match !== "function" || !layer.match(path)) continue;
		const route = layer.route;
		if (route) {
			const handles =
				route._handlesMethod?.(method) ?? route._handles_method?.(method);
			if (handles) return `${prefix}${String(route.path)}`;
			continue;
		}
		if (layer.handle?.stack) {
			const consumed = layer.path ?? "";
			const found = matchExpressRoute(
				layer.handle.stack,
				path.slice(consumed.length) || "/",
				method,
				`${prefix}${consumed}`,
			);
			if (found) return found;
		}
	}
	return null;
}

const TRACE_FLAG = "__servicebridge_trace__";

/**
 * Ставит ровно один trace+emit middleware на app. Идемпотентен. Парсит X-SB-Trace,
 * оборачивает handler chain в `runWithTrace(ctx, () => next())` — downstream
 * middleware и route handlers видят TraceContext через ALS. Эмитит HTTP.HANDLE
 * op (start на запрос, end на `res.finish` / `res.close`).
 */
function installTraceMiddleware(
	app: Express,
	sb: ServiceBridge,
	opts: HttpIntegrationOptions,
): void {
	// biome-ignore lint/suspicious/noExplicitAny: app не хранит произвольные поля в типах
	const tagged = app as any;
	if (tagged[TRACE_FLAG]) return;
	tagged[TRACE_FLAG] = true;

	app.use((req: Request, res: Response, next: NextFunction) => {
		const router = getRootRouter(app);
		const route =
			(router &&
				matchExpressRoute(router.stack, req.path, req.method.toLowerCase())) ||
			UNMATCHED_ROUTE;
		const op = startHttpOp(
			sb,
			{
				method: req.method,
				route,
				traceHeader: req.headers["x-sb-trace"],
				idempotencyKey: req.headers["idempotency-key"],
			},
			opts,
		);
		runWithTrace(op.scope, () => {
			// Capture the response body (OUT) by tapping res.json/res.send. The
			// request body (IN) is read when the op ends, after any body parser ran.
			// Both are skipped while capture is off.
			let outBody: unknown;
			let outSet = false;
			if (op.capturing) {
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
			}
			let ended = false;
			const capture = () => {
				if (!op.capturing) return;
				const inBytes = bodyToBytes((req as { body?: unknown }).body);
				if (inBytes) op.handle.captureIn(inBytes, RAW_JSON_CONTRACT);
				if (outSet) {
					const outBytes = bodyToBytes(outBody);
					if (outBytes) op.handle.captureOut(outBytes, RAW_JSON_CONTRACT);
				}
			};
			res.once("finish", () => {
				if (ended) return;
				ended = true;
				capture();
				op.finish(res.statusCode);
			});
			res.once("close", () => {
				if (ended || res.writableEnded) return;
				ended = true;
				capture();
				op.abort();
			});
			next();
		});
	});
	hoistTraceMiddleware(app);
}

/**
 * `attachExpress` обычно зовут ПОСЛЕ `app.get(...)`. Express middleware,
 * добавленный через `app.use(...)` после роутов, идёт в конец router stack и не
 * вызывается: роуты завершают запрос раньше. Поднимаем последний слой (только
 * что добавленный middleware) в начало.
 */
function hoistTraceMiddleware(app: Express): void {
	const router = getRootRouter(app);
	if (!router) {
		// app.use() выше обязан был материализовать root router. Если его нет —
		// стек Express не той формы, что мы умеем читать, и middleware остался бы
		// за роутами: HTTP.HANDLE не эмиттился бы вообще, молча.
		throw new Error(
			"[servicebridge/express] root router not found after app.use() — " +
				"unsupported Express build; HTTP tracing cannot be installed",
		);
	}
	if (router.stack.length < 2) return;
	const last = router.stack[router.stack.length - 1];
	if (!last) return;
	router.stack.splice(router.stack.length - 1, 1);
	router.stack.unshift(last);
}

// Re-export Router-related type so tests can build fake apps cleanly.
export type { Router } from "express";
