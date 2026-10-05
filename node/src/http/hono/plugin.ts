import type { ReadableStreamDefaultReader as WebReader } from "node:stream/web";
import type { Hono } from "hono";
import type { ServiceBridge } from "../../connection/service-bridge";
import { runWithTrace } from "../../telemetry/context";
import { Status } from "../../telemetry/ops";
import { RAW_JSON_CONTRACT } from "../_common/body-capture";
import { startHttpOp, statusForHttpCode } from "../_common/http-op";
import {
	HttpRequestGuard,
	type HttpSecurityOptions,
} from "../_common/security";
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
	security?: HttpSecurityOptions;
	resolveRemoteAddress?: (
		request: Request,
		env: unknown,
		executionContext: unknown,
	) => string | null | undefined;
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
	if (endpoint.security?.rateLimit && !endpoint.resolveRemoteAddress)
		throw new Error(
			"Hono rate limiting requires resolveRemoteAddress from the server adapter",
		);
	collectHonoRoutes(app, sb);
	const host = resolveHttpAdvertiseHost(endpoint.host);
	sb.routes.publishHttp({ host, port: endpoint.port });
	installHonoTracing(app, sb, endpoint);
}

const TRACE_FLAG = Symbol.for("servicebridge.hono.trace");

function installHonoTracing(
	app: Hono,
	sb: ServiceBridge,
	endpoint: HonoEndpoint,
): void {
	const security = endpoint.security;
	if (security?.rateLimit && !endpoint.resolveRemoteAddress) {
		throw new Error(
			"Hono rate limiting requires resolveRemoteAddress from the server adapter",
		);
	}
	const tagged = app as Hono & { [TRACE_FLAG]?: boolean };
	if (tagged[TRACE_FLAG]) return;
	tagged[TRACE_FLAG] = true;

	// Hono.use(...) после регистрации роутов не догоняет — порядок матчит.
	// Поэтому оборачиваем сам fetch: парсим X-SB-Trace + запускаем downstream
	// chain в runWithTrace, чтобы handler и downstream user-code видели ALS,
	// и эмитим HTTP.HANDLE op (start/end по response.status).
	const origFetch = app.fetch.bind(app);
	const guard = new HttpRequestGuard(
		endpoint.resolveRemoteAddress
			? security
			: { ...security, rateLimit: false },
	);
	// biome-ignore lint/suspicious/noExplicitAny: env/executionCtx — рантайм-зависимы
	(app as any).fetch = async (req: Request, env?: any, executionCtx?: any) => {
		const url = new URL(req.url);
		const decision = guard.check({
			method: req.method,
			pathname: `${url.pathname}${url.search}`,
			remoteAddress: endpoint.resolveRemoteAddress?.(req, env, executionCtx),
			forwardedFor: req.headers.get("x-forwarded-for"),
		});
		if (!decision.allowed) {
			const headers = new Headers({ "content-type": "application/json" });
			if (decision.retryAfterSeconds) {
				headers.set("Retry-After", String(decision.retryAfterSeconds));
			}
			return new Response(
				JSON.stringify({
					error: decision.status === 429 ? "Too Many Requests" : "Not Found",
				}),
				{ status: decision.status, headers },
			);
		}
		const op = startHttpOp(sb, {
			method: req.method,
			subjectPath: url.pathname,
			keyPath: url.pathname,
			traceHeader: req.headers.get("x-sb-trace"),
			idempotencyKey: req.headers.get("idempotency-key"),
		});
		const handle = op.handle;
		const limit = handle.payloadMaxBytes ?? 65536;
		const request =
			op.capturing && req.body
				? new Request(req, {
						body: passiveCapture(req.body, limit, (bytes, size) =>
							handle.captureIn(bytes, RAW_JSON_CONTRACT, size),
						),
						duplex: "half",
					} as RequestInit)
				: req;
		return runWithTrace(op.scope, async () => {
			try {
				const res = (await origFetch(request, env, executionCtx)) as Response;
				const { status, message } = statusForHttpCode(res.status);
				handle.end(status, message);
				if (!op.capturing || !res.body) return res;
				return new Response(
					passiveCapture(res.body, limit, (bytes, size) =>
						handle.captureOut(bytes, RAW_JSON_CONTRACT, size),
					),
					{
						status: res.status,
						statusText: res.statusText,
						headers: res.headers,
					},
				);
			} catch (err) {
				handle.end(Status.ERROR, (err as Error).message);
				throw err;
			}
		});
	};
}

// Zero queue: read only when the application or response consumer pulls.
function passiveCapture(
	body: ReadableStream<Uint8Array>,
	limit: number,
	capture: (bytes: Uint8Array, size: number) => void,
): ReadableStream<Uint8Array> {
	let reader: WebReader<Uint8Array> | undefined;
	const chunks: Uint8Array[] = [];
	let retained = 0;
	let size = 0;
	let finished = false;
	const finish = () => {
		if (finished) return;
		finished = true;
		if (!retained) return;
		const bytes = new Uint8Array(retained);
		let offset = 0;
		for (const chunk of chunks) {
			bytes.set(chunk, offset);
			offset += chunk.length;
		}
		chunks.length = 0;
		capture(bytes, size);
	};
	return new ReadableStream(
		{
			async pull(controller) {
				reader ??= body.getReader();
				const active = reader;
				try {
					const next = await active.read();
					if (next.done) {
						finish();
						active.releaseLock();
						controller.close();
						return;
					}
					size += next.value.byteLength;
					const n = Math.min(
						next.value.byteLength,
						Math.max(0, limit - retained),
					);
					if (n) {
						chunks.push(next.value.slice(0, n));
						retained += n;
					}
					controller.enqueue(next.value);
				} catch (err) {
					finish();
					controller.error(err);
				}
			},
			async cancel(reason) {
				finish();
				if (reader) await reader.cancel(reason);
				else await body.cancel(reason);
			},
		},
		{ highWaterMark: 0 },
	);
}
