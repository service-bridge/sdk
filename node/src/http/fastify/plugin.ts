import type {
	FastifyInstance,
	FastifyPluginAsync,
	FastifyReply,
	FastifyRequest,
	RouteOptions,
} from "fastify";
import fp from "fastify-plugin";
import type { ServiceBridge } from "../../connection/service-bridge";
import { als } from "../../telemetry/context";
import { type OpHandle, Status } from "../../telemetry/ops";
import type { TraceContext } from "../../telemetry/trace-context";
import { bodyToBytes, RAW_JSON_CONTRACT } from "../_common/body-capture";
import { startHttpOp, statusForHttpCode } from "../_common/http-op";
import { resolveHttpAdvertiseHost } from "../endpoint";

/**
 * Options для `sbFastify` плагина.
 *
 * @public — см. ./README.md
 */
export interface SbFastifyOptions {
	sb: ServiceBridge;
	/**
	 * Опционально: явный host для http_endpoint. По умолчанию идёт
	 * `resolveHttpAdvertiseHost()` — bound socket address, иначе `127.0.0.1`.
	 */
	host?: string;
}

declare module "fastify" {
	interface FastifyRequest {
		sbTraceCtx?: TraceContext;
		sbHttpHandle?: OpHandle;
		sbHttpCapturing?: boolean;
	}
}

interface NetAddress {
	address: string;
	port: number;
}

function netAddressOf(server: { address(): unknown }): NetAddress | null {
	const addr = server.address();
	if (addr && typeof addr === "object" && "port" in addr) {
		const a = addr as { address?: unknown; port?: unknown };
		if (typeof a.port === "number") {
			return {
				address: typeof a.address === "string" ? a.address : "",
				port: a.port,
			};
		}
	}
	return null;
}

const plugin: FastifyPluginAsync<SbFastifyOptions> = async (
	fastify: FastifyInstance,
	opts: SbFastifyOptions,
) => {
	const { sb } = opts;

	// preHandler — последний async-hook перед route-handler'ом. Используем
	// als.enterWith (а не runWithTrace callback-style) потому что Fastify hooks
	// API возвращает Promise, а не принимает next() callback. enterWith
	// устанавливает ALS-фрейм на текущий async-scope, hand'ler и downstream
	// user-code (sb.rpc.call / sb.event.publish / etc.) видят TraceContext.
	// HTTP.HANDLE op стартует здесь, end — в onResponse hook. ALS-фрейм несёт
	// childContext(ctx, handle.opId): downstream-операции вложены под HTTP.HANDLE
	// (симметрично rpc-клиенту, который ставит CALL.op_id родителем для callee).
	fastify.addHook("preHandler", async (req: FastifyRequest) => {
		const op = startHttpOp(sb, {
			method: req.method,
			subjectPath: req.routeOptions?.url ?? req.url,
			keyPath: req.url,
			traceHeader: req.headers["x-sb-trace"],
			idempotencyKey: req.headers["idempotency-key"],
		});
		req.sbTraceCtx = op.incoming;
		req.sbHttpHandle = op.handle;
		req.sbHttpCapturing = op.capturing;
		als.enterWith(op.scope);
		// Request body (IN) — Fastify has already parsed it by preHandler.
		if (op.capturing) {
			const inBytes = bodyToBytes(req.body);
			if (inBytes) op.handle.captureIn(inBytes, RAW_JSON_CONTRACT);
		}
	});

	// onSend exposes the serialized response payload — capture it (OUT) before
	// onResponse ends the op (so "errors"-mode buffering still works).
	fastify.addHook(
		"onSend",
		async (req: FastifyRequest, _reply: FastifyReply, payload: unknown) => {
			if (!req.sbHttpCapturing) return payload;
			const outBytes = bodyToBytes(payload);
			if (outBytes) req.sbHttpHandle?.captureOut(outBytes, RAW_JSON_CONTRACT);
			return payload;
		},
	);

	fastify.addHook(
		"onResponse",
		async (req: FastifyRequest, reply: FastifyReply) => {
			const handle = req.sbHttpHandle;
			if (!handle) return;
			const { status, message } = statusForHttpCode(reply.statusCode);
			handle.end(status, message);
		},
	);

	// onRequestAbort fires when the client disconnects before the response is
	// sent — onResponse never runs in that case, so the HTTP.HANDLE op would
	// otherwise hang RUNNING forever. End it as TIMEOUT, matching express/hono.
	// OpHandle.end is idempotent, so a later onResponse end is a safe no-op.
	fastify.addHook("onRequestAbort", async (req: FastifyRequest) => {
		req.sbHttpHandle?.end(Status.TIMEOUT, "client abort");
	});

	fastify.addHook("onRoute", (route: RouteOptions) => {
		const methods = Array.isArray(route.method) ? route.method : [route.method];
		for (const m of methods) {
			if (typeof m !== "string") continue;
			if (m.toUpperCase() === "HEAD") continue; // авто-генерируется Fastify, дублирует GET
			sb.routes.add({
				method: m.toUpperCase(),
				pattern: route.url,
				source: "fastify",
			});
		}
	});

	// onListen fires AFTER `fastify.listen()` has bound the socket — only here
	// can we read the actual port (especially with `{ port: 0 }`).
	fastify.addHook("onListen", async () => {
		const addr = netAddressOf(fastify.server);
		if (!addr) {
			fastify.log.warn(
				"[servicebridge/fastify] could not read server address — http_endpoint not published",
			);
			return;
		}
		const host = opts.host ?? resolveHttpAdvertiseHost(addr.address);
		sb.routes.publishHttp({ host, port: addr.port });
	});
};

/**
 * Fastify plugin для ServiceBridge: собирает роуты через `onRoute` хук и
 * публикует HTTP-endpoint после `fastify.listen()` через `onReady`. ADR 0001.
 *
 * @public — см. ./README.md
 */
export const sbFastify = fp(plugin, {
	name: "servicebridge/fastify",
	fastify: "4.x || 5.x",
});
