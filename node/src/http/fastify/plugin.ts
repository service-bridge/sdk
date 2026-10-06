import type {
	FastifyInstance,
	FastifyPluginAsync,
	FastifyReply,
	FastifyRequest,
	RouteOptions,
} from "fastify";
import type { ServiceBridge } from "../../connection/service-bridge";
import { als } from "../../telemetry/context";
import { bodyToBytes, RAW_JSON_CONTRACT } from "../_common/body-capture";
import {
	type HttpIntegrationOptions,
	type HttpOp,
	startHttpOp,
	UNMATCHED_ROUTE,
} from "../_common/http-op";
import { resolveHttpAdvertiseHost } from "../endpoint";

/**
 * Options для `sbFastify` плагина.
 *
 * @public — см. ./README.md
 */
export interface SbFastifyOptions extends HttpIntegrationOptions {
	sb: ServiceBridge;
	/**
	 * Опционально: явный host для http_endpoint. По умолчанию идёт
	 * `resolveHttpAdvertiseHost()` — bound socket address, иначе `127.0.0.1`.
	 */
	host?: string;
}

declare module "fastify" {
	interface FastifyRequest {
		sbHttpOp?: HttpOp;
		sbHttpFinish?: (end: () => void) => void;
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

	// preHandler is the last async hook before the route handler; the route
	// template is known here. als.enterWith puts the op's trace scope on the
	// current async context, so the handler and everything it calls nest
	// under HTTP.HANDLE.
	fastify.addHook(
		"preHandler",
		async (req: FastifyRequest, reply: FastifyReply) => {
			const op = startHttpOp(
				sb,
				{
					method: req.method,
					route: req.routeOptions?.url ?? UNMATCHED_ROUTE,
					traceHeader: req.headers["x-sb-trace"],
					idempotencyKey: req.headers["idempotency-key"],
				},
				opts,
			);
			req.sbHttpOp = op;
			// A client may disconnect after its request was read, before or
			// during the reply; Fastify then never calls onResponse.
			let finished = false;
			let responseFinished = false;
			const responseFinish = () => {
				responseFinished = true;
			};
			reply.raw.once("finish", responseFinish);
			const abort = () => {
				if (!responseFinished) req.sbHttpFinish?.(() => op.abort());
			};
			req.sbHttpFinish = (end) => {
				if (finished) return;
				finished = true;
				reply.raw.off("close", abort);
				reply.raw.off("finish", responseFinish);
				req.raw.socket.off("close", abort);
				req.raw.off("aborted", abort);
				end();
			};
			reply.raw.once("close", abort);
			req.raw.socket.once("close", abort);
			req.raw.once("aborted", abort);
			als.enterWith(op.scope);
			if (op.capturing) {
				const inBytes = bodyToBytes(req.body);
				if (inBytes) op.handle.captureIn(inBytes, RAW_JSON_CONTRACT);
			}
		},
	);

	// onSend exposes the serialized response payload — captured before
	// onResponse ends the op ("errors" mode buffers until the status is known).
	fastify.addHook(
		"onSend",
		async (req: FastifyRequest, _reply: FastifyReply, payload: unknown) => {
			if (!req.sbHttpOp?.capturing) return payload;
			const outBytes = bodyToBytes(payload);
			if (outBytes) req.sbHttpOp.handle.captureOut(outBytes, RAW_JSON_CONTRACT);
			return payload;
		},
	);

	fastify.addHook(
		"onResponse",
		async (req: FastifyRequest, reply: FastifyReply) => {
			const op = req.sbHttpOp;
			if (!op) return;
			req.sbHttpFinish?.(() => op.finish(reply.statusCode));
		},
	);
	fastify.addHook("onRequestAbort", async (req: FastifyRequest) => {
		const op = req.sbHttpOp;
		if (op) req.sbHttpFinish?.(() => op.abort());
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
			sb.diagnostics.warn(
				"fastify: could not read the server address — http_endpoint not published",
			);
			return;
		}
		const host = opts.host ?? resolveHttpAdvertiseHost(addr.address, sb.diagnostics);
		sb.routes.publishHttp({ host, port: addr.port });
	});
};

/**
 * Fastify plugin для ServiceBridge: собирает роуты через `onRoute` хук и
 * публикует HTTP-endpoint после `fastify.listen()` через `onReady`. ADR 0001.
 *
 * @public — см. ./README.md
 */
export const sbFastify: FastifyPluginAsync<SbFastifyOptions> = Object.assign(
	plugin,
	{
		// What fastify-plugin would set: run in the parent scope (the hooks must
		// see every route of the app, not only the plugin's own), and declare the
		// name and supported Fastify range. Written out so the SDK does not need
		// fastify-plugin as a dependency.
		[Symbol.for("skip-override")]: true,
		[Symbol.for("fastify.display-name")]: "servicebridge/fastify",
		[Symbol.for("plugin-meta")]: {
			name: "servicebridge/fastify",
			fastify: "4.x || 5.x",
		},
	},
);
