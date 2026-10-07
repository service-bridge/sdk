// http-op.ts — start and end of the HTTP.HANDLE operation. The only place the
// "incoming request → op" logic lives: the express/fastify/hono integrations
// hold only what is framework-specific (route collection, body access, hooks).
// Same rules in the Go SDK (sbhttp).
// @internal — см. ../README.md

import type { ServiceBridge } from "../../connection/service-bridge";
import {
	Channel,
	HttpHandle,
	type OpHandle,
	Status,
} from "../../telemetry/ops";
import {
	mintRootContext,
	type TraceContext,
} from "../../telemetry/trace-context";
import { parseXSbTrace } from "../../telemetry/wire-trace";

/** Node gives repeated headers as an array, the Fetch API a string or null. */
export type HeaderValue = string | string[] | null | undefined;

/** First value of a header, whatever shape the framework gave. */
export function firstHeader(value: HeaderValue): string | undefined {
	if (value == null) return undefined;
	return Array.isArray(value) ? value[0] : value;
}

/** Route template used for a request no route matched. */
export const UNMATCHED_ROUTE = "*";

/**
 * Options every HTTP integration accepts.
 *
 * @public — см. ../README.md
 */
export interface HttpIntegrationOptions {
	/**
	 * Accept an incoming `X-SB-Trace` header and join the caller's trace.
	 * Default false: a public endpoint must not let any client graft its
	 * requests into arbitrary traces. Set true for an HTTP server reached only
	 * by other ServiceBridge services.
	 */
	trustTraceHeader?: boolean;
}

/** Everything the shared logic needs about one incoming request. */
export interface HttpOpRequest {
	method: string;
	/** Route template as the framework declares it, or UNMATCHED_ROUTE. */
	route: string;
	traceHeader: HeaderValue;
	idempotencyKey: HeaderValue;
}

export interface HttpOp {
	handle: OpHandle;
	/** Trace scope for downstream code: HTTP.HANDLE is the parent. */
	scope: TraceContext;
	/**
	 * Whether capturing bodies is worth it at all. While false, bodies are
	 * neither read nor serialized — OpHandle would drop the bytes anyway.
	 */
	capturing: boolean;
	/** Ends the op with the response status (meta.status). */
	finish(statusCode: number): void;
	/** Ends the op as a client abort. */
	abort(): void;
	/** Ends the op as a handler failure. */
	fail(message: string): void;
}

/**
 * Starts the HTTP.HANDLE op. Subject `http.handle:<METHOD>/<route template>`;
 * meta {method, route} at start and {status} at end; businessKey from the
 * `Idempotency-Key` header, else `<METHOD> <route>` (never the raw path or
 * query: one value per route, no user data).
 */
export function startHttpOp(
	sb: ServiceBridge,
	req: HttpOpRequest,
	opts: HttpIntegrationOptions,
): HttpOp {
	const method = req.method.toUpperCase();
	const incoming = opts.trustTraceHeader
		? parseXSbTrace(firstHeader(req.traceHeader) ?? "")
		: null;
	const ctx = incoming ?? mintRootContext();
	const handle = sb.telemetry.startOp({
		traceId: ctx.traceId,
		parentOpId: ctx.parentOpId,
		channel: Channel.HTTP,
		kind: HttpHandle,
		subject: `http.handle:${method}/${req.route}`,
		businessKey: firstHeader(req.idempotencyKey) || `${method} ${req.route}`,
		metaJson: Buffer.from(JSON.stringify({ method, route: req.route })),
	});
	return {
		handle,
		scope: handle.scope,
		capturing: handle.capturing,
		finish(statusCode) {
			const meta = Buffer.from(JSON.stringify({ status: statusCode }));
			if (statusCode >= 400)
				handle.end(Status.ERROR, `HTTP ${statusCode}`, meta);
			else handle.end(Status.SUCCESS, undefined, meta);
		},
		abort() {
			handle.end(Status.TIMEOUT, "client abort");
		},
		fail(message) {
			handle.end(Status.ERROR, message);
		},
	};
}
