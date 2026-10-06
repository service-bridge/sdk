// Wire conventions shared by the outbound transports and the inbound server:
// trace propagation, the not-dispatched trailer and the mapping of a failed
// call onto the SDK error model. Same rules in the Go SDK (internal/rpc).
//
// @internal — см. ./README.md

import { Metadata } from "@grpc/grpc-js";
import {
	HandlerError,
	ServiceBridgeError,
	toServiceBridgeError,
} from "../errors";
import { currentTraceContext } from "../telemetry/context";
import { formatXSbTrace } from "../telemetry/wire-trace";

/** gRPC metadata key carrying the trace context (ADR 0006 §3). */
export const X_SB_TRACE_HEADER = "x-sb-trace";

/**
 * Trailer the callee sets on every rejection made before the handler ran
 * (overload, not ready, draining, access denied). It is the only proof a
 * caller has that a non-OK status left no effect, so it is what allows a retry
 * on another instance.
 */
export const NOT_DISPATCHED_TRAILER = "x-sb-not-dispatched";

/** X-SB-Trace for the active ALS context, "" outside of one. */
export function currentTraceHeader(): string {
	const ctx = currentTraceContext();
	if (!ctx) return "";
	return formatXSbTrace(ctx.traceId, ctx.parentOpId);
}

/** Metadata with x-sb-trace when the header is non-empty. */
export function traceMetadata(header: string): Metadata {
	const md = new Metadata();
	if (header) md.set(X_SB_TRACE_HEADER, header);
	return md;
}

/** Trailers marking a pre-handler rejection. */
export function notDispatchedTrailer(): Metadata {
	const md = new Metadata();
	md.set(NOT_DISPATCHED_TRAILER, "1");
	return md;
}

/**
 * Reinterprets an owned Uint8Array as a Buffer without copying: the generated
 * stubs type wire bytes as Buffer and Buffer.from(view) would copy.
 */
export function asBuffer(bytes: Uint8Array): Buffer {
	return Buffer.isBuffer(bytes)
		? bytes
		: Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

/**
 * A failed call as the client sees it. `preDispatch` is true only when the
 * request provably did not reach a handler, `transport` when the failure says
 * something about the instance rather than the request (circuit breaker input).
 */
export class CallFailure extends Error {
	constructor(
		readonly error: ServiceBridgeError,
		readonly preDispatch: boolean,
	) {
		super(error.message, { cause: error });
		this.name = "CallFailure";
	}

	get transport(): boolean {
		return (
			this.error.code === "CONNECTION" ||
			this.error.code === "TIMEOUT" ||
			this.error.code === "OVERLOADED" ||
			(this.error.code === "INTERNAL" && !(this.error instanceof HandlerError))
		);
	}
}

/** A failure the callee's handler returned in the response body. */
export function handlerFailure(code: string, message: string): CallFailure {
	const error = new HandlerError(code, message || code);
	Object.defineProperty(error, "remote", { value: true });
	return new CallFailure(error, false);
}

/**
 * Classifies a grpc-js error. `notSent` marks a failure the transport proved
 * happened before the request was written (channel never became ready).
 */
export function grpcFailure(
	scope: string,
	err: unknown,
	notSent = false,
): CallFailure {
	if (err instanceof CallFailure) return err;
	const trailers = (err as { metadata?: Metadata } | null)?.metadata;
	const flagged =
		trailers instanceof Metadata &&
		trailers.get(NOT_DISPATCHED_TRAILER)[0] === "1";
	const status = (err as { code?: unknown } | null)?.code;
	const details = (err as { details?: unknown } | null)?.details;
	const message =
		typeof details === "string" && details
			? details
			: err instanceof Error
				? err.message
				: String(err);
	// A channel that never became ready did not carry the request: retrying
	// elsewhere is safe and the condition is a connection one.
	if (notSent)
		return new CallFailure(
			new ServiceBridgeError("CONNECTION", `${scope}: ${message}`, {
				cause: err,
			}),
			true,
		);
	const error = toServiceBridgeError(
		scope,
		typeof status === "number"
			? Object.assign(new Error(message), { code: status })
			: err,
	);
	return new CallFailure(error, flagged);
}
