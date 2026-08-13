import { type ChannelCredentials, Metadata } from "@grpc/grpc-js";
import { type InvokeChunk, InvokeClient } from "../pb/servicebridge/v1/invoke";
import { currentTraceContext } from "../telemetry/context";
import { formatXSbTrace } from "../telemetry/wire-trace";

// X_SB_TRACE_HEADER is the gRPC metadata key under which the runtime reads
// the caller's trace context (ADR 0006 §3). Keep in sync with rpc/server.go.
const X_SB_TRACE_HEADER = "x-sb-trace";

// currentTraceHeader returns the X-SB-Trace wire value for the active ALS
// trace context, or empty string when no context is in scope. Both the gRPC
// metadata header and the CallRequest.xSbTrace body field carry it — kept
// in sync with direct-transport for symmetric Invoke/Direct semantics.
function currentTraceHeader(): string {
	const ctx = currentTraceContext();
	if (!ctx) return "";
	return formatXSbTrace(ctx.traceId, ctx.parentOpId);
}

// buildTraceMetadata returns a Metadata with x-sb-trace set from the given
// header value, or an empty Metadata when the header is empty.
function buildTraceMetadata(header: string): Metadata {
	const md = new Metadata();
	if (header) {
		md.set(X_SB_TRACE_HEADER, header);
	}
	return md;
}

// asBuffer reinterprets an already-owned Uint8Array as a Buffer without
// copying. The generated stubs type wire bytes as Buffer, and Buffer.from(view)
// copies — a second full copy of every request payload on every call.
function asBuffer(bytes: Uint8Array): Buffer {
	return Buffer.isBuffer(bytes)
		? bytes
		: Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

// ProxyTransport routes outbound RPC calls through the runtime's Invoke service.
// Reuses the SDK's mTLS credentials to the runtime — same URL and creds as the
// Control/Registry streams. The caller-side contract hash is forwarded so the
// runtime Resolver can filter target instances by it (ADR 0001).
//
// @internal — см. ./README.md
export class ProxyTransport {
	private client: InvokeClient;

	constructor(runtimeAddr: string, creds: ChannelCredentials) {
		this.client = new InvokeClient(runtimeAddr, creds);
	}

	close(): void {
		this.client.close();
	}

	// callStream invokes Invoke.Stream and returns an AsyncIterable of payload
	// bytes. Application errors (errorCode non-empty in a chunk) terminate the
	// iterator with a thrown Error whose name is the errorCode.
	async *callStream(
		targetServiceId: string,
		method: string,
		payload: Uint8Array,
		requestId: string,
		idempotencyKey: string,
		deadlineMs: number,
		contractHash: Buffer,
	): AsyncIterable<Uint8Array> {
		const deadline = new Date(Date.now() + deadlineMs);
		const traceHeader = currentTraceHeader();
		const stream = this.client.stream(
			{
				targetServiceId,
				method,
				payload: asBuffer(payload),
				requestId,
				idempotencyKey,
				contractHash,
				xSbTrace: traceHeader,
			},
			buildTraceMetadata(traceHeader),
			{ deadline },
		);
		for await (const chunk of stream as AsyncIterable<InvokeChunk>) {
			if (chunk.errorCode) {
				const err = new Error(chunk.errorMessage || chunk.errorCode);
				err.name = chunk.errorCode;
				throw err;
			}
			// The decoder already copied the chunk out of the wire buffer, so the
			// payload is owned — handing it straight to the caller avoids a second
			// full copy of every chunk.
			yield chunk.payload;
		}
	}

	// callUnary forwards a single Invoke.Unary call to the runtime and resolves
	// with the response payload bytes. Application errors (callee handler threw)
	// are returned as a rejected promise with name/message set from error_code.
	callUnary(
		targetServiceId: string,
		method: string,
		payload: Uint8Array,
		requestId: string,
		idempotencyKey: string,
		deadlineMs: number,
		contractHash: Buffer,
	): Promise<Uint8Array> {
		return new Promise((resolve, reject) => {
			const deadline = new Date(Date.now() + deadlineMs);
			const traceHeader = currentTraceHeader();
			this.client.unary(
				{
					targetServiceId,
					method,
					payload: asBuffer(payload),
					requestId,
					idempotencyKey,
					contractHash,
					xSbTrace: traceHeader,
				},
				buildTraceMetadata(traceHeader),
				{ deadline },
				(err, resp) => {
					if (err) {
						reject(err);
						return;
					}
					if (resp.errorCode) {
						const appErr = new Error(resp.errorMessage || resp.errorCode);
						appErr.name = resp.errorCode;
						reject(appErr);
						return;
					}
					resolve(resp.payload);
				},
			);
		});
	}
}
