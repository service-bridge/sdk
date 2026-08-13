import * as grpc from "@grpc/grpc-js";
import { derToPem } from "../connection/pem";
import {
	type CallRequest,
	type CallResponse,
	CallService,
	type StreamChunk,
} from "../pb/servicebridge/v1/call";
import type { PolicyEvaluation } from "../pb/servicebridge/v1/registry";
import { runWithTrace } from "../telemetry/context";
import { mintRootContext, type TraceContext } from "../telemetry/trace-context";
import { parseXSbTrace } from "../telemetry/wire-trace";
import { Semaphore, SemaphoreExhaustedError } from "../utils/semaphore";
import { evaluatePeerAcceptance } from "./acceptance";
import type { DispatchPort } from "./dispatch-port";

// Credentials for the inbound call server: leaf cert + key (DER), CA chain (DER).
// Reuses the same certs the SDK obtained from runtime Bootstrap.
export interface CallServerCredentials {
	caChainDer: Buffer;
	leafCertDer: Buffer;
	privateKeyDer: Buffer;
}

// AdvertiseConfig is the operator-supplied advertise address. host is mandatory
// (no auto-detect — k8s/docker need explicit POD_IP via downward API), port=0
// asks the OS to pick a free port.
export interface AdvertiseConfig {
	host: string;
	port: number;
}

// DEFAULT_MAX_CONCURRENT_CALLS bounds handlers running at once. Each in-flight
// call holds a decoded request plus whatever the handler allocates, so an
// unbounded server turns a caller-side burst into callee-side OOM.
export const DEFAULT_MAX_CONCURRENT_CALLS = 256;

// CallServerLimits bounds inbound load. Both values matter: the concurrency
// limit alone would just move an overload into an unbounded queue, so the
// queue depth is what actually sheds load with RESOURCE_EXHAUSTED.
export interface CallServerLimits {
	maxConcurrentCalls?: number;
	// maxQueuedCalls defaults to maxConcurrentCalls. 0 rejects any call that
	// cannot start immediately.
	maxQueuedCalls?: number;
}

/**
 * Parse the inbound trace context from a CallRequest. Falls back to a fresh
 * root context when xSbTrace is missing or malformed — that keeps the local
 * trace consistent even when the caller side has not yet been wired for
 * propagation.
 * @internal
 */
function inboundTraceContext(req: CallRequest): TraceContext {
	const parsed = parseXSbTrace(req.xSbTrace);
	return parsed ?? mintRootContext();
}

// CallServer hosts the inbound Call.Unary gRPC service on the SDK instance.
// All incoming requests are dispatched to the DispatchPort which owns handler
// registration and schema (de)serialization. Errors thrown by handlers are
// converted to CallResponse.error_code/error_message — gRPC status stays OK.
//
// Acceptance check (Layer 2, ADR-0004): for each incoming direct call, the
// server resolves the peer's identity from its TLS cert and checks it against
// the callee's rpc.handle acceptance rules. A peer whose identity cannot be
// established is rejected with PermissionDenied.
//
// Overload (availability): calls are admitted through a semaphore. Past the
// concurrency limit they queue; past the queue depth they are rejected with
// RESOURCE_EXHAUSTED so the caller can retry or fail fast, instead of the
// callee accumulating work it cannot finish.
//
// Tracing (ADR-0001): the server emits NO op. It runs the handler inside
// the inbound call's trace context (parent = caller CALL.op_id) so the handler's
// nested ops (rpc.call / event.publish) parent to the single caller-owned
// RPC.CALL row. Callee errors flow back in the CallResponse and the caller
// records them on the CALL row.
//
// @internal — см. ./README.md
export class CallServer {
	private server: grpc.Server | null = null;
	private advertised: string | null = null;
	private readonly admission: Semaphore;
	private readonly maxConcurrentStreams: number;

	constructor(
		private readonly dispatch: DispatchPort,
		private readonly creds: CallServerCredentials,
		// Returns the current PolicyEvaluation for this service. May return null
		// when policy is not yet loaded (default-allow in that case).
		private readonly getPolicy: () => PolicyEvaluation | null = () => null,
		limits: CallServerLimits = {},
	) {
		const maxConcurrent =
			limits.maxConcurrentCalls ?? DEFAULT_MAX_CONCURRENT_CALLS;
		const maxQueued = limits.maxQueuedCalls ?? maxConcurrent;
		this.admission = new Semaphore(maxConcurrent, maxQueued);
		this.maxConcurrentStreams = maxConcurrent + maxQueued;
	}

	async start(cfg: AdvertiseConfig): Promise<string> {
		if (this.server) {
			throw new Error("rpc: call server already started");
		}
		if (!cfg.host) {
			throw new Error("rpc: advertise.host is required");
		}

		// HTTP/2-level backpressure: peers stop opening streams past this bound
		// instead of the SDK decoding requests it has no capacity to run. The
		// semaphore below still sheds load across multiple connections.
		const server = new grpc.Server({
			"grpc.max_concurrent_streams": this.maxConcurrentStreams,
		});
		server.addService(CallService, {
			unary: (
				call: grpc.ServerUnaryCall<CallRequest, CallResponse>,
				callback: grpc.sendUnaryData<CallResponse>,
			) => {
				this.handleUnary(call, callback);
			},
			stream: (call: grpc.ServerWritableStream<CallRequest, StreamChunk>) => {
				void this.handleStream(call);
			},
		});

		// grpc-js ServerCredentials.createSsl requires PEM-encoded buffers.
		const caChainPem = derToPem(this.creds.caChainDer, "CERTIFICATE");
		const certPem = derToPem(this.creds.leafCertDer, "CERTIFICATE");
		const keyPem = derToPem(this.creds.privateKeyDer, "PRIVATE KEY");

		const bound = await new Promise<number>((resolve, reject) => {
			const sc = grpc.ServerCredentials.createSsl(
				caChainPem,
				[{ private_key: keyPem, cert_chain: certPem }],
				true, // checkClientCertificate
			);
			// Bind on the advertise host directly so SO_REUSEADDR / IPv4 vs IPv6
			// matches the address that will be published in the registry. grpc-js
			// requires a non-empty hostname (it does not accept 0.0.0.0:0 with the
			// current TLS credentials path).
			server.bindAsync(`${cfg.host}:${cfg.port}`, sc, (err, port) => {
				if (err) reject(err);
				else resolve(port);
			});
		});

		this.server = server;
		this.advertised = `${cfg.host}:${bound}`;
		return this.advertised;
	}

	endpoint(): string {
		if (!this.advertised) throw new Error("rpc: call server not started");
		return this.advertised;
	}

	async stop(): Promise<void> {
		if (!this.server) return;
		const server = this.server;
		this.server = null;
		this.advertised = null;
		await new Promise<void>((resolve) => {
			server.tryShutdown((err) => {
				if (err) server.forceShutdown();
				resolve();
			});
		});
	}

	// admit takes an execution slot, or throws SemaphoreExhaustedError when the
	// server is past both its concurrency and queue bounds.
	private async admit(): Promise<() => void> {
		await this.admission.acquire();
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.admission.release();
		};
	}

	private async handleUnary(
		call: grpc.ServerUnaryCall<CallRequest, CallResponse>,
		callback: grpc.sendUnaryData<CallResponse>,
	): Promise<void> {
		// Acceptance runs before admission: an unauthorized caller must not be able
		// to occupy a slot that authorized callers are queueing for.
		const denial = this.checkPeerAcceptance(call, call.request.method);
		if (denial) {
			callback({ code: grpc.status.PERMISSION_DENIED, message: denial });
			return;
		}

		let release: () => void;
		try {
			release = await this.admit();
		} catch (err) {
			callback({
				code: grpc.status.RESOURCE_EXHAUSTED,
				message: overloadMessage(err),
			});
			return;
		}

		const req = call.request;
		const traceCtx = inboundTraceContext(req);
		// The handler runs in the call's trace context (parent = caller CALL.op_id)
		// so its nested ops parent to CALL. No RPC.HANDLE op is emitted — the single
		// RPC.CALL row owned by the caller SDK is the whole call (ADR-0001).
		try {
			await runWithTrace(traceCtx, async () => {
				try {
					const result = await this.dispatch.dispatchUnary(
						req.method,
						req.payload,
					);
					callback(null, {
						payload: Buffer.from(result.payload),
						errorCode: result.errorCode ?? "",
						errorMessage: result.errorMessage ?? "",
					});
				} catch (err) {
					// Handler failure is an application outcome, not a transport one:
					// gRPC status stays OK and the caller decides on retry from
					// errorCode. Collapsing the two would break that decision.
					const msg = (err as Error).message;
					callback(null, {
						payload: Buffer.alloc(0),
						errorCode: "INTERNAL",
						errorMessage: msg,
					});
				}
			});
		} finally {
			release();
		}
	}

	// checkPeerAcceptance returns a denial reason string when the caller is not
	// permitted, or null when the call should proceed.
	private checkPeerAcceptance(call: object, methodName: string): string | null {
		return evaluatePeerAcceptance(this.getPolicy(), call, methodName);
	}

	private async handleStream(
		call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
	): Promise<void> {
		const denial = this.checkPeerAcceptance(call, call.request.method);
		if (denial) {
			call.write({
				payload: Buffer.alloc(0),
				errorCode: "PERMISSION_DENIED",
				errorMessage: denial,
			});
			call.end();
			return;
		}

		let release: () => void;
		try {
			release = await this.admit();
		} catch (err) {
			// Overload is a transport-level outcome, so it travels as a gRPC status
			// rather than a StreamChunk error: grpc-js turns an 'error' event on the
			// writable side into the call's final status.
			call.emit("error", {
				code: grpc.status.RESOURCE_EXHAUSTED,
				details: overloadMessage(err),
			});
			return;
		}

		let cancelled = false;
		const onCancelled = () => {
			cancelled = true;
		};
		// grpc-js emits 'cancelled' on the stream when the caller goes away. Without
		// this the handler's generator keeps producing into a dead call.
		call.on("cancelled", onCancelled);

		const req = call.request;
		const traceCtx = inboundTraceContext(req);
		// Handler runs in the call's trace context; no RPC.HANDLE op (ADR-0001).
		try {
			await runWithTrace(traceCtx, async () => {
				const iterator = this.dispatch
					.dispatchStream(req.method, req.payload)
					[Symbol.asyncIterator]();
				let drained = false;
				try {
					while (!cancelled) {
						const next = await iterator.next();
						if (next.done) {
							drained = true;
							break;
						}
						const item = next.value;
						if (item.errorCode) {
							this.writeChunk(call, {
								payload: Buffer.alloc(0),
								errorCode: item.errorCode,
								errorMessage: item.errorMessage ?? "",
							});
							return;
						}
						const wrote = this.writeChunk(call, {
							payload: Buffer.from(item.payload ?? new Uint8Array()),
							errorCode: "",
							errorMessage: "",
						});
						// write() returning false means grpc-js is buffering for a slow
						// consumer. Waiting for 'drain' is what stops an unbounded
						// producer from filling memory ahead of the reader.
						if (!wrote) await this.waitForDrain(call, () => cancelled);
					}
				} catch (err) {
					const msg = (err as Error).message;
					this.writeChunk(call, {
						payload: Buffer.alloc(0),
						errorCode: "INTERNAL",
						errorMessage: msg,
					});
				} finally {
					// Any exit other than natural exhaustion leaves the handler's
					// generator suspended mid-body; return() runs its finally blocks and
					// stops it producing values nobody will read.
					if (!drained) await iterator.return?.(undefined).catch(() => {});
					if (!cancelled) call.end();
				}
			});
		} finally {
			call.off("cancelled", onCancelled);
			release();
		}
	}

	// writeChunk pushes a chunk unless the call is already gone. Returns false
	// when grpc-js wants the producer to pause.
	private writeChunk(
		call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
		chunk: StreamChunk,
	): boolean {
		if (call.writableEnded || call.destroyed) return true;
		return call.write(chunk);
	}

	private waitForDrain(
		call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
		isCancelled: () => boolean,
	): Promise<void> {
		if (isCancelled()) return Promise.resolve();
		return new Promise<void>((resolve) => {
			const done = () => {
				call.off("drain", done);
				call.off("cancelled", done);
				call.off("close", done);
				call.off("error", done);
				resolve();
			};
			call.once("drain", done);
			call.once("cancelled", done);
			call.once("close", done);
			call.once("error", done);
		});
	}
}

function overloadMessage(err: unknown): string {
	if (err instanceof SemaphoreExhaustedError) {
		return `rpc: server overloaded — ${err.message}`;
	}
	return `rpc: server overloaded — ${(err as Error).message}`;
}
