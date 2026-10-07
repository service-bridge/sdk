import * as grpc from "@grpc/grpc-js";
import type { CertificateStore } from "../connection/tls-material";
import type { Logger } from "../logger";
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
import { Semaphore } from "../utils/semaphore";
import {
	evaluatePeerAcceptance,
	type PeerIdentity,
	peerOfCall,
} from "./acceptance";
import type { DispatchPort, RpcHandlerContext } from "./dispatch-port";
import { notDispatchedTrailer } from "./wire";

// AdvertiseConfig is the operator-supplied advertise address. host is mandatory
// (no auto-detect — k8s/docker need explicit POD_IP via downward API), port=0
// asks the OS to pick a free port.
export interface AdvertiseConfig {
	host: string;
	port: number;
}

// Inbound bounds. Same defaults in the Go SDK (WithInboundLimits).
export const DEFAULT_MAX_CONCURRENT_CALLS = 256;

// CallServerLimits bounds inbound load. The concurrency limit alone would just
// move an overload into an unbounded queue; the queue depth is what sheds load
// with RESOURCE_EXHAUSTED.
export interface CallServerLimits {
	maxConcurrentCalls?: number;
	// Defaults to maxConcurrentCalls. 0 rejects any call that cannot start at once.
	maxQueuedCalls?: number;
}

/** @internal */
export interface CallServerDeps {
	dispatch: DispatchPort;
	store: CertificateStore;
	// The policy the runtime last pushed; null until the first snapshot. Calls
	// are refused (UNAVAILABLE) until it exists: without it acceptance rules are
	// unknown and default-allow would admit anyone (GSDK-07).
	policy: () => PolicyEvaluation | null;
	// Revoked peers are refused at once (decision 12).
	isRevoked: (serviceId: string, instanceId: string) => boolean;
	limits?: CallServerLimits;
	logger: Logger;
}

// How long a rebind (certificate rotation) or a stop waits for in-flight calls
// on the old listener before cancelling them.
const DEFAULT_DRAIN_MS = 30_000;

// The call shapes the server handles; both expose cancellation and deadline.
interface SurfaceCall {
	cancelled: boolean;
	once(event: "cancelled", listener: () => void): unknown;
	off(event: "cancelled", listener: () => void): unknown;
	getDeadline(): grpc.Deadline;
}

type Refusal = { code: grpc.status; message: string; transient: boolean };

// CallServer hosts the inbound Call service of this instance.
//
// Wire form (identical in the Go SDK):
//   - refusals before the handler runs are gRPC statuses; the transient ones
//     (not ready, draining, overload) carry the x-sb-not-dispatched trailer so
//     the caller may retry elsewhere;
//   - a handler failure is gRPC OK with error_code/error_message.
//
// Tracing (ADR-0001): no op is emitted here. The handler runs in the call's
// trace context (parent = the caller's RPC.CALL op).
//
// @internal — см. ./README.md
export class CallServer {
	private server: grpc.Server | null = null;
	private bindAddress: string | null = null;
	private advertised: string | null = null;
	private draining = false;
	private inflight = 0;
	private idleWaiters: (() => void)[] = [];
	private readonly admission: Semaphore;
	private readonly maxConcurrentStreams: number;

	constructor(private readonly d: CallServerDeps) {
		const maxConcurrent =
			d.limits?.maxConcurrentCalls ?? DEFAULT_MAX_CONCURRENT_CALLS;
		const maxQueued = d.limits?.maxQueuedCalls ?? maxConcurrent;
		this.admission = new Semaphore(maxConcurrent, maxQueued);
		this.maxConcurrentStreams = maxConcurrent + maxQueued;
	}

	/** Binds the listener and returns the advertised host:port. */
	async start(cfg: AdvertiseConfig): Promise<string> {
		if (this.server) throw new Error("rpc: call server already started");
		if (!cfg.host) throw new Error("rpc: advertise.host is required");
		const { server, port } = await this.bind(`${cfg.host}:${cfg.port}`);
		this.server = server;
		// The port the OS handed out is kept: a rebind on rotation must not move
		// the endpoint callers already resolved.
		this.bindAddress = `${cfg.host}:${port}`;
		this.advertised = this.bindAddress;
		return this.advertised;
	}

	endpoint(): string {
		if (!this.advertised) throw new Error("rpc: call server not started");
		return this.advertised;
	}

	/**
	 * Presents the rotated certificate to new connections. The listener is
	 * rebound on the same port with fresh credentials; the old server stops
	 * accepting connections at once and keeps serving its in-flight calls until
	 * they finish (bounded by DEFAULT_DRAIN_MS). In-place secure-context updates
	 * are not honoured by every runtime the SDK supports, a rebind is.
	 */
	async rotate(): Promise<void> {
		const old = this.server;
		const address = this.bindAddress;
		if (!old || !address) return;
		const retired = this.retire(old, DEFAULT_DRAIN_MS);
		for (let attempt = 0; ; attempt++) {
			try {
				const { server } = await this.bind(address);
				this.server = server;
				break;
			} catch (err) {
				if (attempt >= 50) {
					this.d.logger.error("rpc: call server rebind failed after rotation", {
						address,
						error: (err as Error).message,
					});
					this.server = null;
					break;
				}
				await new Promise((r) => setTimeout(r, 100));
			}
		}
		void retired;
	}

	/**
	 * Stops admitting calls: every new call is refused with UNAVAILABLE and the
	 * not-dispatched trailer, so the caller retries on another instance.
	 */
	beginDrain(): void {
		this.draining = true;
	}

	/** Resolves when no call is in flight, or when timeoutMs elapses. */
	waitIdle(timeoutMs: number): Promise<void> {
		if (this.inflight === 0) return Promise.resolve();
		return new Promise((resolve) => {
			const timer = setTimeout(done, timeoutMs);
			const self = this;
			function done() {
				clearTimeout(timer);
				self.idleWaiters = self.idleWaiters.filter((w) => w !== done);
				resolve();
			}
			this.idleWaiters.push(done);
		});
	}

	async stop(timeoutMs = 1000): Promise<void> {
		const server = this.server;
		this.server = null;
		this.advertised = null;
		this.bindAddress = null;
		if (server) await this.retire(server, timeoutMs);
	}

	private async bind(
		address: string,
	): Promise<{ server: grpc.Server; port: number }> {
		// HTTP/2-level backpressure: peers stop opening streams past this bound
		// instead of the SDK decoding requests it has no capacity to run.
		// grpc-js has no server keepalive enforcement policy; client pings are
		// accepted at any rate.
		const server = new grpc.Server({
			"grpc.max_concurrent_streams": this.maxConcurrentStreams,
		});
		server.addService(CallService, {
			unary: (
				call: grpc.ServerUnaryCall<CallRequest, CallResponse>,
				callback: grpc.sendUnaryData<CallResponse>,
			) => {
				void this.handleUnary(call, callback);
			},
			stream: (call: grpc.ServerWritableStream<CallRequest, StreamChunk>) => {
				void this.handleStream(call);
			},
		});
		const port = await new Promise<number>((resolve, reject) => {
			server.bindAsync(address, this.d.store.serverCredentials(), (err, p) => {
				if (err) reject(err);
				else resolve(p);
			});
		});
		return { server, port };
	}

	private retire(server: grpc.Server, timeoutMs: number): Promise<void> {
		return new Promise<void>((resolve) => {
			const timer = setTimeout(() => {
				server.forceShutdown();
				resolve();
			}, timeoutMs);
			server.tryShutdown((err) => {
				clearTimeout(timer);
				if (err) server.forceShutdown();
				resolve();
			});
		});
	}

	// refusal decides whether a call may reach admission at all.
	private refusal(
		call: object,
		method: string,
		peer: PeerIdentity,
	): Refusal | null {
		void call;
		if (this.draining)
			return {
				code: grpc.status.UNAVAILABLE,
				message: "rpc: instance is draining",
				transient: true,
			};
		const policy = this.d.policy();
		if (!policy)
			return {
				code: grpc.status.UNAVAILABLE,
				message: "rpc: instance not ready — no access policy received yet",
				transient: true,
			};
		if (
			peer.kind === "service" &&
			this.d.isRevoked(peer.serviceId, peer.instanceId)
		)
			return {
				code: grpc.status.PERMISSION_DENIED,
				message: `rpc: caller ${peer.serviceId}/${peer.instanceId} is revoked`,
				transient: false,
			};
		const denial = evaluatePeerAcceptance(policy, peer, method);
		if (denial)
			return {
				code: grpc.status.PERMISSION_DENIED,
				message: denial,
				transient: false,
			};
		return null;
	}

	private refuse(r: Refusal): grpc.ServiceError {
		const err = Object.assign(new Error(r.message), {
			code: r.code,
			details: r.message,
			metadata: r.transient ? notDispatchedTrailer() : new grpc.Metadata(),
		});
		return err as grpc.ServiceError;
	}

	private track(): () => void {
		this.inflight++;
		let done = false;
		return () => {
			if (done) return;
			done = true;
			this.inflight--;
			if (this.inflight === 0) {
				const waiters = this.idleWaiters;
				this.idleWaiters = [];
				for (const w of waiters) w();
			}
		};
	}

	// admit takes an execution slot. Throws when the server is past both its
	// concurrency and queue bounds, or the caller went away while queued.
	private async admit(call: SurfaceCall): Promise<() => void> {
		const controller = new AbortController();
		if (call.cancelled) controller.abort();
		const cancel = () => controller.abort();
		call.once("cancelled", cancel);
		try {
			await this.admission.acquire(controller.signal);
		} finally {
			call.off("cancelled", cancel);
		}
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
		const untrack = this.track();
		try {
			const req = call.request;
			const peer = peerOfCall(call);
			const refused = this.refusal(call, req.method, peer);
			if (refused) {
				callback(this.refuse(refused));
				return;
			}
			let release: () => void;
			try {
				release = await this.admit(call);
			} catch {
				callback(
					call.cancelled
						? this.refuse({
								code: grpc.status.CANCELLED,
								message: "rpc: call cancelled",
								transient: false,
							})
						: this.refuse({
								code: grpc.status.RESOURCE_EXHAUSTED,
								message: "rpc: server overloaded",
								transient: true,
							}),
				);
				return;
			}
			const { ctx, dispose } = handlerContext(call, req, peer);
			try {
				const result = await runWithTrace(inboundTraceContext(req), () =>
					this.d.dispatch.dispatchUnary(req.method, req.payload, ctx),
				);
				if (result.status !== undefined) {
					callback(
						this.refuse({
							code: result.status,
							message: result.errorMessage ?? "",
							transient: false,
						}),
					);
					return;
				}
				callback(null, {
					payload: Buffer.from(result.payload ?? new Uint8Array()),
					errorCode: result.errorCode ?? "",
					errorMessage: result.errorMessage ?? "",
				});
			} finally {
				dispose();
				release();
			}
		} finally {
			untrack();
		}
	}

	private async handleStream(
		call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
	): Promise<void> {
		const untrack = this.track();
		try {
			const req = call.request;
			const peer = peerOfCall(call);
			const refused = this.refusal(call, req.method, peer);
			if (refused) {
				call.emit("error", this.refuse(refused));
				return;
			}
			let release: () => void;
			try {
				release = await this.admit(call);
			} catch {
				call.emit(
					"error",
					call.cancelled
						? this.refuse({
								code: grpc.status.CANCELLED,
								message: "rpc: call cancelled",
								transient: false,
							})
						: this.refuse({
								code: grpc.status.RESOURCE_EXHAUSTED,
								message: "rpc: server overloaded",
								transient: true,
							}),
				);
				return;
			}
			const { ctx, dispose } = handlerContext(call, req, peer);
			try {
				await runWithTrace(inboundTraceContext(req), () =>
					this.pumpStream(call, req, ctx),
				);
			} finally {
				dispose();
				release();
			}
		} finally {
			untrack();
		}
	}

	private async pumpStream(
		call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
		req: CallRequest,
		ctx: RpcHandlerContext,
	): Promise<void> {
		const iterator = this.d.dispatch
			.dispatchStream(req.method, req.payload, ctx)
			[Symbol.asyncIterator]();
		let drained = false;
		try {
			while (!ctx.signal.aborted) {
				const next = await iterator.next();
				if (next.done) {
					drained = true;
					break;
				}
				const item = next.value;
				if (item.status !== undefined) {
					call.emit(
						"error",
						this.refuse({
							code: item.status,
							message: item.errorMessage ?? "",
							transient: false,
						}),
					);
					return;
				}
				if (item.errorCode) {
					writeChunk(call, {
						payload: Buffer.alloc(0),
						errorCode: item.errorCode,
						errorMessage: item.errorMessage ?? "",
					});
					break;
				}
				const wrote = writeChunk(call, {
					payload: Buffer.from(item.payload ?? new Uint8Array()),
					errorCode: "",
					errorMessage: "",
				});
				// write() returning false means grpc-js is buffering for a slow
				// consumer; waiting for 'drain' stops the producer running ahead.
				if (!wrote) await waitForDrain(call, ctx.signal);
			}
		} finally {
			// Any exit other than natural exhaustion leaves the handler's
			// generator suspended; return() runs its finally blocks.
			if (!drained) await iterator.return?.(undefined).catch(() => {});
		}
		if (!ctx.signal.aborted && !call.writableEnded) call.end();
	}
}

/**
 * Builds the handler context. The signal aborts when the caller cancels or
 * the deadline passes; dispose() removes the listeners once the call is done.
 */
function handlerContext(
	call: SurfaceCall,
	req: CallRequest,
	peer: PeerIdentity,
): { ctx: RpcHandlerContext; dispose: () => void } {
	const controller = new AbortController();
	const cancel = () =>
		controller.abort(new Error("rpc: cancelled by the caller"));
	if (call.cancelled) cancel();
	call.once("cancelled", cancel);
	const rawDeadline = call.getDeadline();
	const deadline =
		rawDeadline instanceof Date
			? rawDeadline.getTime()
			: Number.isFinite(rawDeadline)
				? Number(rawDeadline)
				: null;
	let timer: ReturnType<typeof setTimeout> | undefined;
	if (deadline !== null) {
		timer = setTimeout(
			() => controller.abort(new Error("rpc: deadline exceeded")),
			Math.max(0, deadline - Date.now()),
		);
		timer.unref?.();
	}
	const caller =
		peer.kind === "service"
			? { serviceId: peer.serviceId, instanceId: peer.instanceId }
			: peer.kind === "runtime" && req.callerService
				? { serviceId: req.callerService, instanceId: "" }
				: null;
	return {
		ctx: {
			signal: controller.signal,
			deadline,
			requestId: req.requestId,
			idempotencyKey: req.idempotencyKey,
			caller,
		},
		dispose: () => {
			call.off("cancelled", cancel);
			if (timer) clearTimeout(timer);
		},
	};
}

// inboundTraceContext parses the caller's trace context, or mints a fresh
// root when it is missing or malformed.
function inboundTraceContext(req: CallRequest): TraceContext {
	return parseXSbTrace(req.xSbTrace) ?? mintRootContext();
}

// writeChunk pushes a chunk unless the call is already gone. Returns false
// when grpc-js wants the producer to pause.
function writeChunk(
	call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
	chunk: StreamChunk,
): boolean {
	if (call.writableEnded || call.destroyed) return true;
	return call.write(chunk);
}

function waitForDrain(
	call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
	signal: AbortSignal,
): Promise<void> {
	if (signal.aborted) return Promise.resolve();
	return new Promise<void>((resolve) => {
		const done = () => {
			call.off("drain", done);
			call.off("close", done);
			call.off("error", done);
			signal.removeEventListener("abort", done);
			resolve();
		};
		call.once("drain", done);
		call.once("close", done);
		call.once("error", done);
		signal.addEventListener("abort", done, { once: true });
	});
}
