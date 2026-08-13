import { randomUUID } from "node:crypto";
import type { ServiceBridge } from "../connection/service-bridge";
import { computeContractHash } from "../serde/contract-hash";
import type { SchemaPair } from "../serde/serializer";
import { runWithTrace, streamWithContext } from "../telemetry/context";
import { Channel, type OpHandle, RpcCall, Status } from "../telemetry/ops";
import type { CircuitBreakerRegistry } from "./circuit-breaker";
import type { DirectTransport } from "./direct-transport";
import type { InstanceCache } from "./instance-cache";
import {
	type Candidate,
	cbKey,
	type LoadBalancer,
	NoLiveInstanceError,
} from "./lb";
import type { ProxyTransport } from "./proxy-transport";
import {
	backoffDelay,
	GRPC_CODE_UNAVAILABLE,
	isRetryable,
	mergeRetryOpts,
} from "./retry";

// CallOpts is the per-call configuration accepted by ServiceBridge.call().
// Defaults are sourced from ServiceBridge.options.callDefaults, then overridden
// by per-call values.
export interface CallOpts {
	timeout?: string; // e.g. "10s", "500ms" — default "30s"
	requestId?: string; // auto-generated UUID v4 if omitted
	// transport selects how the call reaches the callee:
	//   - "direct" : caller → callee mTLS, error if callee has no call_endpoint
	//   - "proxy"  : caller → runtime Invoke → callee (even if direct possible)
	//   - "auto"   : direct if endpoint is known, else proxy (DEFAULT)
	transport?: "direct" | "proxy" | "auto";
	// idempotencyKey opts the call into runtime-side dedup (ADR 0001). When
	// set, the runtime claims the key before forwarding and replays of the
	// same key within the TTL return the cached response. Omit for read-only
	// or otherwise non-idempotent calls — INTERNAL/ABORTED/UNKNOWN errors
	// then surface as non-retryable.
	idempotencyKey?: string;
	// Retry policy. Defaults: maxAttempts=3, baseDelayMs=200, factor=2,
	// maxDelayMs=5000, jitter=0.3. Set maxAttempts=1 to disable retry.
	retry?: Partial<RetryOpts>;
}

export interface RetryOpts {
	maxAttempts: number;
	baseDelayMs: number;
	factor: number;
	maxDelayMs: number;
	jitter: number; // fraction in [0, 1]
}

// CallerSchema bundles the SchemaPair with its precomputed contract hash so
// the LB can filter instances whose hash differs (ADR 0005 — version routing).
export interface CallerSchema {
	pair: SchemaPair;
	contractHash: string;
	// contractHashBytes is the UTF-8 wire form the proxy transport puts on every
	// InvokeRequest. The hash is fixed per method, so encoding the 64-char hex
	// string per call was a constant re-encode on the hot path.
	contractHashBytes: Buffer;
}

// SchemaResolver is invoked by RpcClient when a method's CallerSchema is not yet
// cached. Implementations look up the spec from registered handlers (handler
// side knows the .proto path; caller side reuses the same spec or fetches
// from local config).
export type SchemaResolver = (
	serviceName: string,
	methodName: string,
) => CallerSchema | undefined;

// RpcClient is the entry point for outbound RPC calls.
// @internal — см. ./README.md
export class RpcClient {
	constructor(
		private readonly proxy: ProxyTransport,
		private readonly directTransport: DirectTransport | null,
		private readonly instances: InstanceCache,
		private readonly resolveSchema: SchemaResolver,
		// callerService is resolved per call, not captured at construction: the
		// RpcClient is built from openSession before the first Welcome, so the
		// session identity does not exist yet and the guard that keeps the client
		// alive across reconnects would freeze the empty value forever.
		private readonly callerService: () => string,
		private readonly cb: CircuitBreakerRegistry,
		private readonly lb: LoadBalancer,
		// sb owns the instance identity and telemetry ring used for RPC.CALL emission.
		// Per ADR-0001, the caller SDK emits RPC.CALL for every outbound call.
		private readonly sb: ServiceBridge,
	) {}

	// stream invokes a server-side streaming method, returning an AsyncIterable
	// of decoded chunks. Transport selection follows the same transport opt
	// rules as call(). Retry is NOT applied — streams are single-pick by design
	// (ADR 0001): mid-stream replay would re-deliver already-received chunks.
	async *stream<Req = unknown, Chunk = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		opts?: CallOpts,
	): AsyncIterable<Chunk> {
		const descriptor = this.instances.descriptorFor(serviceName, methodName);
		if (!descriptor) {
			throw new Error(
				`rpc: no descriptor for ${serviceName}/${methodName} — not subscribed or target offline`,
			);
		}
		if (!descriptor.streaming) {
			throw new Error(
				`rpc: ${serviceName}/${methodName} is not a streaming method — use sb.rpc.call()`,
			);
		}

		const schema = this.resolveSchema(serviceName, methodName);
		if (!schema) {
			throw new Error(`rpc: no SchemaPair for ${serviceName}/${methodName}`);
		}

		const reqBytes = schema.pair.input.encode(payload as object);
		const requestId = opts?.requestId ?? randomUUID();
		const idempotencyKey = opts?.idempotencyKey ?? "";
		const timeoutMs = parseTimeout(opts?.timeout) ?? 30_000;
		const transport = opts?.transport ?? "auto";

		const candidate = this.pickCandidate(
			serviceName,
			methodName,
			schema.contractHash,
			transport,
		);
		const useDirect = this.shouldUseDirect(candidate, transport);

		// One RPC.CALL op for the entire stream lifetime (ADR-0001).
		const callOp = this.sb.telemetry.startOp({
			channel: Channel.RPC,
			kind: RpcCall,
			subject: formatRpcCallSubject(serviceName, methodName),
			peerServiceId: candidate.instance.serviceId,
			attempt: 0,
			metaJson: rpcCallMeta(methodName, !useDirect, requestId, idempotencyKey),
		});

		// Streaming captures only the request payload (IN). The response is a
		// chunk stream; concatenating chunks into one blob would break streaming
		// memory bounds, so OUT is not captured for streams.
		callOp.captureIn(reqBytes, schema.contractHash);

		const release = this.lb.acquire(candidate.instance.instanceId);
		const childCtx = { traceId: callOp.traceId, parentOpId: callOp.opId };
		const directTransport = this.directTransport;
		const proxy = this.proxy;
		const callerService = this.callerService();
		const decoded = async function* (): AsyncIterable<Chunk> {
			const source = useDirect
				? directTransport!.callStream(
						{
							endpoint: candidate.instance.callEndpoint,
							serviceId: candidate.instance.serviceId,
							instanceId: candidate.instance.instanceId,
						},
						methodName,
						reqBytes,
						callerService,
						requestId,
						idempotencyKey,
						timeoutMs,
					)
				: proxy.callStream(
						candidate.instance.serviceId,
						methodName,
						reqBytes,
						requestId,
						idempotencyKey,
						timeoutMs,
						schema.contractHashBytes,
					);
			for await (const bytes of source) {
				yield schema.pair.output.decode(bytes) as Chunk;
			}
		};
		let endStatus: Status = Status.SUCCESS;
		let endMsg: string | undefined;
		try {
			yield* streamWithContext(childCtx, decoded);
			this.cb.recordSuccess(cbKey(candidate.instance));
		} catch (err) {
			endStatus = Status.ERROR;
			endMsg = err instanceof Error ? err.message : String(err);
			this.cb.recordFailure(cbKey(candidate.instance));
			throw err;
		} finally {
			callOp.end(endStatus, endMsg);
			release();
		}
	}

	async call<Req = unknown, Res = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		opts?: CallOpts,
	): Promise<Res> {
		const descriptor = this.instances.descriptorFor(serviceName, methodName);
		if (!descriptor) {
			throw new Error(
				`rpc: no descriptor for ${serviceName}/${methodName} — not subscribed or target offline`,
			);
		}
		if (descriptor.streaming) {
			throw new Error(
				`rpc: ${serviceName}/${methodName} is a streaming method — use sb.stream() instead`,
			);
		}

		const schema = this.resolveSchema(serviceName, methodName);
		if (!schema) {
			throw new Error(
				`rpc: no SchemaPair for ${serviceName}/${methodName} — caller must register schema for this method`,
			);
		}

		const reqBytes = schema.pair.input.encode(payload as object);
		const requestId = opts?.requestId ?? randomUUID();
		// Idempotency is opt-in (ADR 0001). Empty string is the wire signal for
		// "no key" — runtime then skips Claim/Save entirely.
		const idempotencyKey = opts?.idempotencyKey ?? "";
		const timeoutMs = parseTimeout(opts?.timeout) ?? 30_000;
		const transport = opts?.transport ?? "auto";
		const retry = mergeRetryOpts(opts?.retry);
		const hasIdempotency = idempotencyKey.length > 0;

		// One RPC.CALL op for the whole logical call (ADR-0001). Retries
		// bump the attempt counter on the SAME row — no op per attempt. The op is
		// started lazily on the first successful candidate pick so peer_service_id
		// and via_proxy reflect the transport actually used.
		let callOp: OpHandle | null = null;
		let lastErr: unknown = null;
		for (let attempt = 0; attempt < retry.maxAttempts; attempt++) {
			let candidate: Candidate;
			try {
				candidate = this.pickCandidate(
					serviceName,
					methodName,
					schema.contractHash,
					transport,
				);
			} catch (err) {
				lastErr = err;
				if (
					attempt === retry.maxAttempts - 1 ||
					!isRetryable(err, hasIdempotency)
				) {
					// Close the CALL row if it was already started on a prior attempt.
					callOp?.setAttempt(attempt);
					callOp?.end(
						Status.ERROR,
						err instanceof Error ? err.message : String(err),
					);
					throw err;
				}
				await sleep(backoffDelay(retry, attempt));
				continue;
			}

			const useDirect = this.shouldUseDirect(candidate, transport);

			if (callOp === null) {
				callOp = this.sb.telemetry.startOp({
					channel: Channel.RPC,
					kind: RpcCall,
					subject: formatRpcCallSubject(serviceName, methodName),
					peerServiceId: candidate.instance.serviceId,
					attempt,
					metaJson: rpcCallMeta(
						methodName,
						!useDirect,
						requestId,
						idempotencyKey,
					),
				});
				// Caller owns the RPC.CALL payloads: request (IN) once per logical
				// call, response (OUT) captured below before decode.
				callOp.captureIn(reqBytes, schema.contractHash);
			} else {
				callOp.setAttempt(attempt);
			}

			const release = this.lb.acquire(candidate.instance.instanceId);
			try {
				// Wrap invoke in child context so transport reads callOp.opId as
				// parentOpId — the callee handler then runs under parent=CALL.op_id.
				const childCtx = { traceId: callOp.traceId, parentOpId: callOp.opId };
				const invoke = async () => {
					const respBytes = useDirect
						? await this.directTransport!.callUnary(
								{
									endpoint: candidate.instance.callEndpoint,
									serviceId: candidate.instance.serviceId,
									instanceId: candidate.instance.instanceId,
								},
								methodName,
								reqBytes,
								this.callerService(),
								requestId,
								idempotencyKey,
								timeoutMs,
							)
						: await this.proxy.callUnary(
								candidate.instance.serviceId,
								methodName,
								reqBytes,
								requestId,
								idempotencyKey,
								timeoutMs,
								schema.contractHashBytes,
							);
					callOp?.captureOut(respBytes, schema.contractHash);
					return schema.pair.output.decode(respBytes) as Res;
				};
				const result = (await runWithTrace(childCtx, invoke)) as Res;
				callOp.end(Status.SUCCESS);
				this.cb.recordSuccess(cbKey(candidate.instance));
				return result;
			} catch (err) {
				lastErr = err;
				this.cb.recordFailure(cbKey(candidate.instance));
				if (
					attempt === retry.maxAttempts - 1 ||
					!isRetryable(err, hasIdempotency)
				) {
					callOp.end(
						Status.ERROR,
						err instanceof Error ? err.message : String(err),
					);
					throw err;
				}
				await sleep(backoffDelay(retry, attempt));
			} finally {
				release();
			}
		}
		// unreachable, but keep TS happy
		throw lastErr as Error;
	}

	// pickCandidate reads the contract-matched candidate list from the instance
	// cache, runs P2C through the LB, and surfaces the errors callers need to map
	// onto retry decisions.
	private pickCandidate(
		serviceName: string,
		methodName: string,
		callerHash: string,
		transport: "direct" | "proxy" | "auto",
	): Candidate {
		const all = this.instances.candidatesFor(
			serviceName,
			methodName,
			callerHash,
		);
		if (all.length === 0) {
			throw noLiveInstance(
				`rpc: no instance of ${serviceName}/${methodName} matches caller contract ${callerHash}`,
			);
		}
		let candidate: Candidate;
		try {
			candidate = this.lb.pick(all);
		} catch (err) {
			// The LB reports "everything is filtered out" without knowing which
			// call it was serving; keep the type and add the coordinates.
			if (err instanceof NoLiveInstanceError) {
				throw noLiveInstance(
					`rpc: no live instance of ${serviceName}/${methodName} matching contract ${callerHash} — all candidates unhealthy or circuit-open`,
				);
			}
			throw err;
		}
		if (transport === "direct" && !candidate.instance.callEndpoint) {
			throw new Error(
				`rpc: transport="direct" requested but no endpoint for ${serviceName}/${methodName} matching contract ${callerHash}`,
			);
		}
		return candidate;
	}

	private shouldUseDirect(
		candidate: Candidate,
		transport: "direct" | "proxy" | "auto",
	): boolean {
		return (
			this.directTransport !== null &&
			transport !== "proxy" &&
			candidate.instance.callEndpoint !== ""
		);
	}
}

// formatRpcCallSubject mirrors Go's telemetry.FormatSubject(ChannelRPC, KindRPCCall, svc, method)
// → "rpc.call:<svc>/<method>" (ADR-0007).
function formatRpcCallSubject(serviceName: string, methodName: string): string {
	return `rpc.call:${serviceName}/${methodName}`;
}

// noLiveInstance keeps the callee-fleet-is-empty condition on its own error
// type while carrying gRPC UNAVAILABLE, so retry treats it as a connection that
// never happened and callers can tell it apart from a callee that answered
// UNAVAILABLE without matching on message text.
function noLiveInstance(message: string): NoLiveInstanceError {
	return Object.assign(new NoLiveInstanceError(message), {
		code: GRPC_CODE_UNAVAILABLE,
	});
}

// rpcCallMeta builds the RPC.CALL meta JSON. Written out instead of
// JSON.stringify over an object literal: it runs on every call, and the literal
// plus the generic object walk were short-lived garbage on the hot path.
function rpcCallMeta(
	methodName: string,
	viaProxy: boolean,
	requestId: string,
	idempotencyKey: string,
): Buffer {
	return Buffer.from(
		`{"method":${JSON.stringify(methodName)},"via_proxy":${viaProxy},` +
			`"requestId":${JSON.stringify(requestId)},"idempotencyKey":${JSON.stringify(idempotencyKey)}}`,
	);
}

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

function parseTimeout(s: string | undefined): number | undefined {
	if (!s) return undefined;
	const m = /^(\d+)(ms|s|m)$/.exec(s.trim());
	if (!m) throw new Error(`rpc: invalid timeout ${s}`);
	const n = Number.parseInt(m[1]!, 10);
	switch (m[2]) {
		case "ms":
			return n;
		case "s":
			return n * 1000;
		case "m":
			return n * 60_000;
		default:
			return undefined;
	}
}

// SchemaRegistry is the caller-side mapping from (serviceName, methodName)
// to a SchemaPair. Populated by ServiceBridge when the user calls
// sb.service(..., { schemas: ... }) or directly via a low-level setter that
// registers the same .proto schemas used by callees.
export class SchemaRegistry {
	private map = new Map<string, CallerSchema>();

	set(serviceName: string, methodName: string, pair: SchemaPair): void {
		const contractHash = computeContractHash(pair);
		this.map.set(`${serviceName}/${methodName}`, {
			pair,
			contractHash,
			contractHashBytes: Buffer.from(contractHash, "utf8"),
		});
	}

	get(serviceName: string, methodName: string): CallerSchema | undefined {
		return this.map.get(`${serviceName}/${methodName}`);
	}

	asResolver(): SchemaResolver {
		return (service, method) => this.get(service, method);
	}
}
