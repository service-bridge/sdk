import { randomUUID } from "node:crypto";
import type { ServiceBridge } from "../connection/service-bridge";
import {
	ConfigurationError,
	NoLiveInstanceError,
	ServiceBridgeError,
	ValidationError,
} from "../errors";
import { computeContractHash } from "../serde/contract-hash";
import type { SchemaPair } from "../serde/serializer";
import { runWithTrace, streamWithContext } from "../telemetry/context";
import { Channel, type OpHandle, RpcCall, Status } from "../telemetry/ops";
import type { CircuitBreakerRegistry } from "./circuit-breaker";
import type { DirectTransport, WireCall } from "./direct-transport";
import type { InstanceCache } from "./instance-cache";
import { type Candidate, cbKey, type LoadBalancer } from "./lb";
import type { ProxyTransport } from "./proxy-transport";
import { backoffDelay, mergeRetryOpts } from "./retry";
import { CallFailure } from "./wire";

/**
 * Per-call options. `ServiceBridgeOptions.callDefaults` apply to every call
 * path (sb.rpc.call, sb.stream, typed clients); a per-call value wins.
 *
 * @public — см. ./README.md
 */
export interface CallOpts {
	/** Cancels local waiting and the gRPC call. Remote effects may have happened. */
	signal?: AbortSignal;
	/** Deadline for the whole logical call, e.g. "10s", "500ms". Default "30s". */
	timeout?: string;
	/** Correlation id carried to the callee. Default: random UUID. */
	requestId?: string;
	/**
	 * "auto" (default): direct to the picked instance when it advertises an
	 * endpoint, falling back to the runtime proxy after a pre-dispatch failure.
	 * "direct": never via the runtime. "proxy": always via the runtime.
	 */
	transport?: "direct" | "proxy" | "auto";
	/**
	 * Handed to the callee (ctx.idempotencyKey) and to the runtime proxy's
	 * dedup. It does not make a dispatched call retryable: the SDK retries only
	 * failures proven to have happened before the handler ran.
	 */
	idempotencyKey?: string;
	/** Retry policy for pre-dispatch failures. maxAttempts=1 disables retry. */
	retry?: Partial<RetryOpts>;
}

/** @public — см. ./README.md */
export interface RetryOpts {
	maxAttempts: number;
	baseDelayMs: number;
	factor: number;
	maxDelayMs: number;
	jitter: number; // fraction in [0, 1]
}

const DEFAULT_TIMEOUT_MS = 30_000;

// CallerSchema bundles the SchemaPair with its precomputed contract hash so
// the LB can filter instances whose hash differs (ADR 0005 — version routing).
export interface CallerSchema {
	pair: SchemaPair;
	contractHash: string;
	// UTF-8 wire form for InvokeRequest.contract_hash, encoded once per method.
	contractHashBytes: Buffer;
}

export type SchemaResolver = (
	serviceName: string,
	methodName: string,
) => CallerSchema | undefined;

/** What the domain needs from an outbound client. @internal */
export type RpcCaller = Pick<RpcClient, "call" | "stream">;

/** @internal */
export interface RpcClientDeps {
	proxy: ProxyTransport;
	direct: DirectTransport;
	instances: InstanceCache;
	resolveSchema: SchemaResolver;
	cb: CircuitBreakerRegistry;
	lb: LoadBalancer;
	callDefaults: () => CallOpts;
	// sb owns the telemetry surface used for the RPC.CALL op (ADR-0001).
	sb: ServiceBridge;
}

// RpcClient is the entry point for outbound RPC calls.
// @internal — см. ./README.md
export class RpcClient {
	constructor(private readonly d: RpcClientDeps) {}

	// stream invokes a server-side streaming method. A stream picks its instance
	// once and is never retried: a repeat would re-deliver chunks already read.
	async *stream<Req = unknown, Chunk = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		callOpts?: CallOpts,
	): AsyncIterable<Chunk> {
		const opts = { ...this.d.callDefaults(), ...(callOpts ?? {}) };
		opts.signal?.throwIfAborted();
		const schema = this.prepare(serviceName, methodName, true);
		const reqBytes = schema.pair.input.encode(payload as object);
		const requestId = opts.requestId ?? randomUUID();
		const idempotencyKey = opts.idempotencyKey ?? "";
		const deadline = new Date(Date.now() + timeoutMs(opts.timeout));
		const transport = opts.transport ?? "auto";

		const candidate = this.pickCandidate(
			serviceName,
			methodName,
			schema.contractHash,
		);
		const useDirect = useDirectFor(candidate, transport);

		const callOp = this.d.sb.telemetry.startOp({
			channel: Channel.RPC,
			kind: RpcCall,
			subject: formatRpcCallSubject(serviceName, methodName),
			peerServiceId: candidate.instance.serviceId,
			attempt: 0,
			metaJson: rpcCallMeta(methodName, !useDirect, requestId, idempotencyKey),
		});
		// Streaming captures only the request: concatenating chunks would break
		// streaming memory bounds.
		callOp.captureIn(reqBytes, schema.contractHash);

		const release = this.d.lb.acquire(candidate.instance.instanceId);
		const childCtx = { traceId: callOp.traceId, parentOpId: callOp.opId };
		const wire: WireCall = {
			method: methodName,
			payload: reqBytes,
			requestId,
			idempotencyKey,
			deadline,
			signal: opts.signal,
		};
		const { direct, proxy } = this.d;
		const decoded = async function* (): AsyncIterable<Chunk> {
			const source = useDirect
				? direct.callStream(directTarget(candidate), wire)
				: proxy.callStream(
						candidate.instance.serviceId,
						schema.contractHashBytes,
						wire,
					);
			for await (const bytes of source) {
				yield schema.pair.output.decode(bytes) as Chunk;
			}
		};
		let endStatus: Status = Status.SUCCESS;
		let endMsg: string | undefined;
		try {
			yield* streamWithContext(childCtx, decoded);
			if (useDirect) this.d.cb.recordSuccess(cbKey(candidate.instance));
		} catch (err) {
			const failure = asFailure(err, opts.signal);
			endStatus = Status.ERROR;
			endMsg = failure.error.message;
			if (useDirect) this.recordOutcome(candidate, failure);
			throw failure.error;
		} finally {
			callOp.end(endStatus, endMsg);
			release();
		}
	}

	async call<Req = unknown, Res = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		callOpts?: CallOpts,
	): Promise<Res> {
		const opts = { ...this.d.callDefaults(), ...(callOpts ?? {}) };
		opts.signal?.throwIfAborted();
		const schema = this.prepare(serviceName, methodName, false);
		const reqBytes = schema.pair.input.encode(payload as object);
		const requestId = opts.requestId ?? randomUUID();
		const idempotencyKey = opts.idempotencyKey ?? "";
		const deadlineAt = Date.now() + timeoutMs(opts.timeout);
		const deadline = new Date(deadlineAt);
		const transport = opts.transport ?? "auto";
		const retry = mergeRetryOpts(opts.retry);

		// One RPC.CALL op for the whole logical call (ADR-0001); retries bump
		// the attempt on the same row. Started on the first successful pick so
		// peer_service_id and via_proxy reflect the transport actually used.
		let callOp: OpHandle | null = null;
		// After a pre-dispatch failure of the direct path, "auto" moves to the
		// runtime proxy: a dead pod may still sit in the local snapshot while
		// the runtime already knows a live instance.
		let viaProxy = transport === "proxy";
		// Instances the direct path could not reach: the proxy tries them last.
		const unreachable: string[] = [];
		let lastError: ServiceBridgeError | null = null;

		let fallback = false;
		for (let attempt = 0; attempt < retry.maxAttempts; attempt++) {
			// The switch to the proxy after a direct pre-dispatch failure is not a
			// retry of the same path and waits for nothing.
			if (attempt > 0 && !fallback) {
				const pause = Math.min(
					backoffDelay(retry, attempt - 1),
					deadlineAt - Date.now(),
				);
				if (pause <= 0) break;
				await sleep(pause, opts.signal);
			}
			fallback = false;
			let candidate: Candidate;
			try {
				candidate = this.pickCandidate(
					serviceName,
					methodName,
					schema.contractHash,
				);
			} catch (err) {
				lastError = err as ServiceBridgeError;
				continue;
			}
			const useDirect = !viaProxy && useDirectFor(candidate, transport);
			if (transport === "direct" && !useDirect) {
				lastError = new NoLiveInstanceError(
					`rpc: ${serviceName}/${methodName}: the picked instance advertises no endpoint and transport is "direct"`,
				);
				continue;
			}

			if (callOp === null) {
				callOp = this.d.sb.telemetry.startOp({
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
				callOp.captureIn(reqBytes, schema.contractHash);
			} else {
				callOp.setAttempt(attempt);
			}

			const release = this.d.lb.acquire(candidate.instance.instanceId);
			const op = callOp;
			const wire: WireCall = {
				method: methodName,
				payload: reqBytes,
				requestId,
				idempotencyKey,
				deadline,
				signal: opts.signal,
				excludeInstanceIds: unreachable,
			};
			try {
				const respBytes = await runWithTrace(
					{ traceId: op.traceId, parentOpId: op.opId },
					() =>
						useDirect
							? this.d.direct.callUnary(directTarget(candidate), wire)
							: this.d.proxy.callUnary(
									candidate.instance.serviceId,
									schema.contractHashBytes,
									wire,
								),
				);
				op.captureOut(respBytes, schema.contractHash);
				const result = schema.pair.output.decode(respBytes) as Res;
				op.end(Status.SUCCESS);
				if (useDirect) this.d.cb.recordSuccess(cbKey(candidate.instance));
				return result;
			} catch (err) {
				const failure = asFailure(err, opts.signal);
				if (useDirect) this.recordOutcome(candidate, failure);
				if (!failure.preDispatch) {
					op.end(Status.ERROR, failure.error.message);
					throw failure.error;
				}
				lastError = failure.error;
				if (useDirect && transport === "auto") {
					viaProxy = true;
					fallback = true;
					unreachable.push(candidate.instance.instanceId);
				}
			} finally {
				release();
			}
		}
		const error =
			lastError ??
			new NoLiveInstanceError(
				`rpc: ${serviceName}/${methodName}: deadline exhausted before any attempt`,
			);
		callOp?.end(Status.ERROR, error.message);
		throw error;
	}

	// prepare checks the call against the local contract view. A method the
	// mesh does not describe yet is NO_LIVE_INSTANCE (retryable: the callee may
	// be starting), a missing caller schema is a programming error.
	private prepare(
		serviceName: string,
		methodName: string,
		streaming: boolean,
	): CallerSchema {
		const schema = this.d.resolveSchema(serviceName, methodName);
		if (!schema) {
			throw new ConfigurationError(
				`rpc: no schema for ${serviceName}/${methodName} — declare it with sb.client() or sb.useSchema() before start()`,
			);
		}
		const descriptor = this.d.instances.descriptorFor(serviceName, methodName);
		if (descriptor && descriptor.streaming !== streaming) {
			throw new ValidationError(
				streaming
					? `rpc: ${serviceName}/${methodName} is not a streaming method — use sb.rpc.call()`
					: `rpc: ${serviceName}/${methodName} is a streaming method — use sb.stream()`,
			);
		}
		return schema;
	}

	// pickCandidate reads the contract-matched candidates and runs P2C.
	private pickCandidate(
		serviceName: string,
		methodName: string,
		callerHash: string,
	): Candidate {
		const all = this.d.instances.candidatesFor(
			serviceName,
			methodName,
			callerHash,
		);
		if (all.length === 0) {
			throw new NoLiveInstanceError(
				`rpc: no live instance of ${serviceName}/${methodName} matches caller contract ${callerHash}`,
			);
		}
		try {
			return this.d.lb.pick(all);
		} catch (err) {
			if (!(err instanceof NoLiveInstanceError)) throw err;
			// "Nothing eligible" has two causes with opposite fixes: nobody
			// advertised an inbound address, or the fleet is circuit-open.
			const advertised = all.some((c) => c.instance.callEndpoint !== "");
			throw new NoLiveInstanceError(
				advertised
					? `rpc: no live instance of ${serviceName}/${methodName} — all candidates circuit-open`
					: `rpc: no endpoint for ${serviceName}/${methodName} — the callee advertises no inbound address`,
			);
		}
	}

	// recordOutcome feeds the breaker. An answer from the instance — including a
	// handler error or a refusal — proves it can serve; only transport-side
	// failures count against it.
	private recordOutcome(candidate: Candidate, failure: CallFailure): void {
		const key = cbKey(candidate.instance);
		if (failure.transport) this.d.cb.recordFailure(key);
		else this.d.cb.recordSuccess(key);
	}
}

function useDirectFor(
	candidate: Candidate,
	transport: "direct" | "proxy" | "auto",
): boolean {
	return transport !== "proxy" && candidate.instance.callEndpoint !== "";
}

function directTarget(candidate: Candidate) {
	return {
		endpoint: candidate.instance.callEndpoint,
		serviceId: candidate.instance.serviceId,
		instanceId: candidate.instance.instanceId,
	};
}

// asFailure normalises whatever a transport threw. An abort by the caller's
// signal is CANCELLED and never retried.
function asFailure(err: unknown, signal?: AbortSignal): CallFailure {
	if (signal?.aborted)
		return new CallFailure(
			new ServiceBridgeError("CANCELLED", "rpc: call cancelled by the caller", {
				cause: err,
			}),
			false,
		);
	if (err instanceof CallFailure) return err;
	if (err instanceof ServiceBridgeError) return new CallFailure(err, false);
	return new CallFailure(
		new ServiceBridgeError(
			"INTERNAL",
			err instanceof Error ? err.message : String(err),
			{ cause: err },
		),
		false,
	);
}

// formatRpcCallSubject mirrors Go telemetry.FormatSubject → "rpc.call:<svc>/<method>".
function formatRpcCallSubject(serviceName: string, methodName: string): string {
	return `rpc.call:${serviceName}/${methodName}`;
}

// rpcCallMeta builds the RPC.CALL meta JSON without an intermediate object:
// it runs on every call.
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

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	signal?.throwIfAborted();
	return new Promise((resolve, reject) => {
		const abort = () => {
			clearTimeout(timer);
			reject(
				new ServiceBridgeError(
					"CANCELLED",
					"rpc: call cancelled by the caller",
				),
			);
		};
		const timer = setTimeout(() => {
			signal?.removeEventListener("abort", abort);
			resolve();
		}, ms);
		signal?.addEventListener("abort", abort, { once: true });
	});
}

// timeoutMs parses "10s" / "500ms" / "2m". Undefined → 30 s default.
export function timeoutMs(s: string | undefined): number {
	if (!s) return DEFAULT_TIMEOUT_MS;
	const m = /^(\d+)(ms|s|m)$/.exec(s.trim());
	if (!m) throw new ConfigurationError(`rpc: invalid timeout ${s}`);
	const n = Number.parseInt(m[1] ?? "0", 10);
	return m[2] === "ms" ? n : m[2] === "s" ? n * 1000 : n * 60_000;
}

// SchemaRegistry is the caller-side mapping (serviceName, methodName) →
// CallerSchema, filled by sb.client() / sb.useSchema().
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
