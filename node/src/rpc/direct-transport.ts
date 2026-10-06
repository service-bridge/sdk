import type * as grpc from "@grpc/grpc-js";
import { makeSpiffeCheck, SPIFFE_TRUST_DOMAIN } from "../connection/spiffe";
import {
	type CertificateStore,
	CLIENT_CHANNEL_OPTIONS,
} from "../connection/tls-material";
import { CallClient, type StreamChunk } from "../pb/servicebridge/v1/call";
import {
	asBuffer,
	CallFailure,
	currentTraceHeader,
	grpcFailure,
	handlerFailure,
	traceMetadata,
} from "./wire";

// DirectTarget is the resolved (endpoint, identity) pair used to build a
// SPIFFE-validating gRPC channel.
export interface DirectTarget {
	endpoint: string; // host:port from ServiceInstanceInfo.call_endpoint
	serviceId: string; // expected SPIFFE service id
	instanceId: string; // expected SPIFFE instance id
}

/** Everything one call carries besides its target. */
export interface WireCall {
	method: string;
	payload: Uint8Array;
	requestId: string;
	idempotencyKey: string;
	deadline: Date;
	signal?: AbortSignal;
}

// IDLE_TTL_MS bounds how long a channel survives without being used. A pod
// that leaves the fleet stops receiving calls by definition, so the idle sweep
// closes its channel; revocation and registry removals close it at once.
const IDLE_TTL_MS = 5 * 60 * 1000;

interface CacheEntry {
	client: CallClient;
	serviceId: string;
	instanceId: string;
	lastUsedAt: number;
}

// targetKey identifies one cached channel. The expected SPIFFE URI is baked
// into the channel credentials, so the identity is part of the cache key: k8s
// reuses endpoints across pods, and keying by endpoint alone would hand a new
// instance a channel pinned to the previous instance's SPIFFE URI.
// @internal
export function targetKey(target: DirectTarget): string {
	return `${target.endpoint}|${target.serviceId}|${target.instanceId}`;
}

// expectedSpiffeUri is the SAN URI the callee's leaf cert must carry.
// @internal
export function expectedSpiffeUri(target: DirectTarget): string {
	return `spiffe://${SPIFFE_TRUST_DOMAIN}/service/${target.serviceId}/instance/${target.instanceId}`;
}

// DirectTransport maintains one mTLS channel per callee instance, with SPIFFE
// verification of the callee's leaf. Channel credentials follow the shared
// CertificateStore, so a certificate rotation reaches every channel's next
// handshake without closing a single channel.
//
// @internal — см. ./README.md
export class DirectTransport {
	private cache = new Map<string, CacheEntry>();
	private lastSweepAt = Date.now();

	constructor(
		private readonly store: CertificateStore,
		// The caller's service id, resolved per call: identity appears with the
		// first Welcome, after the transport is built.
		private readonly callerService: () => string,
	) {}

	close(): void {
		for (const entry of this.cache.values()) entry.client.close();
		this.cache.clear();
	}

	/**
	 * Closes the channels of instances that left the mesh or were revoked.
	 * `keep` returns false for an instance whose channel must go.
	 */
	retain(keep: (serviceId: string, instanceId: string) => boolean): void {
		for (const [key, entry] of this.cache) {
			if (keep(entry.serviceId, entry.instanceId)) continue;
			entry.client.close();
			this.cache.delete(key);
		}
	}

	async *callStream(
		target: DirectTarget,
		call: WireCall,
	): AsyncIterable<Uint8Array> {
		call.signal?.throwIfAborted();
		const key = targetKey(target);
		const client = this.clientFor(key, target);
		await this.ready(client, key, call);
		const traceHeader = currentTraceHeader();
		const stream = client.stream(
			{
				method: call.method,
				payload: asBuffer(call.payload),
				callerService: this.callerService(),
				requestId: call.requestId,
				idempotencyKey: call.idempotencyKey,
				xSbTrace: traceHeader,
			},
			traceMetadata(traceHeader),
			{ deadline: call.deadline },
		);
		const abort = () => stream.cancel();
		call.signal?.addEventListener("abort", abort, { once: true });
		try {
			for await (const chunk of stream as AsyncIterable<StreamChunk>) {
				if (chunk.errorCode)
					throw handlerFailure(chunk.errorCode, chunk.errorMessage);
				yield chunk.payload;
			}
		} catch (err) {
			if (err instanceof CallFailure) throw err;
			throw grpcFailure(`rpc ${call.method}`, err);
		} finally {
			call.signal?.removeEventListener("abort", abort);
			stream.cancel?.();
		}
	}

	async callUnary(target: DirectTarget, call: WireCall): Promise<Uint8Array> {
		call.signal?.throwIfAborted();
		const key = targetKey(target);
		const client = this.clientFor(key, target);
		await this.ready(client, key, call);
		const traceHeader = currentTraceHeader();
		return new Promise((resolve, reject) => {
			let pending: grpc.ClientUnaryCall | undefined;
			const abort = () => pending?.cancel();
			call.signal?.addEventListener("abort", abort, { once: true });
			pending = client.unary(
				{
					method: call.method,
					payload: asBuffer(call.payload),
					callerService: this.callerService(),
					requestId: call.requestId,
					idempotencyKey: call.idempotencyKey,
					xSbTrace: traceHeader,
				},
				traceMetadata(traceHeader),
				{ deadline: call.deadline },
				(err, resp) => {
					call.signal?.removeEventListener("abort", abort);
					if (err) {
						reject(grpcFailure(`rpc ${call.method}`, err));
						return;
					}
					if (resp.errorCode) {
						reject(handlerFailure(resp.errorCode, resp.errorMessage));
						return;
					}
					resolve(resp.payload);
				},
			);
		});
	}

	// ready waits for the channel to the callee within the call's deadline. A
	// channel that never becomes ready never carried the request — the
	// pre-dispatch proof that lets the caller retry elsewhere or via proxy.
	private ready(
		client: CallClient,
		key: string,
		call: WireCall,
	): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			const abort = () =>
				reject(
					grpcFailure(`rpc ${call.method}`, { code: 1, details: "cancelled" }),
				);
			call.signal?.addEventListener("abort", abort, { once: true });
			client.waitForReady(call.deadline, (err) => {
				call.signal?.removeEventListener("abort", abort);
				if (err) {
					this.evict(key);
					reject(grpcFailure(`rpc ${call.method}: connect`, err, true));
				} else resolve();
			});
		});
	}

	private clientFor(key: string, target: DirectTarget): CallClient {
		const now = Date.now();
		this.maybeSweep(now);
		const cached = this.cache.get(key);
		if (cached) {
			cached.lastUsedAt = now;
			return cached.client;
		}
		// SDK callees advertise host:port with an IP host (POD_IP) that Node TLS
		// rejects as SNI; a placeholder name keeps the extension valid. SPIFFE
		// verification is what authenticates the peer.
		const client = new CallClient(
			target.endpoint,
			this.store.channelCredentials(makeSpiffeCheck(expectedSpiffeUri(target))),
			{
				...CLIENT_CHANNEL_OPTIONS,
				"grpc.ssl_target_name_override": "servicebridge.peer",
				"grpc.default_authority": "servicebridge.peer",
			},
		);
		this.cache.set(key, {
			client,
			serviceId: target.serviceId,
			instanceId: target.instanceId,
			lastUsedAt: now,
		});
		return client;
	}

	// maybeSweep closes channels nobody has used for IDLE_TTL_MS. It runs at most
	// once per IDLE_TTL_MS so the scan stays off the per-call path.
	private maybeSweep(now: number): void {
		if (now - this.lastSweepAt < IDLE_TTL_MS) return;
		this.lastSweepAt = now;
		for (const [key, entry] of this.cache) {
			if (now - entry.lastUsedAt < IDLE_TTL_MS) continue;
			entry.client.close();
			this.cache.delete(key);
		}
	}

	private evict(key: string): void {
		const entry = this.cache.get(key);
		if (entry) {
			entry.client.close();
			this.cache.delete(key);
		}
	}

	// Test-only: inspect cache size.
	cacheSize(): number {
		return this.cache.size;
	}
}
