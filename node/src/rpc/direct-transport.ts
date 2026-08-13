import type { PeerCertificate } from "node:tls";
import * as grpc from "@grpc/grpc-js";
import { derToPem } from "../connection/pem";
import { SPIFFE_TRUST_DOMAIN } from "../connection/spiffe";
import { CallClient, type StreamChunk } from "../pb/servicebridge/v1/call";
import { currentTraceContext } from "../telemetry/context";
import { formatXSbTrace } from "../telemetry/wire-trace";

// X_SB_TRACE_HEADER is the gRPC metadata key the runtime + callee read for
// trace context propagation (ADR 0006 §3). Keep in sync with rpc/server.go.
const X_SB_TRACE_HEADER = "x-sb-trace";

// currentTraceHeader returns the X-SB-Trace wire value for the active ALS
// trace context, or empty string when no context is in scope. Both the gRPC
// metadata header and the CallRequest.xSbTrace body field must carry it —
// the runtime reads the header (Invoke path), the SDK CallServer reads the
// body field (Direct path).
function currentTraceHeader(): string {
	const ctx = currentTraceContext();
	if (!ctx) return "";
	return formatXSbTrace(ctx.traceId, ctx.parentOpId);
}

// buildTraceMetadata mints a Metadata with x-sb-trace set from the active ALS
// trace context, or an empty Metadata when no context is in scope.
function buildTraceMetadata(header: string): grpc.Metadata {
	const md = new grpc.Metadata();
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

// DirectCredentials are the same DER-encoded creds the SDK obtained from
// Bootstrap.Provision. Caller uses them as client cert; the same CA chain
// is the trust anchor for callee server cert.
export interface DirectCredentials {
	caChainDer: Buffer;
	leafCertDer: Buffer;
	privateKeyDer: Buffer;
	notAfterUnix: bigint;
}

// DirectTarget is the resolved (endpoint, identity) pair used to build a
// SPIFFE-validating gRPC channel.
export interface DirectTarget {
	endpoint: string; // host:port from ServiceInstanceInfo.call_endpoint
	serviceId: string; // expected SPIFFE service id
	instanceId: string; // expected SPIFFE instance id
}

interface CacheEntry {
	client: CallClient;
	expiresAt: number; // ms epoch — entry discarded after this
	lastUsedAt: number; // ms epoch — drives idle eviction
}

// TTL is set to min(caller leaf cert lifetime, configured lead) - 5min so
// the cache is invalidated before the cert that signs it expires.
const TTL_LEAD_MS = 5 * 60 * 1000;

// TTL_FLOOR_MS keeps a channel usable for at least one call burst even when the
// caller cert is already inside the lead window; rotation drops the cache
// anyway via updateCredentials.
const TTL_FLOOR_MS = 60_000;

// IDLE_TTL_MS bounds how long a channel survives without being used. Channel
// lifetime is otherwise capped only by the cert TTL (hours), so a pod that
// leaves the fleet keeps an open connection to a dead peer for that long: the
// registry drops the instance, but nothing tells the transport. The idle sweep
// is what closes it — a retired pod stops receiving calls by definition.
const IDLE_TTL_MS = 5 * 60 * 1000;

// targetKey identifies one cached channel. The expected SPIFFE URI is baked
// into the channel credentials, so the identity is part of the cache key: k8s
// reuses endpoints across pods, and keying by endpoint alone would hand a new
// instance a channel pinned to the previous instance's SPIFFE URI — every
// handshake failing until an error evicted it.
// @internal
export function targetKey(target: DirectTarget): string {
	return `${target.endpoint}|${target.serviceId}|${target.instanceId}`;
}

// expectedSpiffeUri is the SAN URI the callee's leaf cert must carry. Mirrors
// the URI the runtime mints in Bootstrap.Provision.
// @internal
export function expectedSpiffeUri(target: DirectTarget): string {
	return `spiffe://${SPIFFE_TRUST_DOMAIN}/service/${target.serviceId}/instance/${target.instanceId}`;
}

// channelTtlMs is how long a freshly dialled channel may be reused: the caller
// leaf cert lifetime minus the lead, floored so a near-expiry cert still yields
// a usable channel.
// @internal
export function channelTtlMs(notAfterUnix: bigint, nowMs: number): number {
	return Math.max(
		Number(notAfterUnix) * 1000 - nowMs - TTL_LEAD_MS,
		TTL_FLOOR_MS,
	);
}

// DirectTransport maintains per-(endpoint, identity) gRPC clients with mTLS +
// SPIFFE verification of the callee's leaf cert. Endpoint addresses come from
// the registry (ServiceInstanceInfo.call_endpoint).
//
// @internal — см. ./README.md
export class DirectTransport {
	private cache = new Map<string, CacheEntry>();
	private lastSweepAt = Date.now();

	constructor(private creds: DirectCredentials) {}

	// updateCredentials rotates the caller's cert (overlap rotation in
	// service-bridge.ts). All cached connections are dropped — they used the
	// previous cert and will reject after rotation.
	updateCredentials(creds: DirectCredentials): void {
		this.creds = creds;
		this.close();
	}

	close(): void {
		for (const entry of this.cache.values()) {
			entry.client.close();
		}
		this.cache.clear();
	}

	// callUnary opens (or reuses) a direct channel to target.endpoint and issues
	// Call.Unary. Returns the response payload bytes on success.
	// callStream — direct counterpart of ProxyTransport.callStream. Mirrors the
	// AsyncIterable<Uint8Array> contract.
	async *callStream(
		target: DirectTarget,
		method: string,
		payload: Uint8Array,
		callerService: string,
		requestId: string,
		idempotencyKey: string,
		deadlineMs: number,
	): AsyncIterable<Uint8Array> {
		const key = targetKey(target);
		const client = this.clientFor(key, target);
		const traceHeader = currentTraceHeader();
		const stream = client.stream(
			{
				method,
				payload: asBuffer(payload),
				callerService,
				requestId,
				idempotencyKey,
				xSbTrace: traceHeader,
			},
			buildTraceMetadata(traceHeader),
			{ deadline: new Date(Date.now() + deadlineMs) },
		);
		try {
			for await (const chunk of stream as AsyncIterable<StreamChunk>) {
				if (chunk.errorCode) {
					const err = new Error(chunk.errorMessage || chunk.errorCode);
					err.name = chunk.errorCode;
					throw err;
				}
				// The decoder already copied the chunk out of the wire buffer, so
				// the payload is owned and needs no defensive copy.
				yield chunk.payload;
			}
		} catch (err) {
			this.evict(key);
			throw err;
		}
	}

	callUnary(
		target: DirectTarget,
		method: string,
		payload: Uint8Array,
		callerService: string,
		requestId: string,
		idempotencyKey: string,
		deadlineMs: number,
	): Promise<Uint8Array> {
		const key = targetKey(target);
		const client = this.clientFor(key, target);
		const traceHeader = currentTraceHeader();
		return new Promise((resolve, reject) => {
			client.unary(
				{
					method,
					payload: asBuffer(payload),
					callerService,
					requestId,
					idempotencyKey,
					xSbTrace: traceHeader,
				},
				buildTraceMetadata(traceHeader),
				{ deadline: new Date(Date.now() + deadlineMs) },
				(err, resp) => {
					if (err) {
						// Drop the conn if it is permanently bad — next call will redial.
						this.evict(key);
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

	private clientFor(key: string, target: DirectTarget): CallClient {
		const now = Date.now();
		this.maybeSweep(now);
		const cached = this.cache.get(key);
		if (cached && cached.expiresAt > now) {
			cached.lastUsedAt = now;
			return cached.client;
		}
		if (cached) {
			cached.client.close();
			this.cache.delete(key);
		}

		const caChainPem = derToPem(this.creds.caChainDer, "CERTIFICATE");
		const certPem = derToPem(this.creds.leafCertDer, "CERTIFICATE");
		const keyPem = derToPem(this.creds.privateKeyDer, "PRIVATE KEY");

		const channelCreds = grpc.credentials.createSsl(
			caChainPem,
			keyPem,
			certPem,
			{ checkServerIdentity: makeSpiffeCheck(expectedSpiffeUri(target)) },
		);

		// SDK callee advertises host:port — `host` is often an IP (POD_IP), which
		// Node TLS rejects as servername (SNI). Override SNI with a placeholder
		// so the TLS extension carries a valid DNS-like name; SPIFFE verification
		// above is what actually authenticates the peer.
		const client = new CallClient(target.endpoint, channelCreds, {
			"grpc.ssl_target_name_override": "servicebridge.peer",
			"grpc.default_authority": "servicebridge.peer",
		});
		this.cache.set(key, {
			client,
			expiresAt: now + channelTtlMs(this.creds.notAfterUnix, now),
			lastUsedAt: now,
		});
		return client;
	}

	// maybeSweep closes channels nobody has used for IDLE_TTL_MS. It runs at most
	// once per IDLE_TTL_MS so the scan cost stays off the per-call path.
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

// makeSpiffeCheck returns a Node TLS checkServerIdentity callback that asserts
// the peer cert's SAN URI matches `expectedUri`. Without this, gRPC would only
// verify cert chain — different instances signed by the same CA could
// impersonate each other.
// @internal
export function makeSpiffeCheck(
	expectedUri: string,
): (hostname: string, cert: PeerCertificate) => Error | undefined {
	return (_hostname, cert) => {
		const sanUris = extractSanUris(cert);
		if (!sanUris.includes(expectedUri)) {
			return new Error(
				`rpc: SPIFFE mismatch — expected ${expectedUri}, got ${sanUris.join(", ") || "<none>"}`,
			);
		}
		return undefined;
	};
}

// extractSanUris reads URI SANs from a PeerCertificate. node:tls exposes them
// via subjectaltname (string) and infoAccess; we parse subjectaltname for "URI:"
// entries — same shape on Node and Bun.
function extractSanUris(cert: PeerCertificate): string[] {
	const sub = (cert as PeerCertificate & { subjectaltname?: string })
		.subjectaltname;
	if (!sub) return [];
	return sub
		.split(",")
		.map((s) => s.trim())
		.filter((s) => s.startsWith("URI:"))
		.map((s) => s.slice(4));
}
