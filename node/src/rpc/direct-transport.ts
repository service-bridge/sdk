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
}

// TTL is set to min(caller leaf cert lifetime, configured lead) - 5min so
// the cache is invalidated before the cert that signs it expires.
const TTL_LEAD_MS = 5 * 60 * 1000;

// DirectTransport maintains per-endpoint gRPC clients with mTLS + SPIFFE
// verification of the callee's leaf cert. Endpoint addresses come from the
// registry (ServiceInstanceInfo.call_endpoint).
//
// @internal — см. ./README.md
export class DirectTransport {
	private cache = new Map<string, CacheEntry>();

	constructor(private creds: DirectCredentials) {}

	// updateCredentials rotates the caller's cert (overlap rotation in
	// service-bridge.ts). All cached connections are dropped — they used the
	// previous cert and will reject after rotation.
	updateCredentials(creds: DirectCredentials): void {
		this.creds = creds;
		for (const [endpoint, entry] of this.cache) {
			entry.client.close();
			this.cache.delete(endpoint);
		}
	}

	close(): void {
		for (const [endpoint, entry] of this.cache) {
			entry.client.close();
			this.cache.delete(endpoint);
		}
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
		const client = this.clientFor(target);
		const traceHeader = currentTraceHeader();
		const stream = client.stream(
			{
				method,
				payload: Buffer.from(payload),
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
				yield new Uint8Array(chunk.payload);
			}
		} catch (err) {
			this.evict(target.endpoint);
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
		const client = this.clientFor(target);
		const traceHeader = currentTraceHeader();
		return new Promise((resolve, reject) => {
			client.unary(
				{
					method,
					payload: Buffer.from(payload),
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
						this.evict(target.endpoint);
						reject(err);
						return;
					}
					if (resp.errorCode) {
						const appErr = new Error(resp.errorMessage || resp.errorCode);
						appErr.name = resp.errorCode;
						reject(appErr);
						return;
					}
					resolve(new Uint8Array(resp.payload));
				},
			);
		});
	}

	private clientFor(target: DirectTarget): CallClient {
		const cached = this.cache.get(target.endpoint);
		if (cached && cached.expiresAt > Date.now()) {
			return cached.client;
		}
		if (cached) {
			cached.client.close();
			this.cache.delete(target.endpoint);
		}

		const caChainPem = derToPem(this.creds.caChainDer, "CERTIFICATE");
		const certPem = derToPem(this.creds.leafCertDer, "CERTIFICATE");
		const keyPem = derToPem(this.creds.privateKeyDer, "PRIVATE KEY");

		const expectedSpiffe = `spiffe://${SPIFFE_TRUST_DOMAIN}/service/${target.serviceId}/instance/${target.instanceId}`;

		const channelCreds = grpc.credentials.createSsl(
			caChainPem,
			keyPem,
			certPem,
			{ checkServerIdentity: makeSpiffeCheck(expectedSpiffe) },
		);

		// SDK callee advertises host:port — `host` is often an IP (POD_IP), which
		// Node TLS rejects as servername (SNI). Override SNI with a placeholder
		// so the TLS extension carries a valid DNS-like name; SPIFFE verification
		// above is what actually authenticates the peer.
		const client = new CallClient(target.endpoint, channelCreds, {
			"grpc.ssl_target_name_override": "servicebridge.peer",
			"grpc.default_authority": "servicebridge.peer",
		});
		const ttlMs =
			Number(this.creds.notAfterUnix) * 1000 - Date.now() - TTL_LEAD_MS;
		const expiresAt = Date.now() + Math.max(ttlMs, 60_000);
		this.cache.set(target.endpoint, { client, expiresAt });
		return client;
	}

	private evict(endpoint: string): void {
		const entry = this.cache.get(endpoint);
		if (entry) {
			entry.client.close();
			this.cache.delete(endpoint);
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
function makeSpiffeCheck(
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
