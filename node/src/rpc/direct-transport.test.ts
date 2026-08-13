// reflect-metadata must be loaded before anything pulls in @peculiar/x509 via
// the rpc module graph — tsyringe throws without the polyfill. Same import
// order as acceptance.ts and connection/provision.ts.
import "reflect-metadata";

import { describe, expect, it } from "bun:test";
import type { PeerCertificate } from "node:tls";
import { SPIFFE_TRUST_DOMAIN } from "../connection/spiffe";
import {
	channelTtlMs,
	type DirectCredentials,
	type DirectTarget,
	DirectTransport,
	expectedSpiffeUri,
	makeSpiffeCheck,
	targetKey,
} from "./direct-transport";

function mkTarget(overrides: Partial<DirectTarget> = {}): DirectTarget {
	return {
		endpoint: "10.0.0.7:14446",
		serviceId: "svc-uuid",
		instanceId: "inst-uuid",
		...overrides,
	};
}

// DER content is never parsed by DirectTransport — derToPem base64-wraps it and
// grpc.credentials.createSsl stores the buffers without validating them. A
// fixed byte string is enough to exercise cache + TTL behaviour offline.
function mkCreds(notAfterUnix: bigint): DirectCredentials {
	const der = Buffer.from("der");
	return {
		caChainDer: der,
		leafCertDer: der,
		privateKeyDer: der,
		notAfterUnix,
	};
}

function mkPeerCert(subjectaltname?: string): PeerCertificate {
	return { subjectaltname } as unknown as PeerCertificate;
}

// dial forces a channel into the cache. The RPC itself never completes — these
// targets have no server — and the cache assertions run synchronously before
// the call settles, so the rejection is swallowed on purpose.
function dial(transport: DirectTransport, target: DirectTarget): void {
	void transport
		.callUnary(target, "M", new Uint8Array(), "caller", "r", "", 20)
		.catch(() => {});
}

const FAR_FUTURE = BigInt(Math.floor(Date.now() / 1000) + 24 * 3600);

describe("SPIFFE pinning", () => {
	it("builds the expected SAN URI from the target identity", () => {
		expect(expectedSpiffeUri(mkTarget())).toBe(
			`spiffe://${SPIFFE_TRUST_DOMAIN}/service/svc-uuid/instance/inst-uuid`,
		);
	});

	it("accepts a peer cert carrying the expected URI SAN", () => {
		const expected = expectedSpiffeUri(mkTarget());
		const check = makeSpiffeCheck(expected);
		expect(
			check("ignored", mkPeerCert(`DNS:whatever, URI:${expected}`)),
		).toBeUndefined();
	});

	it("rejects a peer cert whose URI SAN belongs to another instance", () => {
		const check = makeSpiffeCheck(expectedSpiffeUri(mkTarget()));
		const impostor = expectedSpiffeUri(mkTarget({ instanceId: "other-inst" }));
		const err = check("ignored", mkPeerCert(`URI:${impostor}`));
		expect(err).toBeInstanceOf(Error);
		expect(err?.message).toContain("SPIFFE mismatch");
		expect(err?.message).toContain(impostor);
	});

	it("rejects a peer cert whose URI SAN belongs to another service", () => {
		const check = makeSpiffeCheck(expectedSpiffeUri(mkTarget()));
		const impostor = expectedSpiffeUri(mkTarget({ serviceId: "other-svc" }));
		expect(check("ignored", mkPeerCert(`URI:${impostor}`))).toBeInstanceOf(
			Error,
		);
	});

	it("rejects a peer cert with no URI SAN at all", () => {
		const check = makeSpiffeCheck(expectedSpiffeUri(mkTarget()));
		expect(check("ignored", mkPeerCert("DNS:example.com"))).toBeInstanceOf(
			Error,
		);
		expect(check("ignored", mkPeerCert(undefined))).toBeInstanceOf(Error);
	});
});

describe("channel TTL", () => {
	it("is the caller cert lifetime minus the 5-minute lead", () => {
		const now = 1_000_000_000_000;
		const notAfterUnix = BigInt(now / 1000 + 3600); // 1h of cert left
		expect(channelTtlMs(notAfterUnix, now)).toBe(3600_000 - 5 * 60_000);
	});

	it("floors at 60s when the cert is already inside the lead window", () => {
		const now = 1_000_000_000_000;
		const notAfterUnix = BigInt(now / 1000 + 60); // 1min of cert left
		expect(channelTtlMs(notAfterUnix, now)).toBe(60_000);
	});

	it("floors at 60s for an already-expired cert", () => {
		const now = 1_000_000_000_000;
		expect(channelTtlMs(BigInt(now / 1000 - 3600), now)).toBe(60_000);
	});
});

describe("channel cache keying", () => {
	it("keys by endpoint and peer identity together", () => {
		const base = mkTarget();
		expect(targetKey(base)).toBe("10.0.0.7:14446|svc-uuid|inst-uuid");
		expect(targetKey(mkTarget({ instanceId: "other" }))).not.toBe(
			targetKey(base),
		);
	});

	it("reuses one channel per target", () => {
		const t = new DirectTransport(mkCreds(FAR_FUTURE));
		try {
			dial(t, mkTarget());
			dial(t, mkTarget());
			expect(t.cacheSize()).toBe(1);
		} finally {
			t.close();
		}
	});

	it("does not hand a recycled endpoint the previous instance's channel", () => {
		// k8s reuses pod IPs. The expected SPIFFE URI is baked into the channel
		// credentials, so an endpoint-only cache key would pin the new instance to
		// the retired instance's identity and fail every handshake.
		const t = new DirectTransport(mkCreds(FAR_FUTURE));
		try {
			dial(t, mkTarget());
			dial(t, mkTarget({ instanceId: "inst-new" }));
			expect(t.cacheSize()).toBe(2);
		} finally {
			t.close();
		}
	});

	it("updateCredentials drops every cached channel", () => {
		const t = new DirectTransport(mkCreds(FAR_FUTURE));
		try {
			dial(t, mkTarget());
			dial(t, mkTarget({ instanceId: "inst-2" }));
			expect(t.cacheSize()).toBe(2);

			t.updateCredentials(mkCreds(FAR_FUTURE));
			expect(t.cacheSize()).toBe(0);
		} finally {
			t.close();
		}
	});

	it("evicts the target when the transport call fails", async () => {
		// Port 1 refuses immediately — the gRPC error path must drop the channel
		// so the next call redials instead of reusing a broken one.
		const t = new DirectTransport(mkCreds(FAR_FUTURE));
		try {
			await expect(
				t.callUnary(
					mkTarget({ endpoint: "127.0.0.1:1" }),
					"M",
					new Uint8Array(),
					"caller",
					"r1",
					"",
					1_000,
				),
			).rejects.toThrow();
			expect(t.cacheSize()).toBe(0);
		} finally {
			t.close();
		}
	});
});
