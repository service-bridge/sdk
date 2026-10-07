import { describe, expect, it } from "bun:test";
import type { PeerCertificate } from "node:tls";
import { testTls } from "../../tests/helpers/tls";
import { makeSpiffeCheck, SPIFFE_TRUST_DOMAIN } from "../connection/spiffe";
import { CertificateStore } from "../connection/tls-material";
import { ServiceBridgeError } from "../errors";
import {
	type DirectTarget,
	DirectTransport,
	expectedSpiffeUri,
	targetKey,
	type WireCall,
} from "./direct-transport";
import { CallFailure } from "./wire";

function mkTarget(overrides: Partial<DirectTarget> = {}): DirectTarget {
	return {
		endpoint: "10.0.0.7:14446",
		serviceId: "svc-uuid",
		instanceId: "inst-uuid",
		...overrides,
	};
}

function mkTransport(): DirectTransport {
	const store = new CertificateStore({
		caChainDer: testTls.certDer,
		certDer: testTls.certDer,
		privateKeyDer: testTls.privateKeyDer,
	});
	return new DirectTransport(store, () => "caller");
}

function mkCall(timeoutMs: number): WireCall {
	return {
		method: "M",
		payload: new Uint8Array(),
		requestId: "r",
		idempotencyKey: "",
		deadline: new Date(Date.now() + timeoutMs),
	};
}

function mkPeerCert(subjectaltname?: string): PeerCertificate {
	return { subjectaltname } as unknown as PeerCertificate;
}

// dial forces a channel into the cache. The RPC itself never completes — these
// targets have no server — and the cache assertions run synchronously before
// the call settles, so the rejection is swallowed on purpose.
function dial(transport: DirectTransport, target: DirectTarget): void {
	void transport.callUnary(target, mkCall(20)).catch(() => {});
}

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

describe("channel cache keying", () => {
	it("keys by endpoint and peer identity together", () => {
		const base = mkTarget();
		expect(targetKey(base)).toBe("10.0.0.7:14446|svc-uuid|inst-uuid");
		expect(targetKey(mkTarget({ instanceId: "other" }))).not.toBe(
			targetKey(base),
		);
	});

	it("reuses one channel per target", () => {
		const t = mkTransport();
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
		const t = mkTransport();
		try {
			dial(t, mkTarget());
			dial(t, mkTarget({ instanceId: "inst-new" }));
			expect(t.cacheSize()).toBe(2);
		} finally {
			t.close();
		}
	});

	it("retain closes the channels of instances that are gone", () => {
		const t = mkTransport();
		try {
			dial(t, mkTarget());
			dial(t, mkTarget({ instanceId: "inst-2" }));
			expect(t.cacheSize()).toBe(2);
			t.retain((_s, instanceId) => instanceId !== "inst-2");
			expect(t.cacheSize()).toBe(1);
		} finally {
			t.close();
		}
	});

	it("a channel that never connects is a pre-dispatch CONNECTION failure and is evicted", async () => {
		// Port 1 refuses immediately: the request is never written, which is
		// what lets the caller retry elsewhere.
		const t = mkTransport();
		try {
			const err = await t
				.callUnary(mkTarget({ endpoint: "127.0.0.1:1" }), mkCall(300))
				.catch((e) => e);
			expect(err).toBeInstanceOf(CallFailure);
			expect((err as CallFailure).preDispatch).toBe(true);
			expect((err as CallFailure).error).toBeInstanceOf(ServiceBridgeError);
			expect((err as CallFailure).error.code).toBe("CONNECTION");
			expect(t.cacheSize()).toBe(0);
		} finally {
			t.close();
		}
	});
});
