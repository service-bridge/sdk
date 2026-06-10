import "reflect-metadata";
import { describe, expect, test } from "bun:test";
import * as x509 from "@peculiar/x509";
import type { BootstrapClient } from "../pb/servicebridge/v1/bootstrap";
import { BootstrapKeyPayload } from "../pb/servicebridge/v1/bootstrap";
import type { BootstrapKey } from "./key";
import {
	buildPinnedCredentials,
	generateKeypairAndCSR,
	newBootstrapClient,
	parseURL,
	provision,
} from "./provision";

describe("generateKeypairAndCSR", () => {
	test("returns keypair and valid DER CSR", async () => {
		const { privateKey, csrDer } = await generateKeypairAndCSR();
		expect(privateKey).toBeDefined();
		expect(csrDer).toBeInstanceOf(Uint8Array);
		expect(csrDer.length).toBeGreaterThan(0);
	});

	test("produces unique keys on each call", async () => {
		const a = await generateKeypairAndCSR();
		const b = await generateKeypairAndCSR();
		const pubA = await crypto.subtle.exportKey("spki", a.publicKey);
		const pubB = await crypto.subtle.exportKey("spki", b.publicKey);
		expect(Buffer.from(pubA).toString("hex")).not.toBe(
			Buffer.from(pubB).toString("hex"),
		);
	});
});

describe("parseURL", () => {
	test("parses host:port", () => {
		expect(parseURL("localhost:14445")).toEqual({
			host: "localhost",
			port: 14445,
		});
	});
	test("rejects missing port", () => {
		expect(() => parseURL("localhost")).toThrow();
	});
	test("rejects invalid port", () => {
		expect(() => parseURL("localhost:99999")).toThrow();
		expect(() => parseURL("localhost:abc")).toThrow();
	});
});

describe("buildPinnedCredentials / newBootstrapClient", () => {
	test("returns secure ChannelCredentials with real CA cert", async () => {
		const caDer = await mintSelfSignedCertDER();
		const creds = buildPinnedCredentials(caDer);
		expect(creds).toBeDefined();
		expect(creds._isSecure()).toBe(true);
	});

	test("newBootstrapClient constructs without throwing", async () => {
		const caDer = await mintSelfSignedCertDER();
		const client = newBootstrapClient("localhost:14445", caDer);
		expect(client).toBeDefined();
		client.close();
	});
});

describe("provision channel lifecycle", () => {
	function fakeKey(): BootstrapKey {
		return {
			keyID: Buffer.alloc(8, 0x01),
			secret: Buffer.alloc(32, 0x02),
			caCertDer: Buffer.alloc(1, 0xff),
		};
	}

	// makeCountingFactory returns a bootstrap-client factory that records how
	// many clients were created vs closed, and lets each client's provision()
	// resolve or reject deterministically.
	function makeCountingFactory(mode: "ok" | "fail") {
		const counts = { created: 0, closed: 0 };
		const factory = (_url: string, _caCertDer: Buffer): BootstrapClient => {
			counts.created++;
			return {
				provision: (
					_req: unknown,
					cb: (err: Error | null, resp: unknown) => void,
				) => {
					if (mode === "fail") {
						cb(new Error("provision boom"), null);
						return;
					}
					cb(null, {
						certDer: Buffer.alloc(1),
						caChainDer: Buffer.alloc(1),
						serviceId: "svc",
						serviceName: "svc-name",
						instanceId: "inst",
						notAfterUnix: Math.floor(Date.now() / 1000) + 3600,
					});
				},
				close: () => {
					counts.closed++;
				},
			} as unknown as BootstrapClient;
		};
		return { counts, factory };
	}

	test("closes the bootstrap channel on success", async () => {
		const { counts, factory } = makeCountingFactory("ok");
		await provision("localhost:14445", fakeKey(), factory);
		expect(counts.created).toBe(1);
		expect(counts.closed).toBe(1);
	});

	test("closes the bootstrap channel on failure (no leak in reconnect loop)", async () => {
		const { counts, factory } = makeCountingFactory("fail");
		const attempts = 5;
		for (let i = 0; i < attempts; i++) {
			await expect(
				provision("localhost:14445", fakeKey(), factory),
			).rejects.toThrow();
		}
		expect(counts.created).toBe(attempts);
		expect(counts.closed).toBe(attempts);
	});

	// Guard the proto round-trip so the test key shape stays valid if the
	// BootstrapKeyPayload contract changes.
	test("BootstrapKeyPayload encodes the fake key", () => {
		const bytes = BootstrapKeyPayload.encode({
			keyId: fakeKey().keyID,
			secret: fakeKey().secret,
			caCertDer: fakeKey().caCertDer,
		}).finish();
		expect(bytes.length).toBeGreaterThan(0);
	});
});

// ── helpers ─────────────────────────────────────────────────────────────────

async function mintSelfSignedCertDER(): Promise<Buffer> {
	const keyPair = await crypto.subtle.generateKey(
		{ name: "ECDSA", namedCurve: "P-256" },
		true,
		["sign", "verify"],
	);
	const cert = await x509.X509CertificateGenerator.createSelfSigned({
		name: "CN=test-ca",
		notBefore: new Date(),
		notAfter: new Date(Date.now() + 3600_000),
		signingAlgorithm: { name: "ECDSA", hash: "SHA-256" },
		keys: keyPair,
	});
	return Buffer.from(cert.rawData);
}
