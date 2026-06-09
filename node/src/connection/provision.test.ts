import { describe, expect, test } from "bun:test";
import * as x509 from "@peculiar/x509";
import {
	buildPinnedCredentials,
	generateKeypairAndCSR,
	newBootstrapClient,
	parseURL,
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
