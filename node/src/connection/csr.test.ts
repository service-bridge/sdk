import { describe, expect, it } from "bun:test";
import { execFileSync } from "node:child_process";
import { createPublicKey } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildCsr } from "./csr";

describe("buildCsr", () => {
	it("produces a PKCS#10 request openssl verifies, with the requested CN and key", async () => {
		const keys = (await crypto.subtle.generateKey(
			{ name: "ECDSA", namedCurve: "P-256" },
			true,
			["sign", "verify"],
		)) as CryptoKeyPair;
		const dir = mkdtempSync(join(tmpdir(), "sb-csr-"));
		try {
			const csr = join(dir, "req.der");
			// Several rounds: the r/s integers need a leading zero half the time.
			for (let i = 0; i < 8; i++) {
				writeFileSync(csr, await buildCsr(keys, "unit-test"));
				const out = execFileSync(
					"openssl",
					[
						"req",
						"-inform",
						"DER",
						"-in",
						csr,
						"-verify",
						"-noout",
						"-subject",
						// One subject format for LibreSSL and OpenSSL 3 (which spaces "CN = x" by default).
						"-nameopt",
						"RFC2253",
					],
					{ stdio: ["ignore", "pipe", "pipe"] },
				).toString();
				expect(out).toContain("CN=unit-test");
			}
			// The request carries exactly our public key.
			const pubPem = execFileSync(
				"openssl",
				["req", "-inform", "DER", "-in", csr, "-pubkey", "-noout"],
				{ stdio: ["ignore", "pipe", "pipe"] },
			).toString();
			const spki = Buffer.from(
				await crypto.subtle.exportKey("spki", keys.publicKey),
			);
			expect(
				createPublicKey(pubPem).export({ format: "der", type: "spki" }),
			).toEqual(spki);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
