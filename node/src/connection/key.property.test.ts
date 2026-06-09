import { describe, expect, test } from "bun:test";
import fc from "fast-check";
import { BootstrapKeyPayload } from "../pb/servicebridge/v1/bootstrap";
import { parseBootstrapKey } from "./key";

const KEY_ID_LEN = 8;
const SECRET_LEN = 32;

describe("parseBootstrapKey (property-based)", () => {
	test("round-trip: any variable-length CA survives encode→parse", () => {
		fc.assert(
			fc.property(
				fc.uint8Array({ minLength: 1, maxLength: 1000 }),
				(caBytes) => {
					const keyID = new Uint8Array(KEY_ID_LEN).fill(0xaa);
					const secret = new Uint8Array(SECRET_LEN).fill(0xbb);
					const bytes = BootstrapKeyPayload.encode({
						keyId: Buffer.from(keyID),
						secret: Buffer.from(secret),
						caCertDer: Buffer.from(caBytes),
					}).finish();
					const raw = `sb.${Buffer.from(bytes).toString("base64url")}`;
					const parsed = parseBootstrapKey(raw);
					expect(parsed.keyID.equals(Buffer.from(keyID))).toBe(true);
					expect(parsed.secret.equals(Buffer.from(secret))).toBe(true);
					expect(parsed.caCertDer.equals(Buffer.from(caBytes))).toBe(true);
				},
			),
			{ numRuns: 100 },
		);
	});

	test("rejects any input without sb. prefix", () => {
		fc.assert(
			fc.property(
				fc
					.string({ minLength: 1, maxLength: 200 })
					.filter((s) => !s.startsWith("sb.")),
				(input) => {
					expect(() => parseBootstrapKey(input)).toThrow();
				},
			),
			{ numRuns: 100 },
		);
	});
});
