import { describe, expect, test } from "bun:test";
import { BootstrapKeyPayload } from "../pb/servicebridge/v1/bootstrap";
import { parseBootstrapKey } from "./key";

function buildKey(
	keyID: Uint8Array,
	secret: Uint8Array,
	caDer: Uint8Array,
): string {
	const bytes = BootstrapKeyPayload.encode({
		keyId: Buffer.from(keyID),
		secret: Buffer.from(secret),
		caCertDer: Buffer.from(caDer),
	}).finish();
	return `sb.${Buffer.from(bytes).toString("base64url")}`;
}

const validKeyID = new Uint8Array(8).fill(1);
const validSecret = new Uint8Array(32).fill(2);
const validCA = new Uint8Array(200).fill(3);

describe("parseBootstrapKey", () => {
	test("parses valid key with embedded CA", () => {
		const key = buildKey(validKeyID, validSecret, validCA);
		const parsed = parseBootstrapKey(key);
		expect(parsed.keyID).toEqual(Buffer.from(validKeyID));
		expect(parsed.secret).toEqual(Buffer.from(validSecret));
		expect(parsed.caCertDer).toEqual(Buffer.from(validCA));
	});

	test("rejects missing sb. prefix", () => {
		const bytes = BootstrapKeyPayload.encode({
			keyId: Buffer.from(validKeyID),
			secret: Buffer.from(validSecret),
			caCertDer: Buffer.from(validCA),
		}).finish();
		const raw = Buffer.from(bytes).toString("base64url");
		expect(() => parseBootstrapKey(raw)).toThrow(/prefix/i);
	});

	test("rejects empty key_id field", () => {
		const bytes = BootstrapKeyPayload.encode({
			keyId: Buffer.alloc(0),
			secret: Buffer.from(validSecret),
			caCertDer: Buffer.from(validCA),
		}).finish();
		const raw = `sb.${Buffer.from(bytes).toString("base64url")}`;
		expect(() => parseBootstrapKey(raw)).toThrow(/key_id/i);
	});

	test("rejects empty secret field", () => {
		const bytes = BootstrapKeyPayload.encode({
			keyId: Buffer.from(validKeyID),
			secret: Buffer.alloc(0),
			caCertDer: Buffer.from(validCA),
		}).finish();
		const raw = `sb.${Buffer.from(bytes).toString("base64url")}`;
		expect(() => parseBootstrapKey(raw)).toThrow(/secret/i);
	});

	test("rejects empty ca_cert_der field", () => {
		const bytes = BootstrapKeyPayload.encode({
			keyId: Buffer.from(validKeyID),
			secret: Buffer.from(validSecret),
			caCertDer: Buffer.alloc(0),
		}).finish();
		const raw = `sb.${Buffer.from(bytes).toString("base64url")}`;
		expect(() => parseBootstrapKey(raw)).toThrow(/ca_cert_der/i);
	});

	test("rejects invalid base64url", () => {
		expect(() => parseBootstrapKey("sb.!!!invalid!!!")).toThrow();
	});
});
