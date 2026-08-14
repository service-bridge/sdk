import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import { computeContractHash } from "./contract-hash";
import { buildSchemaPair } from "./serializer";

const protoFile = join(import.meta.dir, "testdata", "payment.proto");
const formattedFile = join(
	import.meta.dir,
	"testdata",
	"payment_formatted.proto",
);

describe("buildSchemaPair", () => {
	it("encode/decode round-trip preserves values", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		const value = { userId: "u-1", amount: 42.5 };
		const bytes = pair.input.encode(value);
		expect(bytes).toBeInstanceOf(Uint8Array);
		expect(bytes.length).toBeGreaterThan(0);

		const decoded = pair.input.decode(bytes) as Record<string, unknown>;
		expect(decoded.userId).toBe("u-1");
		expect(decoded.amount).toBe(42.5);
	});

	it("output serializer encodes/decodes ChargeResponse", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		const out = { transactionId: "tx-99", ok: true };
		const bytes = pair.output.encode(out);
		const decoded = pair.output.decode(bytes) as Record<string, unknown>;
		expect(decoded.transactionId).toBe("tx-99");
		expect(decoded.ok).toBe(true);
	});

	it("contract hash stable across formatting differences", async () => {
		const a = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const b = await buildSchemaPair({
			protoFile: formattedFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		expect(computeContractHash(a)).toBe(computeContractHash(b));
	});

	it("toJsonSchema returns descriptor", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const schema = pair.input.toJsonSchema();
		expect(schema).toHaveProperty("fields");
	});

	it("rejects unknown proto file", async () => {
		await expect(
			buildSchemaPair({
				protoFile: "/nonexistent/missing.proto",
				input: "X",
				output: "Y",
			}),
		).rejects.toThrow(/load proto file/);
	});

	it("rejects unknown message name", async () => {
		await expect(
			buildSchemaPair({
				protoFile,
				input: "DoesNotExist",
				output: "ChargeResponse",
			}),
		).rejects.toThrow(/DoesNotExist/);
	});

	it("rejects bad input on encode", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		expect(() => pair.input.encode({ userId: 42 })).toThrow();
	});
});

describe("64-bit fields", () => {
	it("decode to numbers, not to Long objects", async () => {
		const pair = await buildSchemaPair({
			protoFile: `${import.meta.dir}/testdata/wide-ints.proto`,
			input: "WideRequest",
			output: "WideResponse",
		});

		const bytes = pair.input.encode({ amountMs: 1_700_000_000_000, counter: 42 });
		const back = pair.input.decode(bytes) as {
			amountMs: unknown;
			counter: unknown;
		};

		// Without this the value arrives as {low, high, unsigned}: arithmetic on
		// it silently yields NaN, and a Go peer sending the same field reads back
		// a plain number.
		expect(typeof back.amountMs).toBe("number");
		expect(back.amountMs).toBe(1_700_000_000_000);
		expect(typeof back.counter).toBe("number");
		expect(back.counter).toBe(42);
	});
});
