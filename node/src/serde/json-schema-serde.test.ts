import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import { computeContractHash } from "./contract-hash";
import { buildSchemaPair } from "./serializer";

const simpleFile = join(import.meta.dir, "testdata", "payment.schema.json");
const nestedFile = join(
	import.meta.dir,
	"testdata",
	"payment-nested.schema.json",
);

describe("JsonSchemaSerde — simple schema", () => {
	it("encode/decode round-trip", async () => {
		const pair = await buildSchemaPair({ schemaFile: simpleFile });
		const bytes = pair.input.encode({ userId: "u-1", amount: 42.5 });
		const back = pair.input.decode(bytes) as Record<string, unknown>;
		expect(back.userId).toBe("u-1");
		expect(back.amount).toBe(42.5);
	});

	it("output encode/decode", async () => {
		const pair = await buildSchemaPair({ schemaFile: simpleFile });
		const bytes = pair.output.encode({ transactionId: "tx-1", ok: true });
		const back = pair.output.decode(bytes) as Record<string, unknown>;
		expect(back.transactionId).toBe("tx-1");
		expect(back.ok).toBe(true);
	});

	it("contract hash stable", async () => {
		const a = await buildSchemaPair({ schemaFile: simpleFile });
		const b = await buildSchemaPair({ schemaFile: simpleFile });
		expect(computeContractHash(a)).toBe(computeContractHash(b));
	});
});

describe("JsonSchemaSerde — nested + repeated", () => {
	it("nested object + repeated message round-trip", async () => {
		const pair = await buildSchemaPair({ schemaFile: nestedFile });
		const value = {
			userId: "u-7",
			items: [
				{ sku: "sku-a", qty: 2 },
				{ sku: "sku-b", qty: 5 },
			],
			metadata: { source: "web" },
		};
		const bytes = pair.input.encode(value);
		const back = pair.input.decode(bytes) as Record<string, unknown>;
		expect(back.userId).toBe("u-7");
		expect(back.items).toEqual([
			{ sku: "sku-a", qty: 2 },
			{ sku: "sku-b", qty: 5 },
		]);
		expect(back.metadata).toEqual({ source: "web" });
	});
});

describe("JsonSchemaSerde — error paths", () => {
	it("missing file rejected", async () => {
		await expect(
			buildSchemaPair({ schemaFile: "/no/such/path.schema.json" }),
		).rejects.toThrow(/read schema file/);
	});

	it("missing fieldNumber rejected", async () => {
		const tmp = join("/tmp", `bad-${Date.now()}.schema.json`);
		const { writeFileSync, unlinkSync } = await import("node:fs");
		writeFileSync(
			tmp,
			JSON.stringify({
				input: { M: { x: { type: "string" } } },
				output: { M: { y: { type: "string", fieldNumber: 1 } } },
			}),
		);
		try {
			await expect(buildSchemaPair({ schemaFile: tmp })).rejects.toThrow(
				/missing required fieldNumber/,
			);
		} finally {
			unlinkSync(tmp);
		}
	});

	it("duplicate fieldNumber rejected", async () => {
		const tmp = join("/tmp", `dup-${Date.now()}.schema.json`);
		const { writeFileSync, unlinkSync } = await import("node:fs");
		writeFileSync(
			tmp,
			JSON.stringify({
				input: {
					M: {
						a: { type: "string", fieldNumber: 1 },
						b: { type: "string", fieldNumber: 1 },
					},
				},
				output: { M: { y: { type: "string", fieldNumber: 1 } } },
			}),
		);
		try {
			await expect(buildSchemaPair({ schemaFile: tmp })).rejects.toThrow(
				/duplicate fieldNumber/,
			);
		} finally {
			unlinkSync(tmp);
		}
	});

	it("multiple input messages rejected", async () => {
		const tmp = join("/tmp", `multi-${Date.now()}.schema.json`);
		const { writeFileSync, unlinkSync } = await import("node:fs");
		writeFileSync(
			tmp,
			JSON.stringify({
				input: {
					A: { x: { type: "string", fieldNumber: 1 } },
					B: { y: { type: "string", fieldNumber: 1 } },
				},
				output: { M: { y: { type: "string", fieldNumber: 1 } } },
			}),
		);
		try {
			await expect(buildSchemaPair({ schemaFile: tmp })).rejects.toThrow(
				/exactly one message/,
			);
		} finally {
			unlinkSync(tmp);
		}
	});
});
