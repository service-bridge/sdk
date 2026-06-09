import { describe, expect, it } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildSchemaPair } from "./serializer";

const protoServiceFile = join(
	import.meta.dir,
	"testdata",
	"payment-service.proto",
);
const protoNoServiceFile = join(import.meta.dir, "testdata", "payment.proto");

describe("ProtoFileSpec auto-resolve", () => {
	it("service definition: resolves request/response from rpc <method>(...)", async () => {
		const pair = await buildSchemaPair({
			protoFile: protoServiceFile,
			method: "Charge",
		});
		const bytes = pair.input.encode({ userId: "u", amount: 1 });
		const back = pair.input.decode(bytes) as Record<string, unknown>;
		expect(back.userId).toBe("u");
	});

	it("service definition: different methods resolve to different types", async () => {
		const charge = await buildSchemaPair({
			protoFile: protoServiceFile,
			method: "Charge",
		});
		const refund = await buildSchemaPair({
			protoFile: protoServiceFile,
			method: "Refund",
		});
		// Different request shapes → different contract hashes.
		const { computeContractHash } = await import("./contract-hash");
		expect(computeContractHash(charge)).not.toBe(computeContractHash(refund));
	});

	it("no service block: explicit input/output required", async () => {
		// payment.proto has no service definition — convention fallback removed
		// (#32). Caller must pass `input` and `output` explicitly.
		await expect(
			buildSchemaPair({ protoFile: protoNoServiceFile, method: "Charge" }),
		).rejects.toThrow(/cannot resolve input\/output/);

		const pair = await buildSchemaPair({
			protoFile: protoNoServiceFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const bytes = pair.input.encode({ userId: "u", amount: 1 });
		expect(bytes.length).toBeGreaterThan(0);
	});

	it("explicit input/output overrides auto-resolve", async () => {
		const pair = await buildSchemaPair({
			protoFile: protoServiceFile,
			input: "RefundRequest", // intentionally cross-wire
			output: "ChargeResponse",
		});
		const bytes = pair.input.encode({ transactionId: "tx-1" });
		const back = pair.input.decode(bytes) as Record<string, unknown>;
		expect(back.transactionId).toBe("tx-1");
	});

	it("missing method in service: clear error", async () => {
		await expect(
			buildSchemaPair({
				protoFile: protoServiceFile,
				method: "Nonexistent",
			}),
		).rejects.toThrow(/cannot resolve input\/output/);
	});

	it("no service block + non-convention names: error", async () => {
		const dir = mkdtempSync(join(tmpdir(), "sb-resolve-"));
		const p = join(dir, "weird.proto");
		writeFileSync(
			p,
			`syntax = "proto3";
message Foo { string x = 1; }
message Bar { string y = 1; }
`,
		);
		await expect(
			buildSchemaPair({ protoFile: p, method: "Charge" }),
		).rejects.toThrow(/cannot resolve input\/output/);
	});
});
