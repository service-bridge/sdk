import { describe, expect, it } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { extractServiceMethods } from "./typed-client";

const protoServiceFile = join(
	import.meta.dir,
	"..",
	"serde",
	"testdata",
	"payment-service.proto",
);

const protoNoServiceFile = join(
	import.meta.dir,
	"..",
	"serde",
	"testdata",
	"payment.proto",
);

describe("extractServiceMethods", () => {
	it("lists all methods from a service block", async () => {
		const methods = await extractServiceMethods(protoServiceFile);
		const names = methods.map((m) => m.name).sort();
		expect(names).toEqual(["Charge", "Refund"]);
	});

	it("captures request/response types", async () => {
		const methods = await extractServiceMethods(protoServiceFile);
		const charge = methods.find((m) => m.name === "Charge");
		expect(charge?.requestType).toBe("ChargeRequest");
		expect(charge?.responseType).toBe("ChargeResponse");
		expect(charge?.responseStream).toBe(false);
	});

	it("marks streaming methods via responseStream", async () => {
		const dir = mkdtempSync(join(tmpdir(), "sb-tc-"));
		const p = join(dir, "ai.proto");
		writeFileSync(
			p,
			`syntax = "proto3";
service AIService {
  rpc Generate(GenRequest) returns (stream GenChunk);
  rpc Embed(EmbedRequest) returns (EmbedResponse);
}
message GenRequest { string prompt = 1; }
message GenChunk { string token = 1; }
message EmbedRequest { string text = 1; }
message EmbedResponse { repeated double vec = 1; }
`,
		);
		const methods = await extractServiceMethods(p);
		const gen = methods.find((m) => m.name === "Generate");
		const embed = methods.find((m) => m.name === "Embed");
		expect(gen?.responseStream).toBe(true);
		expect(embed?.responseStream).toBe(false);
	});

	it("throws when proto has no service block", async () => {
		await expect(extractServiceMethods(protoNoServiceFile)).rejects.toThrow(
			/no service block/,
		);
	});

	it("throws when file cannot be loaded", async () => {
		await expect(
			extractServiceMethods("/nonexistent/missing.proto"),
		).rejects.toThrow(/load proto/);
	});
});
