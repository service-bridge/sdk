import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import { SchemaPairCache } from "./type-cache";

const protoFile = join(import.meta.dir, "testdata", "payment.proto");

describe("SchemaPairCache", () => {
	it("returns same SchemaPair on repeated get with identical spec", async () => {
		const cache = new SchemaPairCache();
		const spec = {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		};

		const a = await cache.get(spec);
		const b = await cache.get(spec);

		expect(a).toBe(b);
		expect(cache.size()).toBe(1);
	});

	it("deduplicates concurrent loads of the same spec", async () => {
		const cache = new SchemaPairCache();
		const spec = {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		};

		const [a, b, c] = await Promise.all([
			cache.get(spec),
			cache.get(spec),
			cache.get(spec),
		]);
		expect(a).toBe(b);
		expect(b).toBe(c);
		expect(cache.size()).toBe(1);
	});

	it("different specs produce different entries", async () => {
		const cache = new SchemaPairCache();
		await cache.get({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		await cache.get({
			protoFile,
			input: "ChargeResponse",
			output: "ChargeRequest",
		});
		expect(cache.size()).toBe(2);
	});

	it("clear() empties cache", async () => {
		const cache = new SchemaPairCache();
		await cache.get({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		cache.clear();
		expect(cache.size()).toBe(0);
	});
});
