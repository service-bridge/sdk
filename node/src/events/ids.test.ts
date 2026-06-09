import { describe, expect, it } from "bun:test";
import { uuidv7 } from "./ids";

const UUID_V7_RE =
	/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe("uuidv7", () => {
	it("matches UUID v7 regex", () => {
		const id = uuidv7();
		expect(id).toMatch(UUID_V7_RE);
	});

	it("generates 1000 unique IDs", () => {
		const ids = new Set<string>();
		for (let i = 0; i < 1000; i++) {
			ids.add(uuidv7());
		}
		expect(ids.size).toBe(1000);
	});

	it("lexicographic sort matches insertion order (monotonic)", () => {
		const ids: string[] = [];
		for (let i = 0; i < 100; i++) {
			ids.push(uuidv7());
		}
		const sorted = [...ids].sort();
		expect(sorted).toEqual(ids);
	});

	it("all IDs have version nibble 7", () => {
		for (let i = 0; i < 20; i++) {
			const id = uuidv7();
			expect(id.split("-")[2]![0]).toBe("7");
		}
	});

	it("all IDs have correct variant bits (8, 9, a, or b)", () => {
		for (let i = 0; i < 20; i++) {
			const id = uuidv7();
			const variantNibble = id.split("-")[3]![0]!;
			expect(["8", "9", "a", "b"].includes(variantNibble)).toBe(true);
		}
	});
});
