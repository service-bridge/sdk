import { describe, expect, it } from "bun:test";
import { canonicalize, fingerprint } from "./canonical";

describe("canonicalize", () => {
	it("sorts object keys lexicographically", () => {
		expect(canonicalize({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
	});

	it("preserves array order", () => {
		expect(canonicalize([3, 1, 2])).toBe("[3,1,2]");
	});

	it("strips undefined and functions from objects", () => {
		expect(canonicalize({ a: 1, b: undefined, c: () => 0 })).toBe('{"a":1}');
	});

	it("nested objects sorted recursively", () => {
		expect(canonicalize({ z: { y: 1, x: 2 }, a: 1 })).toBe(
			'{"a":1,"z":{"x":2,"y":1}}',
		);
	});

	it("byte-stable on permuted equivalent inputs", () => {
		const a = canonicalize({ steps: [{ id: "x", type: "call" }], retry: 1 });
		const b = canonicalize({ retry: 1, steps: [{ type: "call", id: "x" }] });
		expect(a).toBe(b);
	});

	it("rejects non-finite numbers", () => {
		expect(() => canonicalize(Number.NaN)).toThrow();
		expect(() => canonicalize(Number.POSITIVE_INFINITY)).toThrow();
	});

	it("handles null, true, false, string", () => {
		expect(canonicalize(null)).toBe("null");
		expect(canonicalize(true)).toBe("true");
		expect(canonicalize("hi")).toBe('"hi"');
	});
});

describe("fingerprint", () => {
	it("64-char hex sha256", () => {
		const fp = fingerprint({ a: 1, b: 2 });
		expect(fp).toMatch(/^[0-9a-f]{64}$/);
	});

	it("equal canonical → equal fingerprint", () => {
		expect(fingerprint({ a: 1, b: 2 })).toBe(fingerprint({ b: 2, a: 1 }));
	});

	it("different shape → different fingerprint", () => {
		expect(fingerprint({ a: 1 })).not.toBe(fingerprint({ a: 2 }));
	});
});
