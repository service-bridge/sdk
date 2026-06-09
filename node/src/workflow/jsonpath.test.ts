import { describe, expect, it } from "bun:test";
import {
	evalLiteralOrPath,
	evalPath,
	evalPredicate,
	JsonPathError,
} from "./jsonpath";

const STATE = {
	input: { userId: "u-1", amount: 100 },
	list_items: {
		items: [
			{ id: "a", qty: 1 },
			{ id: "b", qty: 2 },
			{ id: "c", qty: 3 },
		],
	},
	charge: { requires3ds: true, txId: "tx-7" },
	suspended: false,
	status: "active",
	country: "RU",
};

describe("evalPath", () => {
	it("resolves nested path", () => {
		expect(evalPath("$.input.userId", STATE)).toBe("u-1");
	});

	it("array index", () => {
		expect(evalPath("$.list_items.items[0]", STATE)).toEqual({
			id: "a",
			qty: 1,
		});
		expect(evalPath("$.list_items.items[0].id", STATE)).toBe("a");
	});

	it("[*] returns whole array", () => {
		expect(evalPath("$.list_items.items[*]", STATE)).toEqual(
			STATE.list_items.items,
		);
	});

	it("[*].field maps over array", () => {
		expect(evalPath("$.list_items.items[*].id", STATE)).toEqual([
			"a",
			"b",
			"c",
		]);
	});

	it("missing path returns undefined", () => {
		expect(evalPath("$.input.nope", STATE)).toBeUndefined();
		expect(evalPath("$.nothing.here.at.all", STATE)).toBeUndefined();
	});

	it("rejects unparsable token", () => {
		expect(() => evalPath("$.foo[abc]", STATE)).toThrow(JsonPathError);
	});

	it("rejects missing leading $", () => {
		expect(() => evalPath("foo.bar", STATE)).toThrow(JsonPathError);
	});

	it("[*] on non-array throws", () => {
		expect(() => evalPath("$.input[*]", STATE)).toThrow(JsonPathError);
	});
});

describe("evalLiteralOrPath", () => {
	it("path expression resolves", () => {
		expect(evalLiteralOrPath("$.input.userId", STATE)).toBe("u-1");
	});

	it("non-$ string is literal", () => {
		expect(evalLiteralOrPath("hello", STATE)).toBe("hello");
	});

	it("literal escape", () => {
		expect(evalLiteralOrPath({ literal: "$.weird" }, STATE)).toBe("$.weird");
	});

	it("recursive object", () => {
		expect(
			evalLiteralOrPath({ userId: "$.input.userId", note: "fixed" }, STATE),
		).toEqual({ userId: "u-1", note: "fixed" });
	});

	it("recursive array", () => {
		expect(evalLiteralOrPath(["$.input.userId", "literal", 42], STATE)).toEqual(
			["u-1", "literal", 42],
		);
	});

	it("numbers and booleans pass through", () => {
		expect(evalLiteralOrPath(42, STATE)).toBe(42);
		expect(evalLiteralOrPath(true, STATE)).toBe(true);
		expect(evalLiteralOrPath(null, STATE)).toBe(null);
	});
});

describe("evalPredicate", () => {
	it("truthy path", () => {
		expect(evalPredicate("$.charge.requires3ds", STATE)).toBe(true);
		expect(evalPredicate("$.suspended", STATE)).toBe(false);
	});

	it("not", () => {
		expect(evalPredicate({ not: "$.suspended" }, STATE)).toBe(true);
	});

	it("equals", () => {
		expect(evalPredicate({ equals: ["$.status", "active"] }, STATE)).toBe(true);
		expect(evalPredicate({ equals: ["$.status", "frozen"] }, STATE)).toBe(
			false,
		);
	});

	it("in", () => {
		expect(evalPredicate({ in: ["$.country", ["RU", "KZ"]] }, STATE)).toBe(
			true,
		);
		expect(evalPredicate({ in: ["$.country", ["US", "DE"]] }, STATE)).toBe(
			false,
		);
	});

	it("and / or", () => {
		expect(
			evalPredicate(
				{
					and: [{ equals: ["$.status", "active"] }, { not: "$.suspended" }],
				},
				STATE,
			),
		).toBe(true);
		expect(
			evalPredicate(
				{
					or: [{ equals: ["$.status", "frozen"] }, "$.charge.requires3ds"],
				},
				STATE,
			),
		).toBe(true);
	});
});
