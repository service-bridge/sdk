import { describe, expect, it } from "bun:test";
import { encodeDefinition, encodeExpr } from "./encode";
import type { WorkflowDef } from "./types";

const lit = (b: Buffer | undefined) => JSON.parse(Buffer.from(b!).toString());

describe("encodeExpr", () => {
	it("maps paths, escapes, containers and literals", () => {
		expect(encodeExpr("$.input.id")).toEqual({ path: "$.input.id" });
		expect(lit(encodeExpr({ literal: "$.raw" }).literal)).toBe("$.raw");
		expect(lit(encodeExpr("plain").literal)).toBe("plain");
		expect(lit(encodeExpr(7).literal)).toBe(7);
		expect(lit(encodeExpr(null).literal)).toBeNull();
		const obj = encodeExpr({ a: "$.x", b: [1, "$.y"] });
		expect(obj.object!.fields.a).toEqual({ path: "$.x" });
		expect(obj.object!.fields.b!.list!.items[1]).toEqual({ path: "$.y" });
	});
});

describe("encodeDefinition", () => {
	it("encodes every step kind and keeps local functions aside", () => {
		const fn = () => 1;
		const def: WorkflowDef = {
			version: "v2",
			input: { type: "object" },
			retry: { maxAttempts: 3, baseDelayMs: 100 },
			maxParallelism: 4,
			timeoutMs: 60_000,
			steps: [
				{
					id: "charge",
					type: "call",
					service: "billing",
					method: "charge",
					input: { amount: "$.input.amount" },
					opts: { timeoutMs: 500, idempotencyKey: "$.input.id" },
					compensate: { method: "refund", input: "$.charge" },
				},
				{
					id: "note",
					type: "publish",
					event: "order.charged",
					waitFor: ["charge"],
					when: { equals: ["$.input.notify", true] },
					compensate: { type: "call", service: "billing", method: "void" },
				},
				{ id: "calc", type: "local", fn, retry: { maxAttempts: 2 } },
				{ id: "nap", type: "sleep", durationMs: 1000 },
				{
					id: "evt",
					type: "wait_event",
					event: "paid",
					filter: { "$.id": "$.input.id" },
				},
				{ id: "sig", type: "wait_signal", signal: "approve", timeoutMs: 5000 },
				{ id: "kid", type: "workflow", workflow: "child", input: "$.input" },
				{
					id: "each",
					type: "parallel",
					forEach: { from: "$.input.items", as: "it" },
					steps: [{ id: "inner", type: "local", fn }],
				},
				{ id: "seq", type: "sequence", steps: [] },
			],
		};
		const { definition, locals } = encodeDefinition("order", def);
		expect(definition.name).toBe("order");
		expect(definition.version).toBe("v2");
		expect(JSON.parse(definition.inputSchemaJson.toString())).toEqual({
			type: "object",
		});
		expect(definition.retry?.maxAttempts).toBe(3);
		expect(definition.maxParallelism).toBe(4);
		expect(definition.timeoutMs).toBe(60_000);
		const [charge, note, calc, nap, evt, sig, kid, each, seq] =
			definition.steps;
		expect(charge!.call!.compensate!.call!.method).toEqual({
			literal: Buffer.from('"refund"'),
		});
		expect(charge!.call!.compensate!.input).toEqual({ path: "$.charge" });
		expect(charge!.call!.opts!.timeoutMs).toBe(500);
		expect(note!.waitFor).toEqual(["charge"]);
		expect(note!.when!.equals!.left).toEqual({ path: "$.input.notify" });
		expect(note!.publish!.compensate!.call).toBeDefined();
		expect(calc!.local).toEqual({});
		expect(calc!.retry!.maxAttempts).toBe(2);
		expect(nap!.sleep!.durationMs).toBe(1000);
		expect(evt!.waitEvent!.filter["$.id"]).toEqual({ path: "$.input.id" });
		expect(sig!.timeoutMs).toBe(5000);
		expect(kid!.workflow!.service).toBeUndefined();
		expect(each!.parallel!.forEach).toEqual({
			from: "$.input.items",
			as: "it",
		});
		expect(seq!.sequence!.steps).toEqual([]);
		expect(locals.get("calc")).toBe(fn);
		expect(locals.get("inner")).toBe(fn);
	});

	it("encodes predicate combinators", () => {
		const { definition } = encodeDefinition("p", {
			steps: [
				{
					id: "a",
					type: "local",
					fn: () => null,
					when: {
						and: [
							"$.input.on",
							{ not: { in: ["$.input.kind", ["x", "y"]] } },
							{ or: [] },
						],
					},
				},
			],
		});
		const and = definition.steps[0]!.when!.and!.items;
		expect(and[0]!.truthy).toEqual({ path: "$.input.on" });
		expect(and[1]!.not!.in!.right!.list!.items).toHaveLength(2);
		expect(and[2]!.or!.items).toEqual([]);
	});
});
