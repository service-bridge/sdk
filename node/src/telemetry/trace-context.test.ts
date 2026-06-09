// trace-context.test.ts — TDD for TraceContext UUID shape + helpers.
import { describe, expect, it } from "bun:test";
import { childContext, mintRootContext, ZERO_OP_ID } from "./trace-context";

describe("mintRootContext", () => {
	it("returns a valid UUIDv7 traceId", () => {
		const ctx = mintRootContext();
		expect(ctx.traceId).toMatch(
			/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
		);
	});

	it("parentOpId is ZERO_OP_ID for root", () => {
		const ctx = mintRootContext();
		expect(ctx.parentOpId).toBe(ZERO_OP_ID);
	});

	it("generates unique traceId each call", () => {
		const a = mintRootContext();
		const b = mintRootContext();
		expect(a.traceId).not.toBe(b.traceId);
	});
});

describe("childContext", () => {
	it("inherits traceId from parent", () => {
		const root = mintRootContext();
		const newOpId = "01960000-0000-7000-8000-000000000042";
		const child = childContext(root, newOpId);
		expect(child.traceId).toBe(root.traceId);
	});

	it("sets parentOpId to the provided newOpId", () => {
		const root = mintRootContext();
		const newOpId = "01960000-0000-7000-8000-000000000042";
		const child = childContext(root, newOpId);
		expect(child.parentOpId).toBe(newOpId);
	});

	it("does not mutate parent", () => {
		const root = mintRootContext();
		const originalParent = root.parentOpId;
		const newOpId = "01960000-0000-7000-8000-000000000099";
		childContext(root, newOpId);
		expect(root.parentOpId).toBe(originalParent);
	});
});
