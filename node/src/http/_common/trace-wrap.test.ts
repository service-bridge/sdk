// trace-wrap.test.ts — TDD for contextFromXSbTrace helper.
import { describe, expect, it } from "bun:test";
import { ZERO_OP_ID } from "../../telemetry/trace-context";
import { contextFromXSbTrace } from "./trace-wrap";

const UUID_A = "01960000-0000-7000-8000-000000000001";
const UUID_B = "01960000-0000-7000-8000-000000000002";

describe("contextFromXSbTrace", () => {
	it("parses valid header into trace context", () => {
		const ctx = contextFromXSbTrace(`${UUID_A}-${UUID_B}`);
		expect(ctx.traceId).toBe(UUID_A);
		expect(ctx.parentOpId).toBe(UUID_B);
	});

	it("returns fresh root context for null", () => {
		const ctx = contextFromXSbTrace(null);
		expect(ctx.traceId).toMatch(
			/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
		);
		expect(ctx.parentOpId).toBe(ZERO_OP_ID);
	});

	it("returns fresh root context for undefined", () => {
		const ctx = contextFromXSbTrace(undefined);
		expect(ctx.parentOpId).toBe(ZERO_OP_ID);
	});

	it("returns fresh root context for malformed header", () => {
		const ctx = contextFromXSbTrace("not-valid");
		expect(ctx.parentOpId).toBe(ZERO_OP_ID);
	});

	it("each call with null generates unique traceId (not singleton)", () => {
		const a = contextFromXSbTrace(null);
		const b = contextFromXSbTrace(null);
		expect(a.traceId).not.toBe(b.traceId);
	});
});
