// context.test.ts — TDD for ALS-based trace context propagation.
import { describe, expect, it } from "bun:test";
import { currentTraceContext, runWithTrace } from "./context";
import { mintRootContext, ZERO_OP_ID } from "./trace-context";

describe("currentTraceContext", () => {
	it("returns undefined outside any runWithTrace scope", () => {
		expect(currentTraceContext()).toBeUndefined();
	});
});

describe("runWithTrace", () => {
	it("makes context visible inside the callback", () => {
		const ctx = mintRootContext();
		let seen: ReturnType<typeof currentTraceContext>;
		runWithTrace(ctx, () => {
			seen = currentTraceContext();
		});
		expect(seen).not.toBeUndefined();
		expect(seen!.traceId).toBe(ctx.traceId);
	});

	it("context is gone after the callback returns", () => {
		const ctx = mintRootContext();
		runWithTrace(ctx, () => {});
		expect(currentTraceContext()).toBeUndefined();
	});

	it("propagates through await chains", async () => {
		const ctx = mintRootContext();
		let seen: ReturnType<typeof currentTraceContext>;
		await runWithTrace(ctx, async () => {
			await Promise.resolve();
			seen = currentTraceContext();
		});
		expect(seen!.traceId).toBe(ctx.traceId);
	});

	it("propagates through nested async/await", async () => {
		const ctx = mintRootContext();
		async function inner(): Promise<string | undefined> {
			await Promise.resolve();
			return currentTraceContext()?.traceId;
		}
		const result = await runWithTrace(ctx, async () => {
			return await inner();
		});
		expect(result).toBe(ctx.traceId);
	});

	it("nested runWithTrace overrides context", () => {
		const outer = {
			traceId: "aaaaaaaa-0000-7000-8000-000000000001",
			parentOpId: ZERO_OP_ID,
		};
		const inner = {
			traceId: "bbbbbbbb-0000-7000-8000-000000000002",
			parentOpId: ZERO_OP_ID,
		};
		let outerSeen: string | undefined;
		let innerSeen: string | undefined;
		runWithTrace(outer, () => {
			outerSeen = currentTraceContext()?.traceId;
			runWithTrace(inner, () => {
				innerSeen = currentTraceContext()?.traceId;
			});
		});
		expect(outerSeen).toBe(outer.traceId);
		expect(innerSeen).toBe(inner.traceId);
	});

	it("returns the callback return value (sync)", () => {
		const ctx = mintRootContext();
		const result = runWithTrace(ctx, () => 42);
		expect(result).toBe(42);
	});

	it("returns the callback return value (async)", async () => {
		const ctx = mintRootContext();
		const result = await runWithTrace(ctx, async () => "hello");
		expect(result).toBe("hello");
	});
});
