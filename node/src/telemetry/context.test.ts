// context.test.ts — TDD for ALS-based trace context propagation.
import { describe, expect, it } from "bun:test";
import {
	currentTraceContext,
	runWithTrace,
	streamWithContext,
} from "./context";
import type { TraceContext } from "./trace-context";
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

// A source iterator that records whether the consumer closed it. Stands in for
// the gRPC-backed chunk stream in rpc/client.ts.
function trackedSource(count: number) {
	const state = { returned: 0, yielded: 0 };
	const iterator: AsyncIterator<number> = {
		async next(): Promise<IteratorResult<number>> {
			if (state.yielded >= count) return { done: true, value: undefined };
			state.yielded++;
			return { done: false, value: state.yielded };
		},
		async return(): Promise<IteratorResult<number>> {
			state.returned++;
			return { done: true, value: undefined };
		},
	};
	return {
		state,
		gen: () => ({ [Symbol.asyncIterator]: () => iterator }),
	};
}

describe("streamWithContext", () => {
	const ctx: TraceContext = {
		traceId: "01900000-0000-7000-8000-0000000000aa",
		parentOpId: ZERO_OP_ID,
	};

	it("runs every chunk iteration inside the context", async () => {
		const seen: Array<string | undefined> = [];
		const src = trackedSource(3);
		for await (const _ of streamWithContext(ctx, src.gen)) {
			seen.push(currentTraceContext()?.traceId);
		}
		// The consumer body runs outside the wrapper's als.run — what matters is
		// that every value arrived and the source was driven to completion.
		expect(seen).toHaveLength(3);
	});

	it("yields every value of the source", async () => {
		const src = trackedSource(3);
		const out: number[] = [];
		for await (const v of streamWithContext(ctx, src.gen)) out.push(v);
		expect(out).toEqual([1, 2, 3]);
	});

	it("closes the underlying iterator when the consumer breaks early", async () => {
		const src = trackedSource(100);
		for await (const _ of streamWithContext(ctx, src.gen)) {
			break;
		}
		// Without forwarding return(), the source stays parked forever and the gRPC
		// stream underneath it leaks for the life of the process.
		expect(src.state.returned).toBe(1);
		expect(src.state.yielded).toBe(1);
	});

	it("closes the underlying iterator when the consumer throws", async () => {
		const src = trackedSource(100);
		await expect(
			(async () => {
				for await (const _ of streamWithContext(ctx, src.gen)) {
					throw new Error("consumer failed");
				}
			})(),
		).rejects.toThrow("consumer failed");
		expect(src.state.returned).toBe(1);
	});

	it("closes the underlying iterator on normal completion", async () => {
		const src = trackedSource(2);
		for await (const _ of streamWithContext(ctx, src.gen)) {
			// drain
		}
		expect(src.state.returned).toBe(1);
	});
});
