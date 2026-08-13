// context.ts — AsyncLocalStorage-based trace context carrier.
// Consumers import runWithTrace + currentTraceContext.
// @internal — см. ./README.md

import { AsyncLocalStorage } from "node:async_hooks";
import type { TraceContext } from "./trace-context";

/**
 * @internal — exported only for hook-style frameworks (Fastify) that need
 * `als.enterWith(ctx)` because they don't expose a next() callback.
 */
export const als = new AsyncLocalStorage<TraceContext>();

/**
 * Run fn inside a trace context scope. All async continuations spawned from fn
 * inherit the context automatically via Node/Bun AsyncLocalStorage.
 */
export function runWithTrace<T>(ctx: TraceContext, fn: () => T): T {
	return als.run(ctx, fn);
}

/**
 * Return the active trace context, or undefined if outside any runWithTrace scope.
 */
export function currentTraceContext(): TraceContext | undefined {
	return als.getStore();
}

// streamWithContext wraps an async-generator factory so that each chunk
// iteration runs inside the given ALS context. This is necessary because
// Bun/Node ALS does NOT propagate through async-generator continuations
// based solely on where the generator object was constructed — each .next()
// inherits the context of its call-site, not the construction site.
export async function* streamWithContext<T>(
	ctx: TraceContext,
	gen: () => AsyncIterable<T>,
): AsyncIterable<T> {
	const iter = gen()[Symbol.asyncIterator]();
	try {
		while (true) {
			const result = await als.run(ctx, () => iter.next());
			if (result.done) break;
			yield result.value;
		}
	} finally {
		// A consumer `break`/`return`/`throw` resumes this generator at the yield
		// and unwinds it. Without forwarding return(), the source iterator stays
		// parked on its own yield forever and the gRPC call underneath it is never
		// cancelled — one leaked HTTP/2 stream per abandoned `for await`.
		await als.run(ctx, () => iter.return?.());
	}
}
