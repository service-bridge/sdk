// trace-context.ts — UUID-based trace context (ADR T-017).
// traceId and parentOpId are RFC 9562 UUID strings.
// @internal — см. ./README.md

import { uuidv7 } from "uuidv7";

/** All-zeros UUID used as sentinel parentOpId for root operations. */
export const ZERO_OP_ID = "00000000-0000-0000-0000-000000000000";

export interface TraceContext {
	traceId: string;
	parentOpId: string;
}

/** Mint a new root trace context with a fresh UUIDv7 traceId. */
export function mintRootContext(): TraceContext {
	return {
		traceId: uuidv7(),
		parentOpId: ZERO_OP_ID,
	};
}

/**
 * Derive a child context: inherits traceId, sets parentOpId to newOpId.
 * Use when a parent operation (newOpId) spawns a nested call and needs
 * to propagate the trace with the parent as the new context root.
 */
export function childContext(
	parent: TraceContext,
	newOpId: string,
): TraceContext {
	return {
		traceId: parent.traceId,
		parentOpId: newOpId,
	};
}
