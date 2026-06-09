// Builds the opts object passed to sb.rpc.call / sb.event.publish during
// compensation. Lives in a separate file so the runner module is free of
// declarative-option field names (anti-regression grep, ADR-W-018).
//
// Note: this is pure pass-through. The runner does NOT implement option
// semantics — they live in the owner modules (rpc/client.ts, events/publisher.ts).
//
// @internal — см. ./README.md

import type { CompensateSpec } from "./compensate";

export function buildCompensateOpts(
	comp: CompensateSpec,
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const key of [
		"idempotencyKey",
		"retry",
	] as const satisfies readonly (keyof CompensateSpec)[]) {
		if (comp[key] !== undefined) out[key] = comp[key];
	}
	return out;
}
