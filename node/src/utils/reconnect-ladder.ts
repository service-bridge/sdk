// Domain-neutral reconnect backoff: a fixed delay ladder plus bounded jitter to
// spread a herd of reconnecting clients. No state, no I/O — see ./README.md.

// RECONNECT_LADDER_MS is the shared reconnect backoff ladder. Index by attempt;
// saturates at the last rung. Deterministic rungs keep behaviour predictable;
// jitter (applied in reconnectDelay) is what breaks thundering-herd lockstep.
export const RECONNECT_LADDER_MS = [1000, 5000, 15000, 30000, 60000] as const;

// DEFAULT_JITTER_RATIO spreads each rung by ±this fraction. ±20% turns a herd of
// N clients reconnecting at the same rung into a smear across a window instead of
// a single synchronized spike onto the runtime.
const DEFAULT_JITTER_RATIO = 0.2;

export interface ReconnectDelayOptions {
	// ladder overrides the default rungs. Must be non-empty.
	ladder?: readonly number[];
	// jitterRatio is the ±fraction applied to the chosen rung. 0 disables jitter
	// (deterministic rung) — used where a caller wants exact timing.
	jitterRatio?: number;
	// random returns a float in [0, 1). Injectable so tests can pin the jitter.
	random?: () => number;
}

// reconnectDelay returns the backoff for the given zero-based attempt. The rung
// is clamped into the ladder bounds (attempt < 0 → first rung, attempt beyond the
// end → last rung), then offset by bounded jitter so concurrent reconnects do not
// fire in lockstep.
export function reconnectDelay(
	attempt: number,
	opts: ReconnectDelayOptions = {},
): number {
	const ladder = opts.ladder ?? RECONNECT_LADDER_MS;
	if (ladder.length === 0) {
		throw new Error("reconnect-ladder: reconnectDelay: empty ladder");
	}
	const idx = Math.min(Math.max(attempt, 0), ladder.length - 1);
	const base = ladder[idx]!;
	const ratio = opts.jitterRatio ?? DEFAULT_JITTER_RATIO;
	if (ratio <= 0) return base;
	const random = opts.random ?? Math.random;
	// Map [0,1) → [-ratio, +ratio) so the spread is symmetric around the rung.
	const offset = (random() * 2 - 1) * ratio;
	return Math.max(0, Math.round(base * (1 + offset)));
}
