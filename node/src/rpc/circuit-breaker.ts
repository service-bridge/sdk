// Per-instance circuit breaker. State per (serviceId, instanceId) — scope
// matches ADR 0001: per-pod, not synchronized across caller pods.
// Multi-pod coherence comes from runtime telemetry hints (ADR 0001), not
// from a shared CB state.
//
// State machine:
//   CLOSED → OPEN          when totalRequests >= MIN_REQUESTS and errorRate > ERROR_THRESHOLD
//                          in the sliding window.
//   OPEN   → HALF_OPEN     after OPEN_DURATION_MS since opening (lazy transition
//                          on the next canCall / state read).
//   HALF_OPEN → CLOSED     on probe success.
//   HALF_OPEN → OPEN       on probe failure (re-opens for another OPEN_DURATION_MS).
//
// Only one probe is allowed in HALF_OPEN — concurrent callers see canCall()=false
// until the probe completes or the probe window times out (probe ownership is
// best-effort within a pod; ADR 0001 accepts duplicate probes across pods).
//
// @internal — см. ./README.md

export const WINDOW_SIZE_MS = 10_000;
export const BUCKET_COUNT = 10;
export const BUCKET_MS = WINDOW_SIZE_MS / BUCKET_COUNT;
export const MIN_REQUESTS = 10;
export const ERROR_THRESHOLD = 0.5;
export const OPEN_DURATION_MS = 30_000;

type State = "CLOSED" | "OPEN" | "HALF_OPEN";

interface Bucket {
	start: number; // bucket window start, ms
	successes: number;
	errors: number;
}

interface Entry {
	state: State;
	openedAt: number;
	probeInFlight: boolean;
	buckets: Bucket[]; // ring buffer of length BUCKET_COUNT
}

function newEntry(now: number): Entry {
	const buckets: Bucket[] = [];
	for (let i = 0; i < BUCKET_COUNT; i++) {
		buckets.push({
			start: now - (BUCKET_COUNT - 1 - i) * BUCKET_MS,
			successes: 0,
			errors: 0,
		});
	}
	return { state: "CLOSED", openedAt: 0, probeInFlight: false, buckets };
}

// currentBucket rotates the ring buffer in place so that the head bucket
// always covers `now`. Buckets that fall outside the window are zeroed.
function currentBucket(e: Entry, now: number): Bucket {
	const headStart = Math.floor(now / BUCKET_MS) * BUCKET_MS;
	let head = e.buckets[e.buckets.length - 1]!;
	if (head.start === headStart) return head;
	// Advance the ring forward bucket-by-bucket until the head matches.
	const stepsRaw = Math.floor((headStart - head.start) / BUCKET_MS);
	const steps = Math.min(stepsRaw, BUCKET_COUNT);
	for (let i = 0; i < steps; i++) {
		const oldest = e.buckets.shift()!;
		oldest.start = e.buckets[e.buckets.length - 1]!.start + BUCKET_MS;
		oldest.successes = 0;
		oldest.errors = 0;
		e.buckets.push(oldest);
	}
	// When stepsRaw exceeded BUCKET_COUNT the loop above only reset every
	// bucket once — that's enough since they all become stale at once. Now
	// snap the head start to the exact current bucket.
	head = e.buckets[e.buckets.length - 1]!;
	if (head.start !== headStart) {
		const delta = headStart - head.start;
		for (const b of e.buckets) b.start += delta;
	}
	return e.buckets[e.buckets.length - 1]!;
}

function windowTotals(
	e: Entry,
	now: number,
): { total: number; errors: number } {
	const cutoff = now - WINDOW_SIZE_MS;
	let total = 0;
	let errors = 0;
	for (const b of e.buckets) {
		if (b.start + BUCKET_MS <= cutoff) continue;
		total += b.successes + b.errors;
		errors += b.errors;
	}
	return { total, errors };
}

export class CircuitBreakerRegistry {
	private map = new Map<string, Entry>();
	private now: () => number;

	constructor(now: () => number = Date.now) {
		this.now = now;
	}

	// canCall returns true when the breaker is CLOSED, or when it has been
	// OPEN long enough to transition to HALF_OPEN and no probe is in flight.
	// In HALF_OPEN it CLAIMS the single probe slot as a side effect — the first
	// caller gets true and owns the probe; concurrent callers get false until
	// recordSuccess/recordFailure releases it. Call this only for the instance
	// actually about to be dispatched (the LB winner), never during candidate
	// filtering, or the claim leaks onto instances that are never called.
	canCall(key: string): boolean {
		const e = this.map.get(key);
		if (!e) return true;
		this.maybeHalfOpen(e);
		if (e.state === "CLOSED") return true;
		if (e.state === "HALF_OPEN") {
			if (e.probeInFlight) return false;
			e.probeInFlight = true;
			return true;
		}
		return false;
	}

	// probeAvailable is the side-effect-free read used by the LB to decide
	// whether a HALF_OPEN instance is eligible (probe slot free) before it
	// commits to a winner. Unlike canCall it does NOT claim the probe.
	probeAvailable(key: string): boolean {
		const e = this.map.get(key);
		if (!e) return true;
		this.maybeHalfOpen(e);
		if (e.state === "OPEN") return false;
		if (e.state === "HALF_OPEN") return !e.probeInFlight;
		return true;
	}

	recordSuccess(key: string): void {
		const now = this.now();
		const e = this.entry(key, now);
		if (e.state === "HALF_OPEN") {
			e.state = "CLOSED";
			e.probeInFlight = false;
			this.resetWindow(e, now);
			return;
		}
		currentBucket(e, now).successes++;
	}

	recordFailure(key: string): void {
		const now = this.now();
		const e = this.entry(key, now);
		if (e.state === "HALF_OPEN") {
			e.state = "OPEN";
			e.openedAt = now;
			e.probeInFlight = false;
			return;
		}
		currentBucket(e, now).errors++;
		if (e.state === "OPEN") return;
		const { total, errors } = windowTotals(e, now);
		if (total >= MIN_REQUESTS && errors / total > ERROR_THRESHOLD) {
			e.state = "OPEN";
			e.openedAt = now;
		}
	}

	state(key: string): State {
		const e = this.map.get(key);
		if (!e) return "CLOSED";
		this.maybeHalfOpen(e);
		return e.state;
	}

	reset(key: string): void {
		this.map.delete(key);
	}

	private entry(key: string, now: number): Entry {
		let e = this.map.get(key);
		if (!e) {
			e = newEntry(now);
			this.map.set(key, e);
		}
		return e;
	}

	private maybeHalfOpen(e: Entry): void {
		if (e.state !== "OPEN") return;
		if (this.now() - e.openedAt >= OPEN_DURATION_MS) {
			e.state = "HALF_OPEN";
			e.probeInFlight = false;
		}
	}

	private resetWindow(e: Entry, now: number): void {
		for (let i = 0; i < BUCKET_COUNT; i++) {
			const b = e.buckets[i]!;
			b.start = now - (BUCKET_COUNT - 1 - i) * BUCKET_MS;
			b.successes = 0;
			b.errors = 0;
		}
	}
}
