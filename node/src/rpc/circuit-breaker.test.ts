import { describe, expect, it } from "bun:test";
import {
	BUCKET_MS,
	CircuitBreakerRegistry,
	ERROR_THRESHOLD,
	IDLE_TTL_MS,
	MIN_REQUESTS,
	OPEN_DURATION_MS,
	WINDOW_SIZE_MS,
} from "./circuit-breaker";

function withClock(): {
	cb: CircuitBreakerRegistry;
	advance: (ms: number) => void;
} {
	let now = 1_000_000;
	const cb = new CircuitBreakerRegistry(() => now);
	return { cb, advance: (ms) => (now += ms) };
}

describe("CircuitBreakerRegistry (sliding window)", () => {
	it("starts closed and canCall returns true", () => {
		const { cb } = withClock();
		expect(cb.canCall("svc:i-1")).toBe(true);
		expect(cb.state("svc:i-1")).toBe("CLOSED");
	});

	it("does not trip below MIN_REQUESTS even at 100% errors", () => {
		const { cb } = withClock();
		// One short failure burst — below the gate. The breaker stays closed
		// so low-QPS instances don't flap (ADR 0001).
		for (let i = 0; i < MIN_REQUESTS - 1; i++) cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
		expect(cb.canCall("svc:i-1")).toBe(true);
	});

	it("trips when totalRequests >= MIN_REQUESTS and errorRate > ERROR_THRESHOLD", () => {
		const { cb } = withClock();
		// 6 errors + 4 successes over 10 → errorRate 0.6 > 0.5 → OPEN.
		for (let i = 0; i < 4; i++) cb.recordSuccess("svc:i-1");
		for (let i = 0; i < 5; i++) cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
		cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("OPEN");
		expect(cb.canCall("svc:i-1")).toBe(false);
	});

	it("does not trip at exactly ERROR_THRESHOLD (strict inequality)", () => {
		const { cb } = withClock();
		// 5 errors + 5 successes → errorRate = 0.5 exactly, NOT > 0.5.
		for (let i = 0; i < 5; i++) cb.recordSuccess("svc:i-1");
		for (let i = 0; i < 5; i++) cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
		void ERROR_THRESHOLD;
	});

	it("transitions to HALF_OPEN after OPEN_DURATION_MS", () => {
		const { cb, advance } = withClock();
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("OPEN");
		advance(OPEN_DURATION_MS);
		expect(cb.state("svc:i-1")).toBe("HALF_OPEN");
	});

	it("HALF_OPEN allows exactly one probe", () => {
		const { cb, advance } = withClock();
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
		advance(OPEN_DURATION_MS);
		expect(cb.canCall("svc:i-1")).toBe(true);
		// Second concurrent caller sees a busy probe → blocked.
		expect(cb.canCall("svc:i-1")).toBe(false);
	});

	it("HALF_OPEN success → CLOSED (window resets)", () => {
		const { cb, advance } = withClock();
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
		advance(OPEN_DURATION_MS);
		cb.canCall("svc:i-1"); // claim the probe
		cb.recordSuccess("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
		// Window reset — old failures don't immediately re-trip.
		for (let i = 0; i < MIN_REQUESTS - 1; i++) cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
	});

	it("HALF_OPEN failure → OPEN again for another OPEN_DURATION_MS", () => {
		const { cb, advance } = withClock();
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
		advance(OPEN_DURATION_MS);
		cb.canCall("svc:i-1"); // claim the probe
		cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("OPEN");
		// Still OPEN just before the next window expires.
		advance(OPEN_DURATION_MS - 1);
		expect(cb.canCall("svc:i-1")).toBe(false);
		advance(1);
		expect(cb.canCall("svc:i-1")).toBe(true);
	});

	it("buckets older than WINDOW_SIZE_MS are dropped", () => {
		const { cb, advance } = withClock();
		// Fill window with errors, but do not yet trip (1 short).
		for (let i = 0; i < MIN_REQUESTS - 1; i++) cb.recordFailure("svc:i-1");
		// Advance past the whole window so all buckets are stale.
		advance(WINDOW_SIZE_MS + BUCKET_MS);
		// A single failure in the fresh window is below MIN_REQUESTS → CLOSED.
		cb.recordFailure("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
	});

	it("independent state per key", () => {
		const { cb } = withClock();
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:a");
		expect(cb.state("svc:a")).toBe("OPEN");
		expect(cb.state("svc:b")).toBe("CLOSED");
	});

	it("evict clears the entry", () => {
		const { cb } = withClock();
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
		cb.evict("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
		expect(cb.size()).toBe(0);
	});

	describe("entry lifetime", () => {
		it("retain drops entries for instances that left the registry", () => {
			const { cb } = withClock();
			for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:gone");
			cb.recordSuccess("svc:live");
			expect(cb.size()).toBe(2);

			cb.retain(new Set(["svc:live"]));

			expect(cb.size()).toBe(1);
			expect(cb.state("svc:gone")).toBe("CLOSED"); // entry rebuilt from scratch
			expect(cb.state("svc:live")).toBe("CLOSED");
		});

		it("retain keeps OPEN state for instances that are still live", () => {
			const { cb } = withClock();
			for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
			expect(cb.state("svc:i-1")).toBe("OPEN");
			cb.retain(new Set(["svc:i-1"]));
			expect(cb.state("svc:i-1")).toBe("OPEN");
		});

		it("idle sweep bounds the map when instance ids churn and retain is never called", () => {
			const { cb, advance } = withClock();
			// Simulate a long rolling-deploy sequence: every generation of pods
			// gets fresh instance ids and the previous generation is never touched
			// again. Without eviction this map grows by one entry per pod forever.
			for (let gen = 0; gen < 20; gen++) {
				for (let pod = 0; pod < 5; pod++) {
					cb.recordSuccess(`svc:gen-${gen}-pod-${pod}`);
				}
				advance(IDLE_TTL_MS + 1);
			}
			// Only the last generation is younger than IDLE_TTL_MS; the sweep runs
			// on the next entry creation, so at most two generations survive.
			expect(cb.size()).toBeLessThanOrEqual(10);
		});

		it("idle sweep spares entries still receiving traffic", () => {
			const { cb, advance } = withClock();
			cb.recordSuccess("svc:hot");
			for (let step = 0; step < 10; step++) {
				advance(IDLE_TTL_MS / 4);
				cb.recordSuccess("svc:hot");
				cb.recordSuccess(`svc:cold-${step}`);
			}
			advance(IDLE_TTL_MS + 1);
			cb.recordSuccess("svc:hot"); // triggers the sweep
			expect(cb.state("svc:hot")).toBe("CLOSED");
			expect(cb.size()).toBe(1);
		});

		it("idle sweep does not drop a breaker the LB keeps reading", () => {
			const { cb, advance } = withClock();
			for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:broken");
			expect(cb.state("svc:broken")).toBe("OPEN");
			// The LB reads the breaker on every pick, which counts as a touch.
			for (let i = 0; i < 8; i++) {
				advance(IDLE_TTL_MS / 4);
				cb.probeAvailable("svc:broken");
			}
			// Creating another entry after the interval elapsed runs the sweep.
			cb.recordSuccess("svc:other");
			expect(cb.size()).toBe(2);
			// HALF_OPEN, not CLOSED — the entry survived instead of being recreated.
			expect(cb.state("svc:broken")).toBe("HALF_OPEN");
		});
	});

	describe("probeAvailable (read-only eligibility for the LB)", () => {
		it("is true for an unknown key and a CLOSED breaker", () => {
			const { cb } = withClock();
			expect(cb.probeAvailable("svc:i-1")).toBe(true);
			cb.recordSuccess("svc:i-1");
			expect(cb.probeAvailable("svc:i-1")).toBe(true);
		});

		it("is false while OPEN", () => {
			const { cb } = withClock();
			for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
			expect(cb.state("svc:i-1")).toBe("OPEN");
			expect(cb.probeAvailable("svc:i-1")).toBe(false);
		});

		it("does NOT claim the probe (unlike canCall), so it stays repeatable", () => {
			const { cb, advance } = withClock();
			for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
			advance(OPEN_DURATION_MS);
			expect(cb.state("svc:i-1")).toBe("HALF_OPEN");
			// Repeated reads stay true — no side effect on the probe slot.
			expect(cb.probeAvailable("svc:i-1")).toBe(true);
			expect(cb.probeAvailable("svc:i-1")).toBe(true);
			// canCall claims it; after that probeAvailable flips to false.
			expect(cb.canCall("svc:i-1")).toBe(true);
			expect(cb.probeAvailable("svc:i-1")).toBe(false);
		});
	});
});
