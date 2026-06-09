import { describe, expect, it } from "bun:test";
import {
	BUCKET_MS,
	CircuitBreakerRegistry,
	ERROR_THRESHOLD,
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

	it("reset clears the entry", () => {
		const { cb } = withClock();
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure("svc:i-1");
		cb.reset("svc:i-1");
		expect(cb.state("svc:i-1")).toBe("CLOSED");
	});
});
