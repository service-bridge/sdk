import { describe, expect, it } from "bun:test";
import { backoffDelay, DEFAULT_RETRY, mergeRetryOpts } from "./retry";

describe("backoffDelay", () => {
	it("base delay at attempt 0 (without jitter)", () => {
		const opts = { ...DEFAULT_RETRY, jitter: 0 };
		expect(backoffDelay(opts, 0, () => 0.5)).toBe(opts.baseDelayMs);
	});

	it("exponential growth", () => {
		const opts = { ...DEFAULT_RETRY, jitter: 0 };
		expect(backoffDelay(opts, 1, () => 0.5)).toBe(opts.baseDelayMs * 2);
		expect(backoffDelay(opts, 2, () => 0.5)).toBe(opts.baseDelayMs * 4);
	});

	it("capped at maxDelayMs", () => {
		const opts = {
			...DEFAULT_RETRY,
			jitter: 0,
			baseDelayMs: 1000,
			maxDelayMs: 2500,
		};
		expect(backoffDelay(opts, 5, () => 0.5)).toBe(2500);
	});

	it("jitter ±30% around base", () => {
		const opts = { ...DEFAULT_RETRY }; // jitter=0.3
		// random=0 → multiplier = 0.7 ; random=1 → multiplier = 1.3
		expect(backoffDelay(opts, 0, () => 0)).toBe(
			Math.round(opts.baseDelayMs * 0.7),
		);
		expect(backoffDelay(opts, 0, () => 1)).toBe(
			Math.round(opts.baseDelayMs * 1.3),
		);
	});
});

describe("mergeRetryOpts", () => {
	it("returns defaults when no override", () => {
		expect(mergeRetryOpts(undefined)).toEqual(DEFAULT_RETRY);
	});

	it("partial override merged on top of defaults", () => {
		const merged = mergeRetryOpts({ maxAttempts: 5, jitter: 0.5 });
		expect(merged.maxAttempts).toBe(5);
		expect(merged.jitter).toBe(0.5);
		expect(merged.baseDelayMs).toBe(DEFAULT_RETRY.baseDelayMs);
	});
});
