import { describe, expect, it } from "bun:test";
import {
	backoffDelay,
	DEFAULT_RETRY,
	GRPC_CODE_ABORTED,
	GRPC_CODE_DEADLINE_EXCEEDED,
	GRPC_CODE_INTERNAL,
	GRPC_CODE_RESOURCE_EXHAUSTED,
	GRPC_CODE_UNAVAILABLE,
	GRPC_CODE_UNKNOWN,
	isRetryable,
	mergeRetryOpts,
} from "./retry";

describe("isRetryable", () => {
	it("UNAVAILABLE retryable without an idempotency_key", () => {
		// No connection was established — the callee provably did no work.
		expect(isRetryable({ code: GRPC_CODE_UNAVAILABLE }, false)).toBe(true);
		expect(isRetryable({ code: GRPC_CODE_UNAVAILABLE }, true)).toBe(true);
	});

	it("RESOURCE_EXHAUSTED retryable without an idempotency_key", () => {
		// Rejected before execution by the rate limiter — no side effect.
		expect(isRetryable({ code: GRPC_CODE_RESOURCE_EXHAUSTED }, false)).toBe(
			true,
		);
		expect(isRetryable({ code: GRPC_CODE_RESOURCE_EXHAUSTED }, true)).toBe(
			true,
		);
	});

	it("DEADLINE_EXCEEDED NOT retryable without an idempotency_key", () => {
		// The deadline expired on the caller side; the handler may have completed
		// the effect and answered late. Retrying blind would double-charge.
		expect(isRetryable({ code: GRPC_CODE_DEADLINE_EXCEEDED }, false)).toBe(
			false,
		);
	});

	it("DEADLINE_EXCEEDED retryable with an idempotency_key", () => {
		expect(isRetryable({ code: GRPC_CODE_DEADLINE_EXCEEDED }, true)).toBe(true);
	});

	it("INTERNAL retryable only with idempotency_key", () => {
		expect(isRetryable({ code: GRPC_CODE_INTERNAL }, false)).toBe(false);
		expect(isRetryable({ code: GRPC_CODE_INTERNAL }, true)).toBe(true);
	});

	it("ABORTED retryable only with idempotency_key", () => {
		expect(isRetryable({ code: GRPC_CODE_ABORTED }, false)).toBe(false);
		expect(isRetryable({ code: GRPC_CODE_ABORTED }, true)).toBe(true);
	});

	it("UNKNOWN retryable only with idempotency_key", () => {
		expect(isRetryable({ code: GRPC_CODE_UNKNOWN }, false)).toBe(false);
		expect(isRetryable({ code: GRPC_CODE_UNKNOWN }, true)).toBe(true);
	});

	it("INVALID_ARGUMENT / PERMISSION_DENIED never retryable", () => {
		expect(isRetryable({ code: 3 /* INVALID_ARGUMENT */ }, true)).toBe(false);
		expect(isRetryable({ code: 7 /* PERMISSION_DENIED */ }, true)).toBe(false);
	});

	it("errors without numeric code → not retryable", () => {
		expect(isRetryable(new Error("plain"), true)).toBe(false);
		expect(isRetryable({ code: "STRING_CODE" }, true)).toBe(false);
	});
});

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
