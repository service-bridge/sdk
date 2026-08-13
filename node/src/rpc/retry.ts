import type { RetryOpts } from "./client";

// Retry policy defaults — match the contract from the original user spec:
//   retry: { maxAttempts: 3, baseDelayMs: 200, factor: 2, maxDelayMs: 5000, jitter: 0.3 }
export const DEFAULT_RETRY: RetryOpts = {
	maxAttempts: 3,
	baseDelayMs: 200,
	factor: 2,
	maxDelayMs: 5000,
	jitter: 0.3,
};

// gRPC status codes — keep numeric to avoid a circular import on grpc-js.
export const GRPC_CODE_UNAVAILABLE = 14;
export const GRPC_CODE_RESOURCE_EXHAUSTED = 8;
export const GRPC_CODE_DEADLINE_EXCEEDED = 4;
export const GRPC_CODE_INTERNAL = 13;
export const GRPC_CODE_ABORTED = 10;
export const GRPC_CODE_UNKNOWN = 2;

// Codes that prove the callee did no work, so a replay cannot duplicate an
// effect: UNAVAILABLE means the connection was never established, and
// RESOURCE_EXHAUSTED means the request was rejected before execution (rate
// limit). Safe without an idempotency key.
const ALWAYS_RETRYABLE = new Set([
	GRPC_CODE_UNAVAILABLE,
	GRPC_CODE_RESOURCE_EXHAUSTED,
]);

// Codes that leave the outcome ambiguous — the callee may have completed the
// effect and the caller cannot tell. DEADLINE_EXCEEDED belongs here: the
// deadline expires on the caller side and says nothing about the handler, which
// may have charged the payment and answered one second late. These retry ONLY
// when the caller passed an idempotency_key, so the runtime de-dups the replay
// (ADR 0001).
const RETRYABLE_WITH_IDEMPOTENCY = new Set([
	GRPC_CODE_DEADLINE_EXCEEDED,
	GRPC_CODE_INTERNAL,
	GRPC_CODE_ABORTED,
	GRPC_CODE_UNKNOWN,
]);

export function mergeRetryOpts(override?: Partial<RetryOpts>): RetryOpts {
	return { ...DEFAULT_RETRY, ...(override ?? {}) };
}

// isRetryable inspects a thrown gRPC-js error (or any object with a `.code`)
// and decides whether to retry. Errors without a numeric `.code` are treated
// as non-retryable (application errors thrown by the handler, schema errors).
export function isRetryable(err: unknown, hasIdempotency: boolean): boolean {
	const code = (err as { code?: unknown })?.code;
	if (typeof code !== "number") return false;
	if (ALWAYS_RETRYABLE.has(code)) return true;
	if (hasIdempotency && RETRYABLE_WITH_IDEMPOTENCY.has(code)) return true;
	return false;
}

// backoffDelay returns the actual sleep duration for attempt N (0-indexed)
// given the policy. Formula:
//   delay = min(baseDelayMs * factor^attempt, maxDelayMs)
//   actual = delay * (1 - jitter + random * 2 * jitter)
export function backoffDelay(
	opts: RetryOpts,
	attempt: number,
	rand: () => number = Math.random,
): number {
	const raw = Math.min(
		opts.baseDelayMs * opts.factor ** attempt,
		opts.maxDelayMs,
	);
	const factor = 1 - opts.jitter + rand() * 2 * opts.jitter;
	return Math.max(0, Math.round(raw * factor));
}
