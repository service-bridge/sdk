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

// Wire status and an idempotency key cannot prove absence of execution.
export function mergeRetryOpts(override?: Partial<RetryOpts>): RetryOpts {
	return { ...DEFAULT_RETRY, ...(override ?? {}) };
}

// isRetryable inspects a thrown gRPC-js error (or any object with a `.code`)
// and decides whether to retry. Errors without a numeric `.code` are treated
// as non-retryable (application errors thrown by the handler, schema errors).
export function isRetryable(err: unknown, _hasIdempotency: boolean): boolean {
	return (err as { preDispatch?: unknown })?.preDispatch === true;
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
