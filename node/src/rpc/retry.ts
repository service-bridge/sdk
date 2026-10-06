import type { RetryOpts } from "./client";

// Retry policy defaults. Same numbers in the Go SDK (internal/rpc/retry.go).
export const DEFAULT_RETRY: RetryOpts = {
	maxAttempts: 3,
	baseDelayMs: 200,
	factor: 2,
	maxDelayMs: 5000,
	jitter: 0.3,
};

export function mergeRetryOpts(override?: Partial<RetryOpts>): RetryOpts {
	return { ...DEFAULT_RETRY, ...(override ?? {}) };
}

// backoffDelay returns the sleep before attempt N+1 (0-indexed N):
//   delay  = min(baseDelayMs * factor^attempt, maxDelayMs)
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
