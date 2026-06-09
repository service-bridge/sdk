// jobs-at-least-once-on-crash — handler throws retryable error → retried.
//
// Handler throws a retryable error on attempt 1 → runtime retries →
// second call with attempt===2, same idempotencyKey.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { waitFor } from "./_helpers/events";
import { harnessFromEnv } from "./_helpers/harness";
import { withDb } from "./_helpers/policy-db";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

async function cleanupJobState(): Promise<void> {
	await withDb(async (sql) => {
		await sql`TRUNCATE job_executions, job_schedules, job_definitions, jobs_dlq CASCADE`;
	});
}

describe("jobs-at-least-once-on-crash", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("retryable handler error → retry with attempt===2, same idempotencyKey", async () => {
		const calls: Array<{ attempt: number; idempotencyKey: string }> = [];
		const jobName = `retry-once-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 500 } },
				maxAttempts: 3,
			},
			async (ctx) => {
				calls.push({
					attempt: ctx.attempt,
					idempotencyKey: ctx.idempotencyKey,
				});
				if (ctx.attempt === 1) {
					const err = new Error("transient failure");
					(err as Error & { retryable?: boolean }).retryable = true;
					throw err;
				}
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		await waitFor(() => calls.length >= 2, 30_000, "two attempts");

		expect(calls[0]!.attempt).toBe(1);
		expect(calls[1]!.attempt).toBe(2);
		expect(calls[0]!.idempotencyKey).toBe(calls[1]!.idempotencyKey);
	}, 60_000);
});
