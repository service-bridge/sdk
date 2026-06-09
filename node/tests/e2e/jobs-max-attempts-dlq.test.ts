// jobs-max-attempts-dlq — exhausted retries → row in jobs_dlq, no more calls.
//
// Handler always throws. After maxAttempts=2 the execution is moved to jobs_dlq
// and no further handler invocations happen.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
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

async function dlqCount(): Promise<number> {
	return withDb(async (sql) => {
		const rows =
			(await sql`SELECT COUNT(*)::int AS cnt FROM jobs_dlq`) as Array<{
				cnt: number;
			}>;
		return rows[0]?.cnt ?? 0;
	});
}

describe("jobs-max-attempts-dlq", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("handler always fails → after maxAttempts=2 row in jobs_dlq, no more calls", async () => {
		let callCount = 0;
		const jobName = `dlq-test-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 500 } },
				maxAttempts: 2,
			},
			async () => {
				callCount++;
				throw new Error("always fails");
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Wait for DLQ entry to appear.
		await waitFor(() => dlqCount().then((n) => n > 0), 60_000, "dlq entry");

		// Wait a bit more to confirm no further calls.
		await sleep(3_000);

		expect(callCount).toBe(2);
		expect(await dlqCount()).toBe(1);
	}, 90_000);
});
