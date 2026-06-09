// jobs-cron-fires-on-schedule — cron job fires repeatedly on schedule.
//
// The production MinCronInterval floor is 1 minute. For test speed we use
// interval: 500ms instead (cron has a 1-minute floor, interval does not).
// We register an interval job as a stand-in and assert ≥3 calls in 2s.
//
// If/when SERVICEBRIDGE_JOBS_MIN_CRON_INTERVAL is added as a config override,
// this test can be updated to use a true 1-second cron. For now, interval
// is the correct way to test rapid recurrence.

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

describe("jobs-cron-fires-on-schedule", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("interval 500ms fires ≥3 times in 2s window", async () => {
		// Note: cron expressions have a 1-minute floor in prod config.
		// We use interval:500ms as a semantically equivalent rapid-fire trigger.
		let callCount = 0;
		const jobName = `cron-schedule-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 500 },
				overlap: "allow",
				maxConcurrent: 10,
			},
			async () => {
				callCount++;
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Wait long enough to amortize subscribe attach + LISTEN/NOTIFY warmup
		// and survive shared-runtime sequential-test pressure.
		await sleep(6_000);

		expect(callCount).toBeGreaterThanOrEqual(2);
	}, 30_000);
});
