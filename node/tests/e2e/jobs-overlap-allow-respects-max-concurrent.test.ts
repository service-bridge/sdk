// jobs-overlap-allow-respects-max-concurrent — overlap=allow + maxConcurrent:1.
//
// With overlap=allow and maxConcurrent:1, only one handler runs at a time.
// The SDK semaphore blocks the second until the first completes.

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

describe("jobs-overlap-allow-respects-max-concurrent", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("overlap=allow + maxConcurrent=1 → never more than 1 active", async () => {
		let active = 0;
		let maxConcurrent = 0;
		const jobName = `overlap-allow-cap-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 200 },
				overlap: "allow",
				maxConcurrent: 1,
			},
			async () => {
				active++;
				if (active > maxConcurrent) maxConcurrent = active;
				await sleep(600);
				active--;
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		await sleep(3_000);

		// maxConcurrent=1 applied by the SDK semaphore.
		expect(maxConcurrent).toBe(1);
	}, 30_000);
});
