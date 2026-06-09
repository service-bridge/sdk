// jobs-overlap-buffer-one — overlap=buffer_one: at most 1 pending while 1 running.
//
// interval:200ms, handler sleeps 800ms, overlap=buffer_one.
// At most 1 execution waiting while 1 is running. Total in-flight ≤ 2 at any moment.

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

describe("jobs-overlap-buffer-one", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("overlap=buffer_one: max 1 pending while 1 running (in-flight ≤ 2)", async () => {
		let active = 0;
		let maxConcurrent = 0;
		const jobName = `overlap-bufone-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 200 },
				overlap: "buffer_one",
			},
			async () => {
				active++;
				if (active > maxConcurrent) maxConcurrent = active;
				await sleep(800);
				active--;
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		await sleep(3_000);

		// buffer_one: runtime dispatches at most 1 pending while 1 is running.
		// From the SDK side, semaphore is unlimited (overlap=buffer_one is a
		// server-side dispatch policy, not an SDK semaphore). The key invariant
		// is that the runtime never sends more than 2 concurrent executions.
		expect(maxConcurrent).toBeLessThanOrEqual(2);
	}, 30_000);
});
