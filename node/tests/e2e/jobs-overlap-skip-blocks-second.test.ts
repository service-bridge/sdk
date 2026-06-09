// jobs-overlap-skip-blocks-second — overlap=skip prevents concurrent runs.
//
// interval:200ms, handler sleeps 800ms, overlap=skip → at most 1 active at a time.
// Verify by counting max observed concurrent invocations.

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

describe("jobs-overlap-skip-blocks-second", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("overlap=skip: never more than 1 active invocation at a time", async () => {
		let active = 0;
		let maxConcurrent = 0;
		let totalCalls = 0;
		const jobName = `overlap-skip-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 200 },
				overlap: "skip",
			},
			async () => {
				active++;
				totalCalls++;
				if (active > maxConcurrent) maxConcurrent = active;
				await sleep(800);
				active--;
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Observe for 3 seconds — with 200ms interval and 800ms handler,
		// skip policy means ~1 completion every 800ms → ≈3-4 calls total.
		await sleep(3_000);

		expect(maxConcurrent).toBe(1);
		// With 800ms execution and 200ms interval+skip, expect few actual completions.
		expect(totalCalls).toBeGreaterThanOrEqual(1);
		expect(totalCalls).toBeLessThanOrEqual(5);
	}, 30_000);
});
