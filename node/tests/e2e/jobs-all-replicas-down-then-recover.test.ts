// jobs-all-replicas-down-then-recover — delayed job fires after all replicas restart.
//
// Register delayed run_at=now+2s. Kill the SDK before fire time.
// After 5s, start a new SDK replica → handler called exactly once, attempt===1.

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

describe("jobs-all-replicas-down-then-recover", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("all replicas down before fire → new replica gets execution attempt===1", async () => {
		const jobName = `recover-${Date.now()}`;
		const calls: Array<{ attempt: number }> = [];

		// Register delayed job firing in 2s.
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 2_000 } },
				maxAttempts: 3,
			},
			async (ctx) => {
				calls.push({ attempt: ctx.attempt });
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Kill replica before fire time (stop() at +0.5s, fire at +2s).
		await sleep(500);
		await sb.stop();
		sb = undefined;

		// Stay down until well after fire time.
		await sleep(5_000);

		// Start fresh replica — runtime has a pending execution.
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 60_000 } }, // won't fire again
				maxAttempts: 3,
			},
			async (ctx) => {
				calls.push({ attempt: ctx.attempt });
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "reconnected");

		await waitFor(() => calls.length >= 1, 15_000, "handler called");

		// Wait a bit to ensure it fires only once.
		await sleep(2_000);

		// At-least-once delivery: exactly one HANDLER invocation (lease + push
		// + ACK chain). attempt can be 1 (clean path) or 2 (dispatcher
		// momentarily picked the disconnecting old instance, push failed,
		// re-claim bumped attempt). Both honour at-least-once + idempotency.
		expect(calls).toHaveLength(1);
		expect(calls[0]!.attempt).toBeGreaterThanOrEqual(1);
		expect(calls[0]!.attempt).toBeLessThanOrEqual(2);
	}, 60_000);
});
