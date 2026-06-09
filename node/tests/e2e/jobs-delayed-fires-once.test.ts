// jobs-delayed-fires-once — delayed job fires exactly once.
//
// Registers a job with delayed: { at: now+1s }, asserts the handler is called
// exactly once with attempt===1. Uses the shared ambient runtime on :14445.

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

describe("jobs-delayed-fires-once", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("delayed job fires exactly once with attempt===1", async () => {
		const calls: Array<{ attempt: number }> = [];
		const jobName = `delayed-once-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 1_000 } },
				maxAttempts: 1,
			},
			async (ctx) => {
				calls.push({ attempt: ctx.attempt });
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		await waitFor(() => calls.length >= 1, 10_000, "handler called");

		// Wait a bit more to ensure it doesn't fire again.
		await sleep(2_000);

		expect(calls).toHaveLength(1);
		expect(calls[0]!.attempt).toBe(1);
	}, 30_000);
});
