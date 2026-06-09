// jobs-unregister-stops-firing — removing a job from registration stops it firing.
//
// Register {A, B}. Stop. Restart SDK with only {A}. Verify B handler never called
// after restart.

import {
	afterAll,
	afterEach,
	beforeEach,
	describe,
	expect,
	test,
} from "bun:test";
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

describe("jobs-unregister-stops-firing", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	afterAll(async () => {
		await cleanupJobState();
	});

	test("job B removed from registration → B handler never called after restart", async () => {
		const jobA = `unreg-a-${Date.now()}`;
		const jobB = `unreg-b-${Date.now()}`;
		const callsA: number[] = [];
		const callsB: number[] = [];

		// First start: register both A and B.
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobA,
			{
				trigger: { interval: 300 },
				overlap: "allow",
				maxConcurrent: 5,
			},
			async () => {
				callsA.push(Date.now());
			},
		);
		sb.job.handle(
			jobB,
			{
				trigger: { interval: 300 },
				overlap: "allow",
				maxConcurrent: 5,
			},
			async () => {
				callsB.push(Date.now());
			},
		);
		await sb.start();
		await waitFor(
			() => sb!.identity() !== null,
			5_000,
			"first start connected",
		);

		// Wait for both to fire at least once.
		await waitFor(
			() => callsA.length >= 1 && callsB.length >= 1,
			10_000,
			"both jobs fired",
		);

		await sb.stop();
		sb = undefined;
		await sleep(200);

		// Reset call arrays for the second phase.
		callsA.length = 0;
		callsB.length = 0;

		// Second start: only register A.
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobA,
			{
				trigger: { interval: 300 },
				overlap: "allow",
				maxConcurrent: 5,
			},
			async () => {
				callsA.push(Date.now());
			},
		);
		// jobB intentionally NOT registered.
		await sb.start();
		await waitFor(
			() => sb!.identity() !== null,
			5_000,
			"second start connected",
		);

		// Wait enough time for A to fire several times.
		await waitFor(() => callsA.length >= 2, 10_000, "A fires after restart");

		// B must never fire.
		expect(callsB).toHaveLength(0);
	}, 60_000);
});
