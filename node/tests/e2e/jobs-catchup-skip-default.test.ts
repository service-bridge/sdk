// jobs-catchup-skip-default — catchup=skip: no catch-up calls after restart gap.
//
// Register an interval job with catchup=skip. Kill the dedicated runtime for 5s.
// After restart, only future ticks fire — no "missed" backfill calls.
//
// Uses a dedicated runtime subprocess so the ambient runtime on :14445 is untouched.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	buildDedicatedRuntime,
	type DedicatedRuntime,
	spawnIsolatedRuntime,
} from "./_helpers/dedicated-runtime";
import { sleep, waitFor } from "./_helpers/events";
import { harnessFromEnv } from "./_helpers/harness";
import { withDb } from "./_helpers/policy-db";

const DEDICATED_PORT = 14447;
const DEDICATED_UI_PORT = 19447;
const DEDICATED_BINARY = join(
	import.meta.dir,
	"../../.tmp-catchup-skip-runtime",
);

async function cleanupJobState(): Promise<void> {
	await withDb(async (sql) => {
		await sql`TRUNCATE job_executions, job_schedules, job_definitions, jobs_dlq CASCADE`;
	});
}

describe("jobs-catchup-skip-default", () => {
	const keys = harnessFromEnv();
	let runtime: DedicatedRuntime | undefined;
	let sb: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(DEDICATED_BINARY);
		runtime = await spawnIsolatedRuntime({
			name: "catchup-skip",
			grpcPort: DEDICATED_PORT,
			uiPort: DEDICATED_UI_PORT,
			bootstrapKey: keys.serviceKey,
			binaryPath: DEDICATED_BINARY,
		});
		await cleanupJobState();
	}, 120_000);

	afterAll(async () => {
		await sb?.stop().catch(() => {});
		await runtime?.cleanup();
		try {
			import("node:fs").then((fs) => fs.unlinkSync(DEDICATED_BINARY));
		} catch {}
	}, 30_000);

	test("catchup=skip: no backfill calls after 5s gap; only future ticks", async () => {
		const jobName = `catchup-skip-${Date.now()}`;
		const callTimestamps: number[] = [];

		const FAST_OPTS = {
			reconnectIntervalMs: 500,
			reconnectAttempts: 3,
			certRefreshLeadMs: 60 * 60 * 1000,
		} as const;

		const runtimeUrl = runtime!.url;

		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 500 },
				catchup: "skip",
				overlap: "allow",
				maxConcurrent: 10,
			},
			async () => {
				callTimestamps.push(Date.now());
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Let it fire a couple times to establish baseline.
		await waitFor(() => callTimestamps.length >= 2, 10_000, "baseline fires");
		const firesBefore = callTimestamps.length;

		// Stop SDK and kill runtime.
		await sb.stop();
		sb = undefined;
		await runtime!.kill();

		const gapStart = Date.now();

		// Wait 5 seconds with runtime down.
		await sleep(5_000);

		// Restart runtime.
		await runtime!.restart(keys.serviceKey, 60_000);

		// Reconnect SDK.
		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 500 },
				catchup: "skip",
				overlap: "allow",
				maxConcurrent: 10,
			},
			async () => {
				callTimestamps.push(Date.now());
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 8_000, "reconnected");

		const gapDurationMs = Date.now() - gapStart;
		const expectedMissed = Math.floor(gapDurationMs / 500);

		// Let some fresh ticks fire.
		await waitFor(
			() => callTimestamps.length > firesBefore + 1,
			15_000,
			"fresh ticks after restart",
		);

		// With catchup=skip, calls after restart should NOT be a backfill burst.
		// If catchup fired, we'd see many calls clustered at restart time.
		// Verify: total extra calls after restart ≤ ~4 (not a backfill of expectedMissed).
		const firesAfter = callTimestamps.length - firesBefore;
		const budgetWithNoBackfill = 4; // fresh ticks in observation window
		const backfillThreshold = Math.max(
			budgetWithNoBackfill,
			expectedMissed / 2,
		);

		// firesAfter must be significantly less than what a backfill would produce.
		expect(firesAfter).toBeLessThan(backfillThreshold);
	}, 180_000);
});
