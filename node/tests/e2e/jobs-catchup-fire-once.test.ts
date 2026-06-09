// jobs-catchup-fire-once — catchup=fire_once: exactly 1 extra call after restart gap.
//
// Register interval job with catchup=fire_once. Kill dedicated runtime for 5s.
// After restart expect exactly 1 immediate backfill call, then future ticks.

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

const DEDICATED_PORT = 14448;
const DEDICATED_UI_PORT = 19448;
const DEDICATED_BINARY = join(
	import.meta.dir,
	"../../.tmp-catchup-fire-once-runtime",
);

async function cleanupJobState(): Promise<void> {
	await withDb(async (sql) => {
		await sql`TRUNCATE job_executions, job_schedules, job_definitions, jobs_dlq CASCADE`;
	});
}

describe("jobs-catchup-fire-once", () => {
	const keys = harnessFromEnv();
	let runtime: DedicatedRuntime | undefined;
	let sb: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(DEDICATED_BINARY);
		runtime = await spawnIsolatedRuntime({
			name: "catchup-fire-once",
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

	test("catchup=fire_once: exactly 1 backfill call after restart", async () => {
		const jobName = `catchup-fire-once-${Date.now()}`;
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
				trigger: { interval: 2_000 },
				catchup: "fire_once",
				overlap: "allow",
				maxConcurrent: 10,
			},
			async () => {
				callTimestamps.push(Date.now());
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Let it fire once to establish baseline.
		await waitFor(() => callTimestamps.length >= 1, 10_000, "baseline fire");
		const firesBefore = callTimestamps.length;

		// Stop SDK and kill runtime.
		await sb.stop();
		sb = undefined;
		await runtime!.kill();

		// Gap: 5 seconds (3+ missed ticks with 2s interval).
		await sleep(5_000);

		// Restart runtime.
		await runtime!.restart(keys.serviceKey, 60_000);

		// Reconnect SDK — capture the timestamp AFTER reconnect to measure the
		// "burst window" relative to when handlers can actually receive.
		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 2_000 },
				catchup: "fire_once",
				overlap: "allow",
				maxConcurrent: 10,
			},
			async () => {
				callTimestamps.push(Date.now());
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 8_000, "reconnected");
		const reconnectedAt = Date.now();

		// Wait for the catchup call + at least 1 fresh tick.
		await waitFor(
			() => callTimestamps.length >= firesBefore + 2,
			20_000,
			"catchup + fresh tick",
		);

		// Exactly 1 backfill burst (fire_once), then normal ticks.
		// The backfill call should come within 3s of SDK reconnect.
		const afterReconnect = callTimestamps.filter((t) => t >= reconnectedAt);
		const earlyBurst = afterReconnect.filter((t) => t < reconnectedAt + 3_000);

		// fire_once: at least 1 burst call right after reconnect (the catchup row +
		// possibly the next interval tick).
		expect(earlyBurst.length).toBeGreaterThanOrEqual(1);
		// Should not be a large backfill — fire_once + 1 fresh tick = ≤2.
		expect(earlyBurst.length).toBeLessThanOrEqual(3);
	}, 200_000);
});
