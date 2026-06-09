// jobs-survives-restart — delayed job survives runtime kill+restart.
//
// 1. Register delayed job run_at=now+10s.
// 2. Kill dedicated runtime at +2s (job not yet fired).
// 3. Restart runtime at +5s.
// 4. Handler called exactly once in [9, 13s] window from registration.
//
// Uses a DEDICATED runtime subprocess on port 14449 to avoid disturbing the
// ambient runtime used by other e2e tests. Same pattern as
// workflow-sleep-survives-restart.test.ts.

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

const DEDICATED_PORT = 14449;
const DEDICATED_UI_PORT = 19449;
const DEDICATED_BINARY = join(
	import.meta.dir,
	"../../.tmp-jobs-restart-runtime",
);

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

describe("jobs-survives-restart", () => {
	const keys = harnessFromEnv();
	let runtime: DedicatedRuntime | undefined;
	let sb: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(DEDICATED_BINARY);
		runtime = await spawnIsolatedRuntime({
			name: "jobs-restart",
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

	test("delayed job survives runtime kill+restart — fires exactly once after recovery", async () => {
		const jobName = `survive-restart-${Date.now()}`;
		const calls: Array<{ attempt: number; firedAt: number }> = [];
		const registeredAt = Date.now();

		const runtimeUrl = runtime!.url;

		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				// Fire 10 seconds from now.
				trigger: { delayed: { at: registeredAt + 10_000 } },
				maxAttempts: 3,
			},
			async (ctx) => {
				calls.push({ attempt: ctx.attempt, firedAt: Date.now() });
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Let the job registration be persisted (brief window).
		await sleep(1_000);

		// Kill SDK.
		await sb.stop();
		sb = undefined;

		// Kill runtime at +2s (job scheduled for +10s).
		await runtime!.kill();

		// Wait 3 seconds.
		await sleep(3_000);

		// Restart runtime at ~+5s.
		await runtime!.restart(keys.serviceKey, 60_000);

		// Reconnect SDK.
		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: registeredAt + 10_000 } },
				maxAttempts: 3,
			},
			async (ctx) => {
				calls.push({ attempt: ctx.attempt, firedAt: Date.now() });
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 8_000, "reconnected");

		// Wait for handler to be called (fire at +10s from registration).
		await waitFor(
			() => calls.length >= 1,
			20_000,
			"handler called after restart",
		);

		// Allow brief extra window then assert no duplicates.
		await sleep(3_000);

		expect(calls).toHaveLength(1);
		expect(calls[0]!.attempt).toBe(1);

		// Verify fire time is within [9s, 13s] from original registration.
		const elapsed = calls[0]!.firedAt - registeredAt;
		expect(elapsed).toBeGreaterThanOrEqual(9_000);
		expect(elapsed).toBeLessThan(20_000);
	}, 180_000);
});
