// jobs-replica-crash-failover-via-heartbeat — heartbeat timeout triggers reclaim.
//
// Two SDK replicas A and B. Replica B holds a long-running job execution.
// The dedicated runtime is started with SERVICEBRIDGE_JOBS_HEARTBEAT_TIMEOUT=3s
// so the reclaim sweep fires quickly. We stop replica B's SDK (simulating a crash)
// so it stops sending heartbeats. After HeartbeatTimeout, the runtime reclaims
// the execution and re-dispatches to replica A (attempt===2, same idempotencyKey).
//
// Uses a DEDICATED runtime subprocess on port 14450 with env override for
// fast heartbeat timeout.

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

const DEDICATED_PORT = 14450;
const DEDICATED_UI_PORT = 19450;
const DEDICATED_BINARY = join(
	import.meta.dir,
	"../../.tmp-jobs-heartbeat-runtime",
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

describe("jobs-replica-crash-failover-via-heartbeat", () => {
	const keys = harnessFromEnv();
	let runtime: DedicatedRuntime | undefined;
	let sbA: ServiceBridge | undefined;
	let sbB: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(DEDICATED_BINARY);
		runtime = await spawnIsolatedRuntime({
			name: "hb-failover",
			grpcPort: DEDICATED_PORT,
			uiPort: DEDICATED_UI_PORT,
			bootstrapKey: keys.serviceKey,
			binaryPath: DEDICATED_BINARY,
			// heartbeat timeout 3s + interval 1s so reclaim fires fast in the test.
			extraSettings: {
				"jobs.heartbeat_timeout_ms": "3000",
				"jobs.heartbeat_interval_ms": "1000",
			},
		});
		await cleanupJobState();
	}, 120_000);

	afterAll(async () => {
		await sbA?.stop().catch(() => {});
		await sbB?.stop().catch(() => {});
		await runtime?.cleanup();
		try {
			import("node:fs").then((fs) => fs.unlinkSync(DEDICATED_BINARY));
		} catch {}
	}, 30_000);

	test("first replica crashes mid-execution → after heartbeat timeout runtime reclaims → second replica gets attempt===2", async () => {
		const jobName = `hb-failover-${Date.now()}`;
		const callsOnA: Array<{ attempt: number; idempotencyKey: string }> = [];
		const callsOnB: Array<{ attempt: number; idempotencyKey: string }> = [];

		const runtimeUrl = runtime!.url;

		// Both replicas register the same job. The dispatcher picks one
		// non-deterministically — whichever receives the first execution
		// is treated as the "stuck" one we kill.
		sbA = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sbA.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 1_000 } },
				maxAttempts: 3,
				leaseTtlMs: 30_000,
			},
			async (ctx) => {
				callsOnA.push({
					attempt: ctx.attempt,
					idempotencyKey: ctx.idempotencyKey,
				});
				if (ctx.attempt === 1) await sleep(60_000);
			},
		);
		await sbA.start();
		await waitFor(() => sbA!.identity() !== null, 5_000, "replica A connected");

		sbB = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sbB.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 1_000 } },
				maxAttempts: 3,
				leaseTtlMs: 30_000,
			},
			async (ctx) => {
				callsOnB.push({
					attempt: ctx.attempt,
					idempotencyKey: ctx.idempotencyKey,
				});
				if (ctx.attempt === 1) await sleep(60_000);
			},
		);
		await sbB.start();
		await waitFor(() => sbB!.identity() !== null, 5_000, "replica B connected");

		// Wait for either replica to receive attempt===1.
		await waitFor(
			() => callsOnA.length >= 1 || callsOnB.length >= 1,
			15_000,
			"any replica received execution",
		);

		const firstReceiver: "A" | "B" =
			callsOnA.length >= 1 && callsOnA[0]!.attempt === 1 ? "A" : "B";
		const otherReceiver = firstReceiver === "A" ? "B" : "A";
		const idempotencyKey =
			firstReceiver === "A"
				? callsOnA[0]!.idempotencyKey
				: callsOnB[0]!.idempotencyKey;

		// Kill the SDK that's holding the execution → heartbeats stop → reclaim.
		if (firstReceiver === "A") {
			await sbA.stop();
			sbA = undefined;
		} else {
			await sbB.stop();
			sbB = undefined;
		}

		// Wait for the other replica to receive attempt===2.
		await waitFor(
			() =>
				otherReceiver === "A"
					? callsOnA.some((c) => c.attempt === 2)
					: callsOnB.some((c) => c.attempt === 2),
			25_000,
			`${otherReceiver} received reclaimed execution`,
		);

		const reclaimedCall = (otherReceiver === "A" ? callsOnA : callsOnB).find(
			(c) => c.attempt === 2,
		);
		expect(reclaimedCall).toBeDefined();
		expect(reclaimedCall!.idempotencyKey).toBe(idempotencyKey);
	}, 120_000);
});
