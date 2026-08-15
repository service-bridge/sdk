// jobs-lifecycle — destructive jobs e2e that kill/restart the runtime or need a
// settings override. Each describe block spawns its own ISOLATED runtime (own
// DB + ports) via spawnIsolatedRuntime, so the ambient runtime used by
// jobs.test.ts is never disturbed. Jobs are never pooled: each test owns a
// dedicated ServiceBridge against its block's isolated runtime and stop()s it.
//
// Runtime allocation:
//   - survives-restart: its own runtime (port 14449).
//   - catchup: ONE shared runtime (port 14448) for the fire_once + skip tests
//     (saves a go build + spawn); the runtime is restarted within each test and
//     job names are unique, so the two tests never collide.
//   - heartbeat-failover: its own runtime (port 14450) with the
//     jobs.heartbeat_* settings override the others must not have.
//
// Isolated DBs start empty and job names are unique, so no TRUNCATE is needed.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { unlinkSync } from "node:fs";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	buildDedicatedRuntime,
	type DedicatedRuntime,
	spawnIsolatedRuntime,
} from "./_helpers/dedicated-runtime";
import { sleep, uniqueName, waitFor } from "./_helpers/fixtures";

// Lifecycle tests own raw ServiceBridge instances against their own isolated
// runtime, so they read the per-domain bootstrap key directly (the same key
// pool.ts/dedicated() use) instead of the removed SERVICEBRIDGE_SERVICE_KEY.
// The isolated runtime seeds its services table from the main DB, so this
// domain identity is recognized there too.
function domainKeys(): { url: string; serviceKey: string } {
	const url = process.env.SERVICEBRIDGE_URL;
	if (!url) {
		throw new Error(
			"jobs-lifecycle: SERVICEBRIDGE_URL not set — run `bash scripts/bootstrap-e2e-keys.sh`",
		);
	}
	const domain = process.env.SB_E2E_DOMAIN;
	if (!domain) {
		throw new Error(
			"jobs-lifecycle: SB_E2E_DOMAIN not set — run with `SB_E2E_DOMAIN=jobs`",
		);
	}
	const envName = `SB_E2E_${domain.toUpperCase().replace(/-/g, "_")}_1`;
	const serviceKey = process.env[envName];
	if (!serviceKey) {
		throw new Error(
			`jobs-lifecycle: ${envName} not set — run \`bash scripts/bootstrap-e2e-keys.sh\``,
		);
	}
	return { url, serviceKey };
}

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

describe("jobs-lifecycle: survives runtime kill+restart", () => {
	const keys = domainKeys();
	const binary = join(import.meta.dir, "../../.tmp-jobs-restart-runtime");
	let runtime: DedicatedRuntime | undefined;
	let sb: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(binary);
		runtime = await spawnIsolatedRuntime({
			name: "jobs-restart",
			grpcPort: 14449,
			uiPort: 19449,
			bootstrapKey: keys.serviceKey,
			binaryPath: binary,
		});
	}, 120_000);

	afterAll(async () => {
		await sb?.stop().catch(() => {});
		await runtime?.cleanup();
		try {
			unlinkSync(binary);
		} catch {}
	}, 30_000);

	test("delayed job survives runtime kill+restart — fires exactly once in window", async () => {
		const jobName = uniqueName("survive-restart");
		const calls: Array<{ attempt: number; firedAt: number }> = [];
		const registeredAt = Date.now();
		const runtimeUrl = runtime!.url;

		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{ trigger: { delayed: { at: registeredAt + 10_000 } }, maxAttempts: 3 },
			async (ctx) => {
				calls.push({ attempt: ctx.attempt, firedAt: Date.now() });
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Let the job registration persist, then kill the SDK + runtime well
		// before fire time (job scheduled for +10s).
		await sleep(1_000);
		await sb.stop();
		sb = undefined;
		await runtime!.kill();

		// Runtime down for ~3s, then restart at ~+5s.
		await sleep(3_000);
		await runtime!.restart(keys.serviceKey, 60_000);

		// Reconnect the SDK with the same registration.
		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{ trigger: { delayed: { at: registeredAt + 10_000 } }, maxAttempts: 3 },
			async (ctx) => {
				calls.push({ attempt: ctx.attempt, firedAt: Date.now() });
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 8_000, "reconnected");

		await waitFor(
			() => calls.length >= 1,
			20_000,
			"handler called after restart",
		);
		// Extra window then assert no duplicates.
		await sleep(3_000);

		expect(calls).toHaveLength(1);
		expect(calls[0]!.attempt).toBe(1);

		const elapsed = calls[0]!.firedAt - registeredAt;
		expect(elapsed).toBeGreaterThanOrEqual(9_000);
		expect(elapsed).toBeLessThan(20_000);
	}, 180_000);
});

describe("jobs-lifecycle: catchup after a runtime-down gap", () => {
	const keys = domainKeys();
	const binary = join(import.meta.dir, "../../.tmp-jobs-catchup-runtime");
	let runtime: DedicatedRuntime | undefined;
	let sb: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(binary);
		runtime = await spawnIsolatedRuntime({
			name: "jobs-catchup",
			grpcPort: 14448,
			uiPort: 19448,
			bootstrapKey: keys.serviceKey,
			binaryPath: binary,
		});
	}, 120_000);

	afterAll(async () => {
		await sb?.stop().catch(() => {});
		await runtime?.cleanup();
		try {
			unlinkSync(binary);
		} catch {}
	}, 30_000);

	test("catchup=fire_once → exactly one backfill burst after gap", async () => {
		const jobName = uniqueName("catchup-fire-once");
		const callTimestamps: number[] = [];
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

		await waitFor(() => callTimestamps.length >= 1, 10_000, "baseline fire");
		const firesBefore = callTimestamps.length;

		// Stop SDK and kill runtime for 5s (3+ missed ticks with a 2s interval).
		await sb.stop();
		sb = undefined;
		await runtime!.kill();
		await sleep(5_000);
		await runtime!.restart(keys.serviceKey, 60_000);

		// Reconnect — measure the burst window relative to reconnect.
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

		await waitFor(
			() => callTimestamps.length >= firesBefore + 2,
			20_000,
			"catchup + fresh tick",
		);

		const afterReconnect = callTimestamps.filter((t) => t >= reconnectedAt);
		const earlyBurst = afterReconnect.filter((t) => t < reconnectedAt + 3_000);

		// fire_once: at least 1 burst call right after reconnect, never a large
		// backfill (fire_once + 1 fresh tick = ≤3).
		expect(earlyBurst.length).toBeGreaterThanOrEqual(1);
		expect(earlyBurst.length).toBeLessThanOrEqual(3);
	}, 200_000);

	test("catchup=skip → no backfill burst after gap", async () => {
		const jobName = uniqueName("catchup-skip");
		const callTimestamps: number[] = [];
		const scheduledFor: number[] = [];
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
			async (ctx) => {
				callTimestamps.push(Date.now());
				scheduledFor.push(ctx.scheduledAt.getTime());
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		await waitFor(() => callTimestamps.length >= 2, 10_000, "baseline fires");
		const firesBefore = callTimestamps.length;

		// Stop SDK and kill runtime; runtime down for 5s.
		await sb.stop();
		sb = undefined;
		await runtime!.kill();
		const gapStart = Date.now();
		await sleep(5_000);
		await runtime!.restart(keys.serviceKey, 60_000);

		sb = new ServiceBridge(runtimeUrl, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 500 },
				catchup: "skip",
				overlap: "allow",
				maxConcurrent: 10,
			},
			async (ctx) => {
				callTimestamps.push(Date.now());
				scheduledFor.push(ctx.scheduledAt.getTime());
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 8_000, "reconnected");
		const reconnectedAt = Date.now();

		const gapDurationMs = reconnectedAt - gapStart;
		const expectedMissed = Math.floor(gapDurationMs / 500);
		expect(expectedMissed).toBeGreaterThanOrEqual(8);

		await waitFor(
			() => callTimestamps.length > firesBefore + 1,
			15_000,
			"fresh ticks after restart",
		);

		// Judged by the tick each call was scheduled for, not by how many calls
		// arrived: a redelivery repeats one scheduled_at while a backfill replays
		// the ones missed during the gap, and only the second is what catchup=skip
		// forbids. Counting calls conflates them and turns machine load into a
		// failure.
		const replayed = scheduledFor.filter(
			(t) => t > gapStart && t < reconnectedAt,
		);
		expect(replayed).toEqual([]);
	}, 180_000);
});

describe("jobs-lifecycle: heartbeat timeout reclaim", () => {
	const keys = domainKeys();
	const binary = join(import.meta.dir, "../../.tmp-jobs-heartbeat-runtime");
	let runtime: DedicatedRuntime | undefined;
	let sbA: ServiceBridge | undefined;
	let sbB: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(binary);
		runtime = await spawnIsolatedRuntime({
			name: "hb-failover",
			grpcPort: 14450,
			uiPort: 19450,
			bootstrapKey: keys.serviceKey,
			binaryPath: binary,
			// Fast reclaim: 3s heartbeat timeout, 1s interval.
			extraSettings: {
				"jobs.heartbeat_timeout_ms": "3000",
				"jobs.heartbeat_interval_ms": "1000",
			},
		});
	}, 120_000);

	afterAll(async () => {
		await sbA?.stop().catch(() => {});
		await sbB?.stop().catch(() => {});
		await runtime?.cleanup();
		try {
			unlinkSync(binary);
		} catch {}
	}, 30_000);

	test("first replica crashes mid-execution → other replica gets attempt===2 with same idempotencyKey", async () => {
		const jobName = uniqueName("hb-failover");
		const callsOnA: Array<{ attempt: number; idempotencyKey: string }> = [];
		const callsOnB: Array<{ attempt: number; idempotencyKey: string }> = [];
		const runtimeUrl = runtime!.url;

		// Both replicas register the same job. The dispatcher picks one
		// non-deterministically; whichever gets the first execution is the
		// "stuck" one we kill.
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

		// Kill the SDK holding the execution → heartbeats stop → reclaim sweep.
		if (firstReceiver === "A") {
			await sbA.stop();
			sbA = undefined;
		} else {
			await sbB.stop();
			sbB = undefined;
		}

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

// SDK fully down before fire time → a fresh replica must pick up the pending
// execution exactly once. Runs on its own isolated runtime so the overdue
// delivery window is deterministic: on the ambient runtime the rest of the jobs
// suite leaves interval/cron jobs firing for the whole process, which saturate
// the dispatcher and make this failover timing flaky.
describe("jobs-lifecycle: SDK fully down before fire → fresh replica delivered once", () => {
	const keys = domainKeys();
	const binary = join(import.meta.dir, "../../.tmp-jobs-durability-runtime");
	let runtime: DedicatedRuntime | undefined;
	let first: ServiceBridge | undefined;
	let second: ServiceBridge | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(binary);
		runtime = await spawnIsolatedRuntime({
			name: "jobs-durability",
			grpcPort: 14451,
			uiPort: 19451,
			bootstrapKey: keys.serviceKey,
			binaryPath: binary,
		});
	}, 120_000);

	afterAll(async () => {
		await first?.stop().catch(() => {});
		await second?.stop().catch(() => {});
		await runtime?.cleanup();
		try {
			unlinkSync(binary);
		} catch {}
	}, 30_000);

	test("fresh replica delivers the overdue execution exactly once", async () => {
		const url = runtime!.url;
		const jobName = uniqueName("recover");
		const calls: Array<{ attempt: number }> = [];

		first = new ServiceBridge(url, keys.serviceKey, FAST_OPTS);
		first.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 2_000 } }, maxAttempts: 3 },
			async (ctx) => {
				calls.push({ attempt: ctx.attempt });
			},
		);
		await first.start();
		await waitFor(() => first!.identity() !== null, 5_000, "first connected");

		// Kill the replica before fire time (stop at +0.5s, fire at +2s).
		await sleep(500);
		await first.stop();
		first = undefined;

		// Stay down until well after fire time.
		await sleep(5_000);

		// Fresh replica picks up the pending (now overdue) execution.
		second = new ServiceBridge(url, keys.serviceKey, FAST_OPTS);
		second.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 60_000 } }, maxAttempts: 3 },
			async (ctx) => {
				calls.push({ attempt: ctx.attempt });
			},
		);
		await second.start();
		await waitFor(() => second!.identity() !== null, 8_000, "second connected");

		await waitFor(() => calls.length >= 1, 20_000, "handler called");
		await sleep(2_000);

		// At-least-once: exactly one invocation. attempt is 1 (clean) or 2 (the
		// dispatcher momentarily picked the disconnecting old instance, push
		// failed, re-claim bumped attempt) — both honour at-least-once.
		expect(calls).toHaveLength(1);
		expect(calls[0]!.attempt).toBeGreaterThanOrEqual(1);
		expect(calls[0]!.attempt).toBeLessThanOrEqual(2);
	}, 60_000);
});

// A JobResult carrying a stale lease_epoch (the runtime reclaimed the lease
// mid-flight) must be rejected, not settle the execution, and the execution
// must be re-dispatched as attempt 2. Runs on its own isolated runtime: the
// reclaim window is deterministic only without the sibling interval/cron jobs
// the rest of the jobs suite leaves firing. Pokes job_executions directly in
// the isolated runtime's DB via runtime.dbUrl.
describe("jobs-lifecycle: stale lease_epoch rejected → execution re-dispatched", () => {
	const keys = domainKeys();
	const binary = join(import.meta.dir, "../../.tmp-jobs-stalelease-runtime");
	let runtime: DedicatedRuntime | undefined;
	let sb: ServiceBridge | undefined;
	let db: Bun.SQL | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(binary);
		runtime = await spawnIsolatedRuntime({
			name: "jobs-stalelease",
			grpcPort: 14452,
			uiPort: 19452,
			bootstrapKey: keys.serviceKey,
			binaryPath: binary,
		});
		db = new Bun.SQL(runtime.dbUrl);
	}, 120_000);

	afterAll(async () => {
		await sb?.stop().catch(() => {});
		await db?.close().catch(() => {});
		await runtime?.cleanup();
		try {
			unlinkSync(binary);
		} catch {}
	}, 30_000);

	test("stale ACK is not settled and the execution is re-dispatched as attempt 2", async () => {
		const jobName = uniqueName("stale-ack");
		const calls: number[] = [];
		let unblock: (() => void) | undefined;
		const blocking = new Promise<void>((r) => {
			unblock = r;
		});

		sb = new ServiceBridge(runtime!.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 500 } },
				maxAttempts: 5,
				leaseTtlMs: 3_000,
			},
			async (ctx) => {
				calls.push(ctx.attempt);
				if (ctx.attempt === 1) await blocking;
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		const runningId = async (): Promise<string | null> => {
			const rows = (await db!`
				SELECT e.id FROM job_executions e
				JOIN job_definitions d ON d.id = e.job_definition_id
				WHERE d.name = ${jobName} AND e.status = 'in_flight'
				LIMIT 1
			`) as Array<{ id: string }>;
			return rows[0]?.id ?? null;
		};

		await waitFor(
			async () => (await runningId()) !== null,
			10_000,
			"execution in_flight",
		);
		const execId = await runningId();
		expect(execId).not.toBeNull();

		// Bump lease_epoch to simulate a runtime reclaim mid-flight.
		await db!`UPDATE job_executions SET lease_epoch = lease_epoch + 1 WHERE id = ${execId}`;

		// Unblock — the SDK now ACKs with the OLD (stale) epoch.
		unblock!();
		await sleep(2_000);

		const latestStatus = async (): Promise<string | null> => {
			const rows = (await db!`
				SELECT e.status FROM job_executions e
				JOIN job_definitions d ON d.id = e.job_definition_id
				WHERE d.name = ${jobName}
				ORDER BY e.scheduled_at DESC
				LIMIT 1
			`) as Array<{ status: string }>;
			return rows[0]?.status ?? null;
		};
		// Stale ACK must NOT mark success; still in_flight or reclaimed to pending.
		expect(await latestStatus()).not.toBe("success");

		await waitFor(
			() => calls.length >= 2,
			30_000,
			"re-dispatched as attempt 2",
		);
		expect(calls[1]).toBeGreaterThanOrEqual(2);
	}, 90_000);
});
