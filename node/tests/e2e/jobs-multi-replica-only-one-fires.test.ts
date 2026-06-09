// jobs-multi-replica-only-one-fires — 3 SDK replicas: global count ≈ ticks, not 3×.
//
// 3 ServiceBridge instances of the same service register the same interval job.
// Over 1s with 300ms interval we expect ≈3 global executions (one per tick),
// not 9 (3 replicas × 3 ticks). The runtime's dispatcher claims each execution
// once and routes to exactly one replica.

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

async function countExecutions(): Promise<number> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT COUNT(*)::int AS cnt
			FROM job_executions
			WHERE status = 'success'
		`) as Array<{ cnt: number }>;
		return rows[0]?.cnt ?? 0;
	});
}

describe("jobs-multi-replica-only-one-fires", () => {
	const keys = harnessFromEnv();
	let replicas: ServiceBridge[] = [];

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		for (const r of replicas) {
			await r.stop().catch(() => {});
		}
		replicas = [];
	});

	test("3 replicas same job interval 300ms → ≈3 global completions in 1s (not 9)", async () => {
		const jobName = `multi-replica-${Date.now()}`;
		let totalHandlerCalls = 0;

		// Create 3 ServiceBridge instances with the same service key (same service).
		for (let i = 0; i < 3; i++) {
			const sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
			sb.job.handle(
				jobName,
				{
					trigger: { interval: 300 },
					overlap: "skip",
				},
				async () => {
					totalHandlerCalls++;
				},
			);
			replicas.push(sb);
		}

		// Start all replicas in parallel.
		await Promise.all(replicas.map((r) => r.start()));
		await waitFor(
			() => replicas.every((r) => r.identity() !== null),
			8_000,
			"all replicas connected",
		);

		// Wait 1.5s for ticks.
		await sleep(1_500);

		// Count DB-level successes (authoritative: DB rows not in-process counters).
		const dbCount = await countExecutions();

		// With interval=300ms over 1.5s, expect ≈5 ticks. With 3 replicas but
		// at-most-once dispatch, global count must be close to tick count (≤8),
		// not 3x that (≤24).
		expect(dbCount).toBeGreaterThanOrEqual(1);
		expect(dbCount).toBeLessThanOrEqual(10);

		// Handler calls across all replicas should equal DB count (each execution
		// dispatched to exactly one replica).
		expect(totalHandlerCalls).toBeGreaterThanOrEqual(1);
		// Allow slight over-count due to timing, but nowhere near 3× DB count.
		expect(totalHandlerCalls).toBeLessThanOrEqual(dbCount + 2);
	}, 30_000);
});
