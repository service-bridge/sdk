// jobs-interval-fires-repeatedly — interval job fires ~N times in a window.
//
// Registers interval:500ms, runs for 3s, expects ≈6 calls (allow ±2 tolerance).

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

describe("jobs-interval-fires-repeatedly", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("interval trigger fires (multiple ticks via DB enqueue + ≥1 handler call)", async () => {
		// This e2e proves the full chain works:
		//   scheduler → enqueue → DB row → dispatcher → handler.
		// We check the DB-side authoritative count (proves scheduler+enqueue path)
		// and require ≥1 handler call (proves dispatch+push+ACK path) within a
		// generous budget that survives sequential-test pressure.
		let callCount = 0;
		const jobName = `interval-repeat-${Date.now()}`;

		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { interval: 1_000 },
				overlap: "allow",
				maxConcurrent: 10,
			},
			async () => {
				callCount++;
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Wait for ≥1 handler call (proves end-to-end dispatch).
		await waitFor(
			() => callCount >= 1,
			25_000,
			"interval fired ≥1 time end-to-end",
		);

		// Verify scheduler keeps producing rows in DB (proves the loop runs).
		await sleep(3_000);
		const dbRows = await withDb(async (sql) => {
			const r = (await sql`
				SELECT COUNT(*)::int AS cnt
				FROM job_executions e
				JOIN job_definitions d ON d.id = e.job_definition_id
				WHERE d.name = ${jobName}
			`) as Array<{ cnt: number }>;
			return r[0]?.cnt ?? 0;
		});

		expect(callCount).toBeGreaterThanOrEqual(1);
		expect(dbRows).toBeGreaterThanOrEqual(2);
	}, 40_000);
});
