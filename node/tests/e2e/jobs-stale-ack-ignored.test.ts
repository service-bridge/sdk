// jobs-stale-ack-ignored — stale lease_epoch in JobResult → server returns Accepted:false.
//
// This test verifies that the runtime's stale-ack detection works end-to-end.
// We force a lease reclaim by directly updating job_executions in Postgres to
// bump lease_epoch, then verify via the jobs_dlq or via a direct DB query that
// a subsequent JobResult with the old epoch is rejected.
//
// Since the SDK does not expose JobResult directly (it's sent internally by
// JobSubscriber after handler completion), we test the invariant from the DB side:
//   1. Register a delayed job.
//   2. Wait for execution to be dispatched (status=running in job_executions).
//   3. Directly bump lease_epoch in DB (simulates the runtime reclaiming the lease).
//   4. Let the SDK handler complete and send JobResult with the old epoch.
//   5. Query DB — execution must NOT be in status=succeeded (epoch mismatch → rejected).
//      The runtime will re-dispatch with new epoch.
//
// This test exercises the stale-ack path without needing direct gRPC access.

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

async function getRunningExecution(jobName: string): Promise<{
	id: string;
	leaseEpoch: number;
} | null> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT e.id, e.lease_epoch
			FROM job_executions e
			JOIN job_definitions d ON d.id = e.job_definition_id
			WHERE d.name = ${jobName}
			  AND e.status = 'in_flight'
			LIMIT 1
		`) as Array<{ id: string; lease_epoch: number }>;
		if (!rows[0]) return null;
		return { id: rows[0].id, leaseEpoch: rows[0].lease_epoch };
	});
}

async function bumpLeaseEpoch(executionId: string): Promise<void> {
	await withDb(async (sql) => {
		await sql`
			UPDATE job_executions
			SET lease_epoch = lease_epoch + 1
			WHERE id = ${executionId}
		`;
	});
}

async function getExecutionStatus(jobName: string): Promise<string | null> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT e.status
			FROM job_executions e
			JOIN job_definitions d ON d.id = e.job_definition_id
			WHERE d.name = ${jobName}
			ORDER BY e.scheduled_at DESC
			LIMIT 1
		`) as Array<{ status: string }>;
		return rows[0]?.status ?? null;
	});
}

describe("jobs-stale-ack-ignored", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("stale lease_epoch in JobResult → execution not settled, re-dispatched", async () => {
		const jobName = `stale-ack-${Date.now()}`;
		const calls: number[] = [];
		let handlerBlocker: (() => void) | undefined;
		const handlerBlocking = new Promise<void>((resolve) => {
			handlerBlocker = resolve;
		});

		// Handler blocks until we bump lease_epoch and then unblock.
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 500 } },
				maxAttempts: 5,
				leaseTtlMs: 3_000,
			},
			async (ctx) => {
				calls.push(ctx.attempt);
				if (ctx.attempt === 1) {
					// Block so the execution stays in running state while we bump epoch.
					await handlerBlocking;
				}
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected");

		// Wait for execution to enter running state.
		await waitFor(
			async () => {
				const exec = await getRunningExecution(jobName);
				return exec !== null;
			},
			10_000,
			"execution running",
		);

		// Bump lease_epoch in DB to simulate runtime reclaim.
		const exec = await getRunningExecution(jobName);
		expect(exec).not.toBeNull();
		await bumpLeaseEpoch(exec!.id);

		// Unblock the handler — SDK sends JobResult with OLD (stale) epoch.
		handlerBlocker!();

		// Wait for the stale result to be processed.
		await sleep(2_000);

		// The execution must NOT be in succeeded state because the epoch was stale.
		// It should be back to pending (re-dispatched) or still running.
		const status = await getExecutionStatus(jobName);
		// Stale ACK must NOT mark execution completed; row is still in_flight
		// (lease not expired yet) or already pending (lease reclaimed).
		expect(status).not.toBe("success");

		// Wait for re-dispatch with new epoch (attempt===2).
		// Lease TTL=3s + reclaim sweep cycle (≤5s) + revert backoff (1s) +
		// next claim ≈ 10s worst case; budget 30s.
		await waitFor(
			() => calls.length >= 2,
			30_000,
			"re-dispatched with attempt===2",
		);
		expect(calls[1]).toBeGreaterThanOrEqual(2);
	}, 60_000);
});
