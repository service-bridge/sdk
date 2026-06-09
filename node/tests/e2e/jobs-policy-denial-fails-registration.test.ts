// jobs-policy-denial-fails-registration — cap_job_handle=false → registration fails.
//
// Disables cap_job_handle for the test service in Postgres. Attempts to start
// the SDK. The SDK's registerJobs call should fail; verify via timeout on
// job handler never firing.
//
// Note: the policy check happens in RegisterJobs (gRPC), called from
// maybeStartJobSubscriber on Welcome. The SDK logs a warning but does not
// surface as a start() rejection. Instead we verify by observing no handler
// calls for the duration, and check the DB that no job_definitions row was inserted.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { harnessFromEnv } from "./_helpers/harness";
import { setCapability, withDb } from "./_helpers/policy-db";

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

async function getJobDefCount(): Promise<number> {
	return withDb(async (sql) => {
		const rows =
			(await sql`SELECT COUNT(*)::int AS cnt FROM job_definitions`) as Array<{
				cnt: number;
			}>;
		return rows[0]?.cnt ?? 0;
	});
}

describe("jobs-policy-denial-fails-registration", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;
	let serviceId: string | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		// Restore capability before cleanup.
		if (serviceId) {
			await setCapability(serviceId, "cap_job_handle", true).catch(() => {});
		}
		await sb?.stop().catch(() => {});
		sb = undefined;
		serviceId = undefined;
	});

	test("cap_job_handle=false → no job_definitions inserted, handler never fires", async () => {
		let handlerCalled = false;
		const jobName = `denied-job-${Date.now()}`;

		// Start once to obtain serviceId, then disable capability.
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 500 } },
			},
			async () => {
				handlerCalled = true;
			},
		);
		await sb.start();
		await waitFor(() => sb!.identity() !== null, 5_000, "connected first time");
		serviceId = sb.identity()!.serviceId;

		await sb.stop();
		sb = undefined;
		await sleep(200);
		await cleanupJobState();

		// Disable job handle capability.
		await setCapability(serviceId, "cap_job_handle", false);

		// Start again — registerJobs will be denied.
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 500 } },
			},
			async () => {
				handlerCalled = true;
			},
		);
		await sb.start();
		await waitFor(
			() => sb!.identity() !== null,
			5_000,
			"connected second time",
		);

		// Wait beyond fire time.
		await sleep(5_000);

		// Handler must not have been called because registration was denied.
		expect(handlerCalled).toBe(false);
		// No job_definitions row should exist.
		expect(await getJobDefCount()).toBe(0);
	}, 30_000);
});
