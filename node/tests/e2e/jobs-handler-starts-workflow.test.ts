// jobs-handler-starts-workflow — job handler starts a workflow; run succeeds.
//
// Service 1 fires a delayed job. Inside handler it calls sb.workflow.start().
// The workflow run completes successfully.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { harnessFromEnv } from "./_helpers/harness";
import { clearRules, withDb } from "./_helpers/policy-db";
import { addWorkflowRule, awaitRunStatus, FAST_WF_OPTS } from "./_helpers/wf";

async function cleanupJobState(): Promise<void> {
	await withDb(async (sql) => {
		await sql`TRUNCATE workflow_timers, workflow_signals, workflow_steps, workflow_runs, job_executions, job_schedules, job_definitions, jobs_dlq CASCADE`;
	});
}

describe("jobs-handler-starts-workflow", () => {
	const keys = harnessFromEnv({ needSecond: true });
	let sb1: ServiceBridge | undefined;
	let sb2: ServiceBridge | undefined;
	let sb1Id: string | undefined;
	let sb2Id: string | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb1?.stop().catch(() => {});
		await sb2?.stop().catch(() => {});
		if (sb1Id) await clearRules(sb1Id).catch(() => {});
		if (sb2Id) await clearRules(sb2Id).catch(() => {});
		sb1 = undefined;
		sb2 = undefined;
	});

	test("job handler starts workflow → run completes", async () => {
		const jobName = `handler-wf-${Date.now()}`;
		const wfName = `job-wf-${Date.now()}`;
		let runId: string | undefined;
		let handlerCalled = false;

		// Service 2 — workflow owner.
		sb2 = new ServiceBridge(keys.url, keys.service2Key!, FAST_WF_OPTS);
		sb2.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step1",
					fn: async () => ({ ok: true }),
				},
			],
		});
		await sb2.start();
		await waitFor(() => sb2!.identity() !== null, 5_000, "sb2 connected");
		sb2Id = sb2.identity()!.serviceId;

		// Service 1 — job owner + workflow caller.
		sb1 = new ServiceBridge(keys.url, keys.serviceKey, FAST_WF_OPTS);

		sb1.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 800 } },
				maxAttempts: 1,
			},
			async (_ctx) => {
				const result = await sb1!.workflow.start(wfName, {});
				runId = result.runId;
				handlerCalled = true;
			},
		);

		await sb1.start();
		await waitFor(() => sb1!.identity() !== null, 5_000, "sb1 connected");
		sb1Id = sb1.identity()!.serviceId;

		// Grant policy: sb1 can start workflow on sb2.
		await addWorkflowRule(sb1Id, sb2Id, wfName);
		await sleep(500);

		await waitFor(() => handlerCalled, 15_000, "job handler executed");
		expect(runId).toMatch(/^[0-9a-f-]{36}$/);

		const finalStatus = await awaitRunStatus(
			sb1,
			runId!,
			(s) => s === "success" || s === "failed",
			20_000,
		);
		expect(finalStatus).toBe("success");
	}, 60_000);
});
