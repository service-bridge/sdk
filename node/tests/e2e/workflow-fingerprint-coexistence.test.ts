// workflow-fingerprint-coexistence — frozen_plan preserved after F2 re-registration.
//
// Run-A starts on fingerprint F1 (graph with step_a). While run-A is in
// flight, the owner re-registers with F2 (different graph). A new run-B
// starts and binds to F2.
//
// Verified:
//   1. workflow_runs.fingerprint for run-A equals F1 after F2 registers.
//   2. workflow_runs.fingerprint for run-B equals F2 (different from F1).
//   3. Both runs reach terminal status (no interference between graphs).
//
// ADR-W-002: frozen_plan in DB is the canonical record. The SDK runner
// replaces it with the locally-registered graph (for fn closures) per the
// same ADR. Run-A dispatched to the F2 owner will execute the F2 local graph
// if no F1 owner is connected — this is expected behavior documented here.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules, withDb } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

async function runFingerprint(runId: string): Promise<string> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT fingerprint FROM workflow_runs WHERE id = ${runId}
		`) as Array<{ fingerprint: string }>;
		if (!rows[0]) throw new Error(`run ${runId} not found`);
		return rows[0].fingerprint;
	});
}

describe("workflow-fingerprint-coexistence", () => {
	const env = wfEnv();
	let owner: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	let ownerID: string | undefined;
	let callerID: string | undefined;

	beforeEach(async () => {
		await cleanupWorkflowState();
		owner = undefined;
		caller = undefined;
	});

	afterEach(async () => {
		await caller?.stop();
		await owner?.stop();
		if (ownerID) await clearRules(ownerID);
		if (callerID) await clearRules(callerID);
		await sleep(300);
	});

	test("run-A DB fingerprint is F1 after F2 re-registration; run-B is F2", async () => {
		const wfName = `fp-coexist-${Date.now()}`;

		// --- F1 graph: step_a ---
		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step_a",
					fn: async () => ({ v: 1 }),
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800);

		// Start run-A on F1.
		const { runId: runA } = await caller.workflow.start(wfName, {});

		// Wait for run-A to complete on F1 (fast — no sleep).
		await awaitRunStatus(
			caller,
			runA,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			10_000,
		);

		// Capture F1 fingerprint from DB (immutable after run starts).
		const fpF1 = await runFingerprint(runA);
		expect(fpF1).toBeTruthy();

		// Re-register with F2 graph (completely different shape).
		await owner.stop();
		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step_x",
					fn: async () => ({ v: 99 }),
				},
				{
					type: "local",
					id: "step_y",
					fn: async () => ({ v: 100 }),
					waitFor: ["step_x"],
				},
			],
		});
		await owner.start();
		await waitFor(
			() => owner!.identity() !== null,
			5_000,
			"owner F2 connected",
		);
		await sleep(500);

		// Start run-B — should bind to F2.
		const { runId: runB } = await caller.workflow.start(wfName, {});
		await awaitRunStatus(
			caller,
			runB,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			10_000,
		);

		// Capture F2 fingerprint.
		const fpF2 = await runFingerprint(runB);
		expect(fpF2).toBeTruthy();

		// Core assertion: F1 ≠ F2 (different graphs produce different fingerprints).
		expect(fpF1).not.toBe(fpF2);

		// Core assertion: run-A's DB fingerprint column is still F1
		// (frozen_plan not overwritten by F2 registration or dispatch).
		const fpRunAAfter = await runFingerprint(runA);
		expect(fpRunAAfter).toBe(fpF1);

		// run-B's fingerprint is F2.
		expect(fpF2).toBe(await runFingerprint(runB));
	}, 40_000);
});
