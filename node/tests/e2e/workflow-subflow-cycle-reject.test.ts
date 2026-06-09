// workflow-subflow-cycle-reject — X starts Y; Y starts X. Runtime walks
// the parent_run_id chain when starting Y→X and rejects with
// FailedPrecondition (cycle). The Y run fails, which fails X.
//
// Cycle detection: runtime.internal.workflow.CheckCycleInChain (ADR-W-019),
// wired through StartRunRequest.parent_run_id (SDK runner sets this for
// every `type: "workflow"` step).

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

describe("workflow-subflow-cycle-reject", () => {
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
		await sleep(200);
	});

	test("X→Y→X is rejected with cycle; root run fails", async () => {
		const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		const wfX = `cyc-x-${stamp}`;
		const wfY = `cyc-y-${stamp}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfX, {
			steps: [
				{
					type: "workflow",
					id: "call_y",
					workflow: wfY,
					input: {},
				},
			],
		});
		owner.workflow.handle(wfY, {
			steps: [
				{
					type: "workflow",
					id: "call_x",
					workflow: wfX,
					input: {},
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

		await addWorkflowRule(callerID, ownerID, wfX);
		await addWorkflowRule(callerID, ownerID, wfY);
		// Owner also starts its own sub-workflows internally.
		await addWorkflowRule(ownerID, ownerID, wfX);
		await addWorkflowRule(ownerID, ownerID, wfY);
		await sleep(800);

		const { runId } = await caller.workflow.start(wfX, {});

		const final = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			20_000,
		);
		expect(final).toBe("failed");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("failed");
		const callYStep = q.steps.find((s) => s.stepId === "call_y");
		expect(callYStep?.status).toBe("failed");
	}, 40_000);
});
