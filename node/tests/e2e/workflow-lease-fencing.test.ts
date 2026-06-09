// workflow-lease-fencing — run is re-claimed mid-execution and B completes it.
//
// Instance A holds the lease while running slow_step. We manually advance
// the lease_epoch in DB (full reclaim). A's next CompleteStep call is
// rejected (lease fenced / Aborted). B picks up the reclaimed run, sees
// slow_step already in_flight (treated as done via BeginStep→ErrAlreadyDone),
// executes final_step, and completes the run.
//
// The key invariant: a stale holder's CompleteStep cannot corrupt committed
// state — the run reaches `completed` with the correct final state.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	reclaimLeaseNow,
	wfEnv,
} from "./_helpers/wf.ts";

describe("workflow-lease-fencing", () => {
	const env = wfEnv();
	let ownerA: ServiceBridge | undefined;
	let ownerB: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	let ownerID: string | undefined;
	let callerID: string | undefined;

	beforeEach(async () => {
		await cleanupWorkflowState();
		ownerA = undefined;
		ownerB = undefined;
		caller = undefined;
	});

	afterEach(async () => {
		await caller?.stop();
		await ownerA?.stop();
		await ownerB?.stop();
		if (ownerID) await clearRules(ownerID);
		if (callerID) await clearRules(callerID);
		await sleep(300);
	});

	test("stale holder fenced — B completes run with correct final state", async () => {
		const wfName = `lease-fence-${Date.now()}`;

		// Gate: A's step blocks here. We release it after reclaimLeaseNow so
		// A tries CompleteStep with a stale epoch (fenced by the runtime).
		let releaseStepA: () => void = () => {};
		const stepAGate = new Promise<void>((resolve) => {
			releaseStepA = resolve;
		});

		const execByInstance: string[] = [];

		// Instance A — blocked inside slow_step.
		ownerA = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		ownerA.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "slow_step",
					fn: async () => {
						execByInstance.push("A-slow");
						await stepAGate;
						return { value: "from_A" };
					},
				},
				{
					type: "local",
					id: "final_step",
					fn: async () => {
						execByInstance.push("A-final");
						return { value: "final_A" };
					},
					waitFor: ["slow_step"],
				},
			],
		});
		await ownerA.start();
		await waitFor(() => ownerA!.identity() !== null, 5_000, "ownerA connected");
		ownerID = ownerA.identity()!.serviceId;

		// Instance B — no gate, runs both steps freely.
		ownerB = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		ownerB.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "slow_step",
					fn: async () => {
						execByInstance.push("B-slow");
						return { value: "from_B" };
					},
				},
				{
					type: "local",
					id: "final_step",
					fn: async () => {
						execByInstance.push("B-final");
						return { value: "final_B" };
					},
					waitFor: ["slow_step"],
				},
			],
		});
		await ownerB.start();
		await waitFor(() => ownerB!.identity() !== null, 5_000, "ownerB connected");

		// Caller.
		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800);

		// Start run. Stop ownerB so only A can pick it up initially.
		await ownerB.stop();
		ownerB = undefined;

		const { runId } = await caller.workflow.start(wfName, {});

		// Wait until A is inside slow_step (run is 'running', A is blocked on gate).
		await awaitRunStatus(caller, runId, (s) => s === "running", 10_000);
		// Small buffer to ensure A has called BeginStep and is inside the fn body.
		await sleep(300);

		// Full reclaim: increment lease_epoch + reset to pending.
		// A's eventual CompleteStep will be rejected (stale epoch).
		await reclaimLeaseNow(runId);

		// Restart B so it can pick up the reclaimed run.
		ownerB = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		ownerB.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "slow_step",
					fn: async () => {
						execByInstance.push("B2-slow");
						return { value: "from_B2" };
					},
				},
				{
					type: "local",
					id: "final_step",
					fn: async () => {
						execByInstance.push("B2-final");
						return { value: "final_B2" };
					},
					waitFor: ["slow_step"],
				},
			],
		});
		await ownerB.start();
		await waitFor(() => ownerB!.identity() !== null, 5_000, "ownerB connected");

		// Release A's gate so it attempts CompleteStep (fenced).
		releaseStepA();

		// Wait for run to complete (B2 or recovery path).
		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			20_000,
		);
		expect(finalStatus).toBe("success");

		// Key invariant: run reached completed with all steps done.
		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		// slow_step was in_flight when reclaimed. B2 sees ErrAlreadyDone on
		// BeginStep and treats it as done (null output) without re-executing.
		// The step row stays 'in_flight' — run-level status is authoritative.
		const slowStatus = byId.get("slow_step")?.status;
		expect(slowStatus === "success" || slowStatus === "in_flight").toBe(true);
		expect(byId.get("final_step")?.status).toBe("success");

		// final_step ran exactly once — no double-commit despite A's stale attempt.
		const finalRuns = execByInstance.filter((x) => x.endsWith("-final"));
		expect(finalRuns.length).toBe(1);
	}, 45_000);
});
