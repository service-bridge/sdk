// workflow-lease-reclaim — two SDK instances same service key.
//
// Run starts on instance A (whichever the dispatcher picks first). A stops
// mid-sleep. Lease is force-expired via DB. LeaseManager sweeps (5 s tick)
// and reclaims the run back to pending. Dispatcher (2 s tick) re-dispatches
// to instance B, which completes the remaining steps idempotently.
//
// Idempotency proof: the step_before_sleep local step records executions in a
// shared counter. Because the run is in the sleep phase when A dies, B resumes
// after the sleep completes (via dispatcher timer) and runs step_after_sleep.
// step_before_sleep is NOT re-executed (it completed and its output is in state).

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	forceLeaseReclaim,
	wfEnv,
} from "./_helpers/wf.ts";

describe("workflow-lease-reclaim", () => {
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

	test("instance A dies mid-sleep → lease reclaimed → instance B completes run", async () => {
		const wfName = `lease-reclaim-${Date.now()}`;

		// Shared counter across both owner instances (same process).
		const execCounts = { before: 0, after: 0 };

		// Instance A: register workflow + start.
		ownerA = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		ownerA.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step_before_sleep",
					fn: async () => {
						execCounts.before++;
						return { executed: execCounts.before };
					},
				},
				{
					type: "sleep",
					id: "pause",
					durationSec: 3,
					waitFor: ["step_before_sleep"],
				},
				{
					type: "local",
					id: "step_after_sleep",
					fn: async () => {
						execCounts.after++;
						return { executed: execCounts.after };
					},
					waitFor: ["pause"],
				},
			],
		});
		await ownerA.start();
		await waitFor(() => ownerA!.identity() !== null, 5_000, "ownerA connected");
		ownerID = ownerA.identity()!.serviceId;

		// Instance B: same key, same workflow definition.
		ownerB = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		ownerB.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step_before_sleep",
					fn: async () => {
						execCounts.before++;
						return { executed: execCounts.before };
					},
				},
				{
					type: "sleep",
					id: "pause",
					durationSec: 3,
					waitFor: ["step_before_sleep"],
				},
				{
					type: "local",
					id: "step_after_sleep",
					fn: async () => {
						execCounts.after++;
						return { executed: execCounts.after };
					},
					waitFor: ["pause"],
				},
			],
		});
		await ownerB.start();
		await waitFor(() => ownerB!.identity() !== null, 5_000, "ownerB connected");

		// Caller side.
		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		// Policies.
		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800);

		// Start the run.
		const { runId } = await caller.workflow.start(wfName, {});

		// Wait until the run enters 'waiting' — the sleep timer is parked and the
		// run is holding a lease. This is the exact moment to simulate A's death.
		await awaitRunStatus(caller, runId, (s) => s === "waiting", 15_000);

		// Stop instance A — simulates crash / pod restart.
		await ownerA.stop();
		ownerA = undefined;

		// Force-expire the lease so the reclaim sweep picks it up immediately
		// instead of waiting for the full LeaseTTL (30 s).
		await forceLeaseReclaim(runId);

		// Wait for completion — B picks up the run after reclaim (≤ 15 s budget:
		// 5 s reclaim tick + 2 s dispatch tick + 3 s sleep + 2 s margin).
		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			25_000,
		);
		expect(finalStatus).toBe("success");

		// step_before_sleep must have run exactly once (checkpoint in state prevents re-run).
		// step_after_sleep must have run exactly once (completed by B).
		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		expect(byId.get("step_before_sleep")?.status).toBe("success");
		expect(byId.get("step_after_sleep")?.status).toBe("success");

		// before ran exactly once (B reuses checkpoint, not re-runs it).
		expect(execCounts.before).toBe(1);
		// after ran exactly once on B.
		expect(execCounts.after).toBe(1);
	}, 45_000);
});
