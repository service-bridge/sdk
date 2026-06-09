// workflow-happy-linear — 3 sequential `local` steps via `waitFor` chain.
//
// Validates the end-to-end workflow plumbing: owner registers a workflow,
// caller starts a run, awaits terminal state. Final state contains outputs
// of all three steps and `status === "success"`.
//
// Uses `local` steps (synchronous JS) rather than `call` steps to keep the
// first happy-path test focused on workflow plumbing (Subscribe → runner →
// BeginStep/CompleteStep/CompleteRun) without dragging in RPC schema /
// service-discovery / bilateral-policy setup. Call-step coverage lives in
// later batch tests.

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

describe("workflow-happy-linear", () => {
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

	test("3 sequential local steps via waitFor → completed with all outputs in state", async () => {
		const wfName = `happy-linear-${Date.now()}`;

		// Owner side — register workflow + start subscriber via sb.start().
		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step_one",
					fn: async () => ({ value: 1 }),
				},
				{
					type: "local",
					id: "step_two",
					fn: async () => ({ value: 2 }),
					waitFor: ["step_one"],
				},
				{
					type: "local",
					id: "step_three",
					fn: async () => ({ value: 3 }),
					waitFor: ["step_two"],
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		// Caller side.
		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		// Bilateral policy: caller egress + owner acceptance.
		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800); // NOTIFY → snapshot

		// Start the run and wait for completion.
		const { runId } = await caller.workflow.start(wfName, { hello: "world" });
		expect(runId).toMatch(/^[0-9a-f-]{36}$/);

		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			20_000,
		);
		expect(finalStatus).toBe("success");

		// Query the run to inspect final state + per-step outputs.
		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		expect(q.state.step_one).toEqual({ value: 1 });
		expect(q.state.step_two).toEqual({ value: 2 });
		expect(q.state.step_three).toEqual({ value: 3 });
		// Steps are returned in any order — assert by id.
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		expect(byId.get("step_one")?.status).toBe("success");
		expect(byId.get("step_two")?.status).toBe("success");
		expect(byId.get("step_three")?.status).toBe("success");
	}, 30_000);
});
