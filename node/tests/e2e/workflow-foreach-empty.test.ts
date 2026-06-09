// workflow-foreach-empty — `parallel + forEach` over an empty array
// completes immediately with empty group output; downstream step still runs.

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

describe("workflow-foreach-empty", () => {
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

	test("parallel forEach over [] completes; downstream runs", async () => {
		const wfName = `foreach-empty-${Date.now()}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "parallel",
					id: "fanout",
					forEach: { from: "$.input.items", as: "item" },
					steps: [
						{
							type: "local",
							id: "leaf",
							fn: async (state) => ({ item: state.item }),
						},
					],
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["fanout"],
					fn: async (state) => ({ fanout: state.fanout }),
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

		const { runId } = await caller.workflow.start(wfName, { items: [] });

		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			20_000,
		);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		// fanout completes with empty group output.
		expect(q.state.fanout).toEqual({});
		// tail saw the empty group.
		expect(q.state.tail).toEqual({ fanout: {} });
		const fanoutStep = q.steps.find((s) => s.stepId === "fanout");
		expect(fanoutStep?.status).toBe("success");
	}, 30_000);
});
