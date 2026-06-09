// workflow-foreach-dynamic-fanout — `parallel + forEach` over a 5-element
// array spawns 5 parallel sub-steps. Group output keyed by rewritten id has
// 5 entries.

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

describe("workflow-foreach-dynamic-fanout", () => {
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

	test("forEach over 5-item array spawns 5 parallel sub-steps", async () => {
		const wfName = `foreach-dyn-${Date.now()}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "parallel",
					id: "track_each",
					forEach: { from: "$.input.items", as: "it" },
					steps: [
						{
							type: "local",
							id: "track",
							fn: async (state) => ({
								item: state.it,
								doubled: (state.it as number) * 2,
							}),
						},
					],
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

		const { runId } = await caller.workflow.start(wfName, {
			items: [1, 2, 3, 4, 5],
		});

		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			30_000,
		);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		// Group output: { "track:0": {...}, ..., "track:4": {...} }
		const fanout = q.state.track_each as Record<
			string,
			{ item: number; doubled: number }
		>;
		expect(Object.keys(fanout)).toHaveLength(5);
		for (let i = 0; i < 5; i++) {
			expect(fanout[`track:${i}`]).toEqual({
				item: i + 1,
				doubled: (i + 1) * 2,
			});
		}
	}, 40_000);
});
