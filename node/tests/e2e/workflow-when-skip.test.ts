// workflow-when-skip — `when: false` predicate skips the step; descendant
// observes `$.<id> = null` and still executes.

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

describe("workflow-when-skip", () => {
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

	test("when=false → step skipped, descendant runs with $.<id>=null", async () => {
		const wfName = `when-skip-${Date.now()}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "always",
					fn: async () => ({ ran: true }),
				},
				{
					type: "local",
					id: "maybe",
					when: { equals: ["$.input.feature", "on"] },
					fn: async () => ({ ran: true }),
					waitFor: ["always"],
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["maybe"],
					fn: async (state) => ({
						maybeSeen: state.maybe,
					}),
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

		// Trigger `when=false` by sending feature: "off".
		const { runId } = await caller.workflow.start(wfName, { feature: "off" });

		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			20_000,
		);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		expect(q.state.always).toEqual({ ran: true });
		expect(q.state.maybe).toBeNull();
		// Descendant saw `state.maybe === null`.
		expect(q.state.tail).toEqual({ maybeSeen: null });
	}, 30_000);
});
