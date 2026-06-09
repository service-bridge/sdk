// workflow-wait-signal-external — `wait_signal` step parks the run; external
// `sb.workflow.signal(runId, name, payload)` wakes it. Payload appears as
// $.<step-id> in run state.

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

describe("workflow-wait-signal-external", () => {
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

	test("external Signal wakes parked wait_signal; payload in $.approve", async () => {
		const wfName = `wait-signal-${Date.now()}`;
		const signalName = `approve-${Date.now()}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "pre",
					fn: async () => ({ ready: true }),
				},
				{
					type: "wait_signal",
					id: "approve",
					signal: signalName,
					timeoutSec: 60,
					waitFor: ["pre"],
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["approve"],
					fn: async (state) => ({ got: state.approve }),
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

		const { runId } = await caller.workflow.start(wfName, {});

		// Wait until run is parked on the wait_signal step.
		await waitFor(
			async () => {
				const q = await caller!.workflow.query(runId);
				return q.status === "waiting" || q.status === "running";
			},
			10_000,
			"run parked",
		);

		// Send external signal with payload.
		await caller.workflow.signal(runId, signalName, {
			approver: "alice",
			ok: true,
		});

		const final = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			15_000,
		);
		expect(final).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		expect(q.state.approve).toEqual({ approver: "alice", ok: true });
		expect(q.state.tail).toEqual({ got: { approver: "alice", ok: true } });
	}, 40_000);
});
