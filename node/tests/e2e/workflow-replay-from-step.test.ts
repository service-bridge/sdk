// workflow-replay-from-step — `replay(runId, { fromStepId })` forks a new run
// that reuses prior steps' output_snapshot before fromStepId and re-executes
// steps from fromStepId onward. ADR-W-009.

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

describe("workflow-replay-from-step", () => {
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

	test("replay from middle step reuses earlier outputs, re-runs later steps", async () => {
		const wfName = `replay-${Date.now()}`;

		// Per-step counters increment on every actual execution. Steps before
		// fromStepId must NOT increment on replay (output reused from snapshot).
		// Steps from fromStepId onward MUST increment on replay (re-executed).
		const calls = { a: 0, b: 0, c: 0 };

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "a",
					fn: async () => {
						calls.a++;
						return { which: "a", call: calls.a };
					},
				},
				{
					type: "local",
					id: "b",
					waitFor: ["a"],
					fn: async () => {
						calls.b++;
						return { which: "b", call: calls.b };
					},
				},
				{
					type: "local",
					id: "c",
					waitFor: ["b"],
					fn: async () => {
						calls.c++;
						return { which: "c", call: calls.c };
					},
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

		// Original run.
		const { runId } = await caller.workflow.start(wfName, {});
		const first = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			15_000,
		);
		expect(first).toBe("success");
		expect(calls).toEqual({ a: 1, b: 1, c: 1 });

		// Replay from step "b". Owner-side replay (ADR-W-009: only owner may
		// replay). Caller ≠ owner, so use the owner's SDK client.
		const { runId: replayId } = await owner.workflow.replay(runId, {
			fromStepId: "b",
		});
		expect(replayId).not.toBe(runId);

		const second = await awaitRunStatus(
			owner,
			replayId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			15_000,
		);
		expect(second).toBe("success");

		// "a" was NOT re-executed; "b" and "c" were.
		expect(calls).toEqual({ a: 1, b: 2, c: 2 });

		// Replay state mirrors the reused + freshly computed outputs.
		const q = await owner.workflow.query(replayId);
		expect(q.state.a).toEqual({ which: "a", call: 1 });
		expect(q.state.b).toEqual({ which: "b", call: 2 });
		expect(q.state.c).toEqual({ which: "c", call: 2 });
	}, 40_000);
});
