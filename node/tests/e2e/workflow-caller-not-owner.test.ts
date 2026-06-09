// workflow-caller-not-owner — owner = A registers + executes workflow,
// caller = B starts the run and observes terminal state via Await stream.
//
// Asserts:
//   - DB row: workflow_runs.owner_service_id = A (NOT caller B)
//   - sb.workflow.await(runId) on the caller side resolves with the final
//     state without B needing to register the workflow handler.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules, withDb } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

describe("workflow-caller-not-owner", () => {
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

	test("owner=A executes, caller=B awaits and sees terminal RunStatusUpdate", async () => {
		const wfName = `caller-not-owner-${Date.now()}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "compute",
					fn: async (state) => ({
						doubled: (state.input as { n: number }).n * 2,
					}),
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		// Caller does NOT register the workflow handler.
		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;
		expect(callerID).not.toBe(ownerID);

		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800);

		const { runId } = await caller.workflow.start(wfName, { n: 21 });

		// DB: owner_service_id is the OWNER (A), not the caller (B).
		await sleep(150);
		const runRows = await withDb(async (sql) => {
			return sql<Array<{ owner_service_id: string }>>`
				SELECT owner_service_id FROM workflow_runs WHERE id = ${runId}
			`;
		});
		expect(runRows).toHaveLength(1);
		expect(runRows[0]!.owner_service_id).toBe(ownerID);

		// Caller awaits via the streaming RPC.
		const finalState = await caller.workflow.await(runId);
		expect(finalState.compute).toEqual({ doubled: 42 });

		// Sanity: status terminal via query.
		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
	}, 30_000);
});
