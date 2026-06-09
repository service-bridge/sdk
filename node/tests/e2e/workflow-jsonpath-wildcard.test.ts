// workflow-jsonpath-wildcard — verifies JSONPath wildcard `$.x[*].field`
// collects an array of values when used in a step's `input` templating.
//
// Architecture mirrors compensation-chain:
//   - owner (e2e-registry-svc) owns the workflow definition + executes it.
//   - callee (e2e-registry-consumer) registers an rpc handler that receives
//     the wildcard-aggregated array as its input. Handler echoes the array
//     back so the test can assert via workflow state.
//
// Workflow:
//   step "list_items" (local) → state.list_items = { items: [{id:1,...},...] }
//   step "collect"    (call)  input: { ids: "$.list_items.items[*].id" } →
//                              callee.CollectIds → returns { received, count }
//
// Spec: workflows.md:780 — "финальный publish с input: { ids: "$.list[*].id" }
// корректно собирает массив id". We use a `call` step instead of `publish`
// because the existing proto fixture has no array-typed event field; the
// templating engine is shared between call/publish/workflow step types (see
// runner.ts::snapshotInput), so this exercises the same JSONPath wildcard
// codepath end-to-end.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import {
	addRule,
	allServiceIDs,
	clearRules,
	serviceID,
} from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

const COMP_PROTO = join(import.meta.dir, "_helpers", "compensation.proto");

describe("workflow-jsonpath-wildcard", () => {
	const env = wfEnv();
	let owner: ServiceBridge | undefined;
	let callee: ServiceBridge | undefined;
	let ownerID: string | undefined;

	beforeEach(async () => {
		await cleanupWorkflowState();
		owner = undefined;
		callee = undefined;
		ownerID = await serviceID("e2e-registry-svc");
		await clearRules(ownerID);
	});

	afterEach(async () => {
		await callee?.stop();
		await owner?.stop();
		if (ownerID) await clearRules(ownerID);
		const calleeName =
			callee?.identity()?.serviceName ?? "e2e-registry-consumer";
		const allIDs = await allServiceIDs(calleeName);
		for (const cid of allIDs) {
			await clearRules(cid);
		}
		await sleep(300);
	});

	test("$.list_items.items[*].id resolves to aggregated array at call-step input", async () => {
		const wfName = `jsonpath-wildcard-${Date.now()}`;

		// Callee: registers CollectIds handler — echoes the array back so the
		// test can assert the wildcard aggregation happened correctly server-side.
		callee = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		callee.rpc.handle(
			"CollectIds",
			async (req: { ids: number[] }) => {
				return {
					received: req.ids,
					count: req.ids.length,
				};
			},
			{ schema: { protoFile: COMP_PROTO } },
		);
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		const calleeID = callee.identity()!.serviceId;
		const calleeName = callee.identity()!.serviceName;

		// Owner: declares outgoing rpc dep BEFORE start so runtime resolves the
		// descriptor on owner's registry snapshot.
		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.service(calleeName, { rpc: ["CollectIds"] });
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "list_items",
					fn: async () => ({
						items: [
							{ id: 1, name: "a" },
							{ id: 2, name: "b" },
							{ id: 3, name: "c" },
						],
					}),
				},
				{
					type: "call",
					id: "collect",
					service: calleeName,
					method: "CollectIds",
					input: { ids: "$.list_items.items[*].id" },
					waitFor: ["list_items"],
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		// Bilateral workflow policy: callee → owner workflow.run / workflow.handle.
		await addWorkflowRule(calleeID, ownerID, wfName);
		// owner's rpc.call → callee acceptance for every consumer instance row.
		const allCalleeIDs = await allServiceIDs(calleeName);
		await addRule(ownerID, "E", "rpc.call", null, "CollectIds");
		for (const cid of allCalleeIDs) {
			await addRule(cid, "A", "rpc.handle", ownerID, "CollectIds");
		}
		// Owner needs a schema for the callee method (rpc.call side).
		await owner.useSchema(calleeName, "CollectIds", { protoFile: COMP_PROTO });
		await sleep(2000);

		const { runId } = await callee.workflow.start(wfName, {});
		expect(runId).toMatch(/^[0-9a-f-]{36}$/);

		const finalStatus = await awaitRunStatus(
			callee,
			runId,
			(s) =>
				s === "success" ||
				s === "failed" ||
				s === "failed_compensated" ||
				s === "cancelled",
			30_000,
		);
		expect(finalStatus).toBe("success");

		const q = await callee.workflow.query(runId);
		expect(q.status).toBe("success");

		// list_items step produced the source array.
		const listOut = q.state.list_items as
			| { items?: Array<{ id: number }> }
			| undefined;
		expect(listOut?.items).toHaveLength(3);

		// collect step received the wildcard-aggregated ids and echoed them back.
		const collectOut = q.state.collect as
			| { received?: number[]; count?: number }
			| undefined;
		expect(collectOut?.received).toEqual([1, 2, 3]);
		expect(collectOut?.count).toBe(3);
	}, 45_000);
});
