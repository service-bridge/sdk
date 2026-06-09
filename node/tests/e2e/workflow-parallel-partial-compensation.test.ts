// workflow-parallel-partial-compensation — sequential steps where multiple
// complete before a failure. Only steps with a `compensate` spec are compensated;
// steps without a spec are skipped by the compensation runner.
//
// Steps: reserve (compensatable) → log_event (local, no compensate) → charge (fails).
// On failure: reserve is compensated, log_event is skipped (no compensate spec),
// run ends as failed_compensated.
//
// Architecture mirrors compensation-chain: callee (e2e-registry-consumer) handles
// rpc; owner (e2e-registry-svc) handles workflow definition.

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

describe("workflow-parallel-partial-compensation", () => {
	const env = wfEnv();
	let owner: ServiceBridge | undefined;
	let callee: ServiceBridge | undefined;
	let ownerID: string | undefined;
	let calleeID: string | undefined;

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

	test("reserve+local complete → charge fails → only reserve compensated → failed_compensated", async () => {
		const wfName = `par-partial-comp-${Date.now()}`;

		callee = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);

		callee.rpc.handle(
			"Reserve",
			async (req: { item_id: string; quantity: number }) => ({
				reservation_id: `rsv-${req.item_id}-${Date.now()}`,
				ok: true,
			}),
			{ schema: { protoFile: COMP_PROTO } },
		);

		callee.rpc.handle(
			"Charge",
			async (_req: { reservation_id: string; amount: number }) => {
				throw new Error("payment gateway timeout");
			},
			{ schema: { protoFile: COMP_PROTO } },
		);

		callee.rpc.handle(
			"Release",
			async (_req: { reservation_id: string }) => ({ ok: true }),
			{ schema: { protoFile: COMP_PROTO } },
		);

		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;
		const calleeName = callee.identity()!.serviceName;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.service(calleeName, { rpc: ["Reserve", "Charge", "Release"] });

		owner.workflow.handle(wfName, {
			steps: [
				// Step 1: call with compensate spec.
				{
					type: "call",
					id: "reserve",
					service: calleeName,
					method: "Reserve",
					input: {
						item_id: "$.input.item_id",
						quantity: "$.input.quantity",
					},
					compensate: {
						service: calleeName,
						method: "Release",
						input: { reservation_id: "$.reserve.reservation_id" },
					},
				},
				// Step 2: local step — NO compensate spec. Should be skipped by runner.
				{
					type: "local",
					id: "log_event",
					fn: async () => ({ logged: true }),
					waitFor: ["reserve"],
				},
				// Step 3: call that always fails.
				{
					type: "call",
					id: "charge",
					service: calleeName,
					method: "Charge",
					input: {
						reservation_id: "$.reserve.reservation_id",
						amount: "$.input.amount",
					},
					waitFor: ["log_event"],
				},
			],
		});

		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		await addWorkflowRule(calleeID, ownerID, wfName);
		const allCalleeIDs = await allServiceIDs(calleeName);
		for (const method of ["Reserve", "Charge", "Release"]) {
			await addRule(ownerID, "E", "rpc.call", null, method);
			for (const cid of allCalleeIDs) {
				await addRule(cid, "A", "rpc.handle", ownerID, method);
			}
		}
		for (const method of ["Reserve", "Charge", "Release"]) {
			await owner!.useSchema(calleeName, method, { protoFile: COMP_PROTO });
		}
		await sleep(2000);

		const { runId } = await callee.workflow.start(wfName, {
			item_id: "widget-99",
			quantity: 2,
			amount: 49.0,
		});

		const finalStatus = await awaitRunStatus(
			callee,
			runId,
			(s) =>
				s === "failed_compensated" ||
				s === "failed" ||
				s === "success" ||
				s === "cancelled",
			30_000,
		);
		expect(finalStatus).toBe("failed_compensated");

		const q = await callee.workflow.query(runId);
		expect(q.status).toBe("failed_compensated");
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		// reserve: compensated (has compensate spec).
		expect(byId.get("reserve")?.status).toBe("compensated");
		expect(byId.get("reserve")?.compensatedBy).toBe("reserve.compensate");
		// log_event: completed (local, no compensate spec — runner skips it).
		expect(byId.get("log_event")?.status).toBe("success");
		// charge: failed.
		expect(byId.get("charge")?.status).toBe("failed");
	}, 40_000);
});
