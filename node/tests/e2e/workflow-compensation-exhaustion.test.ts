// workflow-compensation-exhaustion — the very first step fails terminally with
// no prior completed steps. Nothing to compensate → run ends as 'failed' (not
// 'failed_compensated'). Validates the zero-compensation fast-path.

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

describe("workflow-compensation-exhaustion", () => {
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

	test("first step fails with no prior completed steps → run ends as failed", async () => {
		const wfName = `comp-exhaust-${Date.now()}`;

		callee = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);

		// Charge always fails immediately — it is the FIRST (and only) step.
		callee.rpc.handle(
			"Charge",
			async (_req: { reservation_id: string; amount: number }) => {
				throw new Error("immediate terminal failure");
			},
			{ schema: { protoFile: COMP_PROTO } },
		);

		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;
		const calleeName = callee.identity()!.serviceName;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.service(calleeName, { rpc: ["Charge"] });

		// Single call step that always fails — no preceding completed step.
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "call",
					id: "charge",
					service: calleeName,
					method: "Charge",
					input: {
						reservation_id: "no-prior-step",
						amount: "$.input.amount",
					},
				},
			],
		});

		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		await addWorkflowRule(calleeID, ownerID, wfName);
		const allCalleeIDs = await allServiceIDs(calleeName);
		await addRule(ownerID, "E", "rpc.call", null, "Charge");
		for (const cid of allCalleeIDs) {
			await addRule(cid, "A", "rpc.handle", ownerID, "Charge");
		}
		await owner!.useSchema(calleeName, "Charge", { protoFile: COMP_PROTO });
		await sleep(2000);

		const { runId } = await callee.workflow.start(wfName, {
			amount: 99.0,
		});

		const finalStatus = await awaitRunStatus(
			callee,
			runId,
			(s) =>
				s === "failed_compensated" ||
				s === "failed" ||
				s === "success" ||
				s === "cancelled",
			20_000,
		);
		// No completed steps → no compensation → plain 'failed'.
		expect(finalStatus).toBe("failed");

		const q = await callee.workflow.query(runId);
		expect(q.status).toBe("failed");
		const chargeStep = q.steps.find((s) => s.stepId === "charge");
		expect(chargeStep?.status).toBe("failed");
	}, 30_000);
});
