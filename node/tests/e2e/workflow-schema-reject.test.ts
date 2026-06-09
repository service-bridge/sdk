// workflow-schema-reject — verifies that Workflows.Start rejects inputs that
// do not satisfy the workflow's declared input schema BEFORE allocating a
// run row. Owner registers a workflow that requires `userId`; caller calls
// start() without it and expects gRPC INVALID_ARGUMENT.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

describe("workflow-schema-reject", () => {
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

	test("Start with input missing required field → InvalidArgument", async () => {
		const wfName = `schema-reject-${Date.now()}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			input: {
				type: "object",
				required: ["userId"],
				properties: { userId: { type: "string" } },
			},
			steps: [
				{
					type: "local",
					id: "noop",
					fn: async () => ({ ok: true }),
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

		// Missing required `userId`.
		let err: Error | null = null;
		try {
			await caller.workflow.start(wfName, { otherField: "x" });
		} catch (e) {
			err = e as Error;
		}
		expect(err).not.toBeNull();
		// Error message includes runtime's "input does not match schema" prefix.
		expect((err as Error).message.toLowerCase()).toContain("schema");

		// Valid input still works.
		const { runId } = await caller.workflow.start(wfName, { userId: "u-1" });
		expect(runId).toMatch(/^[0-9a-f-]{36}$/);
	}, 30_000);
});
