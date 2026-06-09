// access-policy-scope-workflow.test.ts — ADR-0004 Layer-1 scope filter (workflows).
//
// Caller A declares outgoing workflows on B: [flowA, flowB]. A has a single
// egress rule `workflow.run` allowing only (B, flowA). The scope filter must
// hide flowB from A's snapshot while leaving flowA visible.
//
// Workflow execution endpoint is not yet implemented; visibility filtering on
// `workflow.handle` methods in serviceMap is the assertable surface today.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep, waitFor } from "./_helpers/events";
import { addRule, clearRules, serviceID } from "./_helpers/policy-db";

const CALLEE_SVC = "e2e-registry-svc"; // B (publisher key)
const CALLER_SVC = "e2e-registry-consumer"; // A (subscriber key)

describe("access-policy: scope filter (workflow.run)", () => {
	const env = eventsEnv();
	let calleeID = "";
	let callerID = "";
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	const flowA = `scope-wf-A-${Date.now()}`;
	const flowB = `scope-wf-B-${Date.now()}`;

	beforeEach(() => {
		callee = caller = undefined;
		calleeID = callerID = "";
	});

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		if (callerID) await clearRules(callerID);
		if (calleeID) await clearRules(calleeID);
		await sleep(300);
	});

	test("A with workflow.run (B, flowA) sees B/flowA but not B/flowB", async () => {
		callee = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		// Workflow handlers — registry records the declaration; execution
		// endpoint is dormant per ADR-0004 plan.
		callee.workflow.handle(flowA, {
			steps: [{ id: "noop", type: "sleep", durationSec: 0 }],
		});
		callee.workflow.handle(flowB, {
			steps: [{ id: "noop", type: "sleep", durationSec: 0 }],
		});
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;

		callerID = await serviceID(CALLER_SVC);
		await clearRules(callerID);
		await addRule(callerID, "E", "workflow.run", calleeID, flowA);
		await sleep(400);

		caller = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		caller.service(CALLEE_SVC, { workflows: [flowA, flowB] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		const actualCallerID = caller.identity()!.serviceId;
		if (actualCallerID !== callerID) {
			await clearRules(callerID);
			callerID = actualCallerID;
			await clearRules(callerID);
			await addRule(callerID, "E", "workflow.run", calleeID, flowA);
			await sleep(900);
		}

		// Wait for B/flowA to surface.
		await waitFor(
			() => {
				const peer = caller!.serviceMap().get(CALLEE_SVC);
				if (!peer) return false;
				return peer.methods.some((m) => m.name === flowA);
			},
			6_000,
			"B/flowA visible in caller snapshot",
		);

		const peer = caller.serviceMap().get(CALLEE_SVC);
		expect(peer).toBeDefined();
		const methodNames = peer!.methods.map((m) => m.name);

		expect(methodNames).toContain(flowA);
		expect(methodNames).not.toContain(flowB);
	}, 30_000);
});
