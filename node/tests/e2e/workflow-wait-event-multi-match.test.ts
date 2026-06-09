// workflow-wait-event-multi-match — multiple parked runs on the same event
// name + filter all wake from a single matching publish. Verifies fan-out
// delivery in SignalRouter.RouteEvent (one publish → N DeliverEventMatch
// calls in the matcher loop).

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { ORDER_EVENT_PROTO, sleep, waitFor } from "./_helpers/events";
import { clearRules } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

describe("workflow-wait-event-multi-match", () => {
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

	test("one publish wakes 3 parked runs on the same event", async () => {
		const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		const wfName = `wait-event-multi-${stamp}`;
		const eventName = `orders.created.${stamp}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "wait_event",
					id: "evt",
					event: eventName,
					timeoutSec: 60,
					filter: { "$.currency": "USD" },
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		caller.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800);

		const runs = await Promise.all([
			caller.workflow.start(wfName, { idx: 0 }),
			caller.workflow.start(wfName, { idx: 1 }),
			caller.workflow.start(wfName, { idx: 2 }),
		]);

		// All three parked.
		await waitFor(
			async () => {
				const states = await Promise.all(
					runs.map((r) => caller!.workflow.query(r.runId)),
				);
				return states.every((s) => s.status === "waiting");
			},
			10_000,
			"3 runs parked on wait_event",
		);

		// One matching publish wakes all three.
		await caller.event.publish(eventName, {
			orderId: "multi",
			amount: 1,
			currency: "USD",
		});

		await Promise.all(
			runs.map((r) =>
				awaitRunStatus(
					caller!,
					r.runId,
					(s) => s === "success" || s === "failed",
					15_000,
				),
			),
		);

		for (const r of runs) {
			const q = await caller.workflow.query(r.runId);
			expect(q.status).toBe("success");
			const evt = q.state.evt as { currency?: string };
			expect(evt?.currency).toBe("USD");
		}
	}, 45_000);
});
