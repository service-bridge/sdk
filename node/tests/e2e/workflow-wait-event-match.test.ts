// workflow-wait-event-match — `wait_event` step parks the run; a published
// event whose payload matches the JSON-path filter wakes it. The matched
// payload appears as $.<step-id> in run state.
//
// Verifies the publisher-supplied payload_json + SignalRouter filter
// evaluation path (ADR-0002: runtime keeps the canonical protobuf payload
// opaque, filter matching uses the JSON view supplied by the SDK).

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

describe("workflow-wait-event-match", () => {
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

	test("matching event wakes wait_event; payload in state[stepId]", async () => {
		const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		const wfName = `wait-event-${stamp}`;
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
					filter: { "$.orderId": "$.input.orderId" },
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["evt"],
					fn: async (state) => ({ matched: state.evt }),
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		// Caller also publishes — needs the same schema registered.
		caller.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800);

		const { runId } = await caller.workflow.start(wfName, {
			orderId: "order-match",
		});

		// Wait until run is parked.
		await waitFor(
			async () => {
				const q = await caller!.workflow.query(runId);
				return q.status === "waiting";
			},
			10_000,
			"run parked on wait_event",
		);

		// Non-matching event first — should NOT wake the run.
		await caller.event.publish(eventName, {
			orderId: "order-other",
			amount: 1,
			currency: "USD",
		});
		await sleep(500);
		const stillWaiting = await caller.workflow.query(runId);
		expect(stillWaiting.status).toBe("waiting");

		// Matching event — wakes the run.
		await caller.event.publish(eventName, {
			orderId: "order-match",
			amount: 99.5,
			currency: "USD",
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
		const evt = q.state.evt as {
			orderId?: string;
			amount?: number;
			currency?: string;
		};
		expect(evt?.orderId).toBe("order-match");
		expect(evt?.amount).toBe(99.5);
		expect(evt?.currency).toBe("USD");
		expect((q.state.tail as { matched: unknown }).matched).toEqual(evt);
	}, 40_000);
});
