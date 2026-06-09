// workflow-publish-step — single `publish` step writes a row into event_log
// with publisher_service = owner.
//
// Owner registers a workflow whose only step publishes one event with a
// payload templated from workflow input. Run is started by caller. After
// terminal "success", we query event_log directly (no subscriber needed —
// publish-step is a fire-and-forget durable insert) and assert the row exists
// with the correct name + publisher_service_id.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { ORDER_EVENT_PROTO, sleep, waitFor } from "./_helpers/events";
import { clearRules, withDb } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

describe("workflow-publish-step", () => {
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

	test("publish step inserts event_log row with publisher_service=owner", async () => {
		const wfName = `publish-step-${Date.now()}`;
		const eventName = `order.placed.${Date.now()}.${Math.random().toString(36).slice(2, 8)}`;

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		// Owner must define the event schema — `sb.event.publish` requires it
		// (SchemaIndex lookup throws otherwise).
		owner.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "publish",
					id: "emit",
					event: eventName,
					input: {
						orderId: "$.input.orderId",
						amount: "$.input.amount",
						currency: "$.input.currency",
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

		const { runId } = await caller.workflow.start(wfName, {
			orderId: "order-7",
			amount: 12.5,
			currency: "USD",
		});

		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			20_000,
		);
		expect(finalStatus).toBe("success");

		// Step output: { eventId } from event.publish.
		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		const emit = q.steps.find((s) => s.stepId === "emit");
		expect(emit?.status).toBe("success");
		const stateEmit = q.state.emit as { eventId?: string } | undefined;
		expect(stateEmit?.eventId).toMatch(/^[0-9a-f-]{36}$/);

		// DB verification — event_log row exists, publisher_service_id = owner.
		const rows = await withDb(async (sql) => {
			return sql<
				Array<{
					id: string;
					name: string;
					publisher_service: string;
				}>
			>`
				SELECT id, name, publisher_service
				FROM event_log
				WHERE name = ${eventName}
			`;
		});
		expect(rows).toHaveLength(1);
		expect(rows[0]!.name).toBe(eventName);
		expect(rows[0]!.publisher_service).toBe(ownerID);
		expect(rows[0]!.id).toBe(stateEmit!.eventId!);
	}, 30_000);
});
