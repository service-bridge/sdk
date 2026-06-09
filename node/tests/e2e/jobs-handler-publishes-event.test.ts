// jobs-handler-publishes-event — job handler publishes event; consumer receives it.
//
// Service 1 fires a delayed job. Inside the handler it publishes an event.
// Service 2 (consumer) receives the event.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { ORDER_EVENT_PROTO, uniqueEventName, waitFor } from "./_helpers/events";
import { harnessFromEnv } from "./_helpers/harness";
import { withDb } from "./_helpers/policy-db";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

async function cleanupJobState(): Promise<void> {
	await withDb(async (sql) => {
		await sql`TRUNCATE job_executions, job_schedules, job_definitions, jobs_dlq CASCADE`;
	});
}

describe("jobs-handler-publishes-event", () => {
	const keys = harnessFromEnv({ needSecond: true });
	let sb1: ServiceBridge | undefined;
	let sb2: ServiceBridge | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb1?.stop().catch(() => {});
		await sb2?.stop().catch(() => {});
		sb1 = undefined;
		sb2 = undefined;
	});

	test("job handler publishes event → consumer service receives it", async () => {
		const eventName = uniqueEventName("job-evt");
		const jobName = `handler-evt-${Date.now()}`;
		const received: unknown[] = [];
		let handlerCalled = false;

		// Service 2 — event consumer.
		sb2 = new ServiceBridge(keys.url, keys.service2Key!, FAST_OPTS);
		sb2.event.handle(eventName, async (payload) => {
			received.push(payload);
		});
		sb2.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await sb2.start();
		await waitFor(() => sb2!.identity() !== null, 5_000, "sb2 connected");

		// Service 1 — job owner + event publisher.
		sb1 = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);
		sb1.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});

		sb1.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 800 } },
				maxAttempts: 1,
			},
			async (_ctx) => {
				handlerCalled = true;
				await sb1!.event.publish(eventName, {
					orderId: "from-job",
					amount: 1,
					currency: "USD",
				});
			},
		);

		await sb1.start();
		await waitFor(() => sb1!.identity() !== null, 5_000, "sb1 connected");

		await waitFor(() => handlerCalled, 15_000, "job handler executed");
		await waitFor(() => received.length > 0, 15_000, "event delivered");

		expect(received.length).toBeGreaterThanOrEqual(1);
		expect((received[0] as Record<string, unknown>).orderId).toBe("from-job");
	}, 60_000);
});
