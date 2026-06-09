// jobs-handler-calls-rpc — job handler calls rpc on a 2nd service.
//
// Service 1 (job owner) fires a delayed job. Inside the handler it calls
// an RPC on Service 2. We verify the handler ran and the RPC reached
// service 2 — trace propagation via X-SB-Trace header (ADR 0006)
// and exercised in the telemetry integration tests.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { ORDER_EVENT_PROTO, waitFor } from "./_helpers/events";
import { harnessFromEnv } from "./_helpers/harness";
import { addRule, clearRules, withDb } from "./_helpers/policy-db";

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

describe("jobs-handler-calls-rpc", () => {
	const keys = harnessFromEnv({ needSecond: true });
	let sb1: ServiceBridge | undefined;
	let sb2: ServiceBridge | undefined;
	let sb1Id: string | undefined;
	let sb2Id: string | undefined;

	beforeEach(async () => {
		await cleanupJobState();
	});

	afterEach(async () => {
		await sb1?.stop().catch(() => {});
		await sb2?.stop().catch(() => {});
		if (sb1Id) await clearRules(sb1Id).catch(() => {});
		if (sb2Id) await clearRules(sb2Id).catch(() => {});
		sb1 = undefined;
		sb2 = undefined;
	});

	test("job handler runs and rpc.call inside handler reaches service 2", async () => {
		const jobName = `handler-rpc-${Date.now()}`;
		const method = "Echo";
		const receivedCalls: unknown[] = [];
		let handlerCalled = false;

		// Service 2 — RPC callee using a real proto schema.
		sb2 = new ServiceBridge(keys.url, keys.service2Key!, FAST_OPTS);
		sb2.rpc.handle(
			method,
			async (req) => {
				receivedCalls.push(req);
				return req; // echo
			},
			{
				schema: {
					protoFile: ORDER_EVENT_PROTO,
					input: "OrderCreatedV1",
					output: "OrderCreatedV1",
				},
			},
		);
		await sb2.start();
		await waitFor(() => sb2!.identity() !== null, 5_000, "sb2 connected");
		sb2Id = sb2.identity()!.serviceId;
		const sb2ServiceName = sb2.identity()!.serviceName;

		// Service 1 — job owner + RPC caller.
		sb1 = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);

		// Declare outgoing RPC dep before start.
		sb1.service(sb2ServiceName, { rpc: [method] });
		await sb1.useSchema(sb2ServiceName, method, {
			protoFile: ORDER_EVENT_PROTO,
			input: "OrderCreatedV1",
			output: "OrderCreatedV1",
		});

		sb1.job.handle(
			jobName,
			{
				trigger: { delayed: { at: Date.now() + 500 } },
				maxAttempts: 1,
			},
			async () => {
				try {
					await sb1!.rpc.call(sb2ServiceName, method, {
						orderId: "trace-test",
						amount: 1,
						currency: "USD",
					});
				} catch {
					// best-effort: schema may not load in time.
				}
				handlerCalled = true;
			},
		);

		await sb1.start();
		await waitFor(() => sb1!.identity() !== null, 5_000, "sb1 connected");
		sb1Id = sb1.identity()!.serviceId;

		// Grant policy: sb1 egress → sb2, sb2 accept from sb1.
		await addRule(sb1Id, "E", "rpc.call", sb2Id, method);
		await addRule(sb2Id, "A", "rpc.handle", sb1Id, method);

		await waitFor(() => handlerCalled, 15_000, "job handler executed");

		// The RPC should have reached service 2 at least once.
		expect(receivedCalls.length).toBeGreaterThanOrEqual(1);
	}, 45_000);
});
