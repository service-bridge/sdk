// jobs — consolidated jobs e2e suite against the AMBIENT runtime on :14445.
//
// Jobs are NEVER pooled: a registered job fires on its schedule for the whole
// process, so every test owns a DEDICATED ServiceBridge (register the job
// BEFORE connect, stop() in afterEach). The warm pool is never used here.
//
// Isolation is by namespacing, not cleanup: each job name is unique
// (uniqueName) and every DB assertion is scoped `WHERE d.name = <jobName>`, so
// no shared-table TRUNCATE is needed and parallel shards on the ambient DB do
// not collide. Destructive tests that kill/restart the runtime or override
// settings live in jobs-lifecycle.test.ts (dedicated isolated runtime).

import { afterEach, describe, expect, test } from "bun:test";
import type { ServiceBridge } from "../../src/connection/service-bridge";
import {
	connect,
	dedicated,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueName,
	waitFor,
} from "./_helpers/fixtures";
import {
	addRule,
	clearRules,
	setCapability,
	withDb,
} from "./_helpers/policy-db";
import { addWorkflowRule, awaitRunStatus } from "./_helpers/wf";

async function execCount(jobName: string): Promise<number> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT COUNT(*)::int AS cnt
			FROM job_executions e
			JOIN job_definitions d ON d.id = e.job_definition_id
			WHERE d.name = ${jobName}
		`) as Array<{ cnt: number }>;
		return rows[0]?.cnt ?? 0;
	});
}

async function execSuccessCount(jobName: string): Promise<number> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT COUNT(*)::int AS cnt
			FROM job_executions e
			JOIN job_definitions d ON d.id = e.job_definition_id
			WHERE d.name = ${jobName} AND e.status = 'success'
		`) as Array<{ cnt: number }>;
		return rows[0]?.cnt ?? 0;
	});
}

async function dlqCount(jobName: string): Promise<number> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT COUNT(*)::int AS cnt
			FROM jobs_dlq q
			JOIN job_definitions d ON d.id = q.job_definition_id
			WHERE d.name = ${jobName}
		`) as Array<{ cnt: number }>;
		return rows[0]?.cnt ?? 0;
	});
}

async function jobDefCount(jobName: string): Promise<number> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT COUNT(*)::int AS cnt FROM job_definitions WHERE name = ${jobName}
		`) as Array<{ cnt: number }>;
		return rows[0]?.cnt ?? 0;
	});
}

describe("jobs", () => {
	let clients: ServiceBridge[] = [];

	function track<T extends ServiceBridge>(sb: T): T {
		clients.push(sb);
		return sb;
	}

	afterEach(async () => {
		for (const sb of clients) {
			await sb.stop().catch(() => {});
		}
		clients = [];
	});

	test("delayed trigger fires exactly once with attempt===1", async () => {
		const jobName = uniqueName("delayed-once");
		const calls: Array<{ attempt: number }> = [];

		const sb = track(dedicated("primary"));
		sb.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 1_000 } }, maxAttempts: 1 },
			async (ctx) => {
				calls.push({ attempt: ctx.attempt });
			},
		);
		await connect(sb);

		await waitFor(() => calls.length >= 1, 10_000, "handler called");
		// Settle window: confirm no duplicate delivery.
		await sleep(2_000);

		expect(calls).toHaveLength(1);
		expect(calls[0]!.attempt).toBe(1);
	}, 30_000);

	test("interval trigger fires repeatedly (≥2 DB executions and ≥1 handler call)", async () => {
		// Proves the full chain: scheduler → enqueue → DB row → dispatcher →
		// handler. DB count is authoritative (scheduler+enqueue path); ≥1 handler
		// call proves dispatch+push+ACK. Folds the cron stand-in test, which only
		// asserted interval recurrence.
		const jobName = uniqueName("interval-repeat");
		let callCount = 0;

		const sb = track(dedicated("primary"));
		sb.job.handle(
			jobName,
			{ trigger: { interval: 1_000 }, overlap: "allow", maxConcurrent: 10 },
			async () => {
				callCount++;
			},
		);
		await connect(sb);

		await waitFor(
			() => callCount >= 1,
			25_000,
			"interval fired ≥1 time end-to-end",
		);
		await waitFor(
			() => execCount(jobName).then((n) => n >= 2),
			15_000,
			"scheduler produced ≥2 DB executions",
		);

		expect(callCount).toBeGreaterThanOrEqual(1);
		expect(await execCount(jobName)).toBeGreaterThanOrEqual(2);
	}, 45_000);

	test("retryable handler error → retried with attempt===2, same idempotencyKey", async () => {
		const jobName = uniqueName("retry-once");
		const calls: Array<{ attempt: number; idempotencyKey: string }> = [];

		const sb = track(dedicated("primary"));
		sb.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 500 } }, maxAttempts: 3 },
			async (ctx) => {
				calls.push({
					attempt: ctx.attempt,
					idempotencyKey: ctx.idempotencyKey,
				});
				if (ctx.attempt === 1) {
					const err = new Error("transient failure");
					(err as Error & { retryable?: boolean }).retryable = true;
					throw err;
				}
			},
		);
		await connect(sb);

		await waitFor(() => calls.length >= 2, 30_000, "two attempts");

		expect(calls[0]!.attempt).toBe(1);
		expect(calls[1]!.attempt).toBe(2);
		expect(calls[0]!.idempotencyKey).toBe(calls[1]!.idempotencyKey);
	}, 60_000);

	test("retry exhaustion → DLQ row, no further calls", async () => {
		const jobName = uniqueName("dlq-test");
		let callCount = 0;

		const sb = track(dedicated("primary"));
		sb.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 500 } }, maxAttempts: 2 },
			async () => {
				callCount++;
				throw new Error("always fails");
			},
		);
		await connect(sb);

		await waitFor(
			() => dlqCount(jobName).then((n) => n > 0),
			60_000,
			"dlq entry",
		);
		// Confirm no further calls after the terminal DLQ branch.
		await sleep(3_000);

		expect(callCount).toBe(2);
		expect(await dlqCount(jobName)).toBe(1);
	}, 90_000);

	test("overlap=skip drops overlapping ticks (max 1 active)", async () => {
		const jobName = uniqueName("overlap-skip");
		let active = 0;
		let maxConcurrent = 0;
		let totalCalls = 0;

		const sb = track(dedicated("primary"));
		sb.job.handle(
			jobName,
			{ trigger: { interval: 200 }, overlap: "skip" },
			async () => {
				active++;
				totalCalls++;
				if (active > maxConcurrent) maxConcurrent = active;
				await sleep(800);
				active--;
			},
		);
		await connect(sb);

		// 200ms interval, 800ms handler, skip → ~1 completion every 800ms.
		await sleep(3_000);

		expect(maxConcurrent).toBe(1);
		expect(totalCalls).toBeGreaterThanOrEqual(1);
		expect(totalCalls).toBeLessThanOrEqual(5);
	}, 30_000);

	test("overlap=buffer_one allows at most 1 buffered (in-flight ≤2)", async () => {
		const jobName = uniqueName("overlap-bufone");
		let active = 0;
		let maxConcurrent = 0;

		const sb = track(dedicated("primary"));
		sb.job.handle(
			jobName,
			{ trigger: { interval: 200 }, overlap: "buffer_one" },
			async () => {
				active++;
				if (active > maxConcurrent) maxConcurrent = active;
				await sleep(800);
				active--;
			},
		);
		await connect(sb);

		await sleep(3_000);

		// Server-side dispatch policy: at most 2 concurrent (1 running + 1 buffered).
		expect(maxConcurrent).toBeLessThanOrEqual(2);
	}, 30_000);

	test("overlap=allow with maxConcurrent=1 SDK semaphore caps to 1 active", async () => {
		const jobName = uniqueName("overlap-allow-cap");
		let active = 0;
		let maxConcurrent = 0;

		const sb = track(dedicated("primary"));
		sb.job.handle(
			jobName,
			{ trigger: { interval: 200 }, overlap: "allow", maxConcurrent: 1 },
			async () => {
				active++;
				if (active > maxConcurrent) maxConcurrent = active;
				await sleep(600);
				active--;
			},
		);
		await connect(sb);

		await sleep(3_000);

		// SDK semaphore caps concurrency even though the policy is allow.
		expect(maxConcurrent).toBe(1);
	}, 30_000);

	test("unregistered job stops firing after SDK restart", async () => {
		const jobA = uniqueName("unreg-a");
		const jobB = uniqueName("unreg-b");
		const callsA: number[] = [];
		const callsB: number[] = [];

		// First start: register both A and B (each registered before connect).
		const first = track(dedicated("primary"));
		first.job.handle(
			jobA,
			{ trigger: { interval: 300 }, overlap: "allow", maxConcurrent: 5 },
			async () => {
				callsA.push(Date.now());
			},
		);
		first.job.handle(
			jobB,
			{ trigger: { interval: 300 }, overlap: "allow", maxConcurrent: 5 },
			async () => {
				callsB.push(Date.now());
			},
		);
		await connect(first);

		await waitFor(
			() => callsA.length >= 1 && callsB.length >= 1,
			10_000,
			"both jobs fired",
		);

		await first.stop();
		await sleep(200);

		callsA.length = 0;
		callsB.length = 0;

		// Second start: only register A.
		const second = track(dedicated("primary"));
		second.job.handle(
			jobA,
			{ trigger: { interval: 300 }, overlap: "allow", maxConcurrent: 5 },
			async () => {
				callsA.push(Date.now());
			},
		);
		await connect(second);

		await waitFor(() => callsA.length >= 2, 10_000, "A fires after restart");

		expect(callsB).toHaveLength(0);
	}, 60_000);

	test("cap_job_handle=false denies registration (no job_definitions row, handler never fires)", async () => {
		// ISOLATION: toggles cap_job_handle on this domain's primary identity.
		// Restores cap_job_handle=true in finally. Must NOT run concurrently with
		// any other test using the same identity — flag for the sharder.
		const jobName = uniqueName("denied-job");
		let handlerCalled = false;

		// First connect (no job registered) to learn the serviceId, then disable
		// the capability. Registering nothing keeps job_definitions empty for this
		// job name, so the post-disable count assertion is exact.
		const probe = track(dedicated("primary"));
		await connect(probe);
		const serviceId = probe.identity()!.serviceId;
		await probe.stop();
		await sleep(200);

		try {
			await setCapability(serviceId, "cap_job_handle", false);

			const sb = track(dedicated("primary"));
			sb.job.handle(
				jobName,
				{ trigger: { delayed: { at: Date.now() + 500 } } },
				async () => {
					handlerCalled = true;
				},
			);
			await connect(sb);

			// Wait beyond fire time — registration was denied.
			await sleep(5_000);

			expect(handlerCalled).toBe(false);
			expect(await jobDefCount(jobName)).toBe(0);
		} finally {
			await setCapability(serviceId, "cap_job_handle", true).catch(() => {});
		}
	}, 30_000);

	test("job handler can call RPC on another service", async () => {
		const jobName = uniqueName("handler-rpc");
		const method = "Echo";
		const receivedCalls: unknown[] = [];
		let handlerCalled = false;

		// Service 2 — RPC callee, registers handler before connect.
		const sb2 = track(dedicated("second"));
		sb2.rpc.handle(
			method,
			async (req) => {
				receivedCalls.push(req);
				return req;
			},
			{
				schema: {
					protoFile: ORDER_EVENT_PROTO,
					input: "OrderCreatedV1",
					output: "OrderCreatedV1",
				},
			},
		);
		await connect(sb2);
		const sb2Id = sb2.identity()!.serviceId;
		const sb2ServiceName = sb2.identity()!.serviceName;

		// Service 1 — job owner + RPC caller. Declare the outgoing dep + job
		// before connect.
		const sb1 = track(dedicated("primary"));
		sb1.service(sb2ServiceName, { rpc: [method] });
		await sb1.useSchema(sb2ServiceName, method, {
			protoFile: ORDER_EVENT_PROTO,
			input: "OrderCreatedV1",
			output: "OrderCreatedV1",
		});
		sb1.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 500 } }, maxAttempts: 1 },
			async () => {
				try {
					await sb1.rpc.call(sb2ServiceName, method, {
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
		await connect(sb1);
		const sb1Id = sb1.identity()!.serviceId;

		try {
			await addRule(sb1Id, "E", "rpc.call", sb2Id, method);
			await addRule(sb2Id, "A", "rpc.handle", sb1Id, method);

			await waitFor(() => handlerCalled, 15_000, "job handler executed");

			expect(receivedCalls.length).toBeGreaterThanOrEqual(1);
		} finally {
			await clearRules(sb1Id).catch(() => {});
			await clearRules(sb2Id).catch(() => {});
		}
	}, 45_000);

	test("job handler can publish an event consumed by another service", async () => {
		const eventName = uniqueName("job-evt");
		const jobName = uniqueName("handler-evt");
		const received: unknown[] = [];
		let handlerCalled = false;

		// Service 2 — event consumer, subscribe before connect.
		const sb2 = track(dedicated("second"));
		sb2.event.handle(eventName, async (payload) => {
			received.push(payload);
		});
		sb2.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await connect(sb2);

		// Service 1 — job owner + event publisher.
		const sb1 = track(dedicated("primary"));
		sb1.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		sb1.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 800 } }, maxAttempts: 1 },
			async () => {
				handlerCalled = true;
				await sb1.event.publish(eventName, {
					orderId: "from-job",
					amount: 1,
					currency: "USD",
				});
			},
		);
		await connect(sb1);

		await waitFor(() => handlerCalled, 15_000, "job handler executed");
		await waitFor(() => received.length > 0, 15_000, "event delivered");

		expect(received.length).toBeGreaterThanOrEqual(1);
		expect((received[0] as Record<string, unknown>).orderId).toBe("from-job");
	}, 60_000);

	test("job handler can start a workflow that completes", async () => {
		const jobName = uniqueName("handler-wf");
		const wfName = uniqueName("job-wf");
		let runId: string | undefined;
		let handlerCalled = false;

		// Service 2 — workflow OWNER, registers the workflow before connect
		// (the runtime resolves a run against definitions present at registration).
		const sb2 = track(dedicated("second"));
		sb2.workflow.handle(wfName, {
			steps: [{ type: "local", id: "step1", fn: async () => ({ ok: true }) }],
		});
		await connect(sb2);
		const sb2Id = sb2.identity()!.serviceId;

		// Service 1 — job owner + workflow caller.
		const sb1 = track(dedicated("primary"));
		sb1.job.handle(
			jobName,
			{ trigger: { delayed: { at: Date.now() + 800 } }, maxAttempts: 1 },
			async () => {
				const result = await sb1.workflow.start(wfName, {});
				runId = result.runId;
				handlerCalled = true;
			},
		);
		await connect(sb1);
		const sb1Id = sb1.identity()!.serviceId;

		try {
			await addWorkflowRule(sb1Id, sb2Id, wfName);

			await waitFor(() => handlerCalled, 15_000, "job handler executed");
			expect(runId).toMatch(/^[0-9a-f-]{36}$/);

			const finalStatus = await awaitRunStatus(
				sb1,
				runId!,
				(s) => s === "success" || s === "failed",
				20_000,
			);
			expect(finalStatus).toBe("success");
		} finally {
			await clearRules(sb1Id).catch(() => {});
			await clearRules(sb2Id).catch(() => {});
		}
	}, 60_000);

	test("at-most-once dispatch across 3 replicas of one service", async () => {
		const jobName = uniqueName("multi-replica");
		let totalHandlerCalls = 0;

		// 3 dedicated instances of the SAME service (same per-domain key).
		for (let i = 0; i < 3; i++) {
			const sb = track(dedicated("primary"));
			sb.job.handle(
				jobName,
				{ trigger: { interval: 300 }, overlap: "skip" },
				async () => {
					totalHandlerCalls++;
				},
			);
		}

		await Promise.all(clients.map((r) => connect(r)));

		// 300ms interval over ~1.5s → ≈5 ticks.
		await sleep(1_500);

		// Authoritative DB count scoped to this job: at-most-once dispatch means
		// the global count is close to the tick count (≤10), not 3× that.
		const dbCount = await execSuccessCount(jobName);
		expect(dbCount).toBeGreaterThanOrEqual(1);
		expect(dbCount).toBeLessThanOrEqual(10);

		// Each execution dispatched to exactly one replica (allow slight timing
		// over-count, but nowhere near 3× DB count).
		expect(totalHandlerCalls).toBeGreaterThanOrEqual(1);
		expect(totalHandlerCalls).toBeLessThanOrEqual(dbCount + 2);
	}, 30_000);

	// Two lease/timing-sensitive tests live in jobs-lifecycle.test.ts on their own
	// isolated runtimes, free of the interval/cron jobs the rest of this file
	// leaves firing for the whole process (those create dispatch contention that
	// makes lease windows non-deterministic on the ambient runtime):
	//   - "durability: SDK fully down before fire → fresh replica delivered once"
	//   - "stale lease_epoch in JobResult is rejected and execution re-dispatched"
});
