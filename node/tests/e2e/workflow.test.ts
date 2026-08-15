// workflow — consolidated workflow e2e suite (pooled-friendly tests).
//
// Every OWNER is dedicated: the runtime resolves a run against the workflow
// definitions present at the owner's registration, so `owner.workflow.handle`
// runs BEFORE connect() and the owner is stopped in afterEach. CALLERS that
// only start/await/query runs are the warm pooled client shared("second");
// rpc CALLEES that only upsert rpc handlers (upsert propagates post-start) are
// the warm pooled client shared("third"). Isolation is by uniqueName() — no
// per-test TRUNCATE, no clearRules of pooled identities.
//
// Owner re-registration is folded here (fingerprint coexistence). Access-policy
// mutation tests live in workflow-access-policy.test.ts.

import { afterEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import type { ServiceBridge } from "../../src/connection/service-bridge";
import {
	connect,
	dedicated,
	ORDER_EVENT_PROTO,
	shared,
	uniqueName,
} from "./_helpers/fixtures";
import { addRule, allServiceIDs, withDb } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitPolicyLive,
	awaitRunStatus,
	startWorkflowWhenAllowed,
} from "./_helpers/wf.ts";

const COMP_PROTO = join(import.meta.dir, "_helpers", "compensation.proto");

const TERMINAL = (s: string) =>
	s === "success" || s === "failed" || s === "cancelled";
const TERMINAL_WITH_COMP = (s: string) =>
	s === "success" ||
	s === "failed" ||
	s === "cancelled" ||
	s === "failed_compensated";

async function runFingerprint(runId: string): Promise<string> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT fingerprint FROM workflow_runs WHERE id = ${runId}
		`) as Array<{ fingerprint: string }>;
		if (!rows[0]) throw new Error(`run ${runId} not found`);
		return rows[0].fingerprint;
	});
}

// awaitDefinition waits for the owner's workflow definition row to appear
// (registration is async after connect()).
async function awaitDefinition(ownerID: string, wfName: string): Promise<void> {
	const deadline = Date.now() + 5_000;
	while (Date.now() < deadline) {
		const rows = (await withDb(
			(sql) =>
				sql`SELECT 1 FROM workflow_definitions WHERE service_id = ${ownerID} AND name = ${wfName}` as Promise<
					unknown[]
				>,
		)) as unknown[];
		if (rows.length > 0) return;
		await new Promise((r) => setTimeout(r, 50));
	}
	throw new Error(`workflow definition not registered: ${wfName}`);
}

// awaitDefinitionFingerprint waits until the latest registered definition row
// for (ownerID, wfName) carries a fingerprint different from `notFp`. Re-running
// the same workflow name with a new graph registers a fresh definition row, but
// the registration is async after connect(); without this gate a new run can
// bind to the previous (still-latest) definition.
async function awaitDefinitionFingerprint(
	ownerID: string,
	wfName: string,
	notFp: string,
	timeoutMs = 5_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const rows = (await withDb(
			(sql) =>
				sql`
					SELECT fingerprint FROM workflow_definitions
					WHERE service_id = ${ownerID} AND name = ${wfName}
					ORDER BY registered_at DESC
					LIMIT 1
				` as Promise<Array<{ fingerprint: string }>>,
		)) as Array<{ fingerprint: string }>;
		if (rows[0] && rows[0].fingerprint !== notFp) return;
		await new Promise((r) => setTimeout(r, 50));
	}
	throw new Error(`definition fingerprint did not change from ${notFp}`);
}

// awaitRpcMethodLive waits until the callee's rpc handler is live on a connected
// instance. A call-step inside a workflow fires as soon as the run is dispatched;
// without this gate it can race the callee's post-connect rpc.handle upsert and
// fail with "rpc: no descriptor … not subscribed or target offline".
async function awaitRpcMethodLive(
	calleeName: string,
	method: string,
	timeoutMs = 10_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const rows = (await withDb(
			(sql) =>
				sql`
					SELECT 1
					FROM service_methods sm
					JOIN service_instances si ON si.id = sm.instance_id
					JOIN services s ON s.id = sm.service_id
					WHERE s.name = ${calleeName}
					  AND sm.method_name = ${method}
					  AND sm.method_type = 'rpc'
					  AND si.status = 'connected'
				` as Promise<unknown[]>,
		)) as unknown[];
		if (rows.length > 0) return;
		await new Promise((r) => setTimeout(r, 50));
	}
	throw new Error(`rpc method not live: ${calleeName}/${method}`);
}

// awaitAllWaiting waits until every run is parked in 'waiting'.
async function awaitAllWaiting(
	caller: ServiceBridge,
	runs: Array<{ runId: string }>,
): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		const states = await Promise.all(
			runs.map((r) => caller.workflow.query(r.runId)),
		);
		if (states.every((s) => s.status === "waiting")) return;
		await new Promise((r) => setTimeout(r, 50));
	}
	throw new Error("timeout waiting for: all runs parked on wait_event");
}

describe("workflow", () => {
	let owner: ServiceBridge | undefined;
	// Dedicated rpc callees a test owns. Registered (rpc.handle) BEFORE connect —
	// a post-connect rpc.handle never re-syncs to the runtime, so the call-step
	// would hit "no descriptor … not subscribed". Stopped here.
	let callees: ServiceBridge[] = [];

	afterEach(async () => {
		await owner?.stop();
		owner = undefined;
		for (const c of callees) await c.stop().catch(() => {});
		callees = [];
		// Settle: the runtime's workflow Router round-robins over every owner
		// instance still attached to the hub. stop() cancels the Subscribe stream
		// client-side, but the server-side Detach fires asynchronously when the
		// cancel propagates. Without this gap the next test's run can be dispatched
		// to this just-stopped instance, which no longer holds the local graph
		// (`step.fn is not a function`). This is real teardown elapsed time, not a
		// propagation guess.
		await new Promise((r) => setTimeout(r, 1_500));
	});

	test("linear waitFor chain → success with per-step outputs and statuses", async () => {
		const wfName = uniqueName("happy-linear");

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{ type: "local", id: "step_one", fn: async () => ({ value: 1 }) },
				{
					type: "local",
					id: "step_two",
					fn: async () => ({ value: 2 }),
					waitFor: ["step_one"],
				},
				{
					type: "local",
					id: "step_three",
					fn: async () => ({ value: 3 }),
					waitFor: ["step_two"],
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {
			hello: "world",
		});
		expect(runId).toMatch(/^[0-9a-f-]{36}$/);

		const finalStatus = await awaitRunStatus(caller, runId, TERMINAL, 20_000);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		expect(q.state.step_one).toEqual({ value: 1 });
		expect(q.state.step_two).toEqual({ value: 2 });
		expect(q.state.step_three).toEqual({ value: 3 });
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		expect(byId.get("step_one")?.status).toBe("success");
		expect(byId.get("step_two")?.status).toBe("success");
		expect(byId.get("step_three")?.status).toBe("success");
	}, 30_000);

	test("caller≠owner: owner_service_id is owner and caller.await() resolves without handler", async () => {
		const wfName = uniqueName("caller-not-owner");

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "compute",
					fn: async (state) => ({
						doubled: (state.input as { n: number }).n * 2,
					}),
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		// Caller never registers the workflow handler.
		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		expect(callerID).not.toBe(ownerID);
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, { n: 21 });

		// DB: owner_service_id is the OWNER, not the caller.
		await awaitRunStatus(caller, runId, TERMINAL, 20_000);
		const runRows = await withDb(async (sql) => {
			return sql<Array<{ owner_service_id: string }>>`
				SELECT owner_service_id FROM workflow_runs WHERE id = ${runId}
			`;
		});
		expect(runRows).toHaveLength(1);
		expect(runRows[0]!.owner_service_id).toBe(ownerID);

		// Caller awaits via the streaming RPC without registering the handler.
		const finalState = await caller.workflow.await(runId);
		expect(finalState.compute).toEqual({ doubled: 42 });

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
	}, 30_000);

	test("input schema persists to workflow_definitions.input_schema (not graph)", async () => {
		const wfName = uniqueName("schema-demo");
		const inputSchema = {
			type: "object",
			required: ["orderId", "amount"],
			properties: {
				orderId: { type: "string" },
				amount: { type: "number", minimum: 0 },
				quantity: { type: "integer", minimum: 1 },
				express: { type: "boolean" },
				address: {
					type: "object",
					required: ["city"],
					properties: {
						city: { type: "string" },
						zip: { type: "string" },
					},
				},
			},
		};

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			input: inputSchema,
			steps: [{ type: "local", id: "noop", fn: async () => ({ ok: true }) }],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		// Registration is async after connect; wait for the definition row.
		await awaitDefinition(ownerID, wfName);

		const rows = (await withDb(
			(sql) =>
				sql`
					SELECT input_schema, graph
					FROM workflow_definitions
					WHERE service_id = ${ownerID} AND name = ${wfName}
					ORDER BY registered_at DESC
					LIMIT 1
				` as Promise<Array<{ input_schema: unknown; graph: unknown }>>,
		)) as Array<{ input_schema: Record<string, unknown>; graph: unknown }>;

		expect(rows.length).toBe(1);
		const row = rows[0]!;
		const stored = row.input_schema;

		// The input_schema column holds the declared JSON Schema verbatim.
		expect(stored).toEqual(inputSchema);
		// It is the schema, not the pipeline.
		expect(stored).not.toHaveProperty("graph");
		expect(stored).not.toHaveProperty("steps");
		expect(stored.type).toBe("object");
		// The pipeline lives in its own column.
		expect(row.graph).not.toBeNull();
	}, 30_000);

	test("Start rejects input violating declared schema, accepts valid input", async () => {
		const wfName = uniqueName("schema-reject");

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			input: {
				type: "object",
				required: ["userId"],
				properties: { userId: { type: "string" } },
			},
			steps: [{ type: "local", id: "noop", fn: async () => ({ ok: true }) }],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		// Valid input first settles registration/policy propagation (it retries
		// WorkflowNotFound/AccessDenied) and proves valid input still starts a run.
		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {
			userId: "u-1",
		});
		expect(runId).toMatch(/^[0-9a-f-]{36}$/);

		// Missing required `userId` — runtime validates BEFORE allocating a run.
		let err: Error | null = null;
		try {
			await caller.workflow.start(wfName, { otherField: "x" });
		} catch (e) {
			err = e as Error;
		}
		expect(err).not.toBeNull();
		expect((err as Error).message.toLowerCase()).toContain("schema");
	}, 30_000);

	test("when=false skips step; descendant sees $.<id>=null and still runs", async () => {
		const wfName = uniqueName("when-skip");

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{ type: "local", id: "always", fn: async () => ({ ran: true }) },
				{
					type: "local",
					id: "maybe",
					when: { equals: ["$.input.feature", "on"] },
					fn: async () => ({ ran: true }),
					waitFor: ["always"],
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["maybe"],
					fn: async (state) => ({ maybeSeen: state.maybe }),
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		// feature "off" makes the `when` predicate false → maybe is skipped.
		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {
			feature: "off",
		});
		const finalStatus = await awaitRunStatus(caller, runId, TERMINAL, 20_000);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		expect(q.state.always).toEqual({ ran: true });
		expect(q.state.maybe).toBeNull();
		// Descendant observed `state.maybe === null`.
		expect(q.state.tail).toEqual({ maybeSeen: null });
	}, 30_000);

	test("publish step writes event_log row with publisher_service=owner", async () => {
		const wfName = uniqueName("publish-step");
		const eventName = uniqueName("order.placed");

		owner = dedicated("primary");
		// Owner must define the event schema — event.publish requires it.
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
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {
			orderId: "order-7",
			amount: 12.5,
			currency: "USD",
		});
		const finalStatus = await awaitRunStatus(caller, runId, TERMINAL, 20_000);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		const emit = q.steps.find((s) => s.stepId === "emit");
		expect(emit?.status).toBe("success");
		const stateEmit = q.state.emit as { eventId?: string } | undefined;
		expect(stateEmit?.eventId).toMatch(/^[0-9a-f-]{36}$/);

		// DB: event_log row exists with publisher_service = owner.
		const rows = await withDb(async (sql) => {
			return sql<
				Array<{ id: string; name: string; publisher_service: string }>
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

	test("replay from middle step reuses prior outputs, re-runs from fromStepId", async () => {
		const wfName = uniqueName("replay");

		// Per-step counters increment on every actual execution. Steps before
		// fromStepId must NOT increment on replay (output reused from snapshot).
		const calls = { a: 0, b: 0, c: 0 };

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "a",
					fn: async () => {
						calls.a++;
						return { which: "a", call: calls.a };
					},
				},
				{
					type: "local",
					id: "b",
					waitFor: ["a"],
					fn: async () => {
						calls.b++;
						return { which: "b", call: calls.b };
					},
				},
				{
					type: "local",
					id: "c",
					waitFor: ["b"],
					fn: async () => {
						calls.c++;
						return { which: "c", call: calls.c };
					},
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});
		const first = await awaitRunStatus(caller, runId, TERMINAL, 15_000);
		expect(first).toBe("success");
		expect(calls).toEqual({ a: 1, b: 1, c: 1 });

		// Replay from step "b". Owner-only replay (ADR-W-009).
		const { runId: replayId } = await owner.workflow.replay(runId, {
			fromStepId: "b",
		});
		expect(replayId).not.toBe(runId);

		const second = await awaitRunStatus(owner, replayId, TERMINAL, 15_000);
		expect(second).toBe("success");

		// "a" was NOT re-executed; "b" and "c" were.
		expect(calls).toEqual({ a: 1, b: 2, c: 2 });

		const q = await owner.workflow.query(replayId);
		expect(q.state.a).toEqual({ which: "a", call: 1 });
		expect(q.state.b).toEqual({ which: "b", call: 2 });
		expect(q.state.c).toEqual({ which: "c", call: 2 });
	}, 40_000);

	test("static parallel group overlaps — wall-clock ≤ critical-path budget and siblings overlap", async () => {
		const wfName = uniqueName("fanout");
		const SLEEP_MS = 300;

		const leaf = (id: string) => ({
			type: "local" as const,
			id,
			fn: async () => {
				await new Promise((r) => setTimeout(r, SLEEP_MS));
				return { id, doneAt: Date.now() };
			},
		});

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				leaf("a"),
				{
					type: "parallel",
					id: "g1",
					waitFor: ["a"],
					steps: [leaf("b1"), leaf("b2"), leaf("b3")],
				},
				{ ...leaf("c"), waitFor: ["g1"] },
				{
					type: "parallel",
					id: "g2",
					waitFor: ["c"],
					steps: [leaf("d1"), leaf("d2")],
				},
				{ ...leaf("e"), waitFor: ["g2"] },
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const t0 = Date.now();
		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});
		const finalStatus = await awaitRunStatus(caller, runId, TERMINAL, 20_000);
		const elapsed = Date.now() - t0;
		expect(finalStatus).toBe("success");

		// Critical path = 5 sequential steps × SLEEP_MS. Budget absorbs gRPC
		// overhead (each of 9 steps makes BeginStep + CompleteStep against PG).
		const criticalPath = 5 * SLEEP_MS;
		expect(elapsed).toBeLessThanOrEqual(criticalPath * 2 + 500);

		// Cross-check group siblings overlap: doneAt timestamps within SLEEP_MS×0.7.
		const q = await caller.workflow.query(runId);
		const g1 = q.state.g1 as Record<string, { doneAt: number }>;
		const dones = ["b1", "b2", "b3"].map((id) => g1[id]!.doneAt);
		const spread = Math.max(...dones) - Math.min(...dones);
		expect(spread).toBeLessThan(SLEEP_MS * 0.7);
	}, 30_000);

	test("forEach over non-empty array spawns N keyed parallel sub-steps", async () => {
		const wfName = uniqueName("foreach-dyn");

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "parallel",
					id: "track_each",
					forEach: { from: "$.input.items", as: "it" },
					steps: [
						{
							type: "local",
							id: "track",
							fn: async (state) => ({
								item: state.it,
								doubled: (state.it as number) * 2,
							}),
						},
					],
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {
			items: [1, 2, 3, 4, 5],
		});
		const finalStatus = await awaitRunStatus(caller, runId, TERMINAL, 30_000);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		const fanout = q.state.track_each as Record<
			string,
			{ item: number; doubled: number }
		>;
		expect(Object.keys(fanout)).toHaveLength(5);
		for (let i = 0; i < 5; i++) {
			expect(fanout[`track:${i}`]).toEqual({
				item: i + 1,
				doubled: (i + 1) * 2,
			});
		}
	}, 40_000);

	test("forEach over empty array completes; downstream runs with empty group", async () => {
		const wfName = uniqueName("foreach-empty");

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "parallel",
					id: "fanout",
					forEach: { from: "$.input.items", as: "item" },
					steps: [
						{
							type: "local",
							id: "leaf",
							fn: async (state) => ({ item: state.item }),
						},
					],
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["fanout"],
					fn: async (state) => ({ fanout: state.fanout }),
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {
			items: [],
		});
		const finalStatus = await awaitRunStatus(caller, runId, TERMINAL, 20_000);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		// fanout completes with empty group output; tail saw the empty group.
		expect(q.state.fanout).toEqual({});
		expect(q.state.tail).toEqual({ fanout: {} });
		const fanoutStep = q.steps.find((s) => s.stepId === "fanout");
		expect(fanoutStep?.status).toBe("success");
	}, 30_000);

	test("JSONPath wildcard $.x[*].field aggregates into array at call-step input", async () => {
		const wfName = uniqueName("jsonpath-wildcard");
		// Literal proto method name: call-step input/output is auto-resolved from
		// the proto service block, which has no entry for a uniqueName().
		const method = "CollectIds";

		// Callee: dedicated client, registers the rpc handler BEFORE connect — a
		// post-connect rpc.handle never re-syncs to the runtime. Echoes the
		// wildcard-aggregated array back.
		const callee = dedicated("third");
		callee.rpc.handle(
			method,
			async (req: { ids: number[] }) => ({
				received: req.ids,
				count: req.ids.length,
			}),
			{ schema: { protoFile: COMP_PROTO } },
		);
		await connect(callee);
		callees.push(callee);
		const calleeName = callee.identity()!.serviceName;
		const calleeID = callee.identity()!.serviceId;

		// Owner: declares the outgoing rpc dep BEFORE connect.
		owner = dedicated("primary");
		owner.service(calleeName, { rpc: [method] });
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "list_items",
					fn: async () => ({
						items: [
							{ id: 1, name: "a" },
							{ id: 2, name: "b" },
							{ id: 3, name: "c" },
						],
					}),
				},
				{
					type: "call",
					id: "collect",
					service: calleeName,
					method,
					input: { ids: "$.list_items.items[*].id" },
					waitFor: ["list_items"],
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		// Bilateral workflow policy + rpc.call/handle rules, scoped per-method
		// with the unique method name so the pooled callee's rules never collide.
		await addWorkflowRule(calleeID, ownerID, wfName);
		await addRule(ownerID, "E", "rpc.call", null, method);
		for (const cid of await allServiceIDs(calleeName)) {
			await addRule(cid, "A", "rpc.handle", ownerID, method);
		}
		await owner.useSchema(calleeName, method, { protoFile: COMP_PROTO });
		await awaitRpcMethodLive(calleeName, method);
		// The rule reaches Postgres synchronously and the runtime's snapshot
		// asynchronously; starting the run before it lands denies the step.
		await awaitPolicyLive(owner, "egress", "rpc.call", method);
		await awaitPolicyLive(callee, "acceptance", "rpc.handle", method);

		const { runId } = await startWorkflowWhenAllowed(callee, wfName, {});
		expect(runId).toMatch(/^[0-9a-f-]{36}$/);

		const finalStatus = await awaitRunStatus(
			callee,
			runId,
			TERMINAL_WITH_COMP,
			30_000,
		);
		expect(finalStatus).toBe("success");

		const q = await callee.workflow.query(runId);
		expect(q.status).toBe("success");
		const listOut = q.state.list_items as
			| { items?: Array<{ id: number }> }
			| undefined;
		expect(listOut?.items).toHaveLength(3);
		// collect step received the wildcard-aggregated ids and echoed them back.
		const collectOut = q.state.collect as
			| { received?: number[]; count?: number }
			| undefined;
		expect(collectOut?.received).toEqual([1, 2, 3]);
		expect(collectOut?.count).toBe(3);
	}, 45_000);

	test("sub-workflow cycle X→Y→X rejected; root run fails", async () => {
		const stamp = uniqueName("cyc");
		const wfX = `${stamp}-x`;
		const wfY = `${stamp}-y`;

		owner = dedicated("primary");
		owner.workflow.handle(wfX, {
			steps: [{ type: "workflow", id: "call_y", workflow: wfY, input: {} }],
		});
		owner.workflow.handle(wfY, {
			steps: [{ type: "workflow", id: "call_x", workflow: wfX, input: {} }],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfX);
		await addWorkflowRule(callerID, ownerID, wfY);
		// Owner also starts its own sub-workflows internally.
		await addWorkflowRule(ownerID, ownerID, wfX);
		await addWorkflowRule(ownerID, ownerID, wfY);

		const { runId } = await startWorkflowWhenAllowed(caller, wfX, {});
		const final = await awaitRunStatus(caller, runId, TERMINAL, 20_000);
		expect(final).toBe("failed");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("failed");
		const callYStep = q.steps.find((s) => s.stepId === "call_y");
		expect(callYStep?.status).toBe("failed");
	}, 40_000);

	test("run fingerprint immutable across owner re-registration (F1 kept, F2 new)", async () => {
		const wfName = uniqueName("fp-coexist");

		// --- F1 graph: step_a ---
		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [{ type: "local", id: "step_a", fn: async () => ({ v: 1 }) }],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId: runA } = await startWorkflowWhenAllowed(caller, wfName, {});
		await awaitRunStatus(caller, runA, TERMINAL, 10_000);

		const fpF1 = await runFingerprint(runA);
		expect(fpF1).toBeTruthy();

		// Re-register with F2 graph (different shape) under the same key.
		await owner.stop();
		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{ type: "local", id: "step_x", fn: async () => ({ v: 99 }) },
				{
					type: "local",
					id: "step_y",
					fn: async () => ({ v: 100 }),
					waitFor: ["step_x"],
				},
			],
		});
		await connect(owner);
		// Wait for the F2 definition to land so run-B binds to F2, not the
		// still-latest F1 row.
		await awaitDefinitionFingerprint(ownerID, wfName, fpF1);

		// Start run-B — binds to F2.
		const { runId: runB } = await startWorkflowWhenAllowed(caller, wfName, {});
		await awaitRunStatus(caller, runB, TERMINAL, 10_000);

		const fpF2 = await runFingerprint(runB);
		expect(fpF2).toBeTruthy();
		expect(fpF1).not.toBe(fpF2);
		// run-A's fingerprint column is still F1 (frozen_plan not overwritten).
		expect(await runFingerprint(runA)).toBe(fpF1);
		expect(fpF2).toBe(await runFingerprint(runB));
	}, 40_000);

	test("wait_signal parks run; external signal wakes it with payload in state", async () => {
		const wfName = uniqueName("wait-signal");
		const signalName = uniqueName("approve");

		owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [
				{ type: "local", id: "pre", fn: async () => ({ ready: true }) },
				{
					type: "wait_signal",
					id: "approve",
					signal: signalName,
					timeoutSec: 60,
					waitFor: ["pre"],
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["approve"],
					fn: async (state) => ({ got: state.approve }),
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});

		// Wait until parked on the wait_signal step.
		await awaitRunStatus(
			caller,
			runId,
			(s) => s === "waiting" || s === "running",
			10_000,
		);

		await caller.workflow.signal(runId, signalName, {
			approver: "alice",
			ok: true,
		});

		const final = await awaitRunStatus(caller, runId, TERMINAL, 15_000);
		expect(final).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		expect(q.state.approve).toEqual({ approver: "alice", ok: true });
		expect(q.state.tail).toEqual({ got: { approver: "alice", ok: true } });
	}, 40_000);

	test("wait_event: non-matching publish ignored, matching publish wakes run with payload", async () => {
		const wfName = uniqueName("wait-event");
		const eventName = uniqueName("orders.created");

		owner = dedicated("primary");
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
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		// Caller also publishes — needs the same schema registered.
		caller.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {
			orderId: "order-match",
		});

		await awaitRunStatus(caller, runId, (s) => s === "waiting", 10_000);

		// Non-matching event — must NOT wake the run.
		await caller.event.publish(eventName, {
			orderId: "order-other",
			amount: 1,
			currency: "USD",
		});
		await new Promise((r) => setTimeout(r, 500));
		const stillWaiting = await caller.workflow.query(runId);
		expect(stillWaiting.status).toBe("waiting");

		// Matching event — wakes the run.
		await caller.event.publish(eventName, {
			orderId: "order-match",
			amount: 99.5,
			currency: "USD",
		});

		const final = await awaitRunStatus(caller, runId, TERMINAL, 15_000);
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

	test("wait_event fan-out: one publish wakes N parked runs on same event+filter", async () => {
		const wfName = uniqueName("wait-event-multi");
		const eventName = uniqueName("orders.created");

		owner = dedicated("primary");
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
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		caller.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		const callerID = caller.identity()!.serviceId;
		await addWorkflowRule(callerID, ownerID, wfName);

		// First run gates registration/policy propagation; the rest start clean.
		const first = await startWorkflowWhenAllowed(caller, wfName, { idx: 0 });
		const rest = await Promise.all([
			caller.workflow.start(wfName, { idx: 1 }),
			caller.workflow.start(wfName, { idx: 2 }),
		]);
		const runs = [first, ...rest];

		// All three parked.
		await awaitAllWaiting(caller, runs);

		// One matching publish wakes all three.
		await caller.event.publish(eventName, {
			orderId: "multi",
			amount: 1,
			currency: "USD",
		});

		await Promise.all(
			runs.map((r) =>
				awaitRunStatus(
					caller,
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

	test("compensation exhaustion: first/only step fails with nothing to compensate → failed", async () => {
		const wfName = uniqueName("comp-exhaust");
		// Literal proto method name (auto-resolved from the proto service block).
		const method = "Charge";

		// Charge always fails — it is the FIRST (and only) step. Dedicated callee,
		// rpc handler registered BEFORE connect (post-connect handle never syncs).
		const callee = dedicated("third");
		callee.rpc.handle(
			method,
			async (_req: { reservation_id: string; amount: number }) => {
				throw new Error("immediate terminal failure");
			},
			{ schema: { protoFile: COMP_PROTO } },
		);
		await connect(callee);
		callees.push(callee);
		const calleeName = callee.identity()!.serviceName;
		const calleeID = callee.identity()!.serviceId;

		owner = dedicated("primary");
		owner.service(calleeName, { rpc: [method] });
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "call",
					id: "charge",
					service: calleeName,
					method,
					input: { reservation_id: "no-prior-step", amount: "$.input.amount" },
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		await addWorkflowRule(calleeID, ownerID, wfName);
		await addRule(ownerID, "E", "rpc.call", null, method);
		for (const cid of await allServiceIDs(calleeName)) {
			await addRule(cid, "A", "rpc.handle", ownerID, method);
		}
		await owner.useSchema(calleeName, method, { protoFile: COMP_PROTO });
		await awaitRpcMethodLive(calleeName, method);
		// The rule reaches Postgres synchronously and the runtime's snapshot
		// asynchronously; starting the run before it lands denies the step.
		await awaitPolicyLive(owner, "egress", "rpc.call", method);
		await awaitPolicyLive(callee, "acceptance", "rpc.handle", method);

		const { runId } = await startWorkflowWhenAllowed(callee, wfName, {
			amount: 99.0,
		});

		// No completed steps → no compensation → plain 'failed'.
		const finalStatus = await awaitRunStatus(
			callee,
			runId,
			TERMINAL_WITH_COMP,
			20_000,
		);
		expect(finalStatus).toBe("failed");

		const q = await callee.workflow.query(runId);
		expect(q.status).toBe("failed");
		const chargeStep = q.steps.find((s) => s.stepId === "charge");
		expect(chargeStep?.status).toBe("failed");
	}, 30_000);

	test("partial compensation: only steps with a compensate spec are compensated", async () => {
		const wfName = uniqueName("par-partial-comp");
		// Literal proto method names (auto-resolved from the proto service block).
		const reserveM = "Reserve";
		const chargeM = "Charge";
		const releaseM = "Release";

		// Dedicated callee, all rpc handlers registered BEFORE connect (a
		// post-connect rpc.handle never re-syncs to the runtime).
		const callee = dedicated("third");
		callee.rpc.handle(
			reserveM,
			async (req: { item_id: string; quantity: number }) => ({
				reservation_id: `rsv-${req.item_id}-${Date.now()}`,
				ok: true,
			}),
			{ schema: { protoFile: COMP_PROTO } },
		);
		callee.rpc.handle(
			chargeM,
			async (_req: { reservation_id: string; amount: number }) => {
				throw new Error("payment gateway timeout");
			},
			{ schema: { protoFile: COMP_PROTO } },
		);
		callee.rpc.handle(
			releaseM,
			async (_req: { reservation_id: string }) => ({ ok: true }),
			{ schema: { protoFile: COMP_PROTO } },
		);
		await connect(callee);
		callees.push(callee);
		const calleeName = callee.identity()!.serviceName;
		const calleeID = callee.identity()!.serviceId;

		owner = dedicated("primary");
		owner.service(calleeName, { rpc: [reserveM, chargeM, releaseM] });
		owner.workflow.handle(wfName, {
			steps: [
				// Step 1: call with compensate spec.
				{
					type: "call",
					id: "reserve",
					service: calleeName,
					method: reserveM,
					input: { item_id: "$.input.item_id", quantity: "$.input.quantity" },
					compensate: {
						service: calleeName,
						method: releaseM,
						input: { reservation_id: "$.reserve.reservation_id" },
					},
				},
				// Step 2: local step — NO compensate spec. Runner skips it.
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
					method: chargeM,
					input: {
						reservation_id: "$.reserve.reservation_id",
						amount: "$.input.amount",
					},
					waitFor: ["log_event"],
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		await addWorkflowRule(calleeID, ownerID, wfName);
		const allCalleeIDs = await allServiceIDs(calleeName);
		for (const method of [reserveM, chargeM, releaseM]) {
			await addRule(ownerID, "E", "rpc.call", null, method);
			for (const cid of allCalleeIDs) {
				await addRule(cid, "A", "rpc.handle", ownerID, method);
			}
			await owner.useSchema(calleeName, method, { protoFile: COMP_PROTO });
			await awaitRpcMethodLive(calleeName, method);
			// The rule reaches Postgres synchronously and the runtime's snapshot
			// asynchronously; starting the run before it lands denies the step.
			await awaitPolicyLive(owner, "egress", "rpc.call", method);
			await awaitPolicyLive(callee, "acceptance", "rpc.handle", method);
		}

		const { runId } = await startWorkflowWhenAllowed(callee, wfName, {
			item_id: "widget-99",
			quantity: 2,
			amount: 49.0,
		});

		const finalStatus = await awaitRunStatus(
			callee,
			runId,
			TERMINAL_WITH_COMP,
			30_000,
		);
		expect(finalStatus).toBe("failed_compensated");

		const q = await callee.workflow.query(runId);
		expect(q.status).toBe("failed_compensated");
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		// reserve: compensated (has compensate spec).
		expect(byId.get("reserve")?.status).toBe("compensated");
		expect(byId.get("reserve")?.compensatedBy).toBe("reserve.compensate");
		// log_event: success (local, no compensate spec — runner skips it).
		expect(byId.get("log_event")?.status).toBe("success");
		// charge: failed.
		expect(byId.get("charge")?.status).toBe("failed");
	}, 40_000);
});
