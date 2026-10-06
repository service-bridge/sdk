// workflow-engine — guarantees of the runtime-interpreted DAG with per-step
// leases (runtime ADR 0003), end to end over the real runtime:
//   - a parked branch waking up never re-executes a live sibling (WF-01);
//   - two parallel wait_event steps both receive their events (WF-02);
//   - an owner instance dying while the run is parked does not strand it, and
//     an expired task lease moves the step to another instance (WF-03);
//   - a failing compensation ends failed_compensated and can be retried (WF-04);
//   - signals form a queue with signal_id dedup (decision 5b);
//   - a stranger service is refused (SEC-01);
//   - the run timeout ends timed_out and cancelling a parent cascades (WF-16).
//
// Owners are dedicated (definitions travel in their registration); callers are
// pooled.

import { afterEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import type { ServiceBridge } from "../../src/connection/service-bridge";
import {
	WorkflowAccessDeniedError,
	WorkflowRunFailedError,
} from "../../src/workflow/errors";
import type { WorkflowDef } from "../../src/workflow/types";
import {
	connect,
	dedicated,
	ORDER_EVENT_PROTO,
	shared,
	sleep,
	uniqueName,
	waitFor,
} from "./_helpers/fixtures";
import { addRule, allServiceIDs, withDb } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitParked,
	awaitPolicyLive,
	awaitRunStatus,
	expireTaskLeases,
	startWorkflowWhenAllowed,
	stateOf,
	stepStatus,
	svcName,
} from "./_helpers/wf.ts";

const COMP_PROTO = join(import.meta.dir, "_helpers", "compensation.proto");
const TERMINAL = (s: string) =>
	[
		"success",
		"failed",
		"cancelled",
		"timed_out",
		"failed_compensated",
	].includes(s);

function gate(): { wait: Promise<void>; open: () => void } {
	let open!: () => void;
	const wait = new Promise<void>((resolve) => {
		open = resolve;
	});
	return { wait, open };
}

async function awaitRpcMethodLive(
	calleeName: string,
	method: string,
): Promise<void> {
	await waitFor(
		async () => {
			const rows = (await withDb(
				(sql) =>
					sql`SELECT 1 FROM service_methods sm
						JOIN service_instances si ON si.id = sm.instance_id
						JOIN services s ON s.id = sm.service_id
						WHERE s.name = ${calleeName} AND sm.method_name = ${method}
						  AND sm.method_type = 'rpc' AND si.status = 'connected'` as Promise<
						unknown[]
					>,
			)) as unknown[];
			return rows.length > 0;
		},
		10_000,
		`rpc ${calleeName}/${method} live`,
	);
}

describe("workflow engine", () => {
	let owners: ServiceBridge[] = [];
	const openGates: Array<() => void> = [];

	afterEach(async () => {
		for (const open of openGates) open();
		openGates.length = 0;
		for (const o of owners) await o.stop().catch(() => {});
		owners = [];
	});

	async function owner(
		name: string,
		def: WorkflowDef,
		extra?: (sb: ServiceBridge) => void,
	) {
		const sb = dedicated("primary");
		owners.push(sb);
		sb.workflow.handle(name, def);
		extra?.(sb);
		await connect(sb);
		return sb;
	}

	async function callerFor(o: ServiceBridge, ...names: string[]) {
		const caller = await shared("second");
		for (const n of names)
			await addWorkflowRule(
				caller.identity()!.serviceId,
				o.identity()!.serviceId,
				n,
			);
		return caller;
	}

	test("a parked sibling waking up does not re-execute a live branch (WF-01)", async () => {
		const wf = uniqueName("par-live");
		const held = gate();
		openGates.push(held.open);
		let executions = 0;
		const o = await owner(wf, {
			version: "v1",
			steps: [
				{ id: "nap", type: "sleep", durationMs: 300 },
				{
					id: "work",
					type: "local",
					fn: async () => {
						executions++;
						await held.wait;
						return { done: true };
					},
				},
			],
		});
		const caller = await callerFor(o, wf);
		const { runId } = await startWorkflowWhenAllowed(caller, o, wf, {});
		await waitFor(
			async () => (await stepStatus(runId, "nap")) === "success",
			10_000,
			"sleep fired",
		);
		held.open();
		expect(await awaitRunStatus(caller, runId, TERMINAL, 15_000)).toBe(
			"success",
		);
		expect(executions).toBe(1);
		const q = await caller.workflow.query(runId);
		expect(q.steps.find((s) => s.stepId === "work")?.attempt).toBe(1);
	}, 30_000);

	test("two parallel wait_event steps both receive their events (WF-02)", async () => {
		const wf = uniqueName("par-events");
		const evA = uniqueName("orders.a");
		const evB = uniqueName("orders.b");
		const o = await owner(
			wf,
			{
				version: "v1",
				steps: [
					{ id: "a", type: "wait_event", event: evA },
					{
						id: "b",
						type: "wait_event",
						event: evB,
						filter: { "$.orderId": "$.input.id" },
					},
					{ id: "nap", type: "sleep", durationMs: 200 },
				],
			},
			(sb) => {
				sb.event.define(evA, {
					protoFile: ORDER_EVENT_PROTO,
					method: "orders_created",
				});
				sb.event.define(evB, {
					protoFile: ORDER_EVENT_PROTO,
					method: "orders_created",
				});
			},
		);
		const caller = await callerFor(o, wf);
		caller.event.define(evA, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		caller.event.define(evB, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		const { runId } = await startWorkflowWhenAllowed(caller, o, wf, {
			id: "o-1",
		});
		await waitFor(
			async () => (await stepStatus(runId, "nap")) === "success",
			10_000,
			"sleep fired",
		);
		await caller.event.publish(evB, {
			orderId: "o-1",
			amount: 2,
			currency: "EUR",
		});
		await caller.event.publish(evA, {
			orderId: "x",
			amount: 1,
			currency: "USD",
		});
		expect(await awaitRunStatus(caller, runId, TERMINAL, 15_000)).toBe(
			"success",
		);
		const state = stateOf(await caller.workflow.query(runId));
		expect((state.a as { currency: string }).currency).toBe("USD");
		expect((state.b as { orderId: string }).orderId).toBe("o-1");
	}, 30_000);

	test("an owner instance dying while the run is parked does not strand it (WF-03)", async () => {
		const wf = uniqueName("park-crash");
		const signal = uniqueName("go");
		let tailOn = "";
		const def = (who: string): WorkflowDef => ({
			version: "v1",
			steps: [
				{ id: "wait", type: "wait_signal", signal },
				{
					id: "tail",
					type: "local",
					waitFor: ["wait"],
					fn: async () => {
						tailOn = who;
						return who;
					},
				},
			],
		});
		const a = await owner(wf, def("a"));
		const caller = await callerFor(a, wf);
		const { runId } = await startWorkflowWhenAllowed(caller, a, wf, {});
		await awaitParked(caller, runId);
		await a.stop();
		owners = owners.filter((o) => o !== a);
		await owner(wf, def("b"));
		await caller.workflow.signal(runId, signal, { ok: true });
		expect(await awaitRunStatus(caller, runId, TERMINAL, 20_000)).toBe(
			"success",
		);
		expect(tailOn).toBe("b");
	}, 40_000);

	test("an expired task lease moves the step to another instance", async () => {
		const wf = uniqueName("lease-move");
		const held = gate();
		openGates.push(held.open);
		let ranOn = "";
		const def = (who: string): WorkflowDef => ({
			version: "v1",
			steps: [
				{
					id: "work",
					type: "local",
					fn: async () => {
						if (who === "a") await held.wait;
						ranOn = who;
						return who;
					},
				},
			],
		});
		const a = await owner(wf, def("a"));
		const caller = await callerFor(a, wf);
		const { runId } = await startWorkflowWhenAllowed(caller, a, wf, {});
		await waitFor(
			async () => (await stepStatus(runId, "work")) === "leased",
			10_000,
			"leased",
		);
		await a.stop();
		owners = owners.filter((o) => o !== a);
		await owner(wf, def("b"));
		await expireTaskLeases(runId);
		expect(await awaitRunStatus(caller, runId, TERMINAL, 20_000)).toBe(
			"success",
		);
		expect(ranOn).toBe("b");
		held.open();
	}, 40_000);

	test("a failing compensation ends failed_compensated and retryCompensation recovers it (WF-04)", async () => {
		const wf = uniqueName("comp-retry");
		let releaseFailures = 1;
		const callee = dedicated("third");
		owners.push(callee);
		callee.rpc.handle(
			"Reserve",
			async () => ({ reservation_id: "r-1", ok: true }),
			{ schema: { protoFile: COMP_PROTO } },
		);
		callee.rpc.handle(
			"Charge",
			async () => {
				throw new Error("declined");
			},
			{ schema: { protoFile: COMP_PROTO } },
		);
		callee.rpc.handle(
			"Release",
			async () => {
				if (releaseFailures-- > 0) throw new Error("release down");
				return { ok: true };
			},
			{ schema: { protoFile: COMP_PROTO } },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;
		const o = await owner(
			wf,
			{
				version: "v1",
				steps: [
					{
						id: "reserve",
						type: "call",
						service: calleeName,
						method: "Reserve",
						input: { item_id: "i", quantity: 1 },
						compensate: {
							method: "Release",
							input: { reservation_id: "$.reserve.reservation_id" },
						},
					},
					{
						id: "charge",
						type: "call",
						service: calleeName,
						method: "Charge",
						waitFor: ["reserve"],
						input: { reservation_id: "$.reserve.reservation_id", amount: 1 },
					},
				],
			},
			(sb) => sb.service(calleeName, { rpc: ["Reserve", "Charge", "Release"] }),
		);
		const ownerID = o.identity()!.serviceId;
		const caller = await callerFor(o, wf);
		for (const m of ["Reserve", "Charge", "Release"]) {
			await addRule(ownerID, "E", "rpc.call", null, m);
			for (const cid of await allServiceIDs(calleeName))
				await addRule(cid, "A", "rpc.handle", ownerID, m);
			await o.useSchema(calleeName, m, { protoFile: COMP_PROTO });
			await awaitRpcMethodLive(calleeName, m);
			await awaitPolicyLive(o, "egress", "rpc.call", m);
		}
		const { runId } = await startWorkflowWhenAllowed(caller, o, wf, {});
		expect(await awaitRunStatus(caller, runId, TERMINAL, 30_000)).toBe(
			"failed_compensated",
		);
		await caller.workflow.retryCompensation(runId);
		expect(await awaitRunStatus(caller, runId, TERMINAL, 30_000)).toBe(
			"failed",
		);
		const q = await caller.workflow.query(runId);
		expect(q.steps.find((s) => s.stepId === "reserve")?.status).toBe(
			"compensated",
		);
		const err = await caller.workflow.await(runId).catch((e) => e);
		expect(err).toBeInstanceOf(WorkflowRunFailedError);
	}, 60_000);

	test("signals are a queue: two identical signals are both delivered, signal_id dedups a resend", async () => {
		const wf = uniqueName("signals");
		const o = await owner(wf, {
			version: "v1",
			steps: [
				{ id: "first", type: "wait_signal", signal: "approve" },
				{
					id: "second",
					type: "wait_signal",
					signal: "approve",
					waitFor: ["first"],
				},
			],
		});
		const caller = await callerFor(o, wf);
		const { runId } = await startWorkflowWhenAllowed(caller, o, wf, {});
		expect(
			await caller.workflow.signal(
				runId,
				"approve",
				{ n: 1 },
				{ signalId: "s-1" },
			),
		).toEqual({ duplicate: false });
		expect(
			await caller.workflow.signal(
				runId,
				"approve",
				{ n: 1 },
				{ signalId: "s-1" },
			),
		).toEqual({ duplicate: true });
		await caller.workflow.signal(runId, "approve", { n: 2 });
		const out = await caller.workflow.await(runId);
		expect(out.first).toEqual({ n: 1 });
		expect(out.second).toEqual({ n: 2 });
	}, 30_000);

	test("a service that is neither owner, starter nor granted is refused (SEC-01)", async () => {
		const wf = uniqueName("stranger");
		const o = await owner(wf, {
			version: "v1",
			steps: [{ id: "w", type: "wait_signal", signal: "x" }],
		});
		const caller = await callerFor(o, wf);
		const { runId } = await startWorkflowWhenAllowed(caller, o, wf, {});
		const stranger = await shared("third");
		for (const op of [
			() => stranger.workflow.query(runId),
			() => stranger.workflow.signal(runId, "x", {}),
			() => stranger.workflow.cancel(runId),
		]) {
			expect(await op().catch((e) => e)).toBeInstanceOf(
				WorkflowAccessDeniedError,
			);
		}
		await caller.workflow.cancel(runId);
		expect(await awaitRunStatus(caller, runId, TERMINAL, 15_000)).toBe(
			"cancelled",
		);
	}, 30_000);

	test("the run timeout ends the run timed_out (WF-16)", async () => {
		const wf = uniqueName("timeout");
		const o = await owner(wf, {
			version: "v1",
			steps: [{ id: "w", type: "wait_signal", signal: "never" }],
		});
		const caller = await callerFor(o, wf);
		await startWorkflowWhenAllowed(caller, o, wf, {}); // settles registration and policy
		const { runId } = await caller.workflow.start(
			svcName(o),
			wf,
			{},
			{ timeoutMs: 300 },
		);
		expect(await awaitRunStatus(caller, runId, TERMINAL, 15_000)).toBe(
			"timed_out",
		);
	}, 30_000);

	test("cancelling a parent cancels its unfinished child (WF-16)", async () => {
		const parent = uniqueName("parent");
		const child = uniqueName("child");
		const sb = dedicated("primary");
		owners.push(sb);
		sb.workflow.handle(parent, {
			version: "v1",
			steps: [{ id: "kid", type: "workflow", workflow: child }],
		});
		sb.workflow.handle(child, {
			version: "v1",
			steps: [{ id: "w", type: "wait_signal", signal: "never" }],
		});
		await connect(sb);
		const ownerID = sb.identity()!.serviceId;
		await addWorkflowRule(ownerID, ownerID, child);
		const caller = await callerFor(sb, parent);
		const { runId } = await startWorkflowWhenAllowed(caller, sb, parent, {});
		let childRun = "";
		await waitFor(
			async () => {
				const q = await caller.workflow.query(runId);
				childRun = q.steps.find((s) => s.stepId === "kid")?.childRunId ?? "";
				return childRun !== "";
			},
			10_000,
			"child started",
		);
		await caller.workflow.cancel(runId);
		expect(await awaitRunStatus(caller, runId, TERMINAL, 15_000)).toBe(
			"cancelled",
		);
		expect(await awaitRunStatus(sb, childRun, TERMINAL, 15_000)).toBe(
			"cancelled",
		);
		await sleep(0);
	}, 30_000);
});
