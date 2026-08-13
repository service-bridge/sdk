// workflow-lease — fencing and reclaim of workflow run leases.
//
// The lease is the only thing standing between a workflow run and two SDK
// instances driving it at once: every checkpoint RPC carries `lease_epoch`, and
// a reclaim bumps that epoch so the previous holder's checkpoints are rejected
// with gRPC ABORTED. These tests drive the real runtime sweeps (LeaseManager
// for running/waiting, the dispatcher's ListStaleCompensating pass for
// compensating) by expiring the lease in Postgres, which is exactly what a dead
// holder looks like to the runtime.
//
// Owners are dedicated: a run is dispatched to whichever instance of the owner
// service is attached, so a test that hands a run from one holder to another
// must own both instances and their lifecycles.

import { afterEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import type { ServiceBridge } from "../../src/connection/service-bridge";
import { connect, dedicated, shared, uniqueName } from "./_helpers/fixtures";
import { addRule, allServiceIDs, withDb } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	forceLeaseReclaim,
	GRPC_ABORTED,
	grpcCodeOf,
	leaseOf,
	reclaimLeaseNow,
	startWorkflowWhenAllowed,
	stepStatus,
	workflowsWire,
} from "./_helpers/wf.ts";

const COMP_PROTO = join(import.meta.dir, "_helpers", "compensation.proto");

// Gate — a promise a step body blocks on, so a test can hold a run inside a
// step for as long as it needs and release it deterministically.
function gate(): { wait: Promise<void>; open: () => void } {
	let open!: () => void;
	const wait = new Promise<void>((resolve) => {
		open = resolve;
	});
	return { wait, open };
}

async function waitForStep(
	runId: string,
	stepId: string,
	want: string,
	timeoutMs = 20_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if ((await stepStatus(runId, stepId)) === want) return;
		await new Promise((r) => setTimeout(r, 100));
	}
	throw new Error(
		`step ${stepId} of run ${runId} never reached "${want}" (last: "${await stepStatus(runId, stepId)}")`,
	);
}

// awaitRpcMethodLive gates on the callee's handler being visible on a connected
// instance. A call-step fires the moment the run is dispatched; without this it
// races the callee's registration and fails with "no descriptor".
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

function completeStepWire(
	sb: ServiceBridge,
	runId: string,
	stepId: string,
	leaseEpoch: number,
): Promise<unknown> {
	return new Promise((resolve, reject) => {
		workflowsWire(sb).completeStep(
			{
				runId,
				stepId,
				output: Buffer.from(JSON.stringify({ fromWire: true }), "utf8"),
				leaseEpoch,
			},
			(err, resp) => (err ? reject(err) : resolve(resp)),
		);
	});
}

function failStepWire(
	sb: ServiceBridge,
	runId: string,
	stepId: string,
	leaseEpoch: number,
): Promise<unknown> {
	return new Promise((resolve, reject) => {
		workflowsWire(sb).failStep(
			{
				runId,
				stepId,
				errorCode: "TEST",
				errorMessage: "stale holder reporting failure",
				leaseEpoch,
				retriable: false,
			},
			(err, resp) => (err ? reject(err) : resolve(resp)),
		);
	});
}

function heartbeatWire(
	sb: ServiceBridge,
	runId: string,
	leaseEpoch: number,
): Promise<unknown> {
	return new Promise((resolve, reject) => {
		workflowsWire(sb).heartbeat(
			{ runId, instanceId: sb.identity()!.instanceId, leaseEpoch },
			(err, resp) => (err ? reject(err) : resolve(resp)),
		);
	});
}

describe("workflow lease", () => {
	let owners: ServiceBridge[] = [];
	let callees: ServiceBridge[] = [];
	const openGates: Array<() => void> = [];

	afterEach(async () => {
		for (const open of openGates) open();
		openGates.length = 0;
		for (const o of owners) await o.stop().catch(() => {});
		owners = [];
		for (const c of callees) await c.stop().catch(() => {});
		callees = [];
		// The runtime's Router round-robins over every owner instance still
		// attached to the hub; server-side Detach lands asynchronously after the
		// client cancels its Subscribe stream. Without this gap the next test's run
		// is dispatched to an instance that is already gone.
		await new Promise((r) => setTimeout(r, 1_500));
	});

	test("reclaim fences the previous holder: checkpoints with the pre-reclaim epoch are ABORTED, the same call under the new epoch is accepted", async () => {
		const wfName = uniqueName("fence-epoch");
		const held = gate();
		openGates.push(held.open);

		const owner = dedicated("primary");
		owners.push(owner);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "hold",
					fn: async () => {
						await held.wait;
						return { held: true };
					},
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["hold"],
					fn: async () => ({ tail: true }),
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		await addWorkflowRule(caller.identity()!.serviceId, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});
		await waitForStep(runId, "hold", "in_flight");

		const before = await leaseOf(runId);
		expect(before.status).toBe("running");

		await reclaimLeaseNow(runId);

		const afterReclaim = await leaseOf(runId);
		expect(afterReclaim.leaseEpoch).toBe(before.leaseEpoch + 1);

		// The holder that was executing `hold` still carries the pre-reclaim epoch.
		// Every checkpoint it can make must be refused.
		expect(
			await grpcCodeOf(() =>
				completeStepWire(owner, runId, "hold", before.leaseEpoch),
			),
		).toBe(GRPC_ABORTED);
		expect(
			await grpcCodeOf(() =>
				failStepWire(owner, runId, "hold", before.leaseEpoch),
			),
		).toBe(GRPC_ABORTED);
		expect(
			await grpcCodeOf(() => heartbeatWire(owner, runId, before.leaseEpoch)),
		).toBe(GRPC_ABORTED);

		// Control: the identical CompleteStep differing only in lease_epoch is
		// accepted, so the rejections above are the epoch and nothing else. The
		// epoch is stable here — a forward re-dispatch reuses it, only a reclaim
		// or an AssignLease bumps it.
		expect(
			await grpcCodeOf(() =>
				completeStepWire(owner, runId, "hold", afterReclaim.leaseEpoch),
			),
		).toBeNull();
		expect(await stepStatus(runId, "hold")).toBe("success");

		held.open();
		expect(
			await awaitRunStatus(caller, runId, (s) => s === "success", 30_000),
		).toBe("success");
	}, 90_000);

	test("expired lease hands the run to a live instance, which drives it to success", async () => {
		const wfName = uniqueName("reclaim-handoff");
		const held = gate();
		openGates.push(held.open);
		let tailRanOnA = false;
		let tailRanOnB = false;

		// Instance A takes the run and never returns from `hold` — the holder is
		// alive on the wire but the run is going nowhere.
		const ownerA = dedicated("primary");
		owners.push(ownerA);
		ownerA.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "hold",
					fn: async () => {
						await held.wait;
						return { on: "a" };
					},
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["hold"],
					fn: async () => {
						tailRanOnA = true;
						return { tailOn: "a" };
					},
				},
			],
		});
		await connect(ownerA);
		const ownerID = ownerA.identity()!.serviceId;

		const caller = await shared("second");
		await addWorkflowRule(caller.identity()!.serviceId, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});
		await waitForStep(runId, "hold", "in_flight");
		expect((await leaseOf(runId)).status).toBe("running");

		// A dies. Its stream Detach is asynchronous server-side, so settle before
		// the run may be handed on — otherwise the Router can still pick A.
		await ownerA.stop();
		await new Promise((r) => setTimeout(r, 2_500));

		// Instance B of the same owner service comes up with the same workflow.
		const ownerB = dedicated("primary");
		owners.push(ownerB);
		ownerB.workflow.handle(wfName, {
			steps: [
				{ type: "local", id: "hold", fn: async () => ({ on: "b" }) },
				{
					type: "local",
					id: "tail",
					waitFor: ["hold"],
					fn: async () => {
						tailRanOnB = true;
						return { tailOn: "b" };
					},
				},
			],
		});
		await connect(ownerB);

		// Expire the lease: this is what the LeaseManager sweep looks for.
		await forceLeaseReclaim(runId);

		expect(
			await awaitRunStatus(caller, runId, (s) => s === "success", 45_000),
		).toBe("success");

		// The run finished on B, not on the instance that was holding it.
		expect(tailRanOnB).toBe(true);
		expect(tailRanOnA).toBe(false);

		// Deliberately no epoch assertion. A 'running' run with an expired lease is
		// matched by two independent loops — the LeaseManager sweep, which bumps
		// the epoch, and the dispatcher's ClaimDueRuns, which does not — and the
		// dispatcher polls faster, so which one reassigns the run is a race. The
		// sweep's bump is asserted where it is the only path: the 'waiting' test
		// below.
		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		expect(q.state.tail).toEqual({ tailOn: "b" });
	}, 120_000);

	test("expired lease on a parked run: the sweep bumps the epoch and releases the holder, the run stays parked and resumes on another instance", async () => {
		const wfName = uniqueName("reclaim-parked");
		const signalName = uniqueName("go");
		let tailRanOnA = false;
		let tailRanOnB = false;

		const ownerA = dedicated("primary");
		owners.push(ownerA);
		ownerA.workflow.handle(wfName, {
			steps: [
				{
					type: "wait_signal",
					id: "approve",
					signal: signalName,
					timeoutSec: 120,
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["approve"],
					fn: async () => {
						tailRanOnA = true;
						return { tailOn: "a" };
					},
				},
			],
		});
		await connect(ownerA);
		const ownerID = ownerA.identity()!.serviceId;

		const caller = await shared("second");
		await addWorkflowRule(caller.identity()!.serviceId, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});
		expect(
			await awaitRunStatus(caller, runId, (s) => s === "waiting", 20_000),
		).toBe("waiting");
		const before = await leaseOf(runId);

		await forceLeaseReclaim(runId);

		// ClaimDueRuns deliberately skips 'waiting', so the LeaseManager sweep is
		// the only thing that can touch this run: it bumps the epoch and drops the
		// holder while leaving the run parked — its resume belongs to the signal.
		const deadline = Date.now() + 20_000;
		let swept = await leaseOf(runId);
		while (swept.leaseEpoch === before.leaseEpoch && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 250));
			swept = await leaseOf(runId);
		}
		expect(swept.leaseEpoch).toBe(before.leaseEpoch + 1);
		expect(swept.status).toBe("waiting");
		expect(swept.leaseHolderInstanceId).toBeNull();
		expect(swept.leaseExpiresAtMs).toBeNull();

		await ownerA.stop();
		await new Promise((r) => setTimeout(r, 2_500));

		const ownerB = dedicated("primary");
		owners.push(ownerB);
		ownerB.workflow.handle(wfName, {
			steps: [
				{
					type: "wait_signal",
					id: "approve",
					signal: signalName,
					timeoutSec: 120,
				},
				{
					type: "local",
					id: "tail",
					waitFor: ["approve"],
					fn: async () => {
						tailRanOnB = true;
						return { tailOn: "b" };
					},
				},
			],
		});
		await connect(ownerB);

		await caller.workflow.signal(runId, signalName, { ok: true });

		expect(
			await awaitRunStatus(caller, runId, (s) => s === "success", 45_000),
		).toBe("success");
		expect(tailRanOnB).toBe(true);
		expect(tailRanOnA).toBe(false);

		// The wake goes through AssignLease, which bumps once more and finally
		// records a holder.
		const after = await leaseOf(runId);
		expect(after.leaseEpoch).toBeGreaterThan(swept.leaseEpoch);
	}, 120_000);

	test("heartbeats keep a lease alive across a step longer than lease_ttl_ms", async () => {
		const leaseTtlMs = await settingMs("workflow.lease_ttl_ms");
		const holdMs = leaseTtlMs + 8_000;
		const wfName = uniqueName("heartbeat-hold");
		const held = gate();
		openGates.push(held.open);

		const owner = dedicated("primary");
		owners.push(owner);
		// The sleep step is what gives the run a lease holder: the timer wake goes
		// through AssignLease, which writes lease_holder_instance_id — the column
		// RenewLease matches on.
		owner.workflow.handle(wfName, {
			steps: [
				{ type: "sleep", id: "nap", durationSec: 1 },
				{
					type: "local",
					id: "work",
					waitFor: ["nap"],
					fn: async () => {
						await held.wait;
						return { worked: true };
					},
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		await addWorkflowRule(caller.identity()!.serviceId, ownerID, wfName);

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});
		await waitForStep(runId, "work", "in_flight");

		const start = await leaseOf(runId);
		expect(start.leaseHolderInstanceId).toBe(owner.identity()!.instanceId);
		const firstExpiry = start.leaseExpiresAtMs!;

		// Watch the whole window the step occupies. The run must stay 'running'
		// under the same epoch — a reclaim would flip it to 'pending' and bump —
		// and the expiry must be pushed out past its original value.
		const deadline = Date.now() + holdMs;
		let maxExpiry = firstExpiry;
		while (Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 1_000));
			const l = await leaseOf(runId);
			expect(l.status).toBe("running");
			expect(l.leaseEpoch).toBe(start.leaseEpoch);
			expect(l.leaseHolderInstanceId).toBe(start.leaseHolderInstanceId);
			maxExpiry = Math.max(maxExpiry, l.leaseExpiresAtMs!);
		}
		// The step outlived the TTL — the expiry stamped at assignment is already in
		// the past — and the lease survived only because heartbeats pushed it out.
		expect(Date.now()).toBeGreaterThan(firstExpiry);
		expect(maxExpiry).toBeGreaterThan(firstExpiry);

		held.open();
		expect(
			await awaitRunStatus(caller, runId, (s) => s === "success", 30_000),
		).toBe("success");
		const finished = await leaseOf(runId);
		expect(finished.leaseEpoch).toBe(start.leaseEpoch);
	}, 180_000);

	test("re-dispatched compensation does not replay a compensation that already succeeded", async () => {
		const wfName = uniqueName("comp-refence");
		const reserveM = "Reserve";
		const chargeM = "Charge";
		const releaseM = "Release";

		// Release counts calls per reservation. `rsv-a` always fails, so the first
		// compensation pass ends mid-walk with the run still 'compensating' and its
		// lease held by a runner that will never finish it.
		const releaseCalls = new Map<string, number>();
		const callee = dedicated("third");
		callees.push(callee);
		callee.rpc.handle(
			reserveM,
			async (req: { itemId: string }) => ({
				reservationId: `rsv-${req.itemId}`,
				ok: true,
			}),
			{ schema: { protoFile: COMP_PROTO } },
		);
		callee.rpc.handle(
			releaseM,
			async (req: { reservationId: string }) => {
				releaseCalls.set(
					req.reservationId,
					(releaseCalls.get(req.reservationId) ?? 0) + 1,
				);
				if (req.reservationId === "rsv-a") {
					throw new Error("release of rsv-a is broken");
				}
				return { ok: true };
			},
			{ schema: { protoFile: COMP_PROTO } },
		);
		callee.rpc.handle(
			chargeM,
			async () => {
				throw new Error("payment gateway refused");
			},
			{ schema: { protoFile: COMP_PROTO } },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const owner = dedicated("primary");
		owners.push(owner);
		owner.service(calleeName, { rpc: [reserveM, chargeM, releaseM] });
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "call",
					id: "reserve_a",
					service: calleeName,
					method: reserveM,
					input: { itemId: "a", quantity: 1 },
					compensate: {
						service: calleeName,
						method: releaseM,
						input: { reservationId: "$.reserve_a.reservationId" },
					},
				},
				{
					type: "call",
					id: "reserve_b",
					service: calleeName,
					method: reserveM,
					input: { itemId: "b", quantity: 1 },
					waitFor: ["reserve_a"],
					compensate: {
						service: calleeName,
						method: releaseM,
						input: { reservationId: "$.reserve_b.reservationId" },
					},
				},
				{
					type: "call",
					id: "charge",
					service: calleeName,
					method: chargeM,
					input: { reservationId: "$.reserve_b.reservationId", amount: 10 },
					waitFor: ["reserve_b"],
				},
			],
		});
		await connect(owner);
		const ownerID = owner.identity()!.serviceId;

		const caller = await shared("second");
		await addWorkflowRule(caller.identity()!.serviceId, ownerID, wfName);
		const calleeIDs = await allServiceIDs(calleeName);
		for (const method of [reserveM, chargeM, releaseM]) {
			await addRule(ownerID, "E", "rpc.call", null, method);
			for (const cid of calleeIDs) {
				await addRule(cid, "A", "rpc.handle", ownerID, method);
			}
			await owner.useSchema(calleeName, method, { protoFile: COMP_PROTO });
			await awaitRpcMethodLive(calleeName, method);
		}

		const { runId } = await startWorkflowWhenAllowed(caller, wfName, {});

		// Pass 1: reserve_b's compensation succeeds, reserve_a's fails and leaves
		// the run parked in 'compensating' under a lease nobody will retire.
		await waitForStep(runId, "reserve_a.compensate", "failed", 40_000);
		expect(releaseCalls.get("rsv-b")).toBe(1);
		expect(await stepStatus(runId, "reserve_b.compensate")).toBe("success");
		const stuck = await leaseOf(runId);
		expect(stuck.status).toBe("compensating");

		// The holder dies. ListStaleCompensating picks the run up and re-dispatches
		// the compensation under a bumped epoch.
		await forceLeaseReclaim(runId);

		expect(
			await awaitRunStatus(
				caller,
				runId,
				(s) =>
					s === "failed_compensated" || s === "failed" || s === "cancelled",
				60_000,
			),
		).toBe("failed_compensated");

		// The whole point: the second compensation pass walks the same steps in
		// reverse and must skip the refund it already issued.
		expect(releaseCalls.get("rsv-b")).toBe(1);

		const after = await leaseOf(runId);
		expect(after.leaseEpoch).toBeGreaterThan(stuck.leaseEpoch);

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("failed_compensated");
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		expect(byId.get("reserve_b")?.status).toBe("compensated");
		expect(byId.get("charge")?.status).toBe("failed");
	}, 180_000);
});

// settingMs reads a duration setting the runtime applies live, so a test that
// has to outlast one is pinned to the deployed value rather than a copy of the
// default.
async function settingMs(key: string): Promise<number> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT value FROM runtime_settings WHERE key = ${key}
		`) as Array<{ value: unknown }>;
		const raw = rows[0]?.value;
		if (raw === undefined) throw new Error(`setting ${key} not in DB`);
		const n = Number(typeof raw === "string" ? raw : JSON.stringify(raw));
		if (!Number.isFinite(n)) {
			throw new Error(`setting ${key} is not numeric: ${String(raw)}`);
		}
		return n;
	});
}
