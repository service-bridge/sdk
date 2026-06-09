// workflow-compensation-chain — sequential [reserve, charge] call steps.
//
// charge fails terminally. reserve has a compensate spec pointing at the
// callee's Release method. The runtime must:
//   1. FailStep(charge) → nextAction=compensate → run enters compensating
//   2. Dispatcher picks up run, dispatches RunAssignment{compensating:true}
//   3. SDK runner calls Release(reservation_id from reserve's output)
//   4. CompleteRun with terminalStatus='failed_compensated'
//
// Architecture:
//   - ownerKey (e2e-registry-svc): owns the workflow definition + executes it.
//     Workflow steps call rpc on the callee (e2e-registry-consumer).
//   - callerKey (e2e-registry-consumer): registers rpc handlers (reserve/charge/release)
//     and starts the run. It is a SEPARATE service — avoids the self-RPC
//     descriptor issue where an owner cannot see its own service map.
//
// Isolation: this test runs against a DEDICATED runtime subprocess on alternate
// ports (gRPC 14447) with its own PostgreSQL database. The shared bootstrap
// service identities (e2e-registry-svc / e2e-registry-consumer) are seeded into
// the isolated DB, but only THIS test's SDK instances ever attach to this
// runtime. On the shared runtime, concurrent workflow tests reusing the same
// identity overlap their connect/detach windows, so the dispatcher's
// round-robin over attached owner instances can hand a compensation step to a
// different test's instance (instance mismatch / lease fenced), stranding the
// run in 'compensating'. A dedicated runtime removes that cross-test
// interference; access policy defaults to allow on the fresh DB (no rules), so
// no explicit egress/acceptance rules are needed.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	buildDedicatedRuntime,
	type DedicatedRuntime,
	spawnIsolatedRuntime,
} from "./_helpers/dedicated-runtime";
import { sleep, waitFor } from "./_helpers/events";
import { awaitRunStatus, FAST_WF_OPTS, wfEnv } from "./_helpers/wf.ts";

const COMP_PROTO = join(import.meta.dir, "_helpers", "compensation.proto");
const DEDICATED_PORT = 14447;
const DEDICATED_UI_PORT = 19447;
const DEDICATED_BINARY = join(import.meta.dir, "../../.tmp-comp-chain-runtime");

describe("workflow-compensation-chain", () => {
	const env = wfEnv();
	let owner: ServiceBridge | undefined;
	let callee: ServiceBridge | undefined;
	let runtime: DedicatedRuntime | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(DEDICATED_BINARY);
		runtime = await spawnIsolatedRuntime({
			name: "comp-chain",
			grpcPort: DEDICATED_PORT,
			uiPort: DEDICATED_UI_PORT,
			bootstrapKey: env.ownerKey,
			binaryPath: DEDICATED_BINARY,
		});
	}, 120_000);

	afterAll(async () => {
		await callee?.stop().catch(() => {});
		await owner?.stop().catch(() => {});
		await runtime?.cleanup();
	}, 30_000);

	test("charge fails → reserve compensated → failed_compensated", async () => {
		const wfName = `comp-chain-${Date.now()}`;
		const runtimeUrl = runtime!.url;

		// Callee: registers rpc handlers (Reserve, Charge, Release).
		// Uses env.callerKey (e2e-registry-consumer) — a separate service so
		// the workflow owner (e2e-registry-svc) can call it via service discovery.
		callee = new ServiceBridge(runtimeUrl, env.callerKey, FAST_WF_OPTS);

		callee.rpc.handle(
			"Reserve",
			async (req: { item_id: string; quantity: number }) => {
				return {
					reservation_id: `rsv-${req.item_id}-${Date.now()}`,
					ok: true,
				};
			},
			{ schema: { protoFile: COMP_PROTO } },
		);

		callee.rpc.handle(
			"Charge",
			async (_req: { reservation_id: string; amount: number }) => {
				throw new Error("payment gateway timeout");
			},
			{ schema: { protoFile: COMP_PROTO } },
		);

		callee.rpc.handle(
			"Release",
			async (_req: { reservation_id: string }) => {
				return { ok: true };
			},
			{ schema: { protoFile: COMP_PROTO } },
		);

		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		const calleeName = callee.identity()!.serviceName;

		// Owner: workflow definition + subscriber (e2e-registry-svc).
		// Declare outgoing rpc deps BEFORE start() so runtime includes callee
		// methods in the owner's registry snapshot.
		owner = new ServiceBridge(runtimeUrl, env.ownerKey, FAST_WF_OPTS);
		owner.service(calleeName, { rpc: ["Reserve", "Charge", "Release"] });

		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "call",
					id: "reserve",
					service: calleeName,
					method: "Reserve",
					input: { item_id: "$.input.item_id", quantity: "$.input.quantity" },
					compensate: {
						service: calleeName,
						method: "Release",
						input: { reservation_id: "$.reserve.reservation_id" },
					},
				},
				{
					type: "call",
					id: "charge",
					service: calleeName,
					method: "Charge",
					input: {
						reservation_id: "$.reserve.reservation_id",
						amount: "$.input.amount",
					},
					waitFor: ["reserve"],
				},
			],
		});

		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");

		// Owner must have schemas for callee methods — workflow runner dispatches
		// call steps via owner's rpc.call which requires a registered SchemaPair.
		for (const method of ["Reserve", "Charge", "Release"]) {
			await owner!.useSchema(calleeName, method, {
				protoFile: COMP_PROTO,
			});
		}
		await sleep(1000);

		const { runId } = await callee.workflow.start(wfName, {
			item_id: "widget-42",
			quantity: 1,
			amount: 99.0,
		});

		const finalStatus = await awaitRunStatus(
			callee,
			runId,
			(s) =>
				s === "failed_compensated" ||
				s === "failed" ||
				s === "success" ||
				s === "cancelled",
			30_000,
		);
		expect(finalStatus).toBe("failed_compensated");

		const q = await callee.workflow.query(runId);
		expect(q.status).toBe("failed_compensated");
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		expect(byId.get("reserve")?.status).toBe("compensated");
		expect(byId.get("reserve")?.compensatedBy).toBe("reserve.compensate");
		expect(byId.get("charge")?.status).toBe("failed");
	}, 60_000);
});
