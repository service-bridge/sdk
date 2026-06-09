// workflow-fanout — `local → parallel(3×local) → local → parallel(2×local) → local`.
//
// Validates that `parallel` truly runs steps concurrently: each inner step
// sleeps for SLEEP_MS, so wall-clock for a fully-serial execution would be
// (1 + 3 + 1 + 2 + 1) × SLEEP_MS, while the critical path through the DAG
// is (1 + 1 + 1 + 1 + 1) × SLEEP_MS = 5 × SLEEP_MS.
//
// The elapsed assertion uses a generous budget (2 × critical-path + 500 ms)
// to absorb gRPC round-trip overhead: each of the 9 steps makes 2 gRPC calls
// (BeginStep + CompleteStep) with 2–3 DB queries each, adding ~100–200 ms per
// step of non-sleep overhead. Parallelism is verified separately by the sibling
// spread check below — the elapsed bound guards only against total serialisation.
//
// Uses `local` steps (in-SDK closures) like happy-linear — call-step
// coverage lives in dedicated tests.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep as wait, waitFor } from "./_helpers/events";
import { clearRules } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

const SLEEP_MS = 300;

describe("workflow-fanout", () => {
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
		await wait(200);
	});

	test("parallel groups overlap; wall-clock ≤ critical-path × 2 + 500ms overhead", async () => {
		const wfName = `fanout-${Date.now()}`;

		const leaf = (id: string) => ({
			type: "local" as const,
			id,
			fn: async () => {
				await new Promise((r) => setTimeout(r, SLEEP_MS));
				return { id, doneAt: Date.now() };
			},
		});

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
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
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		caller = new ServiceBridge(env.url, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		await addWorkflowRule(callerID, ownerID, wfName);
		await wait(800);

		const t0 = Date.now();
		const { runId } = await caller.workflow.start(wfName, {});

		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			20_000,
		);
		const elapsed = Date.now() - t0;
		expect(finalStatus).toBe("success");

		// Critical path = 5 sequential steps × SLEEP_MS.
		const criticalPath = 5 * SLEEP_MS;
		// Budget = 2 × critical-path + 500 ms overhead for gRPC + polling.
		// Each of the 9 steps makes 2 unary gRPC calls (BeginStep + CompleteStep)
		// against Postgres; on localhost that is ~100–200 ms per step in the
		// sequential chain. The spread check below separately verifies that
		// parallel siblings actually overlap.
		expect(elapsed).toBeLessThanOrEqual(criticalPath * 2 + 500);

		// Cross-check group siblings overlap: their doneAt timestamps lie within
		// SLEEP_MS × 0.7 of each other (≥30% overlap).
		const q = await caller.workflow.query(runId);
		const g1 = q.state.g1 as Record<string, { doneAt: number }>;
		const dones = ["b1", "b2", "b3"].map((id) => g1[id]!.doneAt);
		const spread = Math.max(...dones) - Math.min(...dones);
		expect(spread).toBeLessThan(SLEEP_MS * 0.7);
	}, 30_000);
});
