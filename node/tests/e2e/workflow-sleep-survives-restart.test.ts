// workflow-sleep-survives-restart — durable sleep survives runtime restart.
//
// 1. Owner registers workflow: step_a → sleep(5s) → step_b.
// 2. Caller starts run → run enters 'waiting' (sleep timer inserted in DB).
// 3. Kill the dedicated runtime subprocess (NOT the ambient shared runtime).
// 4. Restart a fresh dedicated runtime subprocess.
// 5. Verify the run completes within sleep_duration + recovery_budget.
//
// Isolation: this test runs against a DEDICATED runtime subprocess listening
// on an alternate gRPC port (14446). The ambient shared runtime on :14445 used
// by all other e2e tests is never touched. This prevents the destructive
// kill+restart from breaking subsequent tests in the suite.
//
// The dedicated runtime is built as a standalone binary (not `go run`) so
// that killing the subprocess actually frees the listening port — `go run`
// double-forks via an intermediate build/exec wrapper whose SIGTERM does
// not always reap the actual runtime child.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	buildDedicatedRuntime,
	type DedicatedRuntime,
	spawnIsolatedRuntime,
} from "./_helpers/dedicated-runtime";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	cleanupWorkflowState,
	FAST_WF_OPTS,
	wfEnv,
} from "./_helpers/wf.ts";

const DEDICATED_PORT = 14446;
const DEDICATED_UI_PORT = 19446;
const DEDICATED_BINARY = join(import.meta.dir, "../../.tmp-sleep-test-runtime");

describe("workflow-sleep-survives-restart", () => {
	const env = wfEnv();
	let owner: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	let ownerID: string | undefined;
	let callerID: string | undefined;
	let runtime: DedicatedRuntime | undefined;

	beforeAll(async () => {
		await buildDedicatedRuntime(DEDICATED_BINARY);
		runtime = await spawnIsolatedRuntime({
			name: "sleep-restart",
			grpcPort: DEDICATED_PORT,
			uiPort: DEDICATED_UI_PORT,
			bootstrapKey: env.ownerKey,
			binaryPath: DEDICATED_BINARY,
		});
		await cleanupWorkflowState();
	}, 120_000);

	afterAll(async () => {
		await caller?.stop().catch(() => {});
		await owner?.stop().catch(() => {});
		if (ownerID) await clearRules(ownerID).catch(() => {});
		if (callerID) await clearRules(callerID).catch(() => {});
		await runtime?.cleanup();
	}, 30_000);

	test("durable sleep survives runtime kill+restart — run completes on resumed timer", async () => {
		const wfName = `sleep-survives-${Date.now()}`;
		const execCounts = { before: 0, after: 0 };

		const runtimeUrl = runtime!.url;

		owner = new ServiceBridge(runtimeUrl, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step_a",
					fn: async () => {
						execCounts.before++;
						return { v: 1 };
					},
				},
				{
					type: "sleep",
					id: "pause",
					durationSec: 5,
					waitFor: ["step_a"],
				},
				{
					type: "local",
					id: "step_b",
					fn: async () => {
						execCounts.after++;
						return { v: 2 };
					},
					waitFor: ["pause"],
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		caller = new ServiceBridge(runtimeUrl, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;

		await addWorkflowRule(callerID, ownerID, wfName);
		await sleep(800);

		const { runId } = await caller.workflow.start(wfName, {});

		// Wait for the run to enter 'waiting' (sleep timer persisted in DB).
		await awaitRunStatus(caller, runId, (s) => s === "waiting", 10_000);

		// Stop SDK connections before killing dedicated runtime.
		await owner.stop();
		owner = undefined;
		await caller.stop();
		caller = undefined;

		// Kill dedicated runtime — sleep timer remains in DB.
		await runtime!.kill();

		// Spawn fresh dedicated runtime subprocess on same port.
		await runtime!.restart(env.ownerKey, 60_000);

		// Reconnect owner (needed to receive the run re-dispatch after timer fires).
		owner = new ServiceBridge(runtimeUrl, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			steps: [
				{
					type: "local",
					id: "step_a",
					fn: async () => {
						execCounts.before++;
						return { v: 1 };
					},
				},
				{
					type: "sleep",
					id: "pause",
					durationSec: 5,
					waitFor: ["step_a"],
				},
				{
					type: "local",
					id: "step_b",
					fn: async () => {
						execCounts.after++;
						return { v: 2 };
					},
					waitFor: ["pause"],
				},
			],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 8_000, "owner reconnected");

		caller = new ServiceBridge(runtimeUrl, env.callerKey, FAST_WF_OPTS);
		await caller.start();
		await waitFor(
			() => caller!.identity() !== null,
			8_000,
			"caller reconnected",
		);

		// Give owner a beat to register handler before sleep timer fires and
		// the dispatcher tries to invoke step_b.
		await sleep(500);

		// Wait for completion (sleep timer fires, dispatcher re-dispatches).
		const finalStatus = await awaitRunStatus(
			caller,
			runId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			60_000,
		);
		expect(finalStatus).toBe("success");

		const q = await caller.workflow.query(runId);
		expect(q.status).toBe("success");
		const byId = new Map(q.steps.map((s) => [s.stepId, s]));
		expect(byId.get("step_a")?.status).toBe("success");
		expect(byId.get("step_b")?.status).toBe("success");

		// step_a ran once (checkpointed, not re-run after restart).
		expect(execCounts.before).toBe(1);
		// step_b ran once on the resumed instance.
		expect(execCounts.after).toBe(1);
	}, 180_000);
});
