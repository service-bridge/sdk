// workflow-lifecycle — workflow access-policy facets.
//
// Every sub-test mutates service_policy_rules / toggles default-allow on its
// OWN identities, so all clients are DEDICATED (never pooled): clearRules on a
// pooled identity would delete sibling tests' rules. serviceIds are resolved
// from identity() after connect; rules are cleared on those dedicated ids in
// afterEach. Isolation is by unique names plus per-identity rule cleanup.

import { afterEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	connect,
	dedicated,
	sleep,
	uniqueName,
	waitFor,
} from "./_helpers/fixtures";
import { addRule, clearRules, serviceID } from "./_helpers/policy-db";
import {
	addWorkflowRule,
	awaitRunStatus,
	FAST_WF_OPTS,
} from "./_helpers/wf.ts";

// waitForWarning polls policyEvaluation() until a warning matches the predicate.
async function waitForWarning(
	sb: ServiceBridge,
	pred: (w: { declaration: string; value: string }) => boolean,
	timeoutMs = 5_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const pe = sb.policyEvaluation();
		if (pe?.warnings.some(pred)) return;
		await sleep(200);
	}
	const pe = sb.policyEvaluation();
	throw new Error(
		`waitForWarning: timed out. last evaluation: ${JSON.stringify(pe)}`,
	);
}

describe("workflow-lifecycle", () => {
	let clients: ServiceBridge[] = [];
	let dirtyIDs: string[] = [];

	afterEach(async () => {
		for (const c of clients) await c.stop().catch(() => {});
		clients = [];
		for (const id of dirtyIDs) await clearRules(id).catch(() => {});
		dirtyIDs = [];
		// Settle: the workflow Router round-robins over every owner instance still
		// attached to the hub. stop() cancels the Subscribe stream client-side, but
		// the server-side Detach fires asynchronously. Without this gap the next
		// run can be dispatched to a just-stopped owner that no longer holds the
		// local graph (`step.fn is not a function`). Real teardown elapsed time.
		await sleep(1_500);
	});

	test("access-policy: tightened egress rule denies workflow.start", async () => {
		const wfName = uniqueName("ap-static-deny");

		const owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [{ type: "local", id: "s", fn: async () => ({ ok: true }) }],
		});
		await connect(owner);
		clients.push(owner);
		const ownerID = owner.identity()!.serviceId;
		dirtyIDs.push(ownerID);

		const caller = dedicated("second");
		await connect(caller);
		clients.push(caller);
		const callerID = caller.identity()!.serviceId;
		dirtyIDs.push(callerID);

		// Egress rule that does NOT cover wfName disables default-allow.
		const dummyWf = uniqueName("ap-static-deny-dummy");
		await addRule(callerID, "E", "workflow.run", ownerID, dummyWf);
		// Owner acceptance for wfName, so the only missing piece is caller egress.
		await addRule(ownerID, "A", "workflow.handle", callerID, wfName);
		await sleep(800);

		let denied = false;
		try {
			await caller.workflow.start(wfName, {});
		} catch (e) {
			denied = true;
			expect(String(e)).toMatch(
				/PermissionDenied|denied|access|WorkflowAccessDenied/i,
			);
		}
		expect(denied).toBe(true);
	}, 25_000);

	test("access-policy: allowed wf run succeeds while denied wf start fails", async () => {
		const base = uniqueName("ap-dynamic");
		const wfAllowed = `${base}-allow`;
		const wfDenied = `${base}-deny`;

		const owner = dedicated("primary");
		owner.workflow.handle(wfAllowed, {
			steps: [{ type: "local", id: "s", fn: async () => ({ ok: true }) }],
		});
		owner.workflow.handle(wfDenied, {
			steps: [{ type: "local", id: "s", fn: async () => ({ ok: true }) }],
		});
		await connect(owner);
		clients.push(owner);
		const ownerID = owner.identity()!.serviceId;
		dirtyIDs.push(ownerID);

		const caller = dedicated("second");
		await connect(caller);
		clients.push(caller);
		const callerID = caller.identity()!.serviceId;
		dirtyIDs.push(callerID);

		// Grant bilateral access to wfAllowed only. The egress rule also disables
		// default-allow on the caller side, so wfDenied (no egress) is denied.
		await addWorkflowRule(callerID, ownerID, wfAllowed);
		await addRule(ownerID, "A", "workflow.handle", callerID, wfDenied);
		await sleep(800);

		const { runId: allowedRunId } = await caller.workflow.start(wfAllowed, {});
		const allowedStatus = await awaitRunStatus(
			caller,
			allowedRunId,
			(s) => s === "success" || s === "failed" || s === "cancelled",
			15_000,
		);
		expect(allowedStatus).toBe("success");

		let denied = false;
		try {
			await caller.workflow.start(wfDenied, {});
		} catch (e) {
			denied = true;
			expect(String(e)).toMatch(
				/PermissionDenied|denied|access|WorkflowAccessDenied/i,
			);
		}
		expect(denied).toBe(true);
	}, 30_000);

	// "access-policy: sub-workflow bilateral acceptance gates the sub-step" lives
	// in workflow-access-policy.test.ts (its domain home), not here.

	test("access-policy: mid-flight rule tightening surfaces in policyEvaluation() within 5s", async () => {
		const wfName = uniqueName("ap-live");
		const liveTarget = uniqueName("ap-live-dummy");

		const owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [{ type: "sleep", id: "long_pause", durationSec: 60 }],
		});
		await connect(owner);
		clients.push(owner);
		const ownerID = owner.identity()!.serviceId;
		dirtyIDs.push(ownerID);
		const ownerSvcName = owner.identity()!.serviceName;

		// Caller declares wfName as an outgoing dep; the only egress rule it has
		// covers liveTarget, not wfName → a workflow.run warning must appear.
		const caller = dedicated("second");
		caller.service(ownerSvcName, { workflows: [wfName] });
		await connect(caller);
		clients.push(caller);
		const callerID = caller.identity()!.serviceId;
		dirtyIDs.push(callerID);

		await waitFor(
			() => caller.policyEvaluation() !== null,
			5_000,
			"initial policyEvaluation present",
		);

		await clearRules(callerID);
		await addRule(callerID, "E", "workflow.run", ownerID, liveTarget);
		await sleep(600);

		await waitForWarning(
			caller,
			(w) => w.declaration === "workflow.run" && w.value.includes(wfName),
			5_000,
		);

		const pe = caller.policyEvaluation();
		expect(pe).not.toBeNull();
		expect(
			pe!.warnings.some(
				(w) => w.declaration === "workflow.run" && w.value.includes(wfName),
			),
		).toBe(true);
	}, 35_000);

	test("access-policy: failOnPolicyViolation disconnects SDK on pre-tightened rule", async () => {
		const wfName = uniqueName("ap-fov");
		const liveTarget = uniqueName("ap-fov-dummy");

		const owner = dedicated("primary");
		owner.workflow.handle(wfName, {
			steps: [{ type: "local", id: "s", fn: async () => ({}) }],
		});
		await connect(owner);
		clients.push(owner);
		const ownerID = owner.identity()!.serviceId;
		dirtyIDs.push(ownerID);
		const ownerSvcName = owner.identity()!.serviceName;

		// Pre-seed a restrictive egress rule on the caller's service ID BEFORE the
		// SDK instance starts, so failOnPolicyViolation trips on the first snapshot.
		// The caller is the second-role identity for this domain: e2e-<domain>-2.
		const domain = process.env.SB_E2E_DOMAIN!;
		const consumerName = `e2e-${domain}-2`;
		const consumerID = await serviceID(consumerName);
		await clearRules(consumerID);
		dirtyIDs.push(consumerID);
		await addRule(consumerID, "E", "workflow.run", ownerID, liveTarget);
		await sleep(400);

		const url = process.env.SERVICEBRIDGE_URL!;
		const callerKey = process.env[`SB_E2E_${domain.toUpperCase()}_2`]!;
		const caller = new ServiceBridge(url, callerKey, {
			...FAST_WF_OPTS,
			failOnPolicyViolation: true,
			advertise: { host: "127.0.0.1", port: 0 },
			dataDir: `./.servicebridge-e2e/${domain}-ap-fov`,
		});
		caller.service(ownerSvcName, { workflows: [wfName] });
		clients.push(caller);

		const disconnects: Array<{ reason: string }> = [];
		caller.on("disconnected", (e) => disconnects.push(e));

		let startError: Error | undefined;
		try {
			await caller.start();
			await waitFor(
				() => disconnects.length > 0,
				5_000,
				"disconnect due to policy violation",
			);
		} catch (e) {
			startError = e as Error;
		}

		const hasDisconnect = disconnects.some(
			(d) =>
				d.reason.toLowerCase().includes("policy") ||
				d.reason.toLowerCase().includes("violation"),
		);
		expect(hasDisconnect || startError !== undefined).toBe(true);
	}, 25_000);
});
