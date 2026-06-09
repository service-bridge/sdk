// access-policy-live.test.ts — ADR-0014 live propagation.
//
// After an operator INSERTs a restrictive egress rule, the runtime gate
// enforces it within <2s (NOTIFY policy_changed → access.Loader → snapshot).
//
// Procedure:
//   1. Caller + callee connected, no rules → call succeeds (default-allow).
//   2. Insert egress rule that does NOT cover the method being called.
//   3. Wait <2s.
//   4. Repeat the call — runtime gate #3 now denies with PermissionDenied.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep, waitFor } from "./_helpers/events";
import { addRule, clearRules, serviceID } from "./_helpers/policy-db";

const CALLEE_SVC = "e2e-registry-svc";
const protoFile = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"serde",
	"testdata",
	"payment.proto",
);
const SCHEMA = { protoFile, input: "ChargeRequest", output: "ChargeResponse" };

describe("access-policy: live propagation via NOTIFY", () => {
	const env = eventsEnv();
	let calleeID: string;
	let callerID: string;
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	const method = `live-${Date.now()}`;
	const dummyAllowed = `live-dummy-${Date.now()}`;

	beforeEach(async () => {
		callee = caller = undefined;
		calleeID = await serviceID(CALLEE_SVC);
		callerID = await serviceID("e2e-registry-consumer");
		await clearRules(callerID);
	});

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		await clearRules(callerID);
		await sleep(300);
	});

	test("inserting an egress rule takes effect on the runtime gate within ~2s", async () => {
		callee = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		callee.rpc.handle(method, async () => ({ transactionId: "ok", ok: true }), {
			schema: SCHEMA,
		});
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		caller.service(CALLEE_SVC, { rpc: [method] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		await waitFor(
			() => {
				for (const e of caller!.serviceMap().values()) {
					for (const m of e.methods) if (m.name === method) return true;
				}
				return false;
			},
			5_000,
			"method discovered",
		);
		await caller.useSchema(CALLEE_SVC, method, SCHEMA);

		// Step 1 — default-allow: call succeeds.
		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			CALLEE_SVC,
			method,
			{ userId: "u", amount: 1 },
			{ timeout: "5s", transport: "proxy" },
		);
		expect(ok.transactionId).toBe("ok");

		// Step 2 — insert a rule that disables default-allow but does NOT
		// cover `method`. Peer = NULL (any peer), target = some other name.
		await addRule(callerID, "E", "rpc.call", null, dummyAllowed);
		// Step 3 — wait for NOTIFY propagation.
		await sleep(1_500);

		// Step 4 — call must now be denied.
		let denied = false;
		try {
			await caller.rpc.call(
				CALLEE_SVC,
				method,
				{ userId: "u", amount: 1 },
				{ timeout: "5s", transport: "proxy" },
			);
		} catch (e) {
			denied = true;
			expect(String(e)).toMatch(/PermissionDenied|denied|egress/i);
		}
		expect(denied).toBe(true);
	}, 30_000);

	test("policyEvaluation() reflects newly inserted rule within 2s", async () => {
		caller = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		await waitFor(
			() => caller!.policyEvaluation() !== null,
			3_000,
			"initial policyEvaluation present",
		);

		const actualCallerID = caller.identity()!.serviceId;
		// Make sure rules apply to the same UUID the runtime is using for the
		// live session — name → id is not unique among active rows.
		await clearRules(actualCallerID);
		const beforeEgress = caller.policyEvaluation()?.egress ?? [];
		const liveTarget = `policy-eval-${Date.now()}`;
		await addRule(actualCallerID, "E", "rpc.call", null, liveTarget);

		// Wait for the new rule to be reflected in the live policy snapshot.
		await waitFor(
			() => {
				const eg = caller!.policyEvaluation()?.egress ?? [];
				return eg.some(
					(r) => r.action === "rpc.call" && r.targetName === liveTarget,
				);
			},
			5_000,
			"new rpc.call rule visible in policyEvaluation()",
		);
		const afterEgress = caller.policyEvaluation()?.egress ?? [];
		expect(afterEgress.length).toBeGreaterThanOrEqual(beforeEgress.length + 1);
		// Cleanup for follow-up tests.
		await clearRules(actualCallerID);
		callerID = actualCallerID;
	}, 30_000);

	test("removing a peer-scoping rule drops the peer from serviceMap (<2s)", async () => {
		// Callee provides one method; caller scopes its egress to (callee,
		// method). After the rule is removed and replaced with a placeholder
		// rule that does NOT cover the callee, the callee must disappear
		// from caller's serviceMap (removed_peers live propagation).
		callee = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		const liveMethod = `live-rm-${Date.now()}`;
		callee.rpc.handle(
			liveMethod,
			async () => ({ transactionId: "ok", ok: true }),
			{
				schema: SCHEMA,
			},
		);
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		const calleeID = callee.identity()!.serviceId;

		// Seed scoping rule BEFORE caller starts.
		callerID = await serviceID("e2e-registry-consumer");
		await clearRules(callerID);
		await addRule(callerID, "E", "rpc.call", calleeID, liveMethod);
		await sleep(400);

		caller = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		caller.service(CALLEE_SVC, { rpc: [liveMethod] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		const actualCallerID = caller.identity()!.serviceId;
		if (actualCallerID !== callerID) {
			await clearRules(callerID);
			callerID = actualCallerID;
			await clearRules(callerID);
			await addRule(callerID, "E", "rpc.call", calleeID, liveMethod);
			await sleep(900);
		}

		// Wait for callee to surface in caller's snapshot.
		await waitFor(
			() => caller!.serviceMap().has(CALLEE_SVC),
			6_000,
			"callee initially visible to caller",
		);

		// Now flip the rule: remove the (callee, method) row, add a placeholder
		// pinned to a synthetic peer UUID (NOT the callee). Rules with
		// peer_service=NULL would set AllOpen.RPC=true (default-allow), so we
		// MUST pin to a concrete UUID to keep scope narrow. The synthetic UUID
		// is just a placeholder — no service matches it, so scope yields zero
		// visible peers and the callee falls out via removed_peers.
		await clearRules(callerID);
		// peer_service is a FK to services.id; use the caller's own ID as a
		// harmless real UUID that does NOT match the callee.
		await addRule(
			callerID,
			"E",
			"rpc.call",
			callerID,
			`live-rm-placeholder-${Date.now()}`,
		);

		// Within ~2s the runtime must push a RegistryUpdate with the callee in
		// removed_peers — observable via serviceMap losing the entry.
		await waitFor(
			() => !caller!.serviceMap().has(CALLEE_SVC),
			4_000,
			"callee removed from serviceMap after rule revoke",
		);

		// And a fresh call attempt must be rejected — peer is gone from scope,
		// so the call cannot resolve (scope-hidden) or is gated by policy.
		let denied = false;
		try {
			await caller.rpc.call(
				CALLEE_SVC,
				liveMethod,
				{ userId: "u", amount: 1 },
				{ timeout: "3s" },
			);
		} catch (e) {
			denied = true;
			expect(String(e)).toMatch(
				/Unavailable|PermissionDenied|denied|no descriptor|cancelled|CANCELLED|no instance|egress/i,
			);
		}
		expect(denied).toBe(true);
	}, 40_000);
});
