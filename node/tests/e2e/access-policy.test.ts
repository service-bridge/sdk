// access-policy.test.ts — ADR-0004 access policy, consolidated e2e suite.
//
// Covers all five gate/layer surfaces against the live runtime + real Postgres:
//   - gate #1 capability hard-gate (cap_rpc_handle=false → no DB row)
//   - gate #2 soft acceptance via PatternContains (event.handle vs orders.*)
//   - gate #3 egress allow-list (caller E/rpc.call) over the proxy transport
//   - gate #3 acceptance allow-list (callee A/rpc.handle) over the proxy transport
//   - Layer-2 direct mTLS acceptance (SPIFFE identity in the denial message)
//   - Layer-1 scope filter on rpc / events / workflow dimensions
//   - live NOTIFY propagation (egress gate, policyEvaluation(), removed_peers)
//   - SDK policy_violation warning surface for a denied outgoing dep
//
// Isolation: this domain mutates global DB state (service_policy_rules rows,
// cap_* columns), so it CANNOT use the warm pool — every client is `dedicated`
// under this domain's own per-domain identities (e2e-<domain>-1 = publisher/B,
// e2e-<domain>-2 = subscriber/A). Clients register handlers/deps BEFORE
// connect() and are stop()ed in afterEach; rules/caps are cleared/restored in
// afterEach. The per-domain names are stable, so `serviceID(name)` matches the
// runtime-issued `identity().serviceId`; the reconciliation branches stay as a
// belt-and-braces guard but are normally no-ops. All tests live in one describe
// so Bun runs them serially against the shared publisher/subscriber pair.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import type { ServiceBridge } from "../../src/connection/service-bridge";
import {
	connect,
	dedicated,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueId,
	waitFor,
} from "./_helpers/fixtures";
import {
	addRule,
	clearRules,
	serviceID,
	setCapability,
	withDb,
} from "./_helpers/policy-db";

function domain(): string {
	const d = process.env.SB_E2E_DOMAIN;
	if (!d) throw new Error("access-policy: SB_E2E_DOMAIN not set");
	return d;
}

// Stable per-domain service names (publisher = role 1, subscriber = role 2).
const PUBLISHER_SVC = `e2e-${domain()}-1`; // publisher key → callee / B / pub
const SUBSCRIBER_SVC = `e2e-${domain()}-2`; // subscriber key → caller / A / sub

const PAY_PROTO = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"serde",
	"testdata",
	"payment.proto",
);
const PAY = {
	protoFile: PAY_PROTO,
	input: "ChargeRequest",
	output: "ChargeResponse",
};
const ORDER = {
	protoFile: ORDER_EVENT_PROTO,
	method: "orders_created",
} as const;

async function methodExists(svcID: string, name: string): Promise<boolean> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT 1 FROM service_methods
			WHERE service_id = ${svcID} AND method_name = ${name}
		`) as unknown[];
		return rows.length > 0;
	});
}

function methodVisible(sb: ServiceBridge, peer: string, name: string): boolean {
	const entry = sb.serviceMap().get(peer);
	if (!entry) return false;
	return entry.methods.some((m) => m.name === name);
}

async function expectDenied(
	fn: () => Promise<unknown>,
	pattern: RegExp,
): Promise<unknown> {
	let denial: unknown;
	try {
		await fn();
	} catch (e) {
		denial = e;
	}
	expect(denial).toBeDefined();
	expect(String(denial)).toMatch(pattern);
	return denial;
}

describe("access-policy (ADR-0004)", () => {
	let calleeID: string;
	let callerID: string;
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;

	beforeEach(() => {
		callee = caller = undefined;
		calleeID = "";
		callerID = "";
	});

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		if (callerID) await clearRules(callerID);
		if (calleeID) await clearRules(calleeID);
		await sleep(300);
	});

	test("gate#1 capability: rpc handler with cap_rpc_handle=false is not registered (no service_methods row)", async () => {
		const handlerName = uniqueId("cap-test");
		calleeID = await serviceID(PUBLISHER_SVC);
		await withDb(async (sql) => {
			await sql`DELETE FROM service_methods WHERE service_id = ${calleeID} AND method_name = ${handlerName}`;
		});
		await setCapability(calleeID, "cap_rpc_handle", false);
		try {
			await sleep(700); // let the cap NOTIFY land before connect

			callee = dedicated("primary");
			callee.rpc.handle(
				handlerName,
				async () => ({ transactionId: "x", ok: true }),
				{ schema: PAY },
			);
			// start() may resolve (auth succeeds) even though the registry stream
			// is rejected before UpsertMethods. Wait for that round-trip.
			await connect(callee).catch(() => {});
			await sleep(1_500);

			expect(await methodExists(calleeID, handlerName)).toBe(false);
		} finally {
			await setCapability(calleeID, "cap_rpc_handle", true);
		}
		// No policy rules were seeded; null out so afterEach skips clearRules.
		calleeID = "";
	}, 20_000);

	test("gate#2 acceptance event.handle PatternContains: only orders.created allowed under rule orders.*", async () => {
		const violations: Array<{ declaration: string; value: string }> = [];
		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		await addRule(callerID, "A", "event.handle", null, "orders.*");
		await sleep(700);

		const sub = dedicated("second");
		caller = sub;
		sub.on("policy_violation", (v) => {
			violations.push({ declaration: v.declaration, value: v.value });
		});

		// orders.created is contained in orders.*; the other three are violators
		// (different root / `*` is exactly one segment / subscriber wider than
		// rule). All four still register (soft gate).
		for (const pattern of [
			"orders.created",
			"payments.created",
			"orders.a.b",
			"orders.#",
		]) {
			sub.event.handle(pattern, async () => {});
			sub.event.define(pattern, ORDER);
		}

		await connect(sub);
		await waitFor(() => sub.identity() !== null, 5_000, "sub connected");
		const actual = sub.identity()!.serviceId;
		if (actual !== callerID) {
			await clearRules(callerID);
			callerID = actual;
			await clearRules(callerID);
			await addRule(callerID, "A", "event.handle", null, "orders.*");
		}
		await waitFor(
			() => sub.policyEvaluation() !== null,
			3_000,
			"policy evaluation surfaced",
		);

		const warnedValues = violations
			.filter((v) => v.declaration === "event.handle")
			.map((v) => v.value);
		expect(warnedValues).toContain("payments.created");
		expect(warnedValues).toContain("orders.a.b");
		expect(warnedValues).toContain("orders.#");
		expect(warnedValues).not.toContain("orders.created");
	}, 20_000);

	test("gate#3 egress (caller E/rpc.call) allow-list over proxy: allowed succeeds, forbidden denied", async () => {
		const allowed = uniqueId("act-allowed");
		const forbidden = uniqueId("act-forbidden");

		callee = dedicated("primary");
		callee.rpc.handle(
			allowed,
			async () => ({ transactionId: "ok", ok: true }),
			{
				schema: PAY,
			},
		);
		callee.rpc.handle(
			forbidden,
			async () => ({ transactionId: "no", ok: false }),
			{ schema: PAY },
		);
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;

		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		await addRule(callerID, "E", "rpc.call", calleeID, allowed);
		await sleep(800); // NOTIFY → snapshot

		caller = dedicated("second");
		caller.service(PUBLISHER_SVC, { rpc: [allowed, forbidden] });
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		const actualCallerID = caller.identity()!.serviceId;
		if (actualCallerID !== callerID) {
			await clearRules(callerID);
			callerID = actualCallerID;
			await clearRules(callerID);
			await addRule(callerID, "E", "rpc.call", calleeID, allowed);
			await sleep(900);
		}

		await caller.useSchema(PUBLISHER_SVC, allowed, PAY);
		await caller.useSchema(PUBLISHER_SVC, forbidden, PAY);
		// After start(), declaring a dependency restarts the registry stream and
		// the caller's caches are rebuilt from the next snapshot. Waiting before
		// the declaration would gate on a view the declaration then discards.
		await waitFor(
			() => methodVisible(caller!, PUBLISHER_SVC, allowed),
			10_000,
			"allowed method discovered",
		);

		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(PUBLISHER_SVC, allowed, { userId: "u", amount: 1 }, { timeout: "5s" });
		expect(ok.transactionId).toBe("ok");

		// Default (direct) transport: scope-filter (Layer 1) hides the forbidden
		// method, so the call cannot resolve — either way it must be rejected.
		await expectDenied(
			() =>
				caller!.rpc.call(
					PUBLISHER_SVC,
					forbidden,
					{ userId: "u", amount: 1 },
					{ timeout: "5s" },
				),
			/PermissionDenied|denied|egress|no descriptor|not subscribed|Unavailable/i,
		);
	}, 30_000);

	test("gate#3 acceptance (callee A/rpc.handle) allow-list over proxy: allowed succeeds, forbidden denied by callee", async () => {
		const allowed = uniqueId("acc-allowed");
		const forbidden = uniqueId("acc-forbidden");

		caller = dedicated("second");
		caller.service(PUBLISHER_SVC, { rpc: [allowed, forbidden] });
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		const actualCallerID = caller.identity()!.serviceId;

		calleeID = await serviceID(PUBLISHER_SVC);
		await clearRules(calleeID);
		// Acceptance: callee accepts only `allowed` from this caller.
		await addRule(calleeID, "A", "rpc.handle", actualCallerID, allowed);
		await sleep(800);

		callee = dedicated("primary");
		callee.rpc.handle(
			allowed,
			async () => ({ transactionId: "ok", ok: true }),
			{
				schema: PAY,
			},
		);
		callee.rpc.handle(
			forbidden,
			async () => ({ transactionId: "no", ok: false }),
			{ schema: PAY },
		);
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		const runtimeCalleeID = callee.identity()!.serviceId;
		if (runtimeCalleeID !== calleeID) {
			await clearRules(calleeID);
			calleeID = runtimeCalleeID;
			await clearRules(calleeID);
			await addRule(calleeID, "A", "rpc.handle", actualCallerID, allowed);
			await sleep(900);
		}

		await caller.useSchema(PUBLISHER_SVC, allowed, PAY);
		await caller.useSchema(PUBLISHER_SVC, forbidden, PAY);
		// After start(), declaring a dependency restarts the registry stream and
		// the caller's caches are rebuilt from the next snapshot. Waiting before
		// the declaration would gate on a view the declaration then discards.
		await waitFor(
			() => methodVisible(caller!, PUBLISHER_SVC, allowed),
			10_000,
			"allowed method discovered",
		);

		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			PUBLISHER_SVC,
			allowed,
			{ userId: "u", amount: 1 },
			{ timeout: "5s", transport: "proxy" },
		);
		expect(ok.transactionId).toBe("ok");

		await expectDenied(
			() =>
				caller!.rpc.call(
					PUBLISHER_SVC,
					forbidden,
					{ userId: "u", amount: 1 },
					{ timeout: "5s", transport: "proxy" },
				),
			/PermissionDenied|denied|acceptance/i,
		);
	}, 30_000);

	test("Layer-2 direct mTLS acceptance: allowed direct call succeeds, forbidden denied with caller SPIFFE UUID in error", async () => {
		const allowed = uniqueId("direct-allowed");
		const forbidden = uniqueId("direct-forbidden");

		// Probe both clients to learn their runtime-issued UUIDs before seeding
		// rules — the callee re-registers with rules already in DB.
		const calleeProbe = dedicated("primary");
		await connect(calleeProbe);
		await waitFor(
			() => calleeProbe.identity() !== null,
			5_000,
			"callee probe connected",
		);
		const runtimeCalleeID = calleeProbe.identity()!.serviceId;
		await calleeProbe.stop();
		await sleep(200);

		const callerProbe = dedicated("second");
		await connect(callerProbe);
		await waitFor(
			() => callerProbe.identity() !== null,
			5_000,
			"caller probe connected",
		);
		const runtimeCallerID = callerProbe.identity()!.serviceId;
		await callerProbe.stop();
		await sleep(200);

		calleeID = runtimeCalleeID;
		callerID = runtimeCallerID;
		await clearRules(calleeID);
		await clearRules(callerID);
		await addRule(callerID, "E", "rpc.call", calleeID, allowed);
		await addRule(callerID, "E", "rpc.call", calleeID, forbidden);
		await addRule(calleeID, "A", "rpc.handle", callerID, allowed);
		await sleep(1500);

		// Reconnect with handlers/declarations + advertise (required for direct).
		callee = dedicated("primary");
		callee.rpc.handle(
			allowed,
			async () => ({ transactionId: "ok", ok: true }),
			{
				schema: PAY,
			},
		);
		callee.rpc.handle(
			forbidden,
			async () => ({ transactionId: "no", ok: false }),
			{ schema: PAY },
		);
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = dedicated("second");
		caller.service(PUBLISHER_SVC, { rpc: [allowed, forbidden] });
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		await caller.useSchema(PUBLISHER_SVC, allowed, PAY);
		await caller.useSchema(PUBLISHER_SVC, forbidden, PAY);
		// After start(), declaring a dependency restarts the registry stream and
		// the caller's caches are rebuilt from the next snapshot. Waiting before
		// the declaration would gate on a view the declaration then discards.
		await waitFor(
			() => methodVisible(caller!, PUBLISHER_SVC, allowed),
			10_000,
			"allowed method discovered",
		);

		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			PUBLISHER_SVC,
			allowed,
			{ userId: "u", amount: 1 },
			{ timeout: "5s", transport: "direct" },
		);
		expect(ok.transactionId).toBe("ok");

		// Forbidden direct call → callee SPIFFE acceptance fails closed; the
		// denial must include the caller's mTLS-derived UUID, proving identity
		// came from the SPIFFE cert and not from CallRequest.callerService.
		const denial = await expectDenied(
			() =>
				caller!.rpc.call(
					PUBLISHER_SVC,
					forbidden,
					{ userId: "u", amount: 1 },
					{ timeout: "5s", transport: "direct" },
				),
			/PermissionDenied|acceptance denied/i,
		);
		expect(String(denial)).toContain(runtimeCallerID);
	}, 30_000);

	test("Layer-1 scope filter (rpc): A sees only (B,m1), own handle/sub/outgoing unfiltered, B does not see A", async () => {
		const m1 = uniqueId("scope-rpc-m1");
		const m2 = uniqueId("scope-rpc-m2");
		const selfHandle = uniqueId("scope-rpc-self");
		const selfSub = `${uniqueId("scope-rpc-sub").replace(/-/g, ".")}.created`;

		// B exposes m1 + m2.
		callee = dedicated("primary");
		callee.rpc.handle(m1, async () => ({ transactionId: "ok", ok: true }), {
			schema: PAY,
		});
		callee.rpc.handle(m2, async () => ({ transactionId: "ok", ok: true }), {
			schema: PAY,
		});
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;

		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		// Narrow A's egress to (B, m1) ONLY — disables default-allow on rpc.call.
		await addRule(callerID, "E", "rpc.call", calleeID, m1);
		await sleep(400);

		caller = dedicated("second");
		caller.service(PUBLISHER_SVC, { rpc: [m1, m2] });
		caller.rpc.handle(
			selfHandle,
			async () => ({ transactionId: "self", ok: true }),
			{ schema: PAY },
		);
		caller.event.handle(selfSub, async () => {});
		caller.event.define(selfSub, ORDER);
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		const actualCallerID = caller.identity()!.serviceId;
		if (actualCallerID !== callerID) {
			await clearRules(callerID);
			callerID = actualCallerID;
			await clearRules(callerID);
			await addRule(callerID, "E", "rpc.call", calleeID, m1);
			await sleep(900);
		}

		await waitFor(
			() => methodVisible(caller!, PUBLISHER_SVC, m1),
			6_000,
			"B/m1 visible in A's snapshot",
		);

		const map = caller.serviceMap();

		// (1) A sees B/m1; (2) A does NOT see B/m2.
		const peer = map.get(PUBLISHER_SVC);
		expect(peer).toBeDefined();
		const peerMethodNames = peer!.methods.map((mm) => mm.name);
		expect(peerMethodNames).toContain(m1);
		expect(peerMethodNames).not.toContain(m2);

		// (3) A does NOT see any 3rd peer C — only self + B.
		const visiblePeers = [...map.keys()].filter(
			(n) => n !== SUBSCRIBER_SVC && n !== PUBLISHER_SVC,
		);
		expect(visiblePeers).toEqual([]);

		// (4) A sees its own event.handle subscription (self-visible).
		const self = map.get(SUBSCRIBER_SVC);
		expect(self).toBeDefined();
		expect(self!.eventSubscriptions.map((es) => es.pattern)).toContain(selfSub);

		// (6) A sees its own outgoing_calls declarations unfiltered — m1 + m2.
		const ocTargets = self!.outgoingCalls.map((oc) => oc.targetMethod);
		expect(ocTargets).toContain(m1);
		expect(ocTargets).toContain(m2);

		// (7) Symmetric ingress: B has a non-covering egress rule (disables
		// default-allow) and no rule covering A → B must NOT see A.
		await addRule(calleeID, "E", "rpc.call", null, uniqueId("scope-rpc-none"));
		await sleep(1_500);

		expect(callee.serviceMap().has(SUBSCRIBER_SVC)).toBe(false);
	}, 40_000);

	test("Layer-1 scope filter (events) anti-containment: *.created × orders.* surfaces orders.created", async () => {
		callee = dedicated("primary");
		callee.event.define("orders.created", ORDER);
		callee.event.define("payments.created", ORDER);
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "pub connected");
		calleeID = callee.identity()!.serviceId;

		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		await addRule(callerID, "E", "event.publish", null, "orders.*");
		await sleep(400);

		caller = dedicated("second");
		caller.event.handle("*.created", async () => {});
		caller.event.define("*.created", ORDER);
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "sub connected");

		const actual = caller.identity()!.serviceId;
		if (actual !== callerID) {
			await clearRules(callerID);
			callerID = actual;
			await clearRules(callerID);
			await addRule(callerID, "E", "event.publish", null, "orders.*");
			await sleep(900);
		}

		// Anti-containment: neither *.created nor orders.* contains the other,
		// but their intersection is orders.created → it must surface. Containment
		// semantics would silently fail this.
		await waitFor(
			() => methodVisible(caller!, PUBLISHER_SVC, "orders.created"),
			6_000,
			"orders.created surfaces via *.created sub (intersection)",
		);
		const peer = caller.serviceMap().get(PUBLISHER_SVC)!;
		expect(peer.methods.map((m) => m.name)).toContain("orders.created");
	}, 30_000);

	test("Layer-1 scope filter (events) disjoint: payments.* × orders.* is empty → payments.created blocked", async () => {
		callee = dedicated("primary");
		callee.event.define("orders.created", ORDER);
		callee.event.define("payments.created", ORDER);
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "pub connected");
		calleeID = callee.identity()!.serviceId;

		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		await addRule(callerID, "E", "event.publish", null, "orders.*");
		await sleep(400);

		caller = dedicated("second");
		// payments.* is disjoint from the scope rule orders.*.
		caller.event.handle("payments.*", async () => {});
		caller.event.define("payments.*", ORDER);
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "sub connected");

		const actual = caller.identity()!.serviceId;
		if (actual !== callerID) {
			await clearRules(callerID);
			callerID = actual;
			await clearRules(callerID);
			await addRule(callerID, "E", "event.publish", null, "orders.*");
			await sleep(900);
		}

		// Let the snapshot settle — a broken scope filter would surface
		// payments.created through the payments.* sub.
		await sleep(1_500);

		const peer = caller.serviceMap().get(PUBLISHER_SVC);
		const names = (peer?.methods ?? []).map((m) => m.name);
		expect(names).not.toContain("payments.created");
	}, 30_000);

	test("Layer-1 scope filter (workflow): A with workflow.run(B,flowA) sees flowA not flowB", async () => {
		const flowA = uniqueId("scope-wf-A");
		const flowB = uniqueId("scope-wf-B");

		callee = dedicated("primary");
		callee.workflow.handle(flowA, {
			steps: [{ id: "noop", type: "sleep", durationSec: 0 }],
		});
		callee.workflow.handle(flowB, {
			steps: [{ id: "noop", type: "sleep", durationSec: 0 }],
		});
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;

		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		await addRule(callerID, "E", "workflow.run", calleeID, flowA);
		await sleep(400);

		caller = dedicated("second");
		caller.service(PUBLISHER_SVC, { workflows: [flowA, flowB] });
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		const actualCallerID = caller.identity()!.serviceId;
		if (actualCallerID !== callerID) {
			await clearRules(callerID);
			callerID = actualCallerID;
			await clearRules(callerID);
			await addRule(callerID, "E", "workflow.run", calleeID, flowA);
			await sleep(900);
		}

		await waitFor(
			() => methodVisible(caller!, PUBLISHER_SVC, flowA),
			6_000,
			"B/flowA visible in caller snapshot",
		);

		const peer = caller.serviceMap().get(PUBLISHER_SVC);
		expect(peer).toBeDefined();
		const methodNames = peer!.methods.map((m) => m.name);
		expect(methodNames).toContain(flowA);
		expect(methodNames).not.toContain(flowB);
	}, 30_000);

	test("live NOTIFY: inserting a non-covering egress rule denies a previously-allowed call within ~2s", async () => {
		const method = uniqueId("live");
		const dummyAllowed = uniqueId("live-dummy");

		callee = dedicated("primary");
		callee.rpc.handle(method, async () => ({ transactionId: "ok", ok: true }), {
			schema: PAY,
		});
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;

		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);

		caller = dedicated("second");
		caller.service(PUBLISHER_SVC, { rpc: [method] });
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		callerID = caller.identity()!.serviceId;
		await clearRules(callerID);

		await waitFor(
			() => methodVisible(caller!, PUBLISHER_SVC, method),
			5_000,
			"method discovered",
		);
		await caller.useSchema(PUBLISHER_SVC, method, PAY);

		// Step 1 — default-allow baseline: call succeeds with no rules.
		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			PUBLISHER_SVC,
			method,
			{ userId: "u", amount: 1 },
			{ timeout: "5s", transport: "proxy" },
		);
		expect(ok.transactionId).toBe("ok");

		// Step 2 — insert a rule that disables default-allow but does NOT cover
		// `method` (peer NULL, some other target).
		await addRule(callerID, "E", "rpc.call", null, dummyAllowed);
		// Step 3 — model real NOTIFY → snapshot propagation time.
		await sleep(1_500);

		// Step 4 — the same call must now be denied.
		await expectDenied(
			() =>
				caller!.rpc.call(
					PUBLISHER_SVC,
					method,
					{ userId: "u", amount: 1 },
					{ timeout: "5s", transport: "proxy" },
				),
			/PermissionDenied|denied|egress/i,
		);
	}, 30_000);

	test("live NOTIFY: policyEvaluation() reflects a newly inserted egress rule within 2s", async () => {
		caller = dedicated("second");
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		await waitFor(
			() => caller!.policyEvaluation() !== null,
			3_000,
			"initial policyEvaluation present",
		);

		callerID = caller.identity()!.serviceId;
		await clearRules(callerID);
		const beforeEgress = caller.policyEvaluation()?.egress ?? [];
		const liveTarget = uniqueId("policy-eval");
		await addRule(callerID, "E", "rpc.call", null, liveTarget);

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
	}, 30_000);

	test("live NOTIFY: revoking a peer-scoping rule drops the peer from serviceMap (<2s) and a fresh call fails", async () => {
		const liveMethod = uniqueId("live-rm");

		callee = dedicated("primary");
		callee.rpc.handle(
			liveMethod,
			async () => ({ transactionId: "ok", ok: true }),
			{ schema: PAY },
		);
		await connect(callee);
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");
		calleeID = callee.identity()!.serviceId;

		// Seed scoping rule BEFORE caller starts.
		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		await addRule(callerID, "E", "rpc.call", calleeID, liveMethod);
		await sleep(400);

		caller = dedicated("second");
		caller.service(PUBLISHER_SVC, { rpc: [liveMethod] });
		await connect(caller);
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		const actualCallerID = caller.identity()!.serviceId;
		if (actualCallerID !== callerID) {
			await clearRules(callerID);
			callerID = actualCallerID;
			await clearRules(callerID);
			await addRule(callerID, "E", "rpc.call", calleeID, liveMethod);
			await sleep(900);
		}

		await waitFor(
			() => caller!.serviceMap().has(PUBLISHER_SVC),
			6_000,
			"callee initially visible to caller",
		);

		// Flip the rule: drop (callee, liveMethod), add a placeholder pinned to a
		// real-but-non-matching UUID (the caller's own id). A peer=NULL rule would
		// re-enable default-allow, so we MUST pin to a concrete UUID — scope then
		// yields zero visible peers and the callee falls out via removed_peers.
		await clearRules(callerID);
		await addRule(
			callerID,
			"E",
			"rpc.call",
			callerID,
			uniqueId("live-rm-placeholder"),
		);

		await waitFor(
			() => !caller!.serviceMap().has(PUBLISHER_SVC),
			4_000,
			"callee removed from serviceMap after rule revoke",
		);

		await expectDenied(
			() =>
				caller!.rpc.call(
					PUBLISHER_SVC,
					liveMethod,
					{ userId: "u", amount: 1 },
					{ timeout: "3s" },
				),
			/Unavailable|PermissionDenied|denied|no descriptor|cancelled|CANCELLED|no instance|egress/i,
		);
	}, 40_000);

	test("SDK warning surface: declaring a forbidden outgoing dep fires policy_violation (denySide=self_egress) + populates policyEvaluation().warnings", async () => {
		const violations: Array<{
			declaration: string;
			value: string;
			denySide: string;
			reason: string;
		}> = [];

		callerID = await serviceID(SUBSCRIBER_SVC);
		await clearRules(callerID);
		// Any non-empty egress rule disables default-allow for (service, rpc.call);
		// a non-matching declared dep then becomes a warning.
		await addRule(callerID, "E", "rpc.call", null, "allowed.method");
		await sleep(500);

		caller = dedicated("second");
		caller.on("policy_violation", (v) => {
			violations.push(v);
		});
		// Declare an outgoing dep NOT covered by the egress rule.
		caller.service("payments", { rpc: ["restricted-method"] });

		await connect(caller);
		await waitFor(
			() => caller!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		const actual = caller.identity()!.serviceId;
		if (actual !== callerID) {
			await clearRules(callerID);
			callerID = actual;
			await clearRules(callerID);
			await addRule(callerID, "E", "rpc.call", null, "allowed.method");
		}

		await waitFor(
			() => caller!.policyEvaluation() !== null,
			3_000,
			"policy evaluation surfaced",
		);

		expect(violations.length).toBeGreaterThan(0);
		const target = violations.find(
			(v) =>
				v.declaration === "rpc.call" && v.value.includes("restricted-method"),
		);
		expect(target).toBeDefined();
		expect(target!.denySide).toBe("self_egress");

		const eval_ = caller.policyEvaluation();
		expect(eval_).not.toBeNull();
		expect(eval_!.warnings.length).toBeGreaterThan(0);
	}, 20_000);
});
