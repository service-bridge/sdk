// access-policy-scope-events.test.ts — ADR-0004 Layer-1 scope filter (events).
//
// Verifies the runtime's scope filter on the event dimension uses real
// PatternIntersect (not containment). The shipped semantic: a subscriber's
// `event.handle` subscription Q surfaces matching published events ONLY when
// PatternIntersect(Q, P) is non-empty for at least one P in the subscriber's
// own `event.publish` egress patterns.
//
// Two assertions:
//   1. Anti-containment (positive). Sub Q = `*.created`, scope P = `orders.*`.
//      Neither contains the other; their intersection is `orders.created`
//      (non-empty). Publisher publishes `orders.created`. Event MUST surface
//      to subscriber — proves the gate is intersection-based.
//   2. Disjoint (negative). Sub Q = `payments.*`, scope P = `orders.*`. The
//      intersection is empty. Publisher publishes `payments.created`. Event
//      MUST NOT surface — proves the gate blocks disjoint subs even when a
//      published topic matches the subscription pattern.
//
// Each assertion runs in its own test with full isolation: separate scope
// patterns, fresh sub instance, cleared rules in afterEach.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep, waitFor } from "./_helpers/events";
import { addRule, clearRules, serviceID } from "./_helpers/policy-db";

const PUB_SVC = "e2e-registry-svc";
const SUB_SVC = "e2e-registry-consumer";

const orderProto = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"events",
	"testdata",
	"order-event.proto",
);
const PROTO = { protoFile: orderProto, method: "orders_created" } as const;

interface Ctx {
	pub: ServiceBridge | undefined;
	sub: ServiceBridge | undefined;
	pubID: string;
	subID: string;
}

async function setupPublisher(env: ReturnType<typeof eventsEnv>): Promise<{
	sb: ServiceBridge;
	id: string;
}> {
	const sb = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
	sb.event.define("orders.created", PROTO);
	sb.event.define("payments.created", PROTO);
	await sb.start();
	await waitFor(() => sb.identity() !== null, 5_000, "pub connected");
	return { sb, id: sb.identity()!.serviceId };
}

describe("access-policy: scope filter (event.publish)", () => {
	const env = eventsEnv();
	const ctx: Ctx = { pub: undefined, sub: undefined, pubID: "", subID: "" };

	beforeEach(() => {
		ctx.pub = ctx.sub = undefined;
		ctx.pubID = ctx.subID = "";
	});

	afterEach(async () => {
		await ctx.sub?.stop();
		await ctx.pub?.stop();
		if (ctx.subID) await clearRules(ctx.subID);
		if (ctx.pubID) await clearRules(ctx.pubID);
		await sleep(300);
	});

	test("anti-containment: *.created × orders.* is non-empty → orders.created surfaces", async () => {
		const { sb: pub, id: pubID } = await setupPublisher(env);
		ctx.pub = pub;
		ctx.pubID = pubID;

		// Apply narrowing rule on sub BEFORE start.
		let subID = await serviceID(SUB_SVC);
		await clearRules(subID);
		await addRule(subID, "E", "event.publish", null, "orders.*");
		await sleep(400);

		const sub = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		sub.event.handle("*.created", async () => {});
		sub.event.define("*.created", PROTO);
		await sub.start();
		await waitFor(() => sub.identity() !== null, 5_000, "sub connected");
		ctx.sub = sub;

		const actual = sub.identity()!.serviceId;
		if (actual !== subID) {
			await clearRules(subID);
			subID = actual;
			await clearRules(subID);
			await addRule(subID, "E", "event.publish", null, "orders.*");
			await sleep(900);
		}
		ctx.subID = subID;

		// orders.created must surface via the *.created subscription. If the
		// runtime used containment instead of intersection, neither pattern
		// contains the other and this would silently fail.
		await waitFor(
			() => {
				const peer = sub.serviceMap().get(PUB_SVC);
				if (!peer) return false;
				return peer.methods.some((m) => m.name === "orders.created");
			},
			6_000,
			"orders.created surfaces via *.created sub (anti-containment intersection)",
		);

		const peer = sub.serviceMap().get(PUB_SVC)!;
		const names = peer.methods.map((m) => m.name);
		expect(names).toContain("orders.created");
	}, 30_000);

	test("disjoint: payments.* × orders.* is empty → payments.created blocked", async () => {
		const { sb: pub, id: pubID } = await setupPublisher(env);
		ctx.pub = pub;
		ctx.pubID = pubID;

		let subID = await serviceID(SUB_SVC);
		await clearRules(subID);
		await addRule(subID, "E", "event.publish", null, "orders.*");
		await sleep(400);

		const sub = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		// Only payments.* — disjoint from the scope rule orders.*.
		sub.event.handle("payments.*", async () => {});
		sub.event.define("payments.*", PROTO);
		await sub.start();
		await waitFor(() => sub.identity() !== null, 5_000, "sub connected");
		ctx.sub = sub;

		const actual = sub.identity()!.serviceId;
		if (actual !== subID) {
			await clearRules(subID);
			subID = actual;
			await clearRules(subID);
			await addRule(subID, "E", "event.publish", null, "orders.*");
			await sleep(900);
		}
		ctx.subID = subID;

		// Allow the snapshot to settle. If the scope filter were broken the
		// payments.created event would surface through the payments.* sub.
		await sleep(1_500);

		const peer = sub.serviceMap().get(PUB_SVC);
		// peer may not appear at all (no allowed subs) — that itself is proof
		// of scope blocking. If it does appear, payments.created must NOT be
		// among its methods.
		const names = (peer?.methods ?? []).map((m) => m.name);
		expect(names).not.toContain("payments.created");
	}, 30_000);
});
