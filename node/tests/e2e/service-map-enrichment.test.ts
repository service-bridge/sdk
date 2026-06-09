// service-map-enrichment.test.ts — ADR-0004 service map enrichment.
//
// A service that registers both event-handle subscriptions and outgoing rpc
// deps must see those reflected in its OWN ServiceMapEntry via the enriched
// fields `eventSubscriptions` and `outgoingCalls`. Methods of the declared
// peer service must also be visible in the map.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep, waitFor } from "./_helpers/events";

const CALLEE_SVC = "e2e-registry-svc";
const SUBSCRIBER_SVC = "e2e-registry-consumer";

const paymentProto = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"serde",
	"testdata",
	"payment.proto",
);
const orderProto = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"events",
	"testdata",
	"order-event.proto",
);
const PAY_SCHEMA = {
	protoFile: paymentProto,
	input: "ChargeRequest",
	output: "ChargeResponse",
};

describe("service-map: enriched ServiceMapEntry", () => {
	const env = eventsEnv();
	let callee: ServiceBridge | undefined;
	let consumer: ServiceBridge | undefined;
	const method = `enrich-${Date.now()}`;
	const subPattern = `enrich.${Date.now()}.created`;

	beforeEach(() => {
		callee = consumer = undefined;
	});

	afterEach(async () => {
		await consumer?.stop();
		await callee?.stop();
		await sleep(300);
	});

	test("consumer sees own eventSubscriptions + outgoingCalls + peer methods", async () => {
		// Callee registers an rpc handler the consumer will declare as outgoing.
		callee = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		callee.rpc.handle(method, async () => ({ transactionId: "x", ok: true }), {
			schema: PAY_SCHEMA,
		});
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		// Consumer declares: an outgoing rpc dep on callee + a subscription.
		consumer = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		consumer.service(CALLEE_SVC, { rpc: [method] });
		consumer.event.handle(subPattern, async () => {});
		consumer.event.define(subPattern, {
			protoFile: orderProto,
			method: "orders_created",
		});
		await consumer.start();
		await waitFor(
			() => consumer!.identity() !== null,
			5_000,
			"consumer connected",
		);

		// Wait for the snapshot to surface the declared subscription + outgoing.
		await waitFor(
			() => {
				const map = consumer!.serviceMap();
				const self = map.get(SUBSCRIBER_SVC);
				if (!self) return false;
				return (
					self.eventSubscriptions.some((es) => es.pattern === subPattern) &&
					self.outgoingCalls.some((oc) => oc.targetMethod === method)
				);
			},
			6_000,
			"self enrichment populated",
		);

		const map = consumer.serviceMap();
		const self = map.get(SUBSCRIBER_SVC)!;
		expect(self.eventSubscriptions.map((es) => es.pattern)).toContain(
			subPattern,
		);
		expect(self.outgoingCalls.map((oc) => oc.targetMethod)).toContain(method);

		// Peer service must be present with its method.
		const peer = map.get(CALLEE_SVC);
		expect(peer).toBeDefined();
		expect(peer!.methods.map((m) => m.name)).toContain(method);
	}, 25_000);
});
