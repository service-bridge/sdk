/**
 * events-idempotency.test.ts — publisher-side idempotency key deduplication.
 *
 * Scenario: publishing the same event twice with the same idempotencyKey
 * produces exactly ONE delivery to the subscriber.
 *
 * Runtime behaviour (ingest.go::Accept):
 *   - First publish: idempotency.Claim grants the slot → event inserted →
 *     idempotency.Save records the original eventId.
 *   - Second publish (same publisherSvcId + same idempotencyKey): Claim returns
 *     (claimed=false, existing) → returns AcceptResult with
 *     Status=REJECTED_DUPLICATE and EventID = original eventId.
 *
 * SDK drainer behaviour (drainer.ts):
 *   - PUBLISH_STATUS_REJECTED_DUPLICATE is treated the same as ACCEPTED —
 *     the outbox row is deleted without error. publish() itself returns
 *     { eventId } derived from the local outbox INSERT (the SDK-generated UUID),
 *     NOT the runtime-canonical id. So the two publish() calls return different
 *     eventIds locally, but the subscriber receives the event exactly once.
 *
 * Idempotency key: scoped by publisherSvcId (runtime composes
 * publisherSvcId + ":" + idempotencyKey). Two SDK instances that use the
 * same service key share the same service_id, so their idempotency keys ARE
 * deduplicated against each other — even if published from different JS
 * processes. The test asserts dedup applies in this case.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	eventsEnv,
	FAST_OPTS,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueEventName,
	waitFor,
} from "./_helpers/events";

describe("events e2e: publisher-side idempotency", () => {
	const env = eventsEnv();
	let publisher: ServiceBridge | undefined;
	let publisher2: ServiceBridge | undefined;
	let subscriber: ServiceBridge | undefined;

	beforeEach(() => {
		publisher = undefined;
		publisher2 = undefined;
		subscriber = undefined;
	});

	afterEach(async () => {
		await publisher?.stop();
		await publisher2?.stop();
		await subscriber?.stop();
	});

	test("duplicate publish with same idempotencyKey delivers exactly once", async () => {
		const eventName = uniqueEventName("idem");
		const received: Array<{ orderId: string }> = [];

		// Subscriber must be up before publish so the delivery is created on ingest.
		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			received.push(payload as { orderId: string });
		});
		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		publisher = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await publisher.start();
		await waitFor(
			() => publisher!.identity() !== null,
			5_000,
			"publisher connected",
		);

		// Give subscription stream time to register on server side.
		await sleep(500);

		const idemKey = `order-dedup-${Date.now()}`;

		// First publish: accepted, inserted into outbox → drained → delivery created.
		const { eventId: id1 } = await publisher.event.publish(
			eventName,
			{ orderId: "order-idem-1", amount: 10, currency: "USD" },
			{ idempotencyKey: idemKey },
		);

		// Second publish with the same key from the same publisher SDK instance.
		// The drainer will receive REJECTED_DUPLICATE from the runtime and
		// silently discard the outbox row. No error is thrown.
		const { eventId: id2 } = await publisher.event.publish(
			eventName,
			{ orderId: "order-idem-1", amount: 10, currency: "USD" },
			{ idempotencyKey: idemKey },
		);

		// Both calls return well-formed UUIDs — they are the locally-generated
		// outbox IDs, not the runtime canonical id.
		expect(id1).toMatch(/^[0-9a-f-]{36}$/);
		expect(id2).toMatch(/^[0-9a-f-]{36}$/);

		// Wait for the delivery and then a bit more to confirm no second delivery.
		await waitFor(() => received.length > 0, 15_000, "first delivery");
		await sleep(2_000);

		// Subscriber receives exactly ONE delivery — dedup fires on the runtime.
		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("order-idem-1");
	}, 30_000);

	test("same idempotencyKey from two SDK instances sharing same service key deduplicates", async () => {
		// Both publisher and publisher2 use the same publisherKey → same service_id
		// in the runtime. The runtime composes: publisherSvcId + ":" + idempotencyKey.
		// So even from distinct SDK instances, the same key is deduplicated.
		const eventName = uniqueEventName("idem-multi");
		const received: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			received.push(payload as { orderId: string });
		});
		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		// Two SDK instances, SAME publisherKey → same service_id.
		publisher = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await publisher.start();
		await waitFor(
			() => publisher!.identity() !== null,
			5_000,
			"publisher1 connected",
		);

		publisher2 = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher2.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await publisher2.start();
		await waitFor(
			() => publisher2!.identity() !== null,
			5_000,
			"publisher2 connected",
		);

		await sleep(500);

		const sharedKey = `shared-idem-${Date.now()}`;

		// Each publishes with the same idempotencyKey. Both share the same
		// service_id, so runtime deduplication fires: only one delivery is created.
		await publisher.event.publish(
			eventName,
			{ orderId: "shared-1", amount: 5, currency: "EUR" },
			{ idempotencyKey: sharedKey },
		);
		await publisher2.event.publish(
			eventName,
			{ orderId: "shared-1", amount: 5, currency: "EUR" },
			{ idempotencyKey: sharedKey },
		);

		await waitFor(() => received.length > 0, 15_000, "delivery");
		await sleep(2_000);

		// Dedup applies across SDK instances sharing the same service identity.
		expect(received).toHaveLength(1);
	}, 30_000);
});
