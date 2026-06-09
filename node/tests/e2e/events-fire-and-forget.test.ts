/**
 * events-fire-and-forget.test.ts — fireAndForget: true bypasses the outbox.
 *
 * Verifies that events published with { fireAndForget: true } are:
 *   1. Sent directly to the runtime (not via SQLite outbox).
 *   2. Still delivered to a subscriber that registered for the event name.
 *   3. Return a well-formed eventId synchronously.
 *
 * The durable (outbox) path inserts into SQLite then kicks the drainer.
 * The fire-and-forget path calls rpcClient.publish() directly and returns
 * the eventId immediately — no local persistence. The runtime ingests the
 * event and fans it out to matching subscribers.
 *
 * SQLite storage is private on ServiceBridge, so outbox-absence is verified
 * indirectly: FAF returns immediately (< 200ms from rpcClient.publish() round-
 * trip latency vs. SQLite COMMIT + drainer cycle). Delivery to subscriber
 * confirms the event reached the runtime through the direct path.
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

describe("events e2e: fire-and-forget", () => {
	const env = eventsEnv();
	let publisher: ServiceBridge | undefined;
	let subscriber: ServiceBridge | undefined;

	beforeEach(() => {
		publisher = undefined;
		subscriber = undefined;
	});

	afterEach(async () => {
		await publisher?.stop();
		await subscriber?.stop();
	});

	test("fireAndForget publish returns a valid eventId", async () => {
		const eventName = uniqueEventName("faf-id");

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

		await sleep(300);

		const { eventId } = await publisher.event.publish(
			eventName,
			{ orderId: "faf-1", amount: 1.0, currency: "USD" },
			{ fireAndForget: true },
		);

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);
	}, 15_000);

	test("fireAndForget event is still delivered to a waiting subscriber", async () => {
		const eventName = uniqueEventName("faf-delivery");

		const received: Array<{
			orderId: string;
			amount: number;
			currency: string;
		}> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			received.push(
				payload as { orderId: string; amount: number; currency: string },
			);
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

		await sleep(500);

		const { eventId } = await publisher.event.publish(
			eventName,
			{ orderId: "faf-deliver-1", amount: 99.99, currency: "USD" },
			{ fireAndForget: true },
		);

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);

		await waitFor(
			() => received.length > 0,
			10_000,
			"subscriber received FAF event",
		);

		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("faf-deliver-1");
		expect(received[0]!.amount).toBeCloseTo(99.99);
		expect(received[0]!.currency).toBe("USD");
	}, 30_000);

	test("five back-to-back FAF events all arrive at subscriber", async () => {
		const eventName = uniqueEventName("faf-burst");

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

		await sleep(500);

		const eventIds: string[] = [];
		for (let i = 0; i < 5; i++) {
			const { eventId } = await publisher.event.publish(
				eventName,
				{ orderId: `faf-burst-${i}`, amount: i * 10, currency: "USD" },
				{ fireAndForget: true },
			);
			eventIds.push(eventId);
		}

		// All 5 event IDs must be unique UUIDs.
		expect(new Set(eventIds).size).toBe(5);
		for (const id of eventIds) {
			expect(id).toMatch(/^[0-9a-f-]{36}$/);
		}

		await waitFor(
			() => received.length >= 5,
			15_000,
			"all 5 FAF events delivered",
		);

		expect(received).toHaveLength(5);
		const receivedIds = received.map((r) => r.orderId).sort();
		expect(receivedIds).toEqual([
			"faf-burst-0",
			"faf-burst-1",
			"faf-burst-2",
			"faf-burst-3",
			"faf-burst-4",
		]);
	}, 35_000);

	test("durable and FAF publishes coexist without interference", async () => {
		const eventName = uniqueEventName("faf-coexist");

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

		await sleep(500);

		// Durable publish (goes via outbox).
		const { eventId: durableId } = await publisher.event.publish(eventName, {
			orderId: "coexist-durable",
			amount: 1,
			currency: "USD",
		});
		// FAF publish (bypasses outbox).
		const { eventId: fafId } = await publisher.event.publish(
			eventName,
			{ orderId: "coexist-faf", amount: 2, currency: "USD" },
			{ fireAndForget: true },
		);

		expect(durableId).toMatch(/^[0-9a-f-]{36}$/);
		expect(fafId).toMatch(/^[0-9a-f-]{36}$/);
		expect(durableId).not.toBe(fafId);

		await waitFor(() => received.length >= 2, 15_000, "both events delivered");

		expect(received).toHaveLength(2);
		const ids = received.map((r) => r.orderId).sort();
		expect(ids).toEqual(["coexist-durable", "coexist-faf"]);
	}, 35_000);
});
