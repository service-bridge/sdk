/**
 * events-multi-handler.test.ts — multiple handlers on the same exact event name.
 *
 * Verifies that when a single ServiceBridge instance registers two event.handle()
 * calls for the same event name, BOTH handlers are invoked for each delivery.
 * The subscriber dispatches incoming events to all registered handlers whose
 * pattern matches the exact event name (ADR-0011: server-only routing, SDK
 * dispatches by exact name).
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

describe("events e2e: multiple handlers for same event name", () => {
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

	test("two handlers registered for the same exact name both fire on each delivery", async () => {
		const eventName = uniqueEventName("multi-handler");

		const handler1Received: Array<{ orderId: string }> = [];
		const handler2Received: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		// Register two handlers for the SAME exact event name.
		subscriber.event.handle(eventName, async (payload) => {
			handler1Received.push(payload as { orderId: string });
		});
		subscriber.event.handle(eventName, async (payload) => {
			handler2Received.push(payload as { orderId: string });
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

		const { eventId } = await publisher.event.publish(eventName, {
			orderId: "multi-1",
			amount: 25.0,
			currency: "GBP",
		});

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);

		await waitFor(
			() => handler1Received.length > 0 && handler2Received.length > 0,
			10_000,
			"both handlers fired",
		);

		// Both handlers must have been called exactly once.
		expect(handler1Received).toHaveLength(1);
		expect(handler2Received).toHaveLength(1);

		// Both decoded the same payload.
		expect(handler1Received[0]!.orderId).toBe("multi-1");
		expect(handler2Received[0]!.orderId).toBe("multi-1");
	}, 30_000);

	test("two deliveries of the same event invoke both handlers twice each", async () => {
		const eventName = uniqueEventName("multi-handler-double");

		const handler1Received: Array<{ orderId: string }> = [];
		const handler2Received: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			handler1Received.push(payload as { orderId: string });
		});
		subscriber.event.handle(eventName, async (payload) => {
			handler2Received.push(payload as { orderId: string });
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

		await publisher.event.publish(eventName, {
			orderId: "multi-a",
			amount: 10,
			currency: "USD",
		});
		await publisher.event.publish(eventName, {
			orderId: "multi-b",
			amount: 20,
			currency: "USD",
		});

		await waitFor(
			() => handler1Received.length >= 2 && handler2Received.length >= 2,
			15_000,
			"both handlers received both events",
		);

		expect(handler1Received).toHaveLength(2);
		expect(handler2Received).toHaveLength(2);

		const ids1 = handler1Received.map((r) => r.orderId).sort();
		const ids2 = handler2Received.map((r) => r.orderId).sort();
		expect(ids1).toEqual(["multi-a", "multi-b"]);
		expect(ids2).toEqual(["multi-a", "multi-b"]);
	}, 35_000);
});
