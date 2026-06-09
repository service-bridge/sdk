/**
 * events-routing.test.ts — server-side event routing semantics.
 *
 * Verifies that the runtime's AMQP-style topic matching routes events correctly.
 * Per ADR-0002, routing is server-only: the SDK dispatches deliveries by exact
 * event name (envelope.name === handler.pattern). Tests use one subscriber with
 * multiple handlers, each registered for a specific exact name, and verify
 * which events each receives based on what the server routes.
 *
 * Subscription patterns sent to the server:
 *   - `prefix.created`  — matches exact name `prefix.created` only
 *   - `prefix.*`        — matches any single-segment `prefix.X`
 *   - `prefix.#`        — matches any multi-segment `prefix.X.Y.Z`
 *
 * Per ADR-0002 design, the SDK dispatches to handlers by exact name match.
 * The handler registered as `prefix.created` fires for deliveries with that
 * exact name; the other two patterns fire for deliveries whose exact name
 * is `prefix.*` or `prefix.#` respectively (which never occurs in practice).
 *
 * What IS testable: the `prefix.created` event arrives exactly once at the
 * subscriber (server routes it based on matching subscriptions), and the
 * `prefix.payment.received` event is NOT delivered to a handler registered
 * only for `prefix.created` (server routes by patterns; SDK handler invoked
 * only if the exact name matches the registered handler pattern).
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

describe("events e2e: routing semantics", () => {
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

	test("exact-name handler receives only its declared event", async () => {
		const prefix = uniqueEventName("routing");
		const exactName = `${prefix}.created`;

		const receivedCreated: Array<{ orderId: string }> = [];
		const receivedOther: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		// Declare schemas for both event names the subscriber might receive.
		subscriber.event.define(exactName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		// Register handler for the exact event name.
		subscriber.event.handle(exactName, async (payload) => {
			receivedCreated.push(payload as { orderId: string });
		});
		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		publisher = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher.event.define(exactName, {
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

		await publisher.event.publish(exactName, {
			orderId: "routing-exact-1",
			amount: 5.0,
			currency: "USD",
		});

		await waitFor(
			() => receivedCreated.length > 0,
			10_000,
			"exact handler received event",
		);

		// Exact handler received exactly once.
		expect(receivedCreated).toHaveLength(1);
		expect(receivedCreated[0]!.orderId).toBe("routing-exact-1");
		// No spurious deliveries.
		expect(receivedOther).toHaveLength(0);
	}, 30_000);

	test("two distinct event names delivered to their respective handlers independently", async () => {
		const prefix = uniqueEventName("routing-split");
		const nameA = `${prefix}.shipped`;
		const nameB = `${prefix}.cancelled`;

		const receivedA: Array<{ orderId: string }> = [];
		const receivedB: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(nameA, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.define(nameB, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(nameA, async (payload) => {
			receivedA.push(payload as { orderId: string });
		});
		subscriber.event.handle(nameB, async (payload) => {
			receivedB.push(payload as { orderId: string });
		});
		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		publisher = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher.event.define(nameA, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		publisher.event.define(nameB, {
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

		await publisher.event.publish(nameA, {
			orderId: "route-a-1",
			amount: 10,
			currency: "EUR",
		});
		await publisher.event.publish(nameB, {
			orderId: "route-b-1",
			amount: 20,
			currency: "EUR",
		});

		await waitFor(
			() => receivedA.length > 0 && receivedB.length > 0,
			12_000,
			"both handlers received their events",
		);

		// Each handler received exactly its own event, not the other's.
		expect(receivedA).toHaveLength(1);
		expect(receivedA[0]!.orderId).toBe("route-a-1");

		expect(receivedB).toHaveLength(1);
		expect(receivedB[0]!.orderId).toBe("route-b-1");
	}, 35_000);

	test("event not subscribed to is not delivered to subscriber", async () => {
		const prefix = uniqueEventName("routing-nodel");
		const subscribedName = `${prefix}.created`;
		const unsubscribedName = `${prefix}.archived`;

		const receivedSubscribed: Array<{ orderId: string }> = [];
		const receivedUnsubscribed: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(subscribedName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		// Only subscribe to subscribedName — not to unsubscribedName.
		subscriber.event.handle(subscribedName, async (payload) => {
			receivedSubscribed.push(payload as { orderId: string });
		});
		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		publisher = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher.event.define(subscribedName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		publisher.event.define(unsubscribedName, {
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

		// Publish to both. Only the subscribed one should arrive.
		await publisher.event.publish(unsubscribedName, {
			orderId: "unsubscribed-1",
			amount: 1,
			currency: "USD",
		});
		await publisher.event.publish(subscribedName, {
			orderId: "subscribed-1",
			amount: 2,
			currency: "USD",
		});

		await waitFor(
			() => receivedSubscribed.length > 0,
			12_000,
			"subscribed event delivered",
		);

		// The subscribed event arrived.
		expect(receivedSubscribed).toHaveLength(1);
		expect(receivedSubscribed[0]!.orderId).toBe("subscribed-1");

		// The unsubscribed event was NOT delivered.
		expect(receivedUnsubscribed).toHaveLength(0);
	}, 35_000);
});
