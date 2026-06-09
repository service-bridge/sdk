/**
 * events-happy.test.ts — basic publish → subscribe round-trip.
 *
 * Verifies the simplest possible Durable Events flow:
 *   1. Publisher declares an event with a Protobuf schema.
 *   2. Subscriber registers a handler for the exact event name.
 *   3. Publisher emits one event.
 *   4. Subscriber's handler is invoked with the decoded payload.
 *
 * Runs against a live runtime + real Postgres (see _helpers/events.ts).
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

describe("events e2e: happy path", () => {
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

	test("publish → subscribe exact match delivers decoded payload", async () => {
		const eventName = uniqueEventName("happy");
		const received: Array<{
			orderId: string;
			amount: number;
			currency: string;
		}> = [];

		// Subscriber is started first so the subscription pattern is registered
		// in the runtime before the publisher emits. Order matters: a publish
		// against an unregistered name is accepted but fan-out finds zero
		// matches and the event is never delivered.
		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.handle(eventName, async (payload) => {
			received.push(payload as (typeof received)[number]);
		});
		// Subscribers must also declare the schema so the SDK has the SchemaPair
		// to decode incoming payload bytes (publisher and subscriber both index
		// schemas locally — there's no global registry of decoders).
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
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

		// Give the subscription stream a moment to register on the server side.
		// The runtime's MatchingFor() is in-memory and updated by RegisterRequest
		// + subscription stream, both of which are async after start() returns.
		await sleep(500);

		const { eventId } = await publisher.event.publish(eventName, {
			orderId: "order-42",
			amount: 99.5,
			currency: "USD",
		});

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);

		await waitFor(() => received.length > 0, 10_000, "delivery to subscriber");

		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("order-42");
		expect(received[0]!.amount).toBeCloseTo(99.5);
		expect(received[0]!.currency).toBe("USD");
	}, 30_000);

	test("publisher.event.publish returns synchronously after outbox insert", async () => {
		// Durable path persists into the SDK's SQLite outbox before returning.
		// We verify by emitting and observing that the returned eventId is a
		// well-formed UUID — the drainer takes over async delivery to runtime.
		const eventName = uniqueEventName("happy-outbox");

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

		const t0 = Date.now();
		const { eventId } = await publisher.event.publish(eventName, {
			orderId: "outbox-1",
			amount: 1,
			currency: "EUR",
		});
		const elapsed = Date.now() - t0;

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);
		// Outbox INSERT + COMMIT is local SQLite — should be milliseconds.
		// 500ms is generous to accommodate cold-start fsync.
		expect(elapsed).toBeLessThan(500);
	}, 15_000);
});
