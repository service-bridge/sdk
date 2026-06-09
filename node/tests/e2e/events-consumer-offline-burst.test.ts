/**
 * events-consumer-offline-burst.test.ts — consumer offline → backlog
 * accumulates → consumer reconnects → drains everything.
 *
 * Background:
 *   Durable events are written to event_deliveries with status='pending' when
 *   the dispatcher claims them but the target subscriber has no active stream.
 *   When the subscriber reconnects, the runtime's Dispatcher wakes via the
 *   kick channel (or its 200ms poll tick) and pushes pending deliveries through
 *   the re-established Subscribe stream.
 *
 * Scenario:
 *   1. Subscriber starts, registers event.handle(name, ...), then stops.
 *      The subscription row remains in event_subscriptions (durable=true in DB)
 *      even after the stream closes — the consumer service is still known.
 *   2. Publisher emits 10 events while subscriber is down.
 *      The dispatcher creates event_deliveries rows with status='pending';
 *      no stream is available, so they stay pending.
 *   3. Subscriber restarts with the SAME service key and SAME pattern.
 *      On Register, the server calls Replace() with the same pattern set
 *      (no orphans). The Subscribe stream re-attaches; the dispatcher finds
 *      the 10 pending deliveries and pushes them through the stream.
 *   4. Assertion: handler is invoked 10 times within a 15s window.
 *      All 10 distinct orderId values are received.
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

const BURST_SIZE = 10;

describe("events e2e: consumer offline burst — backlog drains on reconnect", () => {
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

	test(`${BURST_SIZE} events published while consumer is offline are all delivered after reconnect`, async () => {
		const eventName = uniqueEventName("offline-burst");

		const received: Array<{ orderId: string }> = [];

		// --- Phase 1: connect subscriber so the subscription row is created ---
		// The subscription must exist in event_subscriptions BEFORE we publish,
		// otherwise the fan-out finds no matching subscribers and no
		// event_deliveries rows are created at all (fan-out is at ingest time).
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
			"first subscriber connection",
		);

		// Give the subscription stream time to register.
		await sleep(500);

		// Stop the subscriber — stream closes, but event_subscriptions row stays.
		await subscriber.stop();
		subscriber = undefined;

		// Brief pause so the stream teardown completes on the server side.
		await sleep(300);

		// --- Phase 2: publish BURST_SIZE events while subscriber is down ---
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

		// Publish all events in sequence. Each publish() returns only after the
		// event is durably written to the outbox (then drained to event_log).
		const publishedIds: string[] = [];
		for (let i = 0; i < BURST_SIZE; i++) {
			const { eventId } = await publisher.event.publish(eventName, {
				orderId: `burst-${i}`,
				amount: Number(i),
				currency: "USD",
			});
			publishedIds.push(eventId);
		}

		expect(publishedIds).toHaveLength(BURST_SIZE);

		// Allow the drainer to flush all events to event_log and the dispatcher
		// to create event_deliveries rows. Dispatcher tick = 200ms; allow 2s.
		await sleep(2_000);

		// --- Phase 3: subscriber reconnects (new ServiceBridge, same key) ---
		// The subscription pattern is re-registered via Replace() with the same
		// pattern → no orphaning. The dispatcher detects the new stream and
		// pushes the 10 pending deliveries.
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
			"subscriber reconnected",
		);

		// Wait for all BURST_SIZE deliveries to arrive. Generous 15s window
		// to cover dispatcher tick (200ms) + stream round-trip + ack latency.
		await waitFor(
			() => received.length >= BURST_SIZE,
			15_000,
			`all ${BURST_SIZE} deliveries after reconnect`,
		);

		// Verify all 10 burst events were received (order not guaranteed).
		expect(received.length).toBeGreaterThanOrEqual(BURST_SIZE);
		const receivedIds = received.map((r) => r.orderId).sort();
		for (let i = 0; i < BURST_SIZE; i++) {
			expect(receivedIds).toContain(`burst-${i}`);
		}
	}, 50_000);
});
