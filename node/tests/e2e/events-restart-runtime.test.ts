/**
 * events-restart-runtime.test.ts
 *
 * Scenario: subscriber reconnect — subscriber restarts and continues receiving
 * events from the same publisher.
 *
 * IMPORTANT — scope limitation:
 *   This test covers the SUBSCRIBER RESTART subset of the "runtime restart"
 *   scenario. A full runtime restart in a shared environment would kill the
 *   runtime process used by all parallel agents — that is explicitly forbidden
 *   by the test constraints. Instead we test that:
 *     1. A subscriber can stop and restart (new ServiceBridge, same key).
 *     2. After restart, the new subscriber instance receives newly emitted
 *        events — proving the SDK re-registers the subscription pattern
 *        correctly on reconnect.
 *
 *   What is NOT covered here:
 *     - In-flight events that were accepted by the runtime (status='pending'
 *       in event_deliveries) surviving a runtime process crash. That invariant
 *       is tested at the DB level in events-runtime-restart-ingested.test.ts.
 *     - Runtime-level session continuity across a runtime restart (not the SDK
 *       responsibility — the runtime reconstructs delivery state from Postgres
 *       on boot).
 *
 * Real assertions: 2 events received (1 before subscriber restart, 1 after),
 * both decoded correctly.
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

describe("events e2e: subscriber restart (reconnect after stop)", () => {
	const env = eventsEnv();

	let publisher: ServiceBridge | undefined;
	let subscriber1: ServiceBridge | undefined;
	let subscriber2: ServiceBridge | undefined;

	beforeEach(() => {
		publisher = undefined;
		subscriber1 = undefined;
		subscriber2 = undefined;
	});

	afterEach(async () => {
		await publisher?.stop();
		await subscriber1?.stop();
		await subscriber2?.stop();
	});

	test("subscriber restart — new instance receives events after re-registration", async () => {
		const eventName = uniqueEventName("sub-restart");

		const receivedByFirst: Array<{
			orderId: string;
			amount: number;
			currency: string;
		}> = [];
		const receivedBySecond: Array<{
			orderId: string;
			amount: number;
			currency: string;
		}> = [];

		// ── Phase 1: first subscriber ──────────────────────────────────────

		subscriber1 = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber1.event.handle(eventName, async (payload) => {
			receivedByFirst.push(payload as (typeof receivedByFirst)[number]);
		});
		subscriber1.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await subscriber1.start();
		await waitFor(
			() => subscriber1!.identity() !== null,
			5_000,
			"subscriber1 connected",
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

		// Let the subscription stream propagate.
		await sleep(500);

		const { eventId: firstEventId } = await publisher.event.publish(eventName, {
			orderId: "restart-order-1",
			amount: 11.5,
			currency: "USD",
		});

		expect(firstEventId).toMatch(/^[0-9a-f-]{36}$/);

		// First subscriber must receive the first event.
		await waitFor(
			() => receivedByFirst.length > 0,
			10_000,
			"first event delivered to subscriber1",
		);

		expect(receivedByFirst).toHaveLength(1);
		expect(receivedByFirst[0]!.orderId).toBe("restart-order-1");
		expect(receivedByFirst[0]!.amount).toBeCloseTo(11.5);
		expect(receivedByFirst[0]!.currency).toBe("USD");

		// Stop the first subscriber — simulating a subscriber process restart.
		await subscriber1.stop();
		subscriber1 = undefined;

		// ── Phase 2: second subscriber (same key, new instance) ────────────

		subscriber2 = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber2.event.handle(eventName, async (payload) => {
			receivedBySecond.push(payload as (typeof receivedBySecond)[number]);
		});
		subscriber2.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await subscriber2.start();
		await waitFor(
			() => subscriber2!.identity() !== null,
			5_000,
			"subscriber2 connected",
		);

		// Let the re-subscription propagate.
		await sleep(500);

		const { eventId: secondEventId } = await publisher.event.publish(
			eventName,
			{
				orderId: "restart-order-2",
				amount: 22,
				currency: "EUR",
			},
		);

		expect(secondEventId).toMatch(/^[0-9a-f-]{36}$/);

		// Second subscriber must receive the new event.
		await waitFor(
			() => receivedBySecond.length > 0,
			10_000,
			"second event delivered to subscriber2",
		);

		expect(receivedBySecond).toHaveLength(1);
		expect(receivedBySecond[0]!.orderId).toBe("restart-order-2");
		expect(receivedBySecond[0]!.amount).toBeCloseTo(22);
		expect(receivedBySecond[0]!.currency).toBe("EUR");

		// The first subscriber received exactly 1 event and has been stopped.
		// The second subscriber received exactly 1 event.
		// Total: 2 events received across both subscriber instances.
		expect(receivedByFirst.length + receivedBySecond.length).toBe(2);
	}, 60_000);
});
