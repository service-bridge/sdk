/**
 * events-visibility-timeout.test.ts — handler hangs → runtime reclaims delivery → redelivery.
 *
 * Visibility timeout behaviour (runtime):
 *   - ClaimDue sets visibility_expires_at = now() + VisibilityTimeout (default 30 s).
 *   - ReclaimExpiredVisibility (paced by DefaultReclaimTick = 5 s) moves
 *     in_flight rows with visibility_expires_at < now() back to pending.
 *   - The delivery is then picked up again by ClaimDue on the next dispatcher tick.
 *
 * Test scenario:
 *   - Handler on first delivery: hangs for longer than VisibilityTimeout (30 s).
 *     After the visibility window expires the runtime reclaims the delivery and
 *     re-queues it. The STILL-RUNNING handler eventually calls sendAck on the
 *     old deliveryId, but repo.Ack requires visibility_expires_at > now() — so
 *     the late Ack is silently ignored (acked=false, warning logged).
 *   - Handler on second delivery: succeeds immediately.
 *
 * Expected outcome: invocationCount === 2, received.length === 1.
 *
 * Timing budget:
 *   - Default VisibilityTimeout: 30 s.
 *   - ReclaimTick: 5 s (worst case reclaim fires 35 s after delivery).
 *   - Dispatcher tick: 200 ms.
 *   - Handler hang duration: 35 s (to ensure visibility has definitely expired).
 *   - Total test budget: ~80 s → test timeout 90_000 ms.
 *
 * Note: this test is intentionally slow because the default VisibilityTimeout
 * is 30 s and cannot be shortened from the SDK side. If the runtime is
 * started with EVENTS_VISIBILITY_TIMEOUT=5s the test completes in ~15 s.
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

describe("events e2e: visibility timeout reclaim", () => {
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

	test("handler hangs past visibility timeout → runtime reclaims → redelivered → succeeds (invocationCount=2, received=1)", async () => {
		const eventName = uniqueEventName("vis-timeout");

		let invocationCount = 0;
		const received: Array<{ orderId: string }> = [];

		// Runtime default VisibilityTimeout is 30 s.
		// We hang for 35 s to ensure expiry even if the runtime is slightly slow.
		// ReclaimTick is 5 s, so reclaim fires within 5 s of expiry.
		// Total hang: 35 s; reclaim: up to 35 s + 5 s = 40 s; second delivery
		// arrives shortly after. 70 s budget with 10 s slack.
		const HANG_MS = 35_000;

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			invocationCount++;
			if (invocationCount === 1) {
				// Hang longer than VisibilityTimeout. The runtime will reclaim
				// the delivery and re-queue it. When this handler finally returns
				// (after HANG_MS), it sends a late Ack which the runtime ignores
				// (visibility_expires_at has passed).
				await sleep(HANG_MS);
				// Do NOT push to received — this is the "lost" first delivery.
				return;
			}
			// Second delivery succeeds.
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

		await publisher.event.publish(eventName, {
			orderId: "vis-hang",
			amount: 3,
			currency: "USD",
		});

		// Wait for the second delivery (after reclaim).
		// Budget: hang(35s) + reclaim-tick(≤5s) + dispatcher(≤1s) + delivery = ~42s.
		// Use 70 s to be safe on slow CI.
		await waitFor(
			() => received.length > 0,
			70_000,
			"second delivery after reclaim",
		);

		// Allow a few extra seconds to confirm no phantom third delivery.
		await sleep(2_000);

		expect(invocationCount).toBe(2);
		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("vis-hang");
	}, 90_000);
});
