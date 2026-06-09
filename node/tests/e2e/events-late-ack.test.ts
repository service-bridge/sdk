/**
 * events-late-ack.test.ts — handler that throws on first attempt succeeds on second.
 *
 * Simpler scenario for the "late ack" family: verifies that the SDK's Nack
 * path → runtime backoff → redelivery cycle works end to end.
 *
 * Flow:
 *   1. Handler throws on attempt 1 → SDK sends Nack → runtime records error,
 *      schedules retry after backoff(1) = 1 s (±25%).
 *   2. Runtime re-delivers → handler succeeds on attempt 2 → SDK sends Ack.
 *   3. invocationCount === 2, received.length === 1 (only successful delivery counted).
 *
 * The "late ack is a no-op" property (if an Ack arrives after visibility
 * expires, repo.Ack returns acked=false and logs a warning) is covered
 * implicitly: the subscriber stream always sends an Ack or Nack immediately
 * after the handler returns — the only case where an Ack arrives "late" is
 * when the visibility window expires while the handler is still running,
 * which is the events-visibility-timeout test scenario.
 *
 * Timing: first-attempt backoff is ~1 s + ±25%, dispatcher tick is 200 ms,
 * so the second delivery arrives roughly 1–2 s after the Nack. 15 s budget
 * is generous.
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

describe("events e2e: nack → retry → success", () => {
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

	test("handler throws on first attempt → nack → redelivered → succeeds (invocationCount=2, received=1)", async () => {
		const eventName = uniqueEventName("late-ack");

		let invocationCount = 0;
		// Only record successful completions (handler did not throw).
		const received: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			invocationCount++;
			if (invocationCount === 1) {
				// Throw on first delivery → SDK sends Nack.
				throw new Error("simulated first-attempt failure");
			}
			// Succeed on second delivery.
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

		const { eventId } = await publisher.event.publish(eventName, {
			orderId: "retry-me",
			amount: 7,
			currency: "USD",
		});

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);

		// Wait for the successful second delivery.
		// Backoff after attempt-1 Nack is ~1 s; dispatcher tick is 200 ms;
		// total latency is typically ~2 s; 15 s budget covers slow CI.
		await waitFor(() => received.length > 0, 15_000, "second delivery");

		// Give extra time to confirm no phantom third delivery.
		await sleep(2_000);

		expect(invocationCount).toBe(2);
		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("retry-me");
	}, 30_000);
});
