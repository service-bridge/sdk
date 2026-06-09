/**
 * events-ordering.test.ts — partitionKey enforces FIFO delivery per consumer.
 *
 * The runtime's ClaimDue query (repo.go) uses a NOT EXISTS guard:
 *   AND ( partition_key = ''
 *         OR NOT EXISTS (
 *           SELECT 1 FROM event_deliveries d3
 *           WHERE d3.consumer_service = d2.consumer_service
 *             AND d3.partition_key   = d2.partition_key
 *             AND d3.partition_key   <> ''
 *             AND d3.status          = 'in_flight'
 *         )
 *       )
 *
 * This means: for a given (consumer_service, partition_key), only ONE delivery
 * is in_flight at a time. The next delivery for that partition is not claimed
 * until the first is acked/nacked. Combined with ORDER BY next_attempt_at, this
 * gives strict FIFO per partition.
 *
 * Scenario 1: 5 events with partitionKey="order-7" → received in publication order.
 * Scenario 2: 5 events with partitionKey="a" interleaved with 5 "b" →
 *   a's relative order preserved AND b's relative order preserved, even if
 *   they interleave with each other in the combined received list.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	eventsEnv,
	FAST_OPTS,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueEventName,
	uniquePartitionKey,
	waitFor,
} from "./_helpers/events";

describe("events e2e: partitionKey ordering", () => {
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

	test("5 events with same partitionKey arrive in publication order", async () => {
		const eventName = uniqueEventName("order");
		const received: string[] = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			received.push((payload as { orderId: string }).orderId);
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

		// Emit N=5 events with the same partitionKey. All go through the
		// local SQLite outbox (durable path). The drainer sends them in FIFO
		// order (enqueued_at_ms). The runtime's ClaimDue FIFO gate ensures
		// the subscriber sees them in order.
		const N = 5;
		const expectedOrder: string[] = [];
		const partKey = uniquePartitionKey("order");
		for (let i = 0; i < N; i++) {
			const orderId = `ord-${i}`;
			expectedOrder.push(orderId);
			await publisher.event.publish(
				eventName,
				{ orderId, amount: i, currency: "USD" },
				{ partitionKey: partKey },
			);
		}

		await waitFor(() => received.length >= N, 20_000, `${N} deliveries`);

		expect(received).toHaveLength(N);
		expect(received).toEqual(expectedOrder);
	}, 40_000);

	test("interleaved partitions a and b preserve per-partition FIFO order", async () => {
		const eventName = uniqueEventName("order-2part");
		const received: Array<{ orderId: string; partition: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			const p = payload as { orderId: string; currency: string };
			received.push({ orderId: p.orderId, partition: p.currency });
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

		// Emit 5 events for partition "a" and 5 for partition "b", interleaved.
		// We use the `currency` field as a partition tag that the handler echoes back.
		const N = 5;
		const partA: string[] = [];
		const partB: string[] = [];
		const keyA = uniquePartitionKey("part-a");
		const keyB = uniquePartitionKey("part-b");
		for (let i = 0; i < N; i++) {
			const oidA = `a-${i}`;
			const oidB = `b-${i}`;
			partA.push(oidA);
			partB.push(oidB);
			// Interleaved: a-0, b-0, a-1, b-1, …
			await publisher.event.publish(
				eventName,
				{ orderId: oidA, amount: i, currency: "A" },
				{ partitionKey: keyA },
			);
			await publisher.event.publish(
				eventName,
				{ orderId: oidB, amount: i, currency: "B" },
				{ partitionKey: keyB },
			);
		}

		const totalExpected = N * 2;
		await waitFor(
			() => received.length >= totalExpected,
			30_000,
			`${totalExpected} deliveries`,
		);

		expect(received).toHaveLength(totalExpected);

		// Extract per-partition subsequences from the received list.
		const receivedA = received
			.filter((r) => r.partition === "A")
			.map((r) => r.orderId);
		const receivedB = received
			.filter((r) => r.partition === "B")
			.map((r) => r.orderId);

		// Each partition must arrive in the exact publication order.
		expect(receivedA).toEqual(partA);
		expect(receivedB).toEqual(partB);
	}, 60_000);
});
