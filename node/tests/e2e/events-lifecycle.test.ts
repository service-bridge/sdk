// events-lifecycle.test.ts — restart / reconnect / pattern-change.
//
// These tests need a subscriber DOWN during publish, a subscriber re-registered
// with a CHANGED pattern, or successive publisher instances. A permanently
// connected warm pool client (one per role) cannot give any of that, so every
// party here is a DEDICATED instance built directly from the per-domain role
// key, registering its handlers/schema BEFORE connect() and stopped in afterEach.

import { afterEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	connect,
	ORDER_EVENT_PROTO,
	type Role,
	sleep,
	uniqueName,
	waitFor,
} from "./_helpers/fixtures";

const V1_SCHEMA = { protoFile: ORDER_EVENT_PROTO, method: "orders_created" };
const UUID_RE = /^[0-9a-f-]{36}$/;

type Order = { orderId: string; amount: number; currency: string };

function keyForRole(role: Role): { url: string; key: string } {
	const url = process.env.SERVICEBRIDGE_URL;
	if (!url) throw new Error("SERVICEBRIDGE_URL not set");
	const domain = process.env.SB_E2E_DOMAIN;
	if (!domain) throw new Error("SB_E2E_DOMAIN not set");
	const idx = { primary: 1, second: 2, third: 3 }[role];
	const envName = `SB_E2E_${domain.toUpperCase().replace(/-/g, "_")}_${idx}`;
	const key = process.env[envName];
	if (!key) throw new Error(`${envName} not set`);
	return { url, key };
}

// Builds a dedicated, UNSTARTED instance under a role key.
function instance(role: Role): ServiceBridge {
	const { url, key } = keyForRole(role);
	return new ServiceBridge(url, key, {
		reconnectIntervalMs: 500,
		reconnectAttempts: 2,
		certRefreshLeadMs: 60_000,
		certRefreshJitterMs: 0,
		advertise: { host: "127.0.0.1", port: 0 },
	});
}

describe("events-lifecycle", () => {
	const clients: ServiceBridge[] = [];

	function track<T extends ServiceBridge>(sb: T): T {
		clients.push(sb);
		return sb;
	}

	afterEach(async () => {
		await Promise.allSettled(clients.map((c) => c.stop()));
		clients.length = 0;
	});

	test("subscriber restart re-registers its pattern and receives subsequent events", async () => {
		const name = uniqueName("events.sub-restart");
		const receivedByFirst: Order[] = [];
		const receivedBySecond: Order[] = [];

		// Phase 1: first subscriber instance.
		const subscriber1 = track(instance("second"));
		subscriber1.event.handle(
			name,
			async (p) => {
				receivedByFirst.push(p as Order);
			},
			{ schema: V1_SCHEMA },
		);
		await connect(subscriber1);

		const publisher = track(instance("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const { eventId: firstId } = await publisher.event.publish(name, {
			orderId: "restart-order-1",
			amount: 11.5,
			currency: "USD",
		});
		expect(firstId).toMatch(UUID_RE);

		await waitFor(
			() => receivedByFirst.length > 0,
			12_000,
			"first event to subscriber1",
		);
		expect(receivedByFirst).toHaveLength(1);
		expect(receivedByFirst[0]!.orderId).toBe("restart-order-1");
		expect(receivedByFirst[0]!.amount).toBeCloseTo(11.5);
		expect(receivedByFirst[0]!.currency).toBe("USD");

		await subscriber1.stop();

		// Phase 2: second subscriber instance, same key, fresh registration.
		const subscriber2 = track(instance("second"));
		subscriber2.event.handle(
			name,
			async (p) => {
				receivedBySecond.push(p as Order);
			},
			{ schema: V1_SCHEMA },
		);
		await connect(subscriber2);

		const { eventId: secondId } = await publisher.event.publish(name, {
			orderId: "restart-order-2",
			amount: 22,
			currency: "EUR",
		});
		expect(secondId).toMatch(UUID_RE);

		await waitFor(
			() => receivedBySecond.length > 0,
			12_000,
			"second event to subscriber2",
		);
		expect(receivedBySecond).toHaveLength(1);
		expect(receivedBySecond[0]!.orderId).toBe("restart-order-2");
		expect(receivedBySecond[0]!.amount).toBeCloseTo(22);
		expect(receivedBySecond[0]!.currency).toBe("EUR");

		// Exactly 2 events received across both subscriber instances.
		expect(receivedByFirst.length + receivedBySecond.length).toBe(2);
	}, 60_000);

	test("backlog accumulated while the consumer is offline drains on reconnect", async () => {
		const BURST_SIZE = 10;
		const name = uniqueName("events.offline-burst");
		const received: Array<{ orderId: string }> = [];

		// Phase 1: connect a subscriber so the subscription row exists, then stop.
		const subscriber1 = track(instance("second"));
		subscriber1.event.handle(
			name,
			async (p) => {
				received.push(p as { orderId: string });
			},
			{ schema: V1_SCHEMA },
		);
		await connect(subscriber1);
		// Let the subscription stream register, then take the subscriber down.
		await sleep(500);
		await subscriber1.stop();
		await sleep(300);

		// Phase 2: publish the whole burst while the subscriber is offline.
		const publisher = track(instance("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const publishedIds: string[] = [];
		for (let i = 0; i < BURST_SIZE; i++) {
			const { eventId } = await publisher.event.publish(name, {
				orderId: `burst-${i}`,
				amount: i,
				currency: "USD",
			});
			publishedIds.push(eventId);
		}
		expect(publishedIds).toHaveLength(BURST_SIZE);
		// Let the drainer flush to event_log and deliveries reach 'pending'.
		await sleep(2_000);

		// Phase 3: fresh subscriber instance (same key) drains the backlog.
		const subscriber2 = track(instance("second"));
		subscriber2.event.handle(
			name,
			async (p) => {
				received.push(p as { orderId: string });
			},
			{ schema: V1_SCHEMA },
		);
		await connect(subscriber2);

		await waitFor(
			() => received.length >= BURST_SIZE,
			15_000,
			`all ${BURST_SIZE} deliveries after reconnect`,
		);

		const ids = received.map((r) => r.orderId).sort();
		for (let i = 0; i < BURST_SIZE; i++) {
			expect(ids).toContain(`burst-${i}`);
		}
	}, 50_000);

	test("changing the subscription pattern orphans pending deliveries away from the new handler", async () => {
		// Concrete name under the "foo.*" namespace; it never matches "baz.*".
		const suffix = uniqueName("bar");
		const fooName = `foo.${suffix}`;
		const bazPattern = `baz.${suffix}`;

		const v1Received: unknown[] = [];
		let v2Invocations = 0;

		// Phase 1: subscriber handles foo.<suffix>.
		const subscriberV1 = track(instance("second"));
		subscriberV1.event.handle(
			fooName,
			async (p) => {
				v1Received.push(p);
			},
			{ schema: V1_SCHEMA },
		);
		await connect(subscriberV1);

		const publisher = track(instance("primary"));
		publisher.event.define(fooName, V1_SCHEMA);
		await connect(publisher);

		// Publish 3 foo events, then stop the subscriber before all are acked.
		for (let i = 1; i <= 3; i++) {
			await publisher.event.publish(fooName, {
				orderId: `orphan-${i}`,
				amount: i,
				currency: "USD",
			});
		}
		await sleep(300);
		await subscriberV1.stop();

		// Phase 2: subscriber reconnects with ONLY a baz.<suffix> handler.
		// Register sends the new subscription set → Replace() moves the orphaned
		// foo.* deliveries to DLQ (last_error='orphaned_pattern'); they never reach
		// the baz handler.
		const subscriberV2 = track(instance("second"));
		subscriberV2.event.handle(
			bazPattern,
			async () => {
				v2Invocations++;
			},
			{ schema: V1_SCHEMA },
		);
		await connect(subscriberV2);

		// Generous window: confirm no orphaned foo delivery reaches the baz handler.
		await sleep(6_000);
		expect(v2Invocations).toBe(0);
	}, 40_000);

	test("events published by successive publisher instances are all delivered", async () => {
		const name = uniqueName("events.restart-pub");
		const received: Order[] = [];

		// Subscriber first so the pattern is registered before any publish.
		const subscriber = track(instance("second"));
		subscriber.event.handle(
			name,
			async (p) => {
				received.push(p as Order);
			},
			{ schema: V1_SCHEMA },
		);
		await connect(subscriber);

		// Phase 1: first publisher instance.
		const publisher1 = track(instance("primary"));
		publisher1.event.define(name, V1_SCHEMA);
		await connect(publisher1);

		await publisher1.event.publish(name, {
			orderId: "p1-order-1",
			amount: 10,
			currency: "USD",
		});
		await publisher1.event.publish(name, {
			orderId: "p1-order-2",
			amount: 20,
			currency: "EUR",
		});
		await publisher1.event.publish(name, {
			orderId: "p1-order-3",
			amount: 30,
			currency: "GBP",
		});

		await waitFor(() => received.length >= 3, 15_000, "first 3 events");
		await publisher1.stop();

		// Phase 2: a second publisher instance of the same service.
		const publisher2 = track(instance("primary"));
		publisher2.event.define(name, V1_SCHEMA);
		await connect(publisher2);

		await publisher2.event.publish(name, {
			orderId: "p2-order-1",
			amount: 40,
			currency: "JPY",
		});
		await publisher2.event.publish(name, {
			orderId: "p2-order-2",
			amount: 50,
			currency: "CHF",
		});

		await waitFor(() => received.length >= 5, 15_000, "all 5 events delivered");
		expect(received).toHaveLength(5);
		expect(received.map((r) => r.orderId).sort()).toEqual([
			"p1-order-1",
			"p1-order-2",
			"p1-order-3",
			"p2-order-1",
			"p2-order-2",
		]);

		// Payload integrity, one event from each batch.
		const p1 = received.find((r) => r.orderId === "p1-order-2")!;
		expect(p1.amount).toBeCloseTo(20);
		expect(p1.currency).toBe("EUR");
		const p2 = received.find((r) => r.orderId === "p2-order-1")!;
		expect(p2.amount).toBeCloseTo(40);
		expect(p2.currency).toBe("JPY");
	}, 60_000);
});
