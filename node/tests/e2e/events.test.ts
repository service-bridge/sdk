// events.test.ts — Durable Events: publish/subscribe, routing, ordering,
// fire-and-forget, redelivery, DLQ, idempotency, schema versioning, persistence.
//
// Registration model: a subscriber declares event.handle + event.define BEFORE
// connect(); a publisher declares event.define BEFORE connect(). Both parties
// are therefore DEDICATED clients (register-before-connect), stopped in
// afterEach. Tests that need a SECOND distinct instance under one identity
// (idempotency cross-instance, schema-versioning publisherB) build an extra
// dedicated client under the same role key.
//
// Isolation: every event name is uniqueName()-d per test, every partition /
// idempotency key is uniqueId()-d, so concurrent tests never collide on
// event_log / event_deliveries / idempotency namespace. No TRUNCATE, no rules.
//
// Lifecycle/restart/SB_DATA_DIR tests live in events-lifecycle.test.ts.

import { afterEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { InvalidEventNameError } from "../../src/events/errors";
import type {
	DlqEntry,
	ListDlqRequest,
} from "../../src/pb/servicebridge/v1/events";
import { computeContractHash } from "../../src/serde/contract-hash";
import { buildSchemaPair } from "../../src/serde/serializer";
import { ORDER_EVENT_PROTO } from "./_helpers/events";
import {
	connect,
	dedicated,
	type Role,
	sleep,
	uniqueId,
	uniqueName,
	waitFor,
} from "./_helpers/fixtures";

const V1_SCHEMA = { protoFile: ORDER_EVENT_PROTO, method: "orders_created" };
const V2_SCHEMA = { protoFile: ORDER_EVENT_PROTO, method: "orders_created_v2" };
const UUID_RE = /^[0-9a-f-]{36}$/;

type Order = { orderId: string; amount: number; currency: string };

// Reads the runtime URL + the per-domain key for a role straight from env, the
// same way pool.ts does, so a test can build an extra dedicated instance under a
// role's identity (e.g. a second publisher sharing the primary key).
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

// Builds an extra dedicated instance under a role key, with a private outbox dir
// so it never shares SQLite with another instance.
function extraInstance(role: Role, tag: string): ServiceBridge {
	const { url, key } = keyForRole(role);
	return new ServiceBridge(url, key, {
		reconnectIntervalMs: 500,
		reconnectAttempts: 3,
		certRefreshLeadMs: 60 * 60 * 1000,
		advertise: { host: "127.0.0.1", port: 0 },
		dataDir: `./.servicebridge-e2e/events-${tag}-${Date.now()}`,
	});
}

describe("events", () => {
	const clients: ServiceBridge[] = [];

	function track<T extends ServiceBridge>(sb: T): T {
		clients.push(sb);
		return sb;
	}

	afterEach(async () => {
		await Promise.allSettled(clients.map((c) => c.stop()));
		clients.length = 0;
	});

	test("durable publish→subscribe round-trips a protobuf payload field-exact", async () => {
		const name = uniqueName("events.happy");
		const received: Order[] = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			received.push(p as Order);
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const { eventId } = await publisher.event.publish(name, {
			orderId: "ord-schema-1",
			amount: 42.75,
			currency: "EUR",
		});
		expect(eventId).toMatch(UUID_RE);

		await waitFor(() => received.length > 0, 12_000, "delivery to subscriber");

		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("ord-schema-1");
		expect(received[0]!.amount).toBeCloseTo(42.75);
		expect(received[0]!.currency).toBe("EUR");
	}, 30_000);

	test("durable publish() returns synchronously after local outbox insert (<500ms)", async () => {
		const name = uniqueName("events.outbox");

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const t0 = Date.now();
		const { eventId } = await publisher.event.publish(name, {
			orderId: "outbox-1",
			amount: 1,
			currency: "EUR",
		});
		const elapsed = Date.now() - t0;

		expect(eventId).toMatch(UUID_RE);
		// Outbox INSERT + COMMIT is local SQLite, not a blocking network call.
		expect(elapsed).toBeLessThan(500);
	}, 15_000);

	test("publish with wrong field type throws before any outbox insert", async () => {
		const name = uniqueName("events.invalid-type");

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		// orderId must be a string; a number triggers protobufjs type.verify()
		// synchronously inside publish(), before any outbox INSERT or network.
		await expect(
			publisher.event.publish(name, {
				orderId: 42,
				amount: 10,
				currency: "USD",
			}),
		).rejects.toThrow(/string|verify|invalid|orderId/i);
	}, 15_000);

	test("invalid event names are rejected synchronously and one valid edge name is accepted", async () => {
		const publisher = track(dedicated("primary"));
		// Register one valid name so the publisher has schema context.
		const validName = uniqueName("events.valid");
		publisher.event.define(validName, V1_SCHEMA);
		// The valid edge name exercises the full charset the runtime allows.
		const validEdgeName = "order-created.v1_alpha";
		publisher.event.define(validEdgeName, V1_SCHEMA);
		await connect(publisher);

		const payload = { orderId: "x", amount: 1, currency: "USD" };
		const invalid = [
			"Order.Created", // uppercase
			"order.created!", // trailing punctuation
			"name with spaces", // spaces
			"", // empty
			".leading.dot", // leading dot
			"trailing.dot.", // trailing dot
		];
		for (const bad of invalid) {
			await expect(publisher.event.publish(bad, payload)).rejects.toThrow(
				InvalidEventNameError,
			);
		}

		const { eventId } = await publisher.event.publish(validEdgeName, {
			orderId: "edge-1",
			amount: 1,
			currency: "USD",
		});
		expect(eventId).toMatch(UUID_RE);
	}, 15_000);

	test("exact-name routing delivers only the declared event and never an unsubscribed name", async () => {
		const prefix = uniqueName("events.routing");
		const nameA = `${prefix}.shipped`;
		const nameB = `${prefix}.cancelled`;
		const nameUnsub = `${prefix}.archived`;

		const receivedA: Order[] = [];
		const receivedB: Order[] = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(nameA, V1_SCHEMA);
		subscriber.event.define(nameB, V1_SCHEMA);
		subscriber.event.handle(nameA, async (p) => {
			receivedA.push(p as Order);
		});
		subscriber.event.handle(nameB, async (p) => {
			receivedB.push(p as Order);
		});
		// Note: subscriber NEVER declares a handler for nameUnsub.
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(nameA, V1_SCHEMA);
		publisher.event.define(nameB, V1_SCHEMA);
		publisher.event.define(nameUnsub, V1_SCHEMA);
		await connect(publisher);

		await publisher.event.publish(nameUnsub, {
			orderId: "unsub-1",
			amount: 1,
			currency: "USD",
		});
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
			15_000,
			"both handlers received their events",
		);
		// Settle window: confirm the unsubscribed event never sneaks in.
		await sleep(1_000);

		expect(receivedA).toHaveLength(1);
		expect(receivedA[0]!.orderId).toBe("route-a-1");
		expect(receivedB).toHaveLength(1);
		expect(receivedB[0]!.orderId).toBe("route-b-1");
		// Unsubscribed name delivered to neither handler.
		expect(receivedA.some((r) => r.orderId === "unsub-1")).toBe(false);
		expect(receivedB.some((r) => r.orderId === "unsub-1")).toBe(false);
	}, 35_000);

	test("wildcard prefix.* and an exact handler both match an emitted prefix.<x> event", async () => {
		const prefix = uniqueName("events.wildcard");
		const exactName = `${prefix}.shipped`;
		const wildcard = `${prefix}.*`;

		const receivedExact: Order[] = [];
		const receivedWild: Order[] = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(exactName, V1_SCHEMA);
		subscriber.event.define(wildcard, V1_SCHEMA);
		subscriber.event.handle(exactName, async (p) => {
			receivedExact.push(p as Order);
		});
		subscriber.event.handle(wildcard, async (p) => {
			receivedWild.push(p as Order);
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(exactName, V1_SCHEMA);
		await connect(publisher);

		await publisher.event.publish(exactName, {
			orderId: "fanout-wc-1",
			amount: 10,
			currency: "EUR",
		});

		await waitFor(
			() => receivedExact.length > 0 || receivedWild.length > 0,
			12_000,
			"at least one handler received",
		);

		const all = [...receivedExact, ...receivedWild];
		expect(all.length).toBeGreaterThanOrEqual(1);
		for (const item of all) {
			expect(item.orderId).toBe("fanout-wc-1");
		}
	}, 35_000);

	test("two handlers on the same exact name both fire per delivery and across deliveries", async () => {
		const name = uniqueName("events.multi");
		const h1: Order[] = [];
		const h2: Order[] = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			h1.push(p as Order);
		});
		subscriber.event.handle(name, async (p) => {
			h2.push(p as Order);
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		await publisher.event.publish(name, {
			orderId: "multi-a",
			amount: 10,
			currency: "USD",
		});

		await waitFor(
			() => h1.length >= 1 && h2.length >= 1,
			12_000,
			"both handlers fired once",
		);
		expect(h1).toHaveLength(1);
		expect(h2).toHaveLength(1);
		expect(h1[0]!.orderId).toBe("multi-a");
		expect(h2[0]!.orderId).toBe("multi-a");

		await publisher.event.publish(name, {
			orderId: "multi-b",
			amount: 20,
			currency: "USD",
		});

		await waitFor(
			() => h1.length >= 2 && h2.length >= 2,
			15_000,
			"both handlers fired twice",
		);
		expect(h1).toHaveLength(2);
		expect(h2).toHaveLength(2);
		expect(h1.map((r) => r.orderId).sort()).toEqual(["multi-a", "multi-b"]);
		expect(h2.map((r) => r.orderId).sort()).toEqual(["multi-a", "multi-b"]);
	}, 35_000);

	test("partitionKey enforces per-partition FIFO including interleaved partitions", async () => {
		const name = uniqueName("events.ordering");
		const single: string[] = [];
		const interleaved: Array<{ orderId: string; partition: string }> = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			const o = p as Order;
			single.push(o.orderId);
			if (o.currency === "A" || o.currency === "B") {
				interleaved.push({ orderId: o.orderId, partition: o.currency });
			}
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		// Scenario 1: 5 events on a single partition arrive in publication order.
		const N = 5;
		const singleKey = uniqueId("order");
		const expectedSingle: string[] = [];
		for (let i = 0; i < N; i++) {
			const orderId = `ord-${i}`;
			expectedSingle.push(orderId);
			await publisher.event.publish(
				name,
				{ orderId, amount: i, currency: "USD" },
				{ partitionKey: singleKey },
			);
		}
		await waitFor(() => single.length >= N, 20_000, `${N} single-partition`);
		expect(single.slice(0, N)).toEqual(expectedSingle);

		// Scenario 2: two interleaved partitions each keep their own FIFO order.
		const keyA = uniqueId("part-a");
		const keyB = uniqueId("part-b");
		const partA: string[] = [];
		const partB: string[] = [];
		for (let i = 0; i < N; i++) {
			const oidA = `a-${i}`;
			const oidB = `b-${i}`;
			partA.push(oidA);
			partB.push(oidB);
			await publisher.event.publish(
				name,
				{ orderId: oidA, amount: i, currency: "A" },
				{ partitionKey: keyA },
			);
			await publisher.event.publish(
				name,
				{ orderId: oidB, amount: i, currency: "B" },
				{ partitionKey: keyB },
			);
		}
		await waitFor(
			() => interleaved.length >= N * 2,
			30_000,
			`${N * 2} interleaved deliveries`,
		);
		const recvA = interleaved
			.filter((r) => r.partition === "A")
			.map((r) => r.orderId);
		const recvB = interleaved
			.filter((r) => r.partition === "B")
			.map((r) => r.orderId);
		expect(recvA).toEqual(partA);
		expect(recvB).toEqual(partB);
	}, 60_000);

	test("fireAndForget event bypasses the outbox and is still delivered", async () => {
		const name = uniqueName("events.faf");
		const received: Order[] = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			received.push(p as Order);
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const { eventId } = await publisher.event.publish(
			name,
			{ orderId: "faf-deliver-1", amount: 99.99, currency: "USD" },
			{ fireAndForget: true },
		);
		expect(eventId).toMatch(UUID_RE);

		await waitFor(() => received.length > 0, 12_000, "FAF delivery");
		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("faf-deliver-1");
		expect(received[0]!.amount).toBeCloseTo(99.99);
		expect(received[0]!.currency).toBe("USD");
	}, 30_000);

	test("FAF burst and durable+FAF coexistence both deliver every event", async () => {
		const name = uniqueName("events.faf-coexist");
		const received: Array<{ orderId: string }> = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			received.push(p as { orderId: string });
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		// 5 back-to-back FAF events, all with unique ids.
		const fafIds: string[] = [];
		for (let i = 0; i < 5; i++) {
			const { eventId } = await publisher.event.publish(
				name,
				{ orderId: `faf-burst-${i}`, amount: i * 10, currency: "USD" },
				{ fireAndForget: true },
			);
			fafIds.push(eventId);
		}
		expect(new Set(fafIds).size).toBe(5);
		for (const id of fafIds) expect(id).toMatch(UUID_RE);

		// A durable + a FAF publish with distinct ids both arrive.
		const { eventId: durableId } = await publisher.event.publish(name, {
			orderId: "coexist-durable",
			amount: 1,
			currency: "USD",
		});
		const { eventId: fafId } = await publisher.event.publish(
			name,
			{ orderId: "coexist-faf", amount: 2, currency: "USD" },
			{ fireAndForget: true },
		);
		expect(durableId).toMatch(UUID_RE);
		expect(fafId).toMatch(UUID_RE);
		expect(durableId).not.toBe(fafId);

		await waitFor(() => received.length >= 7, 20_000, "all 7 events delivered");
		const ids = received.map((r) => r.orderId).sort();
		expect(ids).toContain("coexist-durable");
		expect(ids).toContain("coexist-faf");
		for (let i = 0; i < 5; i++) {
			expect(ids).toContain(`faf-burst-${i}`);
		}
	}, 40_000);

	test("nack triggers redelivery and the consumer idempotency guard processes exactly once", async () => {
		const name = uniqueName("events.dedup");
		let invocations = 0;
		const processedKeys = new Set<string>();
		let processedCount = 0;

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			invocations++;
			const key = (p as Order).orderId;
			if (invocations === 1) {
				// Transient failure → Nack → runtime reschedules with backoff.
				throw new Error("transient failure — please retry");
			}
			// Idempotency guard: business effect fires at most once per key.
			if (processedKeys.has(key)) return;
			processedKeys.add(key);
			processedCount++;
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const idemKey = uniqueId("idem");
		const { eventId } = await publisher.event.publish(name, {
			orderId: idemKey,
			amount: 5,
			currency: "USD",
		});
		expect(eventId).toMatch(UUID_RE);

		// First delivery nacks, backoff ~1s, second delivery succeeds.
		await waitFor(() => invocations >= 2, 15_000, "second delivery (retry)");
		// Settle so any stray third delivery would be observed.
		await sleep(2_000);

		// at-least-once: handler ran at least twice.
		expect(invocations).toBeGreaterThanOrEqual(2);
		// idempotency: business counter incremented exactly once.
		expect(processedCount).toBe(1);
	}, 30_000);

	test("handler hanging past the visibility timeout is reclaimed and redelivered", async () => {
		const name = uniqueName("events.vis-timeout");
		let invocationCount = 0;
		const received: Array<{ orderId: string }> = [];
		// Runtime default VisibilityTimeout is 30s; hang 35s to guarantee expiry.
		const HANG_MS = 35_000;

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			invocationCount++;
			if (invocationCount === 1) {
				// Hang past the visibility window → runtime reclaims and re-queues.
				// The late Ack on the old deliveryId is silently ignored.
				await sleep(HANG_MS);
				return;
			}
			received.push(p as { orderId: string });
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		await publisher.event.publish(name, {
			orderId: "vis-hang",
			amount: 3,
			currency: "USD",
		});

		await waitFor(
			() => received.length > 0,
			70_000,
			"second delivery after reclaim",
		);
		// Confirm no phantom third delivery.
		await sleep(2_000);

		expect(invocationCount).toBe(2);
		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("vis-hang");
	}, 90_000);

	test("always-failing handler exhausts max attempts and lands in DLQ", async () => {
		const name = uniqueName("events.dlq");
		let invocationCount = 0;

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async () => {
			invocationCount++;
			throw new Error(`dlq-test: always-failing (attempt ${invocationCount})`);
		});
		await connect(subscriber);

		// Publisher shares the events gRPC channel used for listDlq below.
		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const { eventId } = await publisher.event.publish(name, {
			orderId: "will-dlq",
			amount: 1,
			currency: "USD",
		});
		expect(eventId).toMatch(UUID_RE);

		// listDlq is @internal on ServiceBridge — there is no public DLQ API.
		function listDlq(
			sb: ServiceBridge,
			req: ListDlqRequest,
		): Promise<{ entries: DlqEntry[]; nextCursor: string }> {
			// biome-ignore lint/suspicious/noExplicitAny: intentional @internal access
			const client = (sb as any)._eventsClient;
			if (!client) {
				throw new Error("eventsClient not ready — call start() first");
			}
			return new Promise((resolve, reject) => {
				client.listDlq(
					req,
					(
						err: Error | null,
						res: { entries: DlqEntry[]; nextCursor: string } | undefined,
					) => {
						if (err) reject(err);
						else resolve(res ?? { entries: [], nextCursor: "" });
					},
				);
			});
		}

		// Poll the DLQ until our uniquely-named event appears (worst-case backoff
		// to MaxAttempts=5 is ~156s; 4-min budget covers jitter + CI overhead).
		const DLQ_MAX_WAIT_MS = 240_000;
		let dlqEntry: DlqEntry | undefined;
		const deadline = Date.now() + DLQ_MAX_WAIT_MS;
		while (Date.now() < deadline) {
			await sleep(5_000);
			try {
				const res = await listDlq(publisher, { limit: 50, cursor: "" });
				dlqEntry = res.entries.find((e) => e.eventName === name);
			} catch {
				continue;
			}
			if (dlqEntry) break;
		}

		expect(dlqEntry).toBeDefined();
		expect(dlqEntry!.eventName).toBe(name);
		expect(dlqEntry!.totalAttempts).toBeGreaterThanOrEqual(1);
		expect(invocationCount).toBe(dlqEntry!.totalAttempts);
		expect(dlqEntry!.replayCount).toBe(0);
	}, 300_000);

	test("duplicate publish with same idempotencyKey delivers exactly once, including across instances sharing the identity", async () => {
		const name = uniqueName("events.idem");
		const received: Array<{ orderId: string }> = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			received.push(p as { orderId: string });
		});
		await connect(subscriber);

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const idemKey = uniqueId("order-dedup");

		// Same instance, same key twice → runtime REJECTED_DUPLICATE on the second.
		const { eventId: id1 } = await publisher.event.publish(
			name,
			{ orderId: "order-idem-1", amount: 10, currency: "USD" },
			{ idempotencyKey: idemKey },
		);
		const { eventId: id2 } = await publisher.event.publish(
			name,
			{ orderId: "order-idem-1", amount: 10, currency: "USD" },
			{ idempotencyKey: idemKey },
		);
		expect(id1).toMatch(UUID_RE);
		expect(id2).toMatch(UUID_RE);

		// Second distinct SDK instance under the SAME identity (primary key) →
		// dedup is scoped by publisherSvcId, so it still applies across instances.
		const publisher2 = track(extraInstance("primary", "idem-pub2"));
		publisher2.event.define(name, V1_SCHEMA);
		await connect(publisher2);
		await publisher2.event.publish(
			name,
			{ orderId: "order-idem-1", amount: 10, currency: "USD" },
			{ idempotencyKey: idemKey },
		);

		await waitFor(() => received.length > 0, 15_000, "first delivery");
		// Settle: confirm no additional deliveries from the duplicates.
		await sleep(2_000);

		expect(received).toHaveLength(1);
		expect(received[0]!.orderId).toBe("order-idem-1");
	}, 35_000);

	test("two schemas for one event name keep history and both decode on a v1 subscriber", async () => {
		const name = uniqueName("events.schema-ver");

		// Client-side: the two schemas must hash differently (keep-history key).
		const pairV1 = await buildSchemaPair(V1_SCHEMA);
		const pairV2 = await buildSchemaPair(V2_SCHEMA);
		expect(computeContractHash(pairV1)).not.toBe(computeContractHash(pairV2));

		const received: Order[] = [];
		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			received.push(p as Order);
		});
		await connect(subscriber);

		// PublisherA: v1 schema. start() must not be rejected by the registry.
		const publisherA = track(dedicated("primary"));
		publisherA.event.define(name, V1_SCHEMA);
		await connect(publisherA);

		// PublisherB: v2 schema, a SECOND instance under the same primary key →
		// distinct (service_id, instance_id, method_name, contract_hash) row.
		const publisherB = track(extraInstance("primary", "schema-pubB"));
		publisherB.event.define(name, V2_SCHEMA);
		await connect(publisherB);

		const { eventId: idA } = await publisherA.event.publish(name, {
			orderId: "v1-event",
			amount: 10,
			currency: "USD",
		});
		const { eventId: idB } = await publisherB.event.publish(name, {
			orderId: "v2-event",
			amount: 20,
			currency: "EUR",
			region: "eu-west",
		});
		expect(idA).toMatch(UUID_RE);
		expect(idB).toMatch(UUID_RE);

		await waitFor(() => received.length >= 2, 20_000, "2 deliveries");
		const orderIds = received.map((r) => r.orderId).sort();
		expect(orderIds).toContain("v1-event");
		expect(orderIds).toContain("v2-event");

		// Proto3 forward-compat: v2 payload decoded by v1 schema → region dropped.
		const v2Decoded = received.find((r) => r.orderId === "v2-event")!;
		expect(v2Decoded.amount).toBeCloseTo(20);
		expect(v2Decoded.currency).toBe("EUR");
	}, 45_000);

	test("runtime durably records event_log and delivery transitions to success", async () => {
		const { withDb } = await import("./_helpers/policy-db");
		const name = uniqueName("events.ingested");
		const received: Array<{ orderId: string }> = [];

		const subscriber = track(dedicated("second"));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			received.push(p as { orderId: string });
		});
		await connect(subscriber);
		expect(subscriber.identity()!.serviceId).toBeTruthy();

		const publisher = track(dedicated("primary"));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		const { eventId } = await publisher.event.publish(name, {
			orderId: "ingested-order-1",
			amount: 77,
			currency: "USD",
		});
		expect(eventId).toMatch(UUID_RE);

		// event_log row exists (drainer dispatches asynchronously).
		await waitFor(
			async () => {
				const rows = (await withDb(
					async (sql) =>
						await sql`SELECT COUNT(*)::int AS c FROM event_log WHERE id = ${eventId}`,
				)) as Array<{ c: number }>;
				return (rows[0]?.c ?? 0) >= 1;
			},
			12_000,
			"event_log row to appear",
		);

		// event_deliveries row exists.
		await waitFor(
			async () => {
				const rows = (await withDb(
					async (sql) =>
						await sql`SELECT COUNT(*)::int AS c FROM event_deliveries WHERE event_id = ${eventId}`,
				)) as Array<{ c: number }>;
				return (rows[0]?.c ?? 0) >= 1;
			},
			12_000,
			"event_deliveries row to appear",
		);

		await waitFor(() => received.length > 0, 12_000, "subscriber handler");
		expect(received[0]!.orderId).toBe("ingested-order-1");

		// Delivery transitions to 'success' after the handler acks.
		await waitFor(
			async () => {
				const rows = (await withDb(
					async (sql) =>
						await sql`SELECT status FROM event_deliveries WHERE event_id = ${eventId} LIMIT 1`,
				)) as Array<{ status: string }>;
				return rows[0]?.status === "success";
			},
			15_000,
			"delivery status='success'",
		);
	}, 60_000);
});
