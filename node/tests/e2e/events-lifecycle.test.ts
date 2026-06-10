// events-lifecycle.test.ts — restart / reconnect / pattern-change / SB_DATA_DIR.
//
// These tests need a subscriber DOWN during publish, a subscriber re-registered
// with a CHANGED pattern, or a publisher pointed at a private SQLite outbox dir.
// A permanently-connected warm pool client (one per role) cannot give any of
// that, so every party here is a DEDICATED instance built directly from the
// per-domain role key, registering its handlers/schema BEFORE connect() and
// stopped in afterEach.
//
// SB_DATA_DIR isolation: the publisher-restart and inflight-recovery tests share
// one SQLite outbox dir across two sequential instances. The dir is passed via
// the ServiceBridge `dataDir` constructor option — NEVER process.env.SB_DATA_DIR
// (a global env mutation would corrupt the pool's per-(shard,role) outbox and
// race parallel shards).

import { afterEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { Storage } from "../../src/sqlite/storage";
import { ORDER_EVENT_PROTO } from "./_helpers/events";
import {
	connect,
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

// Builds a dedicated, UNSTARTED instance under a role key. `dataDir` is explicit
// so restart tests can share one outbox dir across sequential instances.
function instance(role: Role, dataDir: string): ServiceBridge {
	const { url, key } = keyForRole(role);
	return new ServiceBridge(url, key, {
		reconnectIntervalMs: 500,
		reconnectAttempts: 2,
		certRefreshLeadMs: 60 * 60 * 1000,
		advertise: { host: "127.0.0.1", port: 0 },
		dataDir,
	});
}

describe("events-lifecycle", () => {
	const clients: ServiceBridge[] = [];
	const tmpDirs: string[] = [];

	function track<T extends ServiceBridge>(sb: T): T {
		clients.push(sb);
		return sb;
	}

	function tmpDir(prefix: string): string {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
		tmpDirs.push(dir);
		return dir;
	}

	function privateDataDir(tag: string): string {
		return `./.servicebridge-e2e/events-lifecycle-${tag}-${Date.now()}`;
	}

	afterEach(async () => {
		await Promise.allSettled(clients.map((c) => c.stop()));
		clients.length = 0;
		for (const dir of tmpDirs) {
			fs.rmSync(dir, { recursive: true, force: true });
		}
		tmpDirs.length = 0;
	});

	test("subscriber restart re-registers its pattern and receives subsequent events", async () => {
		const name = uniqueName("events.sub-restart");
		const receivedByFirst: Order[] = [];
		const receivedBySecond: Order[] = [];

		// Phase 1: first subscriber instance.
		const subscriber1 = track(instance("second", privateDataDir("sub1")));
		subscriber1.event.define(name, V1_SCHEMA);
		subscriber1.event.handle(name, async (p) => {
			receivedByFirst.push(p as Order);
		});
		await connect(subscriber1);

		const publisher = track(instance("primary", privateDataDir("pub")));
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
		const subscriber2 = track(instance("second", privateDataDir("sub2")));
		subscriber2.event.define(name, V1_SCHEMA);
		subscriber2.event.handle(name, async (p) => {
			receivedBySecond.push(p as Order);
		});
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
		const subscriber1 = track(instance("second", privateDataDir("off-sub1")));
		subscriber1.event.define(name, V1_SCHEMA);
		subscriber1.event.handle(name, async (p) => {
			received.push(p as { orderId: string });
		});
		await connect(subscriber1);
		// Let the subscription stream register, then take the subscriber down.
		await sleep(500);
		await subscriber1.stop();
		await sleep(300);

		// Phase 2: publish the whole burst while the subscriber is offline.
		const publisher = track(instance("primary", privateDataDir("off-pub")));
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
		const subscriber2 = track(instance("second", privateDataDir("off-sub2")));
		subscriber2.event.define(name, V1_SCHEMA);
		subscriber2.event.handle(name, async (p) => {
			received.push(p as { orderId: string });
		});
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
		const subscriberV1 = track(instance("second", privateDataDir("pc-sub1")));
		subscriberV1.event.define(fooName, V1_SCHEMA);
		subscriberV1.event.handle(fooName, async (p) => {
			v1Received.push(p);
		});
		await connect(subscriberV1);

		const publisher = track(instance("primary", privateDataDir("pc-pub")));
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
		const subscriberV2 = track(instance("second", privateDataDir("pc-sub2")));
		subscriberV2.event.define(bazPattern, V1_SCHEMA);
		subscriberV2.event.handle(bazPattern, async () => {
			v2Invocations++;
		});
		await connect(subscriberV2);

		// Generous window: confirm no orphaned foo delivery reaches the baz handler.
		await sleep(6_000);
		expect(v2Invocations).toBe(0);
	}, 40_000);

	test("publisher restart across a shared SB_DATA_DIR delivers all events from both instances", async () => {
		const name = uniqueName("events.restart-pub");
		const received: Order[] = [];

		// Subscriber first so the pattern is registered before any publish.
		const subscriber = track(instance("second", privateDataDir("rp-sub")));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			received.push(p as Order);
		});
		await connect(subscriber);

		// Both publisher instances share one private outbox dir via the dataDir
		// option (never the env var).
		const sharedDir = tmpDir("sb-restart-pub-");

		// Phase 1: first publisher instance.
		const publisher1 = track(instance("primary", sharedDir));
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

		// Phase 2: second publisher instance on the SAME outbox dir.
		const publisher2 = track(instance("primary", sharedDir));
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

	test("inflight outbox row is reset to pending on start (crash recovery)", async () => {
		const sharedDir = tmpDir("sb-inflight-recovery-");

		// Pre-seed an 'inflight' outbox row via direct Storage.open() — simulating
		// a publisher killed after the row went inflight but before the ACK.
		const preStorage = Storage.open({ dataDir: sharedDir });
		const CRASH_ROW_ID = "crash-sim-evt-001";
		const CRASH_EVENT_NAME = "test.crash.event";
		const syntheticPayload = new Uint8Array([
			0x0a, 0x04, 0x74, 0x65, 0x73, 0x74,
		]);
		const nowMs = Date.now();
		// Far-future next attempt so the drainer does not pick it up and DELETE it
		// before our verify query runs.
		const farFutureMs = nowMs + 24 * 60 * 60 * 1000;
		preStorage
			.prepare(
				`INSERT INTO event_outbox
						(id, name, payload, contract_hash, occurred_at_ms, enqueued_at_ms,
						 next_attempt_at_ms, status)
						VALUES (?, ?, ?, ?, ?, ?, ?, 'inflight')`,
			)
			.run(
				CRASH_ROW_ID,
				CRASH_EVENT_NAME,
				syntheticPayload,
				"deadbeefhash",
				nowMs,
				nowMs,
				farFutureMs,
			);
		const inflightBefore = preStorage
			.prepare(`SELECT COUNT(*) AS c FROM event_outbox WHERE status='inflight'`)
			.get() as { c: number };
		expect(inflightBefore.c).toBe(1);
		preStorage.close();

		// Subscriber for the bonus delivery assertion.
		const name = uniqueName("events.inflight-recovery");
		const receivedPayloads: Array<{ orderId: string }> = [];
		const subscriber = track(instance("second", privateDataDir("ir-sub")));
		subscriber.event.define(name, V1_SCHEMA);
		subscriber.event.handle(name, async (p) => {
			receivedPayloads.push(p as { orderId: string });
		});
		await connect(subscriber);

		// Publisher opens the SAME outbox dir → Storage.open() runs
		// UPDATE event_outbox SET status='pending' WHERE status='inflight'.
		const publisher = track(instance("primary", sharedDir));
		publisher.event.define(name, V1_SCHEMA);
		await connect(publisher);

		// Verify recovery via a direct Storage query (WAL → committed state visible).
		const verifyStorage = Storage.open({ dataDir: sharedDir });
		try {
			const inflightAfter = verifyStorage
				.prepare(
					`SELECT COUNT(*) AS c FROM event_outbox WHERE status='inflight'`,
				)
				.get() as { c: number };
			const pendingAfter = verifyStorage
				.prepare(
					`SELECT COUNT(*) AS c FROM event_outbox WHERE status='pending' AND id=?`,
				)
				.get(CRASH_ROW_ID) as { c: number };
			expect(inflightAfter.c).toBe(0);
			expect(pendingAfter.c).toBe(1);
		} finally {
			verifyStorage.close();
		}

		// Bonus: a fresh valid event still delivers after crash recovery.
		const { eventId } = await publisher.event.publish(name, {
			orderId: "recovery-order-1",
			amount: 99,
			currency: "USD",
		});
		expect(eventId).toMatch(UUID_RE);

		await waitFor(
			() => receivedPayloads.length > 0,
			12_000,
			"new event delivered after crash recovery",
		);
		expect(receivedPayloads[0]!.orderId).toBe("recovery-order-1");
	}, 60_000);
});
