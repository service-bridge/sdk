/**
 * events-restart-publisher.test.ts
 *
 * Scenario: publisher SDK restart — outbox persists across process boundaries.
 *
 * What this test verifies:
 *   - A publisher can emit events, stop, and a NEW ServiceBridge instance
 *     pointing at the same SB_DATA_DIR can emit additional events.
 *   - The subscriber receives all events from both publisher instances.
 *
 * What this test does NOT verify:
 *   - Mid-flight crash recovery (outbox rows stuck in 'inflight' survive a
 *     SIGKILL). That invariant is tested at the unit level in
 *     sdk/node/src/sqlite/storage.test.ts. At the e2e level we cannot reliably
 *     force a SIGKILL between outbox INSERT and drainer ACK without a
 *     purpose-built synchronisation hook in the drainer.
 *   - The SQLite file is shared across OS processes: in practice this is
 *     guaranteed by SB_DATA_DIR pointing to a real filesystem path, not a
 *     per-process tmpdir. The test uses a unique tmpdir per run — if the three
 *     events were already drained before stop(), the second publisher instance
 *     simply starts from an empty outbox and the test still validates that
 *     sequential publisher instances work correctly.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import {
	eventsEnv,
	FAST_OPTS,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueEventName,
	waitFor,
} from "./_helpers/events";

describe("events e2e: publisher restart (outbox persists)", () => {
	const env = eventsEnv();

	let subscriber: ServiceBridge | undefined;
	let publisher1: ServiceBridge | undefined;
	let publisher2: ServiceBridge | undefined;
	let dataDir: string | undefined;

	beforeEach(() => {
		subscriber = undefined;
		publisher1 = undefined;
		publisher2 = undefined;
		dataDir = undefined;
	});

	afterEach(async () => {
		await publisher1?.stop();
		await publisher2?.stop();
		await subscriber?.stop();
		if (dataDir) {
			fs.rmSync(dataDir, { recursive: true, force: true });
		}
	});

	test("subscriber receives events from two sequential publisher instances (same data dir)", async () => {
		const eventName = uniqueEventName("restart-pub");
		const received: Array<{
			orderId: string;
			amount: number;
			currency: string;
		}> = [];

		// Shared temp dir for the SQLite outbox — both publisher instances will
		// read/write the same sdk.db file.
		dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "sb-restart-pub-"));

		// Start subscriber first so the pattern is registered before any publish.
		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.handle(eventName, async (payload) => {
			received.push(payload as (typeof received)[number]);
		});
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

		// ── Phase 1: first publisher instance ──────────────────────────────

		// Point the storage at our shared dir via env var. Storage.open() reads
		// SB_DATA_DIR, so we set it before creating the first instance.
		process.env.SB_DATA_DIR = dataDir;

		publisher1 = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher1.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await publisher1.start();
		await waitFor(
			() => publisher1!.identity() !== null,
			5_000,
			"publisher1 connected",
		);

		// Allow subscription stream to propagate to the runtime.
		await sleep(500);

		await publisher1.event.publish(eventName, {
			orderId: "p1-order-1",
			amount: 10,
			currency: "USD",
		});
		await publisher1.event.publish(eventName, {
			orderId: "p1-order-2",
			amount: 20,
			currency: "EUR",
		});
		await publisher1.event.publish(eventName, {
			orderId: "p1-order-3",
			amount: 30,
			currency: "GBP",
		});

		// Wait for first batch to arrive, then stop the first publisher.
		await waitFor(() => received.length >= 3, 15_000, "first 3 events");
		await publisher1.stop();
		publisher1 = undefined;

		// ── Phase 2: second publisher instance (same data dir) ─────────────

		// Env var is still set — new ServiceBridge will open the same sdk.db.
		publisher2 = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher2.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await publisher2.start();
		await waitFor(
			() => publisher2!.identity() !== null,
			5_000,
			"publisher2 connected",
		);

		await sleep(300);

		await publisher2.event.publish(eventName, {
			orderId: "p2-order-1",
			amount: 40,
			currency: "JPY",
		});
		await publisher2.event.publish(eventName, {
			orderId: "p2-order-2",
			amount: 50,
			currency: "CHF",
		});

		// Subscriber must eventually see all 5 events (3 + 2).
		await waitFor(() => received.length >= 5, 15_000, "all 5 events delivered");

		expect(received).toHaveLength(5);

		const orderIds = received.map((r) => r.orderId).sort();
		expect(orderIds).toEqual([
			"p1-order-1",
			"p1-order-2",
			"p1-order-3",
			"p2-order-1",
			"p2-order-2",
		]);

		// Verify payload integrity — pick one event from each batch.
		const p1 = received.find((r) => r.orderId === "p1-order-2")!;
		expect(p1.amount).toBeCloseTo(20);
		expect(p1.currency).toBe("EUR");

		const p2 = received.find((r) => r.orderId === "p2-order-1")!;
		expect(p2.amount).toBeCloseTo(40);
		expect(p2.currency).toBe("JPY");
	}, 60_000);
});
