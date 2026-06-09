/**
 * events-publisher-restart-inflight.test.ts
 *
 * Scenario: publisher dies mid-publish — outbox row stays in 'inflight'; the
 * next publisher instance (same data dir) resets it to 'pending' on open.
 *
 * This test directly exercises the crash-recovery path in Storage.open():
 *   UPDATE event_outbox SET status='pending' WHERE status='inflight'
 *
 * Test plan:
 *   1. Create a fresh temp dir as the SQLite data dir.
 *   2. Open the same SQLite db directly via Storage.open() and INSERT a row
 *      with status='inflight' — simulating a publisher that was killed after
 *      transitioning the row to 'inflight' but before receiving the ACK from
 *      the runtime.
 *   3. Create a new ServiceBridge instance with SB_DATA_DIR pointing at the
 *      same dir. Storage.open() in start() will reset the 'inflight' row to
 *      'pending'.
 *   4. Verify the recovery via a direct Storage query: 0 'inflight' rows,
 *      1 'pending' row with the pre-inserted id.
 *   5. Bonus: emit a new event via the publisher and verify the subscriber
 *      receives it — proving the drainer runs after the crash-recovery reset.
 *
 * Note on the bonus assertion:
 *   The pre-inserted 'inflight' row has a synthetic payload (raw bytes) that
 *   does not match any registered schema. After the reset to 'pending', the
 *   drainer will attempt to send it to the runtime. The runtime may reject it
 *   (unknown event name, malformed payload) and the row will end up as
 *   'failed' — this is expected and not asserted. We only assert that the
 *   NEWLY emitted event (with a valid schema) is delivered to the subscriber.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { Storage } from "../../src/sqlite/storage";
import {
	eventsEnv,
	FAST_OPTS,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueEventName,
	waitFor,
} from "./_helpers/events";

describe("events e2e: publisher restart — inflight outbox recovery", () => {
	const env = eventsEnv();

	let publisher: ServiceBridge | undefined;
	let subscriber: ServiceBridge | undefined;
	let dataDir: string | undefined;

	beforeEach(() => {
		publisher = undefined;
		subscriber = undefined;
		dataDir = undefined;
	});

	afterEach(async () => {
		await publisher?.stop();
		await subscriber?.stop();
		if (dataDir) {
			fs.rmSync(dataDir, { recursive: true, force: true });
		}
		// Restore env var in case the test set it.
		delete process.env.SB_DATA_DIR;
	});

	test("inflight outbox row is reset to pending on ServiceBridge start (crash recovery)", async () => {
		// ── Step 1: set up temp dir ────────────────────────────────────────
		dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "sb-inflight-recovery-"));

		// ── Step 2: inject a synthetic 'inflight' row ──────────────────────
		// Open Storage directly (same code path as the SDK) to pre-seed the db.
		const preStorage = Storage.open({ dataDir });

		const CRASH_ROW_ID = "crash-sim-evt-001";
		const CRASH_EVENT_NAME = "test.crash.event";
		const syntheticPayload = new Uint8Array([
			0x0a, 0x04, 0x74, 0x65, 0x73, 0x74,
		]);
		const nowMs = Date.now();

		// next_attempt_at_ms = far future, so the drainer does NOT pick up the
		// recovered row after Storage.open() resets it to 'pending'. Otherwise
		// the drainer races with our verifyStorage query: it sends the row to
		// the runtime, runtime ACCEPTs (the name is syntactically valid; fan-out
		// finds no consumers, so it's still accepted as a no-op), and the
		// drainer DELETEs the outbox row before we can assert on it.
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

		// Verify the 'inflight' row was inserted before closing the pre-seed db.
		const inflightBeforeRecover = preStorage
			.prepare(`SELECT COUNT(*) AS c FROM event_outbox WHERE status='inflight'`)
			.get() as { c: number };
		expect(inflightBeforeRecover.c).toBe(1);

		// Close the pre-seed Storage — Storage.open() in the SDK will reopen it.
		preStorage.close();

		// ── Step 3: point ServiceBridge at the same dir and start ──────────
		process.env.SB_DATA_DIR = dataDir;

		// Start subscriber so the bonus assertion (new event delivered) works.
		const eventName = uniqueEventName("inflight-recovery");
		const receivedPayloads: Array<{ orderId: string }> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.handle(eventName, async (payload) => {
			receivedPayloads.push(payload as { orderId: string });
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

		publisher = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		// start() will call Storage.open() with SB_DATA_DIR=dataDir, which runs
		// UPDATE event_outbox SET status='pending' WHERE status='inflight'.
		await publisher.start();
		await waitFor(
			() => publisher!.identity() !== null,
			5_000,
			"publisher connected",
		);

		// ── Step 4: verify recovery via a direct Storage query ─────────────
		// Open a second Storage instance on the same dir (read-only perspective).
		// We use WAL mode, so a concurrent reader sees committed state.
		// The recovery UPDATE ran during publisher.start() → the row is 'pending'.
		const verifyStorage = Storage.open({ dataDir });
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

			// The crash-recovery reset must have run.
			expect(inflightAfter.c).toBe(0);
			expect(pendingAfter.c).toBe(1);
		} finally {
			verifyStorage.close();
		}

		// ── Step 5 (bonus): new event emitted by the recovered publisher ───
		// This confirms the drainer is active and running after crash recovery.
		await sleep(300);

		const { eventId } = await publisher.event.publish(eventName, {
			orderId: "recovery-order-1",
			amount: 99,
			currency: "USD",
		});

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);

		await waitFor(
			() => receivedPayloads.length > 0,
			10_000,
			"new event delivered after crash recovery",
		);

		expect(receivedPayloads[0]!.orderId).toBe("recovery-order-1");
	}, 60_000);
});
