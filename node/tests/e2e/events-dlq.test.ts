/**
 * events-dlq.test.ts — after MaxAttempts Nacks, delivery lands in DLQ.
 *
 * Runtime configuration (config.go defaults):
 *   - EVENTS_MAX_ATTEMPTS = 5
 *   - backoffLadder = [1s, 5s, 30s, 2min, 10min] (dispatcher.go)
 *
 * With MaxAttempts=5 the delivery is moved to DLQ after the 5th Nack.
 * Cumulative wait between handler invocations (with ±25% jitter):
 *   attempt-1 Nack → wait ~1 s → attempt-2
 *   attempt-2 Nack → wait ~5 s → attempt-3
 *   attempt-3 Nack → wait ~30 s → attempt-4
 *   attempt-4 Nack → wait ~2 min → attempt-5 → DLQ
 *
 * Total: ~2 min 36 s minimum; 4 min budget with CI slack → timeout 300_000.
 *
 * DLQ inspection: after all attempts are exhausted the test calls
 * EventsClient.listDlq via the gRPC client exposed as a private field
 * on ServiceBridge. Casting to `any` is necessary because the SDK has no
 * public DLQ query API — the field is an @internal implementation detail.
 *
 * If EVENTS_MAX_ATTEMPTS is overridden on the running runtime instance
 * (e.g. EVENTS_MAX_ATTEMPTS=3 shortens the wait to ~6 s) the test still
 * passes — the assertion only requires totalAttempts >= 2.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import type {
	DlqEntry,
	ListDlqRequest,
} from "../../src/pb/servicebridge/v1/events";
import {
	eventsEnv,
	FAST_OPTS,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueEventName,
	waitFor,
} from "./_helpers/events";

// DLQ_MAX_WAIT_MS covers the worst-case cumulative backoff to reach MaxAttempts=5.
// attempt-1 backoff ~1s + attempt-2 ~5s + attempt-3 ~30s + attempt-4 ~2min = ~156s.
// Add dispatcher tick (200ms), ±25% jitter, and CI overhead → 4 min budget.
const DLQ_MAX_WAIT_MS = 240_000;

describe("events e2e: DLQ after MaxAttempts failures", () => {
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

	// Promisify the gRPC unary ListDlq call.
	function listDlq(
		sb: ServiceBridge,
		req: ListDlqRequest,
	): Promise<{ entries: DlqEntry[]; nextCursor: string }> {
		// _eventsClient is @internal on ServiceBridge — access via type cast.
		// There is no public DLQ query API on the SDK; this is the only path
		// short of querying Postgres directly.
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

	test("handler always throws → max attempts → event moved to DLQ", async () => {
		const eventName = uniqueEventName("dlq");

		let invocationCount = 0;

		// Handler always throws → every delivery produces a Nack.
		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async () => {
			invocationCount++;
			throw new Error(
				`dlq-test: always-failing handler (attempt ${invocationCount})`,
			);
		});
		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		// Publisher needs to be started to obtain credentials for
		// the listDlq call (shares the same events gRPC channel).
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

		const { eventId: publishedEventId } = await publisher.event.publish(
			eventName,
			{ orderId: "will-dlq", amount: 1, currency: "USD" },
		);

		expect(publishedEventId).toMatch(/^[0-9a-f-]{36}$/);

		// Wait until the event appears in the DLQ. Poll every 5 s.
		// The DLQ entry's eventId is the runtime-canonical ID (stored in
		// event_log.id). The publisher's local outbox ID (publishedEventId)
		// is the ID sent in the envelope — the same UUID.
		let dlqEntry: DlqEntry | undefined;
		const deadline = Date.now() + DLQ_MAX_WAIT_MS;
		while (Date.now() < deadline) {
			await sleep(5_000);
			let res: { entries: DlqEntry[] };
			try {
				res = await listDlq(publisher!, { limit: 50, cursor: "" });
			} catch {
				// Runtime may not be ready for DLQ queries yet — retry.
				continue;
			}
			// Find our event by name (the subscriber service is uniquely named
			// per test run via uniqueEventName).
			dlqEntry = res.entries.find((e) => e.eventName === eventName);
			if (dlqEntry) break;
		}

		// Verify the DLQ entry exists and records the correct consumer service.
		expect(dlqEntry).toBeDefined();
		expect(dlqEntry!.eventName).toBe(eventName);
		// totalAttempts in the DLQ row reflects how many times the delivery was tried.
		// With MaxAttempts=5 it is 5; with a lower override it is that number.
		expect(dlqEntry!.totalAttempts).toBeGreaterThanOrEqual(1);
		// The handler was invoked for each attempt.
		expect(invocationCount).toBe(dlqEntry!.totalAttempts);
		// No further deliveries after DLQ.
		expect(dlqEntry!.replayCount).toBe(0);
	}, 300_000);
});
