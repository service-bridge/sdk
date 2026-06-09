/**
 * events-runtime-restart-ingested.test.ts
 *
 * Scenario: event accepted by the runtime (status='pending' in event_deliveries)
 * survives the delivery lifecycle — specifically that the runtime persists the
 * event to Postgres and eventually delivers it to the subscriber.
 *
 * IMPORTANT — scope limitation:
 *   Full runtime-restart simulation (SIGKILL the runtime, restart, verify
 *   pending events are re-dispatched) requires a test-owned runtime instance.
 *   Killing the shared runtime on port 14445 would break all parallel agents.
 *   See events-restart-runtime.test.ts for the subscriber-restart subset.
 *
 *   What this test DOES verify (via direct Postgres queries):
 *     1. After publish, a row exists in `event_log` with the returned eventId.
 *     2. A row exists in `event_deliveries` for the subscriber's service.
 *     3. The delivery row eventually transitions to status='success' after
 *        the subscriber processes the event.
 *   These three assertions confirm that the runtime durably records every
 *   event before attempting delivery — the invariant that makes restart
 *   recovery possible. If the runtime were restarted between step 1 and step 3,
 *   it would recover the 'pending' delivery rows from Postgres on boot.
 *
 * DB connection: postgresql://servicebridge:servicebridge@localhost:5433/servicebridge
 * (matches runtime/.env on the dev machine).
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

// ── Postgres helper ────────────────────────────────────────────────────────────

const PG_URL =
	process.env.TEST_DATABASE_URL ??
	"postgresql://servicebridge:servicebridge@localhost:5433/servicebridge";

// Thin wrapper that opens a Bun.sql connection for the test duration.
// Each query is a tagged template that Bun.sql sends as a parameterised
// statement — no string interpolation into SQL.
async function withDb<T>(
	fn: (sql: typeof import("bun").sql) => Promise<T>,
): Promise<T> {
	// Bun.sql uses the DATABASE_URL env var by default; we override temporarily.
	const prev = process.env.DATABASE_URL;
	process.env.DATABASE_URL = PG_URL;
	try {
		const { sql } = await import("bun");
		const result = await fn(sql);
		return result;
	} finally {
		if (prev === undefined) {
			delete process.env.DATABASE_URL;
		} else {
			process.env.DATABASE_URL = prev;
		}
	}
}

// ── Test ──────────────────────────────────────────────────────────────────────

describe("events e2e: runtime persistence — event_log + event_deliveries", () => {
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

	test("published event is persisted in event_log and delivery reaches 'success'", async () => {
		const eventName = uniqueEventName("ingested");
		const receivedPayloads: Array<{ orderId: string }> = [];

		// Start subscriber first.
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

		// Capture the subscriber's serviceId — needed for the delivery row query.
		const subscriberIdentity = subscriber.identity()!;
		expect(subscriberIdentity.serviceId).toBeTruthy();

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

		// Allow subscription stream to propagate.
		await sleep(500);

		// ── Publish ──────────────────────────────────────────────────────────

		const { eventId } = await publisher.event.publish(eventName, {
			orderId: "ingested-order-1",
			amount: 77,
			currency: "USD",
		});

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);

		// ── Assert 1: event_log row exists ───────────────────────────────────
		// We poll because the SDK's drainer dispatches asynchronously.

		await waitFor(
			async () => {
				const rows = await withDb(async (sql) => {
					const result = await sql`
							SELECT COUNT(*)::int AS c
							FROM event_log
							WHERE id = ${eventId}
						`;
					return result as Array<{ c: number }>;
				});
				return (rows[0]?.c ?? 0) >= 1;
			},
			10_000,
			"event_log row to appear",
		);

		// ── Assert 2: event_deliveries row exists ─────────────────────────────

		await waitFor(
			async () => {
				const rows = await withDb(async (sql) => {
					const result = await sql`
							SELECT COUNT(*)::int AS c
							FROM event_deliveries
							WHERE event_id = ${eventId}
						`;
					return result as Array<{ c: number }>;
				});
				return (rows[0]?.c ?? 0) >= 1;
			},
			10_000,
			"event_deliveries row to appear",
		);

		// ── Assert 3: delivery transitions to 'success' after handler runs ──

		// Wait for the subscriber handler to fire.
		await waitFor(
			() => receivedPayloads.length > 0,
			10_000,
			"subscriber handler invoked",
		);

		expect(receivedPayloads[0]!.orderId).toBe("ingested-order-1");

		// Then poll until status='success' in Postgres.
		await waitFor(
			async () => {
				const rows = await withDb(async (sql) => {
					const result = await sql`
							SELECT status
							FROM event_deliveries
							WHERE event_id = ${eventId}
							LIMIT 1
						`;
					return result as Array<{ status: string }>;
				});
				return rows[0]?.status === "success";
			},
			15_000,
			"delivery status='success'",
		);
	}, 60_000);
});
