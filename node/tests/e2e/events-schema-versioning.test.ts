/**
 * events-schema-versioning.test.ts — two declarations of the same event name
 * with different schemas coexist (keep-history).
 *
 * Background:
 *   registry/repo.go::UpsertMethods uses `keepHistory = true` for published
 *   events. The uniqueness key is (service_id, instance_id, method_type,
 *   method_name, published, contract_hash). Two publishers that register the
 *   SAME event name with DIFFERENT schemas produce two distinct rows in
 *   service_methods — the runtime stores both, keyed by their contract_hash.
 *
 * Verified:
 *   1. publisherA.event.define(name, v1 schema) — 3-field OrderCreatedV1.
 *   2. publisherB.event.define(name, v2 schema) — 4-field OrderCreatedV2.
 *   3. Both start() calls succeed (no schema-mismatch error from registry).
 *   4. The two schemas produce different contract_hash values (computed
 *      client-side via buildSchemaPair + computeContractHash).
 *   5. A subscriber registered against v1 receives events from publisherA and
 *      decodes them correctly. Events from publisherB arrive with a different
 *      contract_hash; if the subscriber's schemaIndex maps that name to its
 *      own v1 schema, decoding happens on the same bytes — proto3 is forward-
 *      compatible (extra field 4 is ignored), so the value decodes without
 *      error. We assert the subscriber receives payloads from both publishers
 *      and that the orderId field is present in both decoded objects.
 *
 * The test does NOT assert DLQ behaviour; schema-versioning orphan-to-DLQ
 * is covered by events-pattern-change.test.ts.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { computeContractHash } from "../../src/serde/contract-hash";
import { buildSchemaPair } from "../../src/serde/serializer";
import {
	eventsEnv,
	FAST_OPTS,
	ORDER_EVENT_PROTO,
	sleep,
	uniqueEventName,
	waitFor,
} from "./_helpers/events";

describe("events e2e: schema versioning — keep-history for distinct contract_hash", () => {
	const env = eventsEnv();
	let publisherA: ServiceBridge | undefined;
	let publisherB: ServiceBridge | undefined;
	let subscriber: ServiceBridge | undefined;

	beforeEach(() => {
		publisherA = undefined;
		publisherB = undefined;
		subscriber = undefined;
	});

	afterEach(async () => {
		// Stop all three instances; errors are ignored (best-effort cleanup).
		await Promise.allSettled([
			publisherA?.stop(),
			publisherB?.stop(),
			subscriber?.stop(),
		]);
	});

	test("two publishers with same name but different schemas both start and subscriber receives from both", async () => {
		// Use the same event name for both publishers — this exercises keep-history.
		const eventName = uniqueEventName("schema-ver");

		// --- Contract hash verification (client-side, no network needed) ---
		// Compute hashes for v1 and v2 schemas to assert they differ.
		const pairV1 = await buildSchemaPair({
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		const pairV2 = await buildSchemaPair({
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created_v2",
		});
		const hashV1 = computeContractHash(pairV1);
		const hashV2 = computeContractHash(pairV2);

		// Keep-history requires the two schemas to produce distinct hashes.
		// If this assertion fails it means the proto messages are identical,
		// which would make this test meaningless.
		expect(hashV1).not.toBe(hashV2);

		// --- Subscriber: registers against v1 schema ---
		// The subscriber will decode ALL deliveries for eventName using its own
		// v1 schemaIndex entry. Proto3 forward-compatibility means that a v2
		// payload (4 fields) decodes cleanly against a v1 descriptor (3 fields);
		// the unknown `region` field (field 4) is simply dropped.
		const received: Array<{
			orderId: string;
			amount: number;
			currency: string;
		}> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		subscriber.event.handle(eventName, async (payload) => {
			received.push(payload as (typeof received)[number]);
		});
		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);

		// --- PublisherA: v1 schema (OrderCreatedV1 — 3 fields) ---
		publisherA = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisherA.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		// start() must NOT throw. If registry rejects a second schema for the
		// same event name, the test fails here — keep-history would be broken.
		await publisherA.start();
		await waitFor(
			() => publisherA!.identity() !== null,
			5_000,
			"publisherA connected",
		);

		// --- PublisherB: v2 schema (OrderCreatedV2 — 4 fields, adds `region`) ---
		// Uses the same subscriberKey as a different service instance.
		// NOTE: Bun e2e tests have only two keys (publisherKey and subscriberKey).
		// We reuse publisherKey for publisherB with a second ServiceBridge instance
		// — same service, different instance, which still results in a distinct
		// (service_id, instance_id, method_name, contract_hash) row in the registry.
		publisherB = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisherB.event.define(eventName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created_v2",
		});
		// start() must NOT throw, same as publisherA.
		await publisherB.start();
		await waitFor(
			() => publisherB!.identity() !== null,
			5_000,
			"publisherB connected",
		);

		// Allow subscription stream to register on the server side.
		await sleep(500);

		// Publish one v1 event and one v2 event.
		const { eventId: idA } = await publisherA.event.publish(eventName, {
			orderId: "v1-event",
			amount: 10.0,
			currency: "USD",
		});
		const { eventId: idB } = await publisherB.event.publish(eventName, {
			orderId: "v2-event",
			amount: 20.0,
			currency: "EUR",
			region: "eu-west",
		});

		expect(idA).toMatch(/^[0-9a-f-]{36}$/);
		expect(idB).toMatch(/^[0-9a-f-]{36}$/);

		// Wait for both deliveries to land on the subscriber.
		await waitFor(() => received.length >= 2, 15_000, "2 deliveries");

		// Both events arrive; orderId distinguishes them.
		const orderIds = received.map((r) => r.orderId).sort();
		expect(orderIds).toContain("v1-event");
		expect(orderIds).toContain("v2-event");

		// Proto3 forward-compat: v2 payload decoded by v1 schema → region dropped,
		// core fields (orderId, amount, currency) are intact.
		const v2Decoded = received.find((r) => r.orderId === "v2-event")!;
		expect(v2Decoded.amount).toBeCloseTo(20.0);
		expect(v2Decoded.currency).toBe("EUR");
	}, 45_000);
});
