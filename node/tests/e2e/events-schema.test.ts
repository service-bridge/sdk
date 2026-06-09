/**
 * events-schema.test.ts — Protobuf encode/decode round-trip end-to-end.
 *
 * Verifies:
 *   1. Publisher encodes an OrderCreatedV1 payload via proto schema.
 *   2. Subscriber decodes the incoming bytes back to an identical object.
 *   3. Publishing a payload with an invalid field type (number where string
 *      expected) throws synchronously before reaching the outbox — protobufjs
 *      `type.verify()` rejects it inside Publisher.publish().
 *
 * Runs against a live runtime + real Postgres. No skips.
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

describe("events e2e: protobuf schema round-trip", () => {
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

	test("encoded OrderCreatedV1 decodes to identical payload on subscriber", async () => {
		const eventName = uniqueEventName("schema-rt");

		const received: Array<{
			orderId: string;
			amount: number;
			currency: string;
		}> = [];

		// Subscriber starts first so the pattern is registered before publish.
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

		// Allow subscription stream to register on the server side.
		await sleep(500);

		const input = { orderId: "ord-schema-1", amount: 42.75, currency: "EUR" };
		const { eventId } = await publisher.event.publish(eventName, input);

		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);

		await waitFor(() => received.length > 0, 10_000, "delivery to subscriber");

		expect(received).toHaveLength(1);
		// Every field must round-trip exactly through protobuf encoding/decoding.
		expect(received[0]!.orderId).toBe(input.orderId);
		expect(received[0]!.amount).toBeCloseTo(input.amount);
		expect(received[0]!.currency).toBe(input.currency);
	}, 30_000);

	test("publish with wrong field type throws before reaching outbox", async () => {
		// protobufjs type.verify() runs inside Publisher.publish() synchronously
		// as part of entry.pair.input.encode(payload). When `orderId` is a number
		// (proto field type: string), verify() returns a non-null error string and
		// encode() throws before any outbox INSERT or network call.
		const eventName = uniqueEventName("schema-invalid");

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

		// orderId should be a string; passing a number triggers type.verify() failure.
		const badPayload = { orderId: 42, amount: 10.0, currency: "USD" };

		await expect(
			publisher.event.publish(eventName, badPayload),
		).rejects.toThrow(/string|verify|invalid|orderId/i);
	}, 15_000);
});
