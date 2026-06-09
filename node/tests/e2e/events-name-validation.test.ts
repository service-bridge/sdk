/**
 * events-name-validation.test.ts — invalid event names rejected before outbox insert.
 *
 * Verifies that InvalidEventNameError is thrown synchronously (before any I/O)
 * when an event name does not match ^[a-z0-9_-]+(\.[a-z0-9_-]+)*$.
 * Valid edge-case names pass without error.
 *
 * No subscriber is needed — these tests are publisher-only.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { InvalidEventNameError } from "../../src/events/errors";
import {
	eventsEnv,
	FAST_OPTS,
	ORDER_EVENT_PROTO,
	waitFor,
} from "./_helpers/events";

describe("events e2e: event name validation", () => {
	const env = eventsEnv();
	let publisher: ServiceBridge | undefined;

	beforeEach(() => {
		publisher = undefined;
	});

	afterEach(async () => {
		await publisher?.stop();
	});

	async function startPublisher(extraName?: string): Promise<ServiceBridge> {
		const sb = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		// Register at least one valid event so the publisher has schema context.
		const validName = "valid.event.name";
		sb.event.define(validName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		if (extraName) {
			sb.event.define(extraName, {
				protoFile: ORDER_EVENT_PROTO,
				method: "orders_created",
			});
		}
		await sb.start();
		await waitFor(() => sb.identity() !== null, 5_000, "publisher connected");
		return sb;
	}

	test("invalid name with uppercase letters throws InvalidEventNameError", async () => {
		publisher = await startPublisher();
		// Name with uppercase — not registered, validation fires first.
		await expect(
			publisher.event.publish("Order.Created", {
				orderId: "x",
				amount: 1,
				currency: "USD",
			}),
		).rejects.toThrow(InvalidEventNameError);
	}, 15_000);

	test("invalid name with trailing exclamation throws InvalidEventNameError", async () => {
		publisher = await startPublisher();
		await expect(
			publisher.event.publish("order.created!", {
				orderId: "x",
				amount: 1,
				currency: "USD",
			}),
		).rejects.toThrow(/invalid|InvalidEventName/);
	}, 15_000);

	test("name with spaces throws InvalidEventNameError", async () => {
		publisher = await startPublisher();
		await expect(
			publisher.event.publish("name with spaces", {
				orderId: "x",
				amount: 1,
				currency: "USD",
			}),
		).rejects.toThrow(InvalidEventNameError);
	}, 15_000);

	test("empty string throws InvalidEventNameError", async () => {
		publisher = await startPublisher();
		await expect(
			publisher.event.publish("", {
				orderId: "x",
				amount: 1,
				currency: "USD",
			}),
		).rejects.toThrow(InvalidEventNameError);
	}, 15_000);

	test("valid name with edge characters is accepted", async () => {
		const validEdgeName = "order-created.v1_alpha";
		publisher = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		publisher.event.define(validEdgeName, {
			protoFile: ORDER_EVENT_PROTO,
			method: "orders_created",
		});
		await publisher.start();
		await waitFor(
			() => publisher!.identity() !== null,
			5_000,
			"publisher connected",
		);

		// Should not throw — valid name, schema registered.
		const { eventId } = await publisher.event.publish(validEdgeName, {
			orderId: "edge-1",
			amount: 1,
			currency: "USD",
		});
		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);
	}, 15_000);

	test("name with leading dot throws InvalidEventNameError", async () => {
		publisher = await startPublisher();
		await expect(
			publisher.event.publish(".leading.dot", {
				orderId: "x",
				amount: 1,
				currency: "USD",
			}),
		).rejects.toThrow(InvalidEventNameError);
	}, 15_000);

	test("name with trailing dot throws InvalidEventNameError", async () => {
		publisher = await startPublisher();
		await expect(
			publisher.event.publish("trailing.dot.", {
				orderId: "x",
				amount: 1,
				currency: "USD",
			}),
		).rejects.toThrow(InvalidEventNameError);
	}, 15_000);
});
