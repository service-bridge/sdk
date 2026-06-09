/**
 * Registry e2e tests — all scenarios in one file.
 *
 * Requires a running ServiceBridge runtime (SERVICEBRIDGE_URL) and two
 * bootstrap keys:
 *   SERVICEBRIDGE_SERVICE_KEY  — provider service "e2e-registry-svc"
 *   SERVICEBRIDGE_SERVICE2_KEY — consumer service "e2e-registry-consumer"
 *
 * Generate keys:
 *   go run ./cmd/sbkey-gen -name=e2e-registry-svc      -ca-cert=./certs/ca.crt ...
 *   go run ./cmd/sbkey-gen -name=e2e-registry-consumer -ca-cert=./certs/ca.crt ...
 */

import { afterEach, describe, expect, test } from "bun:test";
import type {
	MethodDescriptor,
	ServiceMapEntry,
} from "../../src/connection/service-bridge";
import { ServiceBridge } from "../../src/connection/service-bridge";

const URL = process.env.SERVICEBRIDGE_URL;
const PROVIDER_KEY = process.env.SERVICEBRIDGE_SERVICE_KEY;
const CONSUMER_KEY = process.env.SERVICEBRIDGE_SERVICE2_KEY;

const enabled = Boolean(URL && PROVIDER_KEY && CONSUMER_KEY);

const PROVIDER_SVC = "e2e-registry-svc";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
};

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
	predicate: () => boolean,
	timeoutMs: number,
	what: string,
): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (predicate()) return;
		await sleep(50);
	}
	throw new Error(`timeout waiting for: ${what}`);
}

/** Flatten all method descriptors across services in a ServiceMap. */
function allMethods(
	map: ReadonlyMap<string, ServiceMapEntry>,
): MethodDescriptor[] {
	const out: MethodDescriptor[] = [];
	for (const entry of map.values()) out.push(...entry.methods);
	return out;
}

function hasMethod(
	map: ReadonlyMap<string, ServiceMapEntry>,
	name: string,
): boolean {
	for (const d of allMethods(map)) {
		if (d.name === name) return true;
	}
	return false;
}

describe.skipIf(!enabled)("Registry e2e", () => {
	let provider: ServiceBridge | undefined;
	let consumer: ServiceBridge | undefined;

	afterEach(async () => {
		await provider?.stop();
		await consumer?.stop();
		provider = undefined;
		consumer = undefined;
	});

	test("snapshot delivery — provider registered before consumer connects", async () => {
		provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
		provider.rpc._declareForTests("charge");
		await provider.start();
		await waitFor(
			() => provider!.identity() !== null,
			5_000,
			"provider connected",
		);

		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["charge"] });
		await consumer.start();

		await waitFor(
			() => hasMethod(consumer!.serviceMap(), "charge"),
			5_000,
			"charge in snapshot",
		);

		const desc = allMethods(consumer.serviceMap()).find(
			(d) => d.name === "charge",
		);
		expect(desc).toBeDefined();
		expect(desc!.serviceName).toBe(PROVIDER_SVC);
	});

	test("live update — consumer connects first, provider registers after", async () => {
		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["refund"] });
		await consumer.start();
		await waitFor(
			() => consumer!.identity() !== null,
			5_000,
			"consumer connected",
		);

		// provider not yet up — method must be absent
		expect(hasMethod(consumer.serviceMap(), "refund")).toBe(false);

		provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
		provider.rpc._declareForTests("refund");
		await provider.start();

		await waitFor(
			() => hasMethod(consumer!.serviceMap(), "refund"),
			8_000,
			"refund live update",
		);
	});

	test("disconnect removal — method disappears when provider stops", async () => {
		provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
		provider.rpc._declareForTests("pay");
		await provider.start();
		await waitFor(
			() => provider!.identity() !== null,
			5_000,
			"provider connected",
		);

		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["pay"] });
		await consumer.start();
		await waitFor(
			() => hasMethod(consumer!.serviceMap(), "pay"),
			5_000,
			"pay in snapshot",
		);

		await provider.stop();
		provider = undefined;

		await waitFor(
			() => !hasMethod(consumer!.serviceMap(), "pay"),
			8_000,
			"pay removed after disconnect",
		);
	});

	test("event subscription — consumer registers pattern via Handle.event", async () => {
		// Events are NOT visible through serviceMap() (RPC discovery only).
		// This test confirms that handle.event(pattern) registers the consumer
		// without errors and the consumer stays connected. End-to-end event
		// delivery is covered by tests/e2e/events-*.test.ts.
		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.event.handle("payment.*", async () => {});
		await consumer.start();
		await waitFor(
			() => consumer!.identity() !== null,
			5_000,
			"consumer connected with event subscription",
		);
		expect(consumer.identity()).not.toBeNull();
	});

	test("wildcard subscription — all provider RPC methods appear in snapshot", async () => {
		const methods = ["order-create", "order-list", "order-cancel"];

		provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
		for (const m of methods) provider.rpc._declareForTests(m);
		await provider.start();
		await waitFor(
			() => provider!.identity() !== null,
			5_000,
			"provider connected",
		);

		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["*"] });
		await consumer.start();

		await waitFor(
			() => methods.every((m) => hasMethod(consumer!.serviceMap(), m)),
			5_000,
			"all methods in snapshot",
		);

		for (const m of methods) {
			expect(hasMethod(consumer.serviceMap(), m)).toBe(true);
		}
	});

	test("unknown outgoing dep — connection succeeds, map stays empty", async () => {
		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		// "no-such-svc" is not in the runtime DB → server warns and skips.
		consumer.service("no-such-svc", { rpc: ["anything"] });
		await consumer.start();

		await waitFor(
			() => consumer!.identity() !== null,
			5_000,
			"consumer connected despite unknown dep",
		);

		// Runtime skips unknown deps — no entry for "no-such-svc" in service map.
		// The consumer's own instance may appear (self-subscription for policy
		// updates) but never with methods from the unknown dep.
		expect(consumer.serviceMap().has("no-such-svc")).toBe(false);
		expect(allMethods(consumer.serviceMap())).toHaveLength(0);
	});

	test("schema drift — two provider instances with different schemas both visible", async () => {
		// Two ServiceBridge instances from the same key → two different instanceIds.
		const provider2: ServiceBridge = new ServiceBridge(
			URL!,
			PROVIDER_KEY!,
			FAST_OPTS,
		);

		try {
			provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
			// No schema → contractHash derived from method type+name (different per provider1 vs provider2).
			provider.rpc._declareForTests("price-a");
			await provider.start();
			await waitFor(
				() => provider!.identity() !== null,
				5_000,
				"provider1 connected",
			);

			provider2.rpc._declareForTests("price-b");
			await provider2.start();
			await waitFor(
				() => provider2.identity() !== null,
				5_000,
				"provider2 connected",
			);

			consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
			consumer.service(PROVIDER_SVC, { rpc: ["*"] });
			await consumer.start();

			// Both providers register distinct methods → both visible.
			await waitFor(
				() => {
					const names = new Set<string>();
					for (const d of allMethods(consumer!.serviceMap())) names.add(d.name);
					return names.has("price-a") && names.has("price-b");
				},
				5_000,
				"both price-a and price-b in snapshot",
			);

			// Without schemas both contract hashes are empty (ADR 0005 — no
			// fallback). Distinctness comes from method name + instance_id.
			const seen = new Set<string>();
			for (const d of allMethods(consumer.serviceMap())) {
				if (d.name === "price-a" || d.name === "price-b")
					seen.add(`${d.name}:${d.instanceId}`);
			}
			expect(seen.size).toBe(2);
		} finally {
			await provider2.stop();
		}
	});
});
