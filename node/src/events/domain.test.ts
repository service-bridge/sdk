import { describe, expect, it, mock } from "bun:test";
import path from "node:path";
import { MethodType } from "../pb/servicebridge/v1/registry";
import { Registry } from "../registry/registry";
import { EventDomain } from "./domain";
import type { Publisher } from "./publisher";

const PROTO_PATH = path.join(__dirname, "testdata", "order-event.proto");

function makeDomain(publisher: Publisher | null = null): {
	domain: EventDomain;
	registry: Registry;
} {
	const registry = new Registry();
	const domain = new EventDomain(registry, () => publisher);
	return { domain, registry };
}

describe("EventDomain.define", () => {
	it("registers published event in registry._handle._published", async () => {
		const { domain, registry } = makeDomain();
		domain.define("orders_created", {
			protoFile: PROTO_PATH,
			method: "orders_created",
		});
		await registry._handle.finalize();

		const req = registry.buildRegisterRequest();
		expect(req.published).toHaveLength(1);
		expect(req.published[0]!.name).toBe("orders_created");
		expect(req.published[0]!.schemaJson.length).toBeGreaterThan(0);
		expect(req.published[0]!.contractHash).toMatch(/^[0-9a-f]{64}$/);
	});

	it("define without spec — schemaJson empty, contractHash empty", async () => {
		const { domain, registry } = makeDomain();
		domain.define("orders_created");
		await registry._handle.finalize();

		const req = registry.buildRegisterRequest();
		expect(req.published[0]!.schemaJson.length).toBe(0);
		expect(req.published[0]!.contractHash).toBe("");
	});

	it("getPublishedEvent returns pair + contractHash after finalize", async () => {
		const { domain, registry } = makeDomain();
		domain.define("orders_created", {
			protoFile: PROTO_PATH,
			method: "orders_created",
		});
		await registry._handle.finalize();

		const entry = registry._handle.getPublishedEvent("orders_created");
		expect(entry).toBeDefined();
		expect(entry!.contractHash).toMatch(/^[0-9a-f]{64}$/);
		// Protobuf round-trip works.
		const bytes = entry!.pair.input.encode({
			orderId: "o-1",
			amount: 10.5,
			currency: "USD",
		});
		expect(bytes.length).toBeGreaterThan(0);
		const decoded = entry!.pair.input.decode(bytes) as {
			orderId: string;
			amount: number;
			currency: string;
		};
		expect(decoded.orderId).toBe("o-1");
		expect(decoded.amount).toBeCloseTo(10.5);
		expect(decoded.currency).toBe("USD");
	});

	it("getPublishedEvent returns undefined for schema-less event", async () => {
		const { domain, registry } = makeDomain();
		domain.define("orders_created");
		await registry._handle.finalize();
		expect(
			registry._handle.getPublishedEvent("orders_created"),
		).toBeUndefined();
	});

	it("identical re-define is a no-op", async () => {
		const { domain, registry } = makeDomain();
		const spec = { protoFile: PROTO_PATH, method: "orders_created" };
		domain.define("orders_created", spec);
		domain.define("orders_created", spec);
		await registry._handle.finalize();

		const req = registry.buildRegisterRequest();
		expect(req.published).toHaveLength(1);
	});

	it("re-define with different spec object throws", () => {
		const { domain } = makeDomain();
		domain.define("orders_created", {
			protoFile: PROTO_PATH,
			method: "orders_created",
		});
		expect(() =>
			domain.define("orders_created", {
				protoFile: PROTO_PATH,
				method: "orders_created_v2",
			}),
		).toThrow(/already declared/);
	});

	it("two distinct events get distinct contract hashes (versioning)", async () => {
		const { domain, registry } = makeDomain();
		domain.define("orders_created_v1", {
			protoFile: PROTO_PATH,
			method: "orders_created",
		});
		domain.define("orders_created_v2", {
			protoFile: PROTO_PATH,
			method: "orders_created_v2",
		});
		await registry._handle.finalize();

		const req = registry.buildRegisterRequest();
		const hashes = req.published.map((p) => p.contractHash);
		expect(hashes).toHaveLength(2);
		expect(hashes[0]).not.toBe(hashes[1]);
		for (const h of hashes) expect(h).toMatch(/^[0-9a-f]{64}$/);
	});
});

describe("EventDomain.handle", () => {
	it("registers event subscription in registry._handle._entries", () => {
		const { domain, registry } = makeDomain();
		domain.handle("orders.*", async () => {});
		const entries = registry._handle._entries;
		expect(entries).toHaveLength(1);
		expect(entries[0]!.type).toBe(MethodType.METHOD_TYPE_EVENT);
		expect(entries[0]!.name).toBe("orders.*");
	});

	it("entry appears in eventSubscriptions", () => {
		const { domain, registry } = makeDomain();
		domain.handle("payment.charged", async () => {});
		const req = registry.buildRegisterRequest();
		expect(req.eventSubscriptions).toHaveLength(1);
		expect(req.eventSubscriptions[0]!.pattern).toBe("payment.charged");
		expect(req.eventSubscriptions[0]!.durable).toBe(true);
	});
});

describe("EventDomain.publish", () => {
	it("delegates to Publisher.publish", async () => {
		const mockPublish = mock(async () => ({ eventId: "ev-1" }));
		const publisher = { publish: mockPublish } as unknown as Publisher;
		const { domain } = makeDomain(publisher);

		const result = await domain.publish("orders.created", { orderId: "o-1" });
		expect(result.eventId).toBe("ev-1");
		expect(mockPublish).toHaveBeenCalledTimes(1);
		const args = mockPublish.mock.calls[0] as unknown[];
		expect(args[0]).toBe("orders.created");
		expect(args[1]).toEqual({ orderId: "o-1" });
	});

	it("throws when publisher not ready", async () => {
		const { domain } = makeDomain(null);
		await expect(domain.publish("orders.created", {})).rejects.toThrow(
			/publisher not ready/,
		);
	});
});
