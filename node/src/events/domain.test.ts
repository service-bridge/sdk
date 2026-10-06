import { describe, expect, it, mock } from "bun:test";
import path from "node:path";
import { StateError } from "../errors";
import { Registry } from "../registry/registry";
import {
	computeContractHash,
	computeEventContractHash,
} from "../serde/contract-hash";
import { buildSchemaPair } from "../serde/serializer";
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
		expect(req.published[0]!.contractHash).toMatch(/^v2:[0-9a-f]{64}$/);
	});

	it("stamps the one-way identity: payload against the empty message", async () => {
		const { domain, registry } = makeDomain();
		domain.define("orders_created", {
			protoFile: PROTO_PATH,
			method: "orders_created",
		});
		await registry._handle.finalize();

		const pair = await buildSchemaPair({
			protoFile: PROTO_PATH,
			input: "OrderCreatedV1",
			// A reply the identity must ignore: the service block points
			// orders_created at OrderCreatedV1, this one has an extra field.
			output: "OrderCreatedV2",
		});
		const req = registry.buildRegisterRequest();
		expect(req.published[0]!.contractHash).toBe(
			computeEventContractHash(pair.input),
		);
		expect(req.published[0]!.contractHash).not.toBe(computeContractHash(pair));
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
		expect(entry!.contractHash).toMatch(/^v2:[0-9a-f]{64}$/);
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
		for (const h of hashes) expect(h).toMatch(/^v2:[0-9a-f]{64}$/);
	});
});

describe("EventDomain.handle", () => {
	it("registers a subscription, not an incoming method nor a published event", () => {
		const { domain, registry } = makeDomain();
		domain.handle("orders.*", async () => {}, { filter: { "$.region": "eu" } });
		expect(registry._handle.subscription("orders.*")?.filter).toBe(
			'{"$.region":"eu"}',
		);
		const req = registry.buildRegisterRequest();
		expect(req.incoming).toHaveLength(0);
		expect(req.published).toHaveLength(0);
	});

	it("entry appears in eventSubscriptions", () => {
		const { domain, registry } = makeDomain();
		domain.handle("payment.charged", async () => {});
		const req = registry.buildRegisterRequest();
		expect(req.eventSubscriptions).toHaveLength(1);
		expect(req.eventSubscriptions[0]!.pattern).toBe("payment.charged");
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

	it("rejects with a StateError before start()", async () => {
		const { domain } = makeDomain(null);
		await expect(domain.publish("orders.created", {})).rejects.toBeInstanceOf(
			StateError,
		);
	});
});
