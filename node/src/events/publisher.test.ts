/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { ServiceError } from "@grpc/grpc-js";
import { Storage } from "../sqlite/storage";
import { InvalidEventNameError, OutboxFullError } from "./errors";
import type {
	DrainerHandle,
	Logger,
	PublisherDeps,
	SchemaIndex,
} from "./publisher";
import { Publisher } from "./publisher";

// Minimal Serializer stub that encodes payload as JSON bytes.
function makeSerializer() {
	return {
		encode: (v: unknown) => Buffer.from(JSON.stringify(v)),
		decode: (b: Uint8Array) => JSON.parse(Buffer.from(b).toString()),
		contractHash: () => "stub-hash",
		toJsonSchema: () => ({}) as Record<string, unknown>,
	};
}

function makeSchemaIndex(
	entries: Record<string, { contractHash: string }>,
): SchemaIndex {
	return {
		get(name) {
			const e = entries[name];
			if (!e) return undefined;
			const s = makeSerializer();
			return { contractHash: e.contractHash, pair: { input: s, output: s } };
		},
	};
}

function makeKicker(): DrainerHandle & { count: number } {
	let count = 0;
	return {
		kick() {
			count++;
		},
		get count() {
			return count;
		},
	};
}

function makeLogger(): Logger {
	return { warn() {}, error() {} };
}

// Fake EventsClient.publish that calls callback with null error and response.
function makeRpcClient(
	handler: (
		req: unknown,
		cb: (
			err: ServiceError | null,
			res?: { results: Array<{ eventId: string; status: number }> },
		) => void,
	) => void,
) {
	return {
		publish: handler,
	} as unknown as import("../pb/servicebridge/v1/events").EventsClient;
}

describe("Publisher", () => {
	let tmpDir: string;
	let storage: Storage;

	beforeEach(() => {
		tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "sb-pub-test-"));
		storage = Storage.open({ dataDir: tmpDir });
	});

	afterEach(() => {
		storage.close();
		fs.rmSync(tmpDir, { recursive: true, force: true });
	});

	function makeDeps(overrides: Partial<PublisherDeps> = {}): PublisherDeps {
		return {
			storage,
			rpcClient: makeRpcClient((_req, cb) => cb(null, { results: [] })),
			schemaIndex: makeSchemaIndex({
				"payment.charged": { contractHash: "hash1" },
			}),
			drainer: makeKicker(),
			identity: () => ({
				sessionId: "s",
				serviceId: "svc",
				serviceName: "svc",
				instanceId: "i",
			}),
			maxOutboxRows: 100_000,
			logger: makeLogger(),
			xSbTraceFn: () => "",
			...overrides,
		};
	}

	it("throws InvalidEventNameError for invalid name formats", async () => {
		const p = new Publisher(makeDeps());
		const invalid = [
			"Payment.Charged",
			"payment..charged",
			".payment.charged",
			"payment.charged.",
			"payment charged",
			"",
			"UPPER",
			"a.b.C",
		];
		for (const name of invalid) {
			await expect(p.publish(name, {})).rejects.toBeInstanceOf(
				InvalidEventNameError,
			);
		}
	});

	it("throws error if no schema registered for the event name", async () => {
		const p = new Publisher(makeDeps());
		await expect(p.publish("unknown.event", {})).rejects.toThrow(
			/no schema registered for event/,
		);
	});

	it("happy path: inserts outbox row and kicks drainer", async () => {
		const kicker = makeKicker();
		const p = new Publisher(makeDeps({ drainer: kicker }));

		const { eventId } = await p.publish("payment.charged", { amount: 100 });
		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);
		expect(kicker.count).toBe(1);

		const row = storage
			.prepare("SELECT * FROM event_outbox WHERE id=?")
			.get(eventId) as {
			name: string;
			status: string;
			contract_hash: string;
		};
		expect(row).not.toBeNull();
		expect(row.name).toBe("payment.charged");
		expect(row.status).toBe("pending");
		expect(row.contract_hash).toBe("hash1");
	});

	it("fireAndForget bypasses outbox and calls rpcClient directly", async () => {
		let publishCalled = false;
		const rpcClient = makeRpcClient((_req, cb) => {
			publishCalled = true;
			cb(null, { results: [] });
		});
		const kicker = makeKicker();
		const p = new Publisher(makeDeps({ rpcClient, drainer: kicker }));

		const { eventId } = await p.publish(
			"payment.charged",
			{},
			{ fireAndForget: true },
		);
		expect(publishCalled).toBe(true);
		expect(kicker.count).toBe(0);

		const count = storage
			.prepare("SELECT COUNT(*) AS c FROM event_outbox")
			.get() as { c: number };
		expect(count.c).toBe(0);
		expect(eventId).toBeDefined();
	});

	it("throws when rpcClient errors in fireAndForget", async () => {
		const rpcClient = makeRpcClient((_req, cb) => {
			const err = Object.assign(new Error("grpc error"), {
				code: 14,
			}) as ServiceError;
			cb(err);
		});
		const p = new Publisher(makeDeps({ rpcClient }));
		await expect(
			p.publish("payment.charged", {}, { fireAndForget: true }),
		).rejects.toThrow("grpc error");
	});

	it("throws OutboxFullError when cap is reached", async () => {
		const p = new Publisher(makeDeps({ maxOutboxRows: 2 }));
		await p.publish("payment.charged", { n: 1 });
		await p.publish("payment.charged", { n: 2 });
		await expect(p.publish("payment.charged", { n: 3 })).rejects.toBeInstanceOf(
			OutboxFullError,
		);
	});

	it("tracks the outbox row count without scanning the table", async () => {
		const p = new Publisher(makeDeps({ maxOutboxRows: 3 }));
		await p.publish("payment.charged", { n: 1 });
		await p.publish("payment.charged", { n: 2 });
		expect(storage.outboxRowCount()).toBe(2);

		await p.publish("payment.charged", { n: 3 });
		await expect(p.publish("payment.charged", { n: 4 })).rejects.toBeInstanceOf(
			OutboxFullError,
		);
		// The rejected publish rolled back — the count must not drift.
		expect(storage.outboxRowCount()).toBe(3);
		expect(
			storage.prepare("SELECT COUNT(*) AS c FROM event_outbox").get(),
		).toMatchObject({ c: 3 });
	});

	it("sends payload_json on the fireAndForget path too", async () => {
		let captured: Uint8Array | undefined;
		const rpcClient = makeRpcClient((req, cb) => {
			const events = (req as { events: { payloadJson: Uint8Array }[] }).events;
			captured = events[0]?.payloadJson;
			cb(null, { results: [] });
		});
		const p = new Publisher(makeDeps({ rpcClient }));

		await p.publish("payment.charged", { amount: 7 }, { fireAndForget: true });

		// The runtime feeds payload_json to workflow wait_event filters for every
		// ingested event, fire-and-forget included.
		expect(captured).toBeDefined();
		expect(Buffer.from(captured as Uint8Array).toString()).toBe(
			JSON.stringify({ amount: 7 }),
		);
	});

	it("propagates idempotencyKey and partitionKey to outbox row", async () => {
		const p = new Publisher(makeDeps());
		const { eventId } = await p.publish(
			"payment.charged",
			{},
			{
				idempotencyKey: "idem-42",
				partitionKey: "user-7",
			},
		);
		const row = storage
			.prepare("SELECT * FROM event_outbox WHERE id=?")
			.get(eventId) as {
			idempotency_key: string;
			partition_key: string;
		};
		expect(row.idempotency_key).toBe("idem-42");
		expect(row.partition_key).toBe("user-7");
	});

	it("propagates headers to outbox row as JSON", async () => {
		const p = new Publisher(makeDeps());
		const { eventId } = await p.publish(
			"payment.charged",
			{},
			{
				headers: { "x-trace-id": "abc" },
			},
		);
		const row = storage
			.prepare("SELECT headers FROM event_outbox WHERE id=?")
			.get(eventId) as { headers: string };
		const headers = JSON.parse(row.headers) as Record<string, string>;
		expect(headers["x-trace-id"]).toBe("abc");
	});

	it("uses occurredAtMs override in outbox row", async () => {
		const customTs = 1_700_000_000_000;
		const p = new Publisher(makeDeps());
		const { eventId } = await p.publish(
			"payment.charged",
			{},
			{ occurredAtMs: customTs },
		);
		const row = storage
			.prepare("SELECT occurred_at_ms FROM event_outbox WHERE id=?")
			.get(eventId) as { occurred_at_ms: number };
		expect(row.occurred_at_ms).toBe(customTs);
	});

	it("durable path persists x_sb_trace header from xSbTraceFn in outbox row", async () => {
		const xSbTrace =
			"01890d1e-1234-7890-abcd-ef1234567890-01890d1f-aaaa-7777-bbbb-cccccccccccc";
		const p = new Publisher(makeDeps({ xSbTraceFn: () => xSbTrace }));

		const { eventId } = await p.publish("payment.charged", {});

		const row = storage
			.prepare("SELECT x_sb_trace FROM event_outbox WHERE id=?")
			.get(eventId) as { x_sb_trace: string };
		expect(row).not.toBeNull();
		expect(row.x_sb_trace).toBe(xSbTrace);
	});

	it("durable path stores empty x_sb_trace when no context is active", async () => {
		const p = new Publisher(makeDeps());
		const { eventId } = await p.publish("payment.charged", {});

		const row = storage
			.prepare("SELECT x_sb_trace FROM event_outbox WHERE id=?")
			.get(eventId) as { x_sb_trace: string };
		expect(row).not.toBeNull();
		expect(row.x_sb_trace).toBe("");
	});
});
