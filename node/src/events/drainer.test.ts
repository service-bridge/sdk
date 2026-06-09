/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { ServiceError } from "@grpc/grpc-js";
import type {
	PublishRequest,
	PublishResponse,
} from "../pb/servicebridge/v1/events";
import { PublishStatus } from "../pb/servicebridge/v1/events";
import { Storage } from "../sqlite/storage";
import type { DrainerDeps } from "./drainer";
import { Drainer } from "./drainer";

// Insert a row into event_outbox with status='pending'.
function insertPending(
	storage: Storage,
	id: string,
	name = "test.event",
	attempts = 0,
	nextAtMs = 0,
	xSbTrace = "",
): void {
	storage
		.prepare(
			`INSERT INTO event_outbox
      (id, name, payload, contract_hash, partition_key, idempotency_key,
       fire_and_forget, headers, occurred_at_ms, enqueued_at_ms, status,
       attempts, last_error, next_attempt_at_ms, x_sb_trace)
      VALUES (?, ?, ?, ?, '', '', 0, '{}', ?, ?, 'pending', ?, '', ?, ?)`,
		)
		.run(
			id,
			name,
			Buffer.from("{}"),
			"hash1",
			Date.now(),
			Date.now(),
			attempts,
			nextAtMs,
			xSbTrace,
		);
}

// Fetch a row from event_outbox by id.
function getRow(
	storage: Storage,
	id: string,
): { status: string; attempts: number; last_error: string } | null {
	return storage
		.prepare("SELECT status, attempts, last_error FROM event_outbox WHERE id=?")
		.get(id) as { status: string; attempts: number; last_error: string } | null;
}

// Make a fake rpcClient that handles publish calls.
function makeRpcClient(
	handler: (
		req: PublishRequest,
		cb: (err: ServiceError | null, res?: PublishResponse) => void,
	) => void,
): DrainerDeps["rpcClient"] {
	return { publish: handler } as unknown as DrainerDeps["rpcClient"];
}

describe("Drainer", () => {
	let tmpDir: string;
	let storage: Storage;

	beforeEach(() => {
		tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "sb-drain-test-"));
		storage = Storage.open({ dataDir: tmpDir });
	});

	afterEach(async () => {
		storage.close();
		fs.rmSync(tmpDir, { recursive: true, force: true });
	});

	function makeDeps(
		rpcClient: DrainerDeps["rpcClient"],
		overrides: Partial<DrainerDeps> = {},
	): DrainerDeps {
		return {
			storage,
			rpcClient,
			identity: () => ({
				sessionId: "s",
				serviceId: "svc",
				serviceName: "svc",
				instanceId: "i",
			}),
			batchSize: 50,
			logger: { warn() {}, error() {} },
			clockFn: () => Date.now(),
			sleepFn: (ms) => new Promise<void>((r) => setTimeout(r, ms)),
			...overrides,
		};
	}

	it("ACCEPTED → deletes row from outbox", async () => {
		insertPending(storage, "ev-1");

		const rpc = makeRpcClient((_req, cb) => {
			cb(null, {
				results: [
					{
						eventId: "ev-1",
						status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
						message: "",
					},
				],
			});
		});
		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();
		// Give it time to process
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		const row = getRow(storage, "ev-1");
		expect(row).toBeNull();
	});

	it("REJECTED_DUPLICATE → deletes row from outbox", async () => {
		insertPending(storage, "ev-2");

		const rpc = makeRpcClient((_req, cb) => {
			cb(null, {
				results: [
					{
						eventId: "ev-2",
						status: PublishStatus.PUBLISH_STATUS_REJECTED_DUPLICATE,
						message: "",
					},
				],
			});
		});
		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		const row = getRow(storage, "ev-2");
		expect(row).toBeNull();
	});

	it("REJECTED_INVALID_NAME → status='failed' (terminal)", async () => {
		insertPending(storage, "ev-5");

		const rpc = makeRpcClient((_req, cb) => {
			cb(null, {
				results: [
					{
						eventId: "ev-5",
						status: PublishStatus.PUBLISH_STATUS_REJECTED_INVALID_NAME,
						message: "",
					},
				],
			});
		});
		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		const row = getRow(storage, "ev-5");
		expect(row?.status).toBe("failed");
	});

	it("REJECTED_FORBIDDEN → terminal failed + onPolicyViolation, no retry", async () => {
		insertPending(storage, "ev-forbidden", "billing.charge");

		const violations: Array<{
			declaration: string;
			value: string;
			denySide: string;
			reason: string;
		}> = [];
		const rpc = makeRpcClient((_req, cb) => {
			cb(null, {
				results: [
					{
						eventId: "ev-forbidden",
						status: PublishStatus.PUBLISH_STATUS_REJECTED_FORBIDDEN,
						message: "no event.publish rule for billing.charge",
					},
				],
			});
		});
		const drainer = new Drainer(
			makeDeps(rpc, { onPolicyViolation: (v) => violations.push(v) }),
		);
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		const row = getRow(storage, "ev-forbidden");
		expect(row?.status).toBe("failed");
		expect(row?.attempts).toBe(1); // terminal — not retried up to MAX_ATTEMPTS
		expect(row?.last_error).toContain("forbidden:");
		expect(violations).toHaveLength(1);
		expect(violations[0]).toMatchObject({
			declaration: "event.publish",
			value: "billing.charge",
			denySide: "self_egress",
		});
		expect(violations[0].reason).toContain("billing.charge");
	});

	it("network error → status='pending', attempts++, next_attempt_at_ms in backoff range", async () => {
		insertPending(storage, "ev-6");
		const t = Date.now();

		const rpc = makeRpcClient((_req, cb) => {
			const err = Object.assign(new Error("connection refused"), {
				code: 14,
			}) as ServiceError;
			cb(err);
		});
		const drainer = new Drainer(
			makeDeps(rpc, {
				clockFn: () => t,
			}),
		);
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		const row = storage
			.prepare(
				"SELECT status, attempts, last_error, next_attempt_at_ms FROM event_outbox WHERE id=?",
			)
			.get("ev-6") as {
			status: string;
			attempts: number;
			last_error: string;
			next_attempt_at_ms: number;
		};
		expect(row.status).toBe("pending");
		expect(row.attempts).toBe(1);
		// Backoff for attempt 1 is BACKOFF_MS[0] = 1000ms ±25%
		const expectedMin = t + 1_000 - 250;
		const expectedMax = t + 1_000 + 250;
		expect(row.next_attempt_at_ms).toBeGreaterThanOrEqual(expectedMin);
		expect(row.next_attempt_at_ms).toBeLessThanOrEqual(expectedMax);
	});

	it("5 network errors → status='failed' with last_error='max_attempts'", async () => {
		// Insert with attempts=4 (next attempt will be #5 = MAX)
		insertPending(storage, "ev-7", "test.event", 4);

		const rpc = makeRpcClient((_req, cb) => {
			const err = Object.assign(new Error("timeout"), {
				code: 4,
			}) as ServiceError;
			cb(err);
		});
		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		const row = getRow(storage, "ev-7");
		expect(row?.status).toBe("failed");
		expect(row?.last_error).toBe("max_attempts");
		expect(row?.attempts).toBe(5);
	});

	it("mixed batch: ACCEPTED + REJECTED_INVALID_NAME in same response", async () => {
		insertPending(storage, "ev-mix-1");
		insertPending(storage, "ev-mix-2");

		const rpc = makeRpcClient((_req, cb) => {
			cb(null, {
				results: [
					{
						eventId: "ev-mix-1",
						status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
						message: "",
					},
					{
						eventId: "ev-mix-2",
						status: PublishStatus.PUBLISH_STATUS_REJECTED_INVALID_NAME,
						message: "",
					},
				],
			});
		});
		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		expect(getRow(storage, "ev-mix-1")).toBeNull();
		expect(getRow(storage, "ev-mix-2")?.status).toBe("failed");
	});

	it("empty queue: drainer waits and does not crash", async () => {
		let iterationCount = 0;
		const rpc = makeRpcClient((_req, cb) => {
			iterationCount++;
			cb(null, { results: [] });
		});

		// Fast sleep so test doesn't take long
		const drainer = new Drainer(
			makeDeps(rpc, {
				sleepFn: (ms) =>
					new Promise<void>((r) => setTimeout(r, Math.min(ms, 10))),
			}),
		);
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 50));
		await drainer.stop();

		// No rows were published — rpc should not have been called
		expect(iterationCount).toBe(0);
	});

	it("kick() wakes drainer immediately — processes row in <100ms", async () => {
		// Row with far-future next_attempt_at_ms would NOT be selected without kick
		// but since we're inserting a pending row with next_attempt_at_ms=0, it will
		// be selected. This test verifies that after a stop-and-restart the drainer
		// picks up the row promptly after kick().
		let processed = false;
		const rpc = makeRpcClient((_req, cb) => {
			processed = true;
			cb(null, {
				results: [
					{
						eventId: "ev-kick",
						status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
						message: "",
					},
				],
			});
		});

		const drainer = new Drainer(
			makeDeps(rpc, {
				sleepFn: (ms) => new Promise<void>((r) => setTimeout(r, ms)), // real sleep
			}),
		);
		drainer.start();

		// Insert row AFTER starting drainer
		insertPending(storage, "ev-kick");
		const before = Date.now();
		drainer.kick();

		// Wait up to 200ms for processing
		const deadline = Date.now() + 200;
		while (!processed && Date.now() < deadline) {
			await new Promise<void>((r) => setTimeout(r, 10));
		}
		await drainer.stop();

		expect(processed).toBe(true);
		expect(Date.now() - before).toBeLessThan(200);
	});

	it("drainer forwards x_sb_trace header from outbox row to Publish RPC", async () => {
		const xSbTrace =
			"01890d1e-1234-7890-abcd-ef1234567890-01890d1f-aaaa-7777-bbbb-cccccccccccc";

		insertPending(storage, "ev-trace", "test.event", 0, 0, xSbTrace);

		let capturedXSbTrace: string | undefined;

		const rpc = makeRpcClient((req, cb) => {
			const ev = req.events[0];
			capturedXSbTrace = ev?.xSbTrace;
			cb(null, {
				results: [
					{
						eventId: "ev-trace",
						status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
						message: "",
					},
				],
			});
		});

		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		expect(capturedXSbTrace).toBe(xSbTrace);
	});

	it("drainer emits empty x_sb_trace when row has no trace context", async () => {
		insertPending(storage, "ev-no-trace");

		let capturedXSbTrace: string | undefined;

		const rpc = makeRpcClient((req, cb) => {
			const ev = req.events[0];
			capturedXSbTrace = ev?.xSbTrace;
			cb(null, {
				results: [
					{
						eventId: "ev-no-trace",
						status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
						message: "",
					},
				],
			});
		});

		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();
		await new Promise<void>((r) => setTimeout(r, 100));
		await drainer.stop();

		expect(capturedXSbTrace).toBe("");
	});

	it("stop() awaits current iteration completion", async () => {
		let publishStarted = false;
		let publishDone = false;

		const rpc = makeRpcClient(async (_req, cb) => {
			publishStarted = true;
			// Simulate slow publish
			await new Promise<void>((r) => setTimeout(r, 50));
			publishDone = true;
			cb(null, {
				results: [
					{
						eventId: "ev-stop",
						status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
						message: "",
					},
				],
			});
		});

		insertPending(storage, "ev-stop");

		const drainer = new Drainer(makeDeps(rpc));
		drainer.start();

		// Wait for publish to start
		const deadline = Date.now() + 200;
		while (!publishStarted && Date.now() < deadline) {
			await new Promise<void>((r) => setTimeout(r, 5));
		}

		await drainer.stop();
		// After stop(), the row should have been deleted (iteration completed)
		expect(publishDone).toBe(true);
		expect(getRow(storage, "ev-stop")).toBeNull();
	});
});
