/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Storage } from "./storage";

function countOutbox(storage: Storage): number {
	const row = storage
		.prepare("SELECT COUNT(*) AS c FROM event_outbox")
		.get() as { c: number } | null;
	return row?.c ?? 0;
}

describe("Storage", () => {
	let tmpDir: string;

	beforeEach(() => {
		tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "sb-storage-test-"));
	});

	afterEach(() => {
		fs.rmSync(tmpDir, { recursive: true, force: true });
	});

	it("open creates directory and db file", () => {
		const dataDir = path.join(tmpDir, "nested", "data");
		const storage = Storage.open({ dataDir });
		storage.close();
		expect(fs.existsSync(path.join(dataDir, "sdk.db"))).toBe(true);
	});

	it("schema idempotent on second open", () => {
		const dataDir = path.join(tmpDir, "data");
		const s1 = Storage.open({ dataDir });
		s1.close();
		// Should not throw — CREATE TABLE IF NOT EXISTS is idempotent.
		const s2 = Storage.open({ dataDir });
		s2.close();
	});

	it("creates event_outbox with all expected columns", () => {
		const dataDir = path.join(tmpDir, "data");
		const storage = Storage.open({ dataDir });
		const cols = storage.prepare("PRAGMA table_info(event_outbox)").all() as {
			name: string;
		}[];
		const colNames = cols.map((c) => c.name);
		for (const col of [
			"id",
			"name",
			"payload",
			"contract_hash",
			"partition_key",
			"idempotency_key",
			"fire_and_forget",
			"headers",
			"occurred_at_ms",
			"enqueued_at_ms",
			"status",
			"attempts",
			"last_error",
			"next_attempt_at_ms",
			"x_sb_trace",
		]) {
			expect(colNames).toContain(col);
		}
		storage.close();
	});

	it("resets inflight rows to pending on open (crash recovery)", () => {
		const dataDir = path.join(tmpDir, "data");
		const s1 = Storage.open({ dataDir });

		// Insert a row with status='inflight' simulating a crashed drainer
		s1.prepare(
			`INSERT INTO event_outbox
        (id, name, payload, contract_hash, occurred_at_ms, enqueued_at_ms, status)
        VALUES (?, ?, ?, ?, ?, ?, 'inflight')`,
		).run(
			"evt-1",
			"test.event",
			new Uint8Array([1, 2, 3]),
			"hash1",
			1000,
			1000,
		);

		const countBefore = s1
			.prepare("SELECT COUNT(*) AS c FROM event_outbox WHERE status='inflight'")
			.get() as { c: number };
		expect(countBefore.c).toBe(1);
		s1.close();

		// Re-open — inflight should be reset to pending
		const s2 = Storage.open({ dataDir });
		const countInflight = s2
			.prepare("SELECT COUNT(*) AS c FROM event_outbox WHERE status='inflight'")
			.get() as { c: number };
		const countPending = s2
			.prepare("SELECT COUNT(*) AS c FROM event_outbox WHERE status='pending'")
			.get() as { c: number };
		expect(countInflight.c).toBe(0);
		expect(countPending.c).toBe(1);
		s2.close();
	});

	it("transaction rolls back on error", () => {
		const dataDir = path.join(tmpDir, "data");
		const storage = Storage.open({ dataDir });

		expect(() => {
			storage.transaction(() => {
				storage
					.prepare(
						`INSERT INTO event_outbox
            (id, name, payload, contract_hash, occurred_at_ms, enqueued_at_ms, status)
            VALUES (?, ?, ?, ?, ?, ?, 'pending')`,
					)
					.run("row-1", "e", new Uint8Array([1]), "h", 1, 1);
				throw new Error("rollback me");
			});
		}).toThrow("rollback me");

		expect(countOutbox(storage)).toBe(0);
		storage.close();
	});

	it("defaults to ./.servicebridge when dataDir not provided", () => {
		const origCwd = process.cwd();
		process.chdir(tmpDir);
		try {
			const storage = Storage.open();
			storage.close();
			expect(fs.existsSync(path.join(tmpDir, ".servicebridge", "sdk.db"))).toBe(
				true,
			);
		} finally {
			process.chdir(origCwd);
		}
	});
});
