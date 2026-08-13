import * as fs from "node:fs";
import { createRequire } from "node:module";

// @public — см. ./README.md
export interface StorageOpenOpts {
	dataDir?: string;
}

// Prepared statement surface the outbox relies on. Both bun:sqlite and
// better-sqlite3 satisfy it (positional `?` binds, run/get/all).
// @internal — см. ./README.md
interface SqliteStatement {
	run(...params: unknown[]): unknown;
	get(...params: unknown[]): unknown;
	all(...params: unknown[]): unknown[];
}

// Database surface the outbox relies on. The common subset of bun:sqlite and
// better-sqlite3.
// @internal — см. ./README.md
interface SqliteDatabase {
	exec(sql: string): unknown;
	prepare(sql: string): SqliteStatement;
	transaction<T>(fn: (...args: unknown[]) => T): (...args: unknown[]) => T;
	close(): void;
}

type SqliteConstructor = new (path: string) => SqliteDatabase;

// SCHEMA_VERSION is stamped into the database file via PRAGMA user_version.
// A file carrying any other version is rejected on open — the SDK does not
// migrate outbox schemas.
// @internal — см. ./README.md
const SCHEMA_VERSION = 1;

// openDatabase loads the native SQLite driver for the current runtime and opens
// the file at `path`. Bun ships `bun:sqlite`; plain Node uses `better-sqlite3`.
// Both are loaded synchronously via createRequire so Storage.open stays sync,
// and the specifier stays a runtime string so a Node bundler never tries to
// resolve `bun:sqlite`. The two drivers share the run/get/all/transaction/exec
// surface this module uses.
function openDatabase(path: string): SqliteDatabase {
	const req = createRequire(import.meta.url);
	const isBun = typeof (globalThis as { Bun?: unknown }).Bun !== "undefined";
	if (isBun) {
		const { Database } = req("bun:sqlite") as { Database: SqliteConstructor };
		return new Database(path);
	}
	const Database = req("better-sqlite3") as SqliteConstructor;
	return new Database(path);
}

// assertSchemaVersion rejects a database file written by an SDK with a
// different outbox schema. A file that has no tables yet is a fresh one and is
// stamped by the caller after the schema is created.
// @internal
function assertSchemaVersion(
	db: SqliteDatabase,
	dir: string,
	file: string,
): void {
	const row = db.prepare("PRAGMA user_version").get() as {
		user_version: number;
	} | null;
	const version = row?.user_version ?? 0;
	if (version === SCHEMA_VERSION) return;

	const existing = db
		.prepare(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='event_outbox'",
		)
		.get();
	if (version === 0 && (existing === null || existing === undefined)) return;

	throw new Error(
		`sqlite: ${file} holds outbox schema v${version}, this SDK requires v${SCHEMA_VERSION}. ` +
			`Outbox schemas are not migrated: stop the service, delete ${dir} and start again. ` +
			"Events still buffered in the old file are lost.",
	);
}

// Storage wraps a native SQLite database with the event_outbox schema, WAL
// mode, and crash-recovery reset of in-flight outbox rows. It runs on plain
// Node (better-sqlite3) and on Bun (bun:sqlite) with identical behavior.
// @public — см. ./README.md
export class Storage {
	private readonly db: SqliteDatabase;
	private readonly statements = new Map<string, SqliteStatement>();
	private readonly runTx: (fn: () => unknown) => unknown;
	private outboxRows: number;
	private txDepth = 0;
	private txOutboxDelta = 0;

	private constructor(db: SqliteDatabase, outboxRows: number) {
		this.db = db;
		this.outboxRows = outboxRows;
		// Compiled once: both drivers rebuild BEGIN/COMMIT/ROLLBACK on every
		// transaction() call, and publish() opens a transaction per event.
		this.runTx = db.transaction((...args: unknown[]) =>
			(args[0] as () => unknown)(),
		) as (fn: () => unknown) => unknown;
	}

	// open creates or opens the SQLite database at dataDir/sdk.db, ensures the
	// event_outbox schema exists (CREATE IF NOT EXISTS), enables WAL mode, and
	// resets any rows stuck in 'inflight' status from a previous crash
	// (reset-on-start crash recovery). Throws when the file was written with a
	// different schema version.
	static open(opts?: StorageOpenOpts): Storage {
		const dir = opts?.dataDir ?? "./.servicebridge";
		fs.mkdirSync(dir, { recursive: true });

		const file = `${dir}/sdk.db`;
		const db = openDatabase(file);
		db.exec("PRAGMA journal_mode = WAL");
		db.exec("PRAGMA synchronous = NORMAL");

		assertSchemaVersion(db, dir, file);

		db.exec(`
CREATE TABLE IF NOT EXISTS event_outbox (
  id                TEXT    PRIMARY KEY,
  name              TEXT    NOT NULL,
  payload           BLOB    NOT NULL,
  payload_json      BLOB    NOT NULL DEFAULT x'',
  contract_hash     TEXT    NOT NULL,
  partition_key     TEXT    NOT NULL DEFAULT '',
  idempotency_key   TEXT    NOT NULL DEFAULT '',
  fire_and_forget   INTEGER NOT NULL DEFAULT 0,
  headers           TEXT    NOT NULL DEFAULT '{}',
  occurred_at_ms    INTEGER NOT NULL,
  enqueued_at_ms    INTEGER NOT NULL,
  status            TEXT    NOT NULL CHECK(status IN ('pending','inflight','failed')),
  attempts          INTEGER NOT NULL DEFAULT 0,
  last_error        TEXT    NOT NULL DEFAULT '',
  next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
  x_sb_trace        TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS event_outbox_pending_order_idx
  ON event_outbox(enqueued_at_ms, id)
  WHERE status='pending';
		`);
		db.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);

		// Crash recovery: any rows left as 'inflight' by a previous drainer run
		// that was interrupted (SIGKILL, process crash) are reset to 'pending'
		// so they will be retried on this start.
		db.exec("UPDATE event_outbox SET status='pending' WHERE status='inflight'");

		const counted = db
			.prepare("SELECT COUNT(*) AS c FROM event_outbox")
			.get() as { c: number } | null;

		return new Storage(db, counted?.c ?? 0);
	}

	// transaction executes fn inside a SQLite transaction. The driver serializes
	// all transactions — this is intentional (cap check + INSERT in one atomic
	// op). Outbox row-count deltas recorded by fn are published to the cached
	// count only after the transaction commits.
	transaction<T>(fn: () => T): T {
		if (this.txDepth > 0) {
			throw new Error("sqlite: transaction() is not reentrant");
		}
		this.txDepth = 1;
		this.txOutboxDelta = 0;
		try {
			const result = this.runTx(fn) as T;
			this.outboxRows += this.txOutboxDelta;
			return result;
		} finally {
			this.txDepth = 0;
			this.txOutboxDelta = 0;
		}
	}

	// prepare returns a prepared statement for the given SQL, compiling it on
	// first use and reusing it afterwards. Callers on the hot path (publish,
	// drain) pass constant SQL, so the cache is bounded by the number of
	// distinct statements the outbox issues.
	prepare(sql: string): SqliteStatement {
		const cached = this.statements.get(sql);
		if (cached !== undefined) return cached;
		const stmt = this.db.prepare(sql);
		this.statements.set(sql, stmt);
		return stmt;
	}

	// outboxRowCount returns the number of rows in event_outbox without touching
	// the database: SELECT COUNT(*) is O(rows) and the publish path checks the
	// cap on every event.
	outboxRowCount(): number {
		return this.outboxRows + this.txOutboxDelta;
	}

	// adjustOutboxRowCount records rows added (positive delta) or removed
	// (negative delta) by the caller's own INSERT/DELETE. Inside a transaction
	// the delta is applied on commit, so a rollback leaves the count untouched.
	adjustOutboxRowCount(delta: number): void {
		if (this.txDepth > 0) {
			this.txOutboxDelta += delta;
			return;
		}
		this.outboxRows += delta;
	}

	close(): void {
		this.statements.clear();
		this.db.close();
	}
}
