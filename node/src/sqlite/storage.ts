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

// Storage wraps a native SQLite database with the event_outbox schema, WAL
// mode, and crash-recovery reset of in-flight outbox rows. It runs on plain
// Node (better-sqlite3) and on Bun (bun:sqlite) with identical behavior.
// @public — см. ./README.md
export class Storage {
	private readonly db: SqliteDatabase;

	private constructor(db: SqliteDatabase) {
		this.db = db;
	}

	// open creates or opens the SQLite database at dataDir/sdk.db, ensures the
	// event_outbox schema exists (CREATE IF NOT EXISTS), enables WAL mode, and
	// resets any rows stuck in 'inflight' status from a previous crash
	// (reset-on-start crash recovery).
	static open(opts?: StorageOpenOpts): Storage {
		const dir = opts?.dataDir ?? "./.servicebridge";
		fs.mkdirSync(dir, { recursive: true });

		const db = openDatabase(`${dir}/sdk.db`);
		db.exec("PRAGMA journal_mode = WAL");
		db.exec("PRAGMA synchronous = NORMAL");

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
CREATE INDEX IF NOT EXISTS event_outbox_pending_idx
  ON event_outbox(next_attempt_at_ms)
  WHERE status='pending';
		`);

		// An existing on-disk outbox DB may lack payload_json / x_sb_trace, and
		// CREATE TABLE IF NOT EXISTS won't add columns to an existing table — so
		// Publisher.publish() would INSERT into a missing column. Reconcile the
		// schema via PRAGMA inspection + ADD COLUMN (idempotent).
		const cols = (
			db.prepare("PRAGMA table_info(event_outbox)").all() as { name: string }[]
		).map((r) => r.name);
		if (!cols.includes("payload_json")) {
			db.exec(
				"ALTER TABLE event_outbox ADD COLUMN payload_json BLOB NOT NULL DEFAULT x''",
			);
		}
		if (!cols.includes("x_sb_trace")) {
			db.exec(
				"ALTER TABLE event_outbox ADD COLUMN x_sb_trace TEXT NOT NULL DEFAULT ''",
			);
		}

		// Crash recovery: any rows left as 'inflight' by a previous drainer run
		// that was interrupted (SIGKILL, process crash) are reset to 'pending'
		// so they will be retried on this start.
		db.exec("UPDATE event_outbox SET status='pending' WHERE status='inflight'");

		return new Storage(db);
	}

	// transaction executes fn inside a SQLite transaction. The driver serializes
	// all transactions — this is intentional (cap check + INSERT in one atomic
	// op).
	transaction<T>(fn: () => T): T {
		return this.db.transaction(fn)();
	}

	// prepare returns a prepared statement for the given SQL.
	prepare(sql: string): SqliteStatement {
		return this.db.prepare(sql);
	}

	close(): void {
		this.db.close();
	}
}
