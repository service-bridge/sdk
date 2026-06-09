// dedicated-runtime.ts — helper for spawning an isolated runtime subprocess
// in e2e tests that require destructive kill+restart operations.
//
// Each call to spawnIsolatedRuntime() gets its own PostgreSQL database so it
// never conflicts with the ambient runtime on :14445/:14444 or with any other
// concurrent dedicated-runtime test.
//
// Mechanism:
//   1. Create a fresh DB: test_dedirt_<name>_<rand> via admin connection.
//   2. Run the binary with -migrate to apply migrations + seed defaults into
//      the isolated DB (exits immediately after).
//   3. UPDATE network.grpc_port / network.ui_port in runtime_settings to the
//      requested ports (ON CONFLICT DO NOTHING in Seed means our UPDATE wins).
//   4. Spawn the full runtime with POSTGRES_DB=<isolated>.
//   5. Return a handle with kill()/restart()/cleanup() methods.

import { createConnection } from "node:net";
import { join } from "node:path";
import { sleep } from "./events";

const RUNTIME_DIR = join(import.meta.dir, "../../../../../runtime");

const PG_HOST = process.env.POSTGRES_HOST ?? "localhost";
const PG_PORT = parseInt(process.env.POSTGRES_PORT ?? "5433", 10);
const PG_USER = process.env.POSTGRES_USER ?? "servicebridge";
const PG_PASSWORD = process.env.POSTGRES_PASSWORD ?? "servicebridge";

/**
 * adminUrl builds a connection URL to the postgres maintenance database so we
 * can CREATE / DROP databases without being connected to the target DB.
 */
function adminUrl(): string {
	return `postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/postgres`;
}

/**
 * isolatedDbUrl builds a connection URL to the isolated test database.
 */
function isolatedDbUrl(dbName: string): string {
	return `postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${dbName}`;
}

/**
 * isolatedDsn builds the runtime -pg-url DSN for the isolated test database.
 * sslmode=disable matches the local dev Postgres which has no TLS.
 */
function isolatedDsn(dbName: string): string {
	return `postgres://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${dbName}?sslmode=disable`;
}

async function createDatabase(dbName: string): Promise<void> {
	// Use new Bun.SQL() for explicit-URL connections so we don't rely on the
	// process-wide DATABASE_URL singleton (which is already claimed by policy-db.ts).
	const db = new Bun.SQL(adminUrl());
	try {
		// DDL identifiers cannot be parameterized; dbName is alphanumeric + _ only.
		await db.unsafe(`CREATE DATABASE "${dbName}"`);
	} finally {
		await db.close();
	}
}

async function dropDatabase(dbName: string): Promise<void> {
	const db = new Bun.SQL(adminUrl());
	try {
		await db.unsafe(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
	} finally {
		await db.close();
	}
}

const PG_MAIN_DB =
	process.env.POSTGRES_DB ??
	process.env.TEST_DATABASE_URL?.match(/\/([^/?]+)(\?|$)/)?.[1] ??
	"servicebridge";

/**
 * seedServices copies all rows from the main services table into the isolated
 * DB so that existing bootstrap keys (from .env.e2e) are recognized by the
 * isolated runtime. Both runtimes share the same CA (certs/ca.crt), so TLS
 * trust is preserved. The secret_hash is the bcrypt hash — copying it is safe
 * because the raw secret never leaves the calling process.
 */
async function seedServices(dbName: string): Promise<void> {
	const mainUrl = `postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_MAIN_DB}`;
	const srcDb = new Bun.SQL(mainUrl);
	const dstDb = new Bun.SQL(isolatedDbUrl(dbName));
	try {
		const rows = (await srcDb`
			SELECT id, name, key_id, secret_hash, status, persist_ops,
			       persist_logs, persist_metrics,
			       cap_rpc_handle, cap_event_handle, cap_workflow_handle, cap_job_handle,
			       cap_rpc_call, cap_event_publish, cap_workflow_run, created_at
			FROM services
		`) as Array<Record<string, unknown>>;

		for (const row of rows) {
			await dstDb.unsafe(
				`
				INSERT INTO services
					(id, name, key_id, secret_hash, status, persist_ops,
					 persist_logs, persist_metrics,
					 cap_rpc_handle, cap_event_handle, cap_workflow_handle, cap_job_handle,
					 cap_rpc_call, cap_event_publish, cap_workflow_run, created_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
				ON CONFLICT (id) DO NOTHING
			`,
				[
					row.id,
					row.name,
					row.key_id,
					row.secret_hash,
					row.status,
					row.persist_ops,
					row.persist_logs,
					row.persist_metrics,
					row.cap_rpc_handle,
					row.cap_event_handle,
					row.cap_workflow_handle,
					row.cap_job_handle,
					row.cap_rpc_call,
					row.cap_event_publish,
					row.cap_workflow_run,
					row.created_at,
				],
			);
		}
	} finally {
		await srcDb.close();
		await dstDb.close();
	}
}

/**
 * runMigrateOnly runs the runtime binary with -migrate flag against the
 * isolated database. This applies all migrations and seeds default settings,
 * then the process exits. Throws if the binary exits non-zero.
 */
async function runMigrateOnly(
	binaryPath: string,
	dbName: string,
): Promise<void> {
	const p = Bun.spawn(
		[binaryPath, "-pg-url", isolatedDsn(dbName), "-migrate"],
		{
			cwd: RUNTIME_DIR,
			stdout: "pipe",
			stderr: "pipe",
		},
	);

	const code = await p.exited;
	if (code !== 0) {
		const stderr = await new Response(p.stderr).text();
		const stdout = await new Response(p.stdout).text();
		throw new Error(
			`dedicated-runtime: migrate-only failed (code ${code}):\nstderr: ${stderr}\nstdout: ${stdout}`,
		);
	}
}

/**
 * updateSettings sets runtime_settings rows in the isolated DB. Used to
 * configure ports and any other DB-backed settings before startup. settings.Seed
 * uses ON CONFLICT DO NOTHING, so these UPDATEs win over the seeded defaults.
 */
async function updateSettings(
	dbName: string,
	settings: Record<string, string>,
): Promise<void> {
	const db = new Bun.SQL(isolatedDbUrl(dbName));
	try {
		for (const [key, value] of Object.entries(settings)) {
			await db`
				UPDATE runtime_settings
				SET value = ${value}, updated_at = now()
				WHERE key = ${key}
			`;
		}
	} finally {
		await db.close();
	}
}

/**
 * waitForRuntimeReady polls grpcPort with a plain TCP connect until the port
 * is accepting connections. This avoids authentication (provision()) which
 * requires a key seeded in the target DB — isolated DBs start empty.
 */
async function waitForRuntimeReady(
	grpcPort: number,
	_bootstrapKey: string,
	timeoutMs = 60_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	let lastErr: unknown;
	while (Date.now() < deadline) {
		try {
			await new Promise<void>((resolve, reject) => {
				const socket = createConnection(grpcPort, "localhost");
				socket.once("connect", () => {
					socket.destroy();
					resolve();
				});
				socket.once("error", (err) => {
					socket.destroy();
					reject(err);
				});
			});
			// Give the gRPC server a moment to fully initialize after TCP accept.
			await sleep(300);
			return;
		} catch (e) {
			lastErr = e;
		}
		await sleep(500);
	}
	throw new Error(
		`dedicated-runtime: gRPC port ${grpcPort} not ready after ${timeoutMs}ms: ${String(lastErr)}`,
	);
}

async function killProcessAndWait(proc: Bun.Subprocess): Promise<void> {
	try {
		proc.kill("SIGTERM");
	} catch {}
	const termDeadline = Date.now() + 5_000;
	while (Date.now() < termDeadline) {
		if (proc.exitCode !== null) return;
		await sleep(100);
	}
	try {
		proc.kill("SIGKILL");
	} catch {}
	const killDeadline = Date.now() + 5_000;
	while (Date.now() < killDeadline) {
		if (proc.exitCode !== null) return;
		await sleep(100);
	}
}

export interface DedicatedRuntime {
	/** gRPC port the runtime is listening on. */
	grpcPort: number;
	/** gRPC URL: "localhost:<grpcPort>". */
	url: string;
	/** Underlying subprocess (current process after a restart). */
	proc: Bun.Subprocess;

	/**
	 * Kill the current runtime process and wait for it to exit.
	 * Does NOT restart — call restart() if you want a fresh process.
	 */
	kill(): Promise<void>;

	/**
	 * Restart: spawn a fresh process against the same isolated DB/ports and
	 * wait until gRPC is ready. Replaces this.proc.
	 */
	restart(bootstrapKey: string, timeoutMs?: number): Promise<void>;

	/**
	 * cleanup: kill the process (if still running) and drop the isolated DB.
	 * Call in afterAll. Safe to call multiple times.
	 */
	cleanup(): Promise<void>;
}

export interface SpawnIsolatedRuntimeOpts {
	/** Short identifier used in the DB name (e.g. "sleep-restart"). */
	name: string;
	/** gRPC port the runtime should bind to. */
	grpcPort: number;
	/** UI port the runtime should bind to. */
	uiPort: number;
	/** Bootstrap key used to probe gRPC readiness. */
	bootstrapKey: string;
	/** Path to the pre-built runtime binary. */
	binaryPath: string;
	/** Optional extra env vars to pass to the runtime process. */
	extraEnv?: Record<string, string>;
	/**
	 * Optional extra DB-backed settings to apply before first startup.
	 * Keys are runtime_settings keys (e.g. "jobs.heartbeat_timeout_ms"),
	 * values are string representations of the setting value.
	 */
	extraSettings?: Record<string, string>;
	/** Timeout for waiting until the runtime is ready. Default: 60_000ms. */
	readyTimeoutMs?: number;
}

/**
 * spawnIsolatedRuntime creates a fresh PostgreSQL database, applies
 * migrations, configures the requested ports, spawns the runtime binary
 * against that isolated DB, and waits until gRPC is ready.
 *
 * Returns a DedicatedRuntime handle that supports kill/restart/cleanup.
 */
export async function spawnIsolatedRuntime(
	opts: SpawnIsolatedRuntimeOpts,
): Promise<DedicatedRuntime> {
	const rand = Math.random().toString(36).slice(2, 8);
	// Validate name: only alphanumeric and hyphens allowed to produce a safe DB name.
	const safeName = opts.name.replace(/[^a-z0-9-]/gi, "_").slice(0, 20);
	const dbName = `test_dedirt_${safeName}_${rand}`;

	await createDatabase(dbName);

	await runMigrateOnly(opts.binaryPath, dbName);

	const allSettings: Record<string, string> = {
		"network.grpc_port": String(opts.grpcPort),
		"network.ui_port": String(opts.uiPort),
		...(opts.extraSettings ?? {}),
	};
	await updateSettings(dbName, allSettings);

	// Copy service rows from the main DB so the .env.e2e bootstrap keys are
	// recognized by this isolated runtime (both share the same CA cert).
	await seedServices(dbName);

	const spawnEnv: Record<string, string> = {
		...(process.env as Record<string, string>),
		...(opts.extraEnv ?? {}),
	};

	function spawnProcess(): Bun.Subprocess {
		return Bun.spawn([opts.binaryPath, "-pg-url", isolatedDsn(dbName)], {
			cwd: RUNTIME_DIR,
			stdout: "pipe",
			stderr: "pipe",
			env: spawnEnv,
		});
	}

	let proc = spawnProcess();
	await waitForRuntimeReady(
		opts.grpcPort,
		opts.bootstrapKey,
		opts.readyTimeoutMs ?? 60_000,
	);

	let killed = false;

	const handle: DedicatedRuntime = {
		grpcPort: opts.grpcPort,
		url: `localhost:${opts.grpcPort}`,

		get proc() {
			return proc;
		},

		async kill(): Promise<void> {
			if (killed) return;
			killed = true;
			await killProcessAndWait(proc);
		},

		async restart(bootstrapKey: string, timeoutMs = 60_000): Promise<void> {
			// Kill old process if still alive.
			if (!killed) {
				await killProcessAndWait(proc);
			}
			killed = false;
			proc = spawnProcess();
			await waitForRuntimeReady(opts.grpcPort, bootstrapKey, timeoutMs);
		},

		async cleanup(): Promise<void> {
			if (!killed) {
				try {
					await killProcessAndWait(proc);
				} catch {}
				killed = true;
			}
			await dropDatabase(dbName).catch(() => {});
		},
	};

	return handle;
}

/**
 * buildDedicatedRuntime compiles the runtime binary to outputPath.
 * Shared utility so each test file doesn't duplicate the build logic.
 */
export async function buildDedicatedRuntime(outputPath: string): Promise<void> {
	const p = Bun.spawn(["go", "build", "-o", outputPath, "./cmd/runtime"], {
		cwd: RUNTIME_DIR,
		stdout: "pipe",
		stderr: "pipe",
	});
	const code = await p.exited;
	if (code !== 0) {
		const stderr = await new Response(p.stderr).text();
		throw new Error(`buildDedicatedRuntime: go build failed: ${stderr}`);
	}
}
