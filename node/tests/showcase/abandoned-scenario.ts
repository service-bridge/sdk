#!/usr/bin/env bun
/**
 * abandoned-scenario.ts — produces an RPC.HANDLE op in ABANDONED state so the
 * UI can render the striped gray-red bar with the "instance disconnected"
 * tooltip.
 *
 * Mechanic: caller invokes a long handler that calls Bun.sleep(60_000). After
 * 3 s a side child process kills the *callee* with SIGKILL — the runtime
 * grace+sweep promotes the in-flight RPC.HANDLE to ABANDONED. The caller sees
 * Unavailable on its side.
 *
 * We use a child process for the callee so the parent script can survive the
 * kill and still print the trace URL. Both processes use bootstrap keys from
 * .env.showcase (provisioned by showcase-keys.ts on first run).
 */

import { spawn } from "node:child_process";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { grantRpcWildcard } from "./policy-helpers";
import { ensureShowcaseKeys } from "./showcase-keys";

const SHOWCASE_PROTO = join(import.meta.dir, "showcase.proto");
const ABANDONED_METHOD = "LongHandler";
const CHILD_MARKER_ENV = "SHOWCASE_ABANDONED_CHILD";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
	pred: () => boolean,
	timeoutMs: number,
	what: string,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (pred()) return;
		await sleep(50);
	}
	throw new Error(`waitFor: timed out after ${timeoutMs}ms — ${what}`);
}

async function runCallee(): Promise<void> {
	// Callee child process: register the LongHandler, sleep 60s in the handler,
	// then wait for kill. Stdout/stderr inherit from parent so logs are visible.
	const showcase = ensureShowcaseKeys();
	const callee = new ServiceBridge(
		showcase.runtimeUrl,
		showcase.keys["abandoned-demo-service"],
		FAST_OPTS,
	);
	callee.rpc.handle(
		ABANDONED_METHOD,
		async (req: { note: string }) => {
			console.error(
				`abandoned-callee: handler entered with note=${req.note} — sleeping 60s`,
			);
			await sleep(60_000);
			return { note: req.note };
		},
		{ schema: { protoFile: SHOWCASE_PROTO } },
	);
	await callee.start();
	await waitFor(
		() => callee.identity() !== null,
		5_000,
		"abandoned-callee connected",
	);
	console.error(
		`abandoned-callee: connected as ${callee.identity()?.serviceId} — pid ${process.pid}`,
	);
	// Hold the process until killed.
	await new Promise(() => undefined);
}

async function runCaller(): Promise<{ traceId: string }> {
	const showcase = ensureShowcaseKeys();

	// Caller uses the showcase-service key (a separate identity from the
	// callee). One-time policy grant in both directions.
	const caller = new ServiceBridge(
		showcase.runtimeUrl,
		showcase.keys["showcase-service"],
		FAST_OPTS,
	);
	caller.service("abandoned-demo-service", { rpc: [ABANDONED_METHOD] });
	await caller.useSchema("abandoned-demo-service", ABANDONED_METHOD, {
		protoFile: SHOWCASE_PROTO,
	});
	await caller.start();
	await waitFor(
		() => caller.identity() !== null,
		5_000,
		"abandoned-caller connected",
	);
	const callerIdent = caller.identity();
	if (!callerIdent) throw new Error("abandoned-caller: identity is null");
	const callerId = callerIdent.serviceId;

	// Spawn the callee in a child process so we can SIGKILL just the callee.
	console.error("abandoned-scenario: spawning callee child process ...");
	const child = spawn(process.execPath, [process.argv[1] as string], {
		env: { ...process.env, [CHILD_MARKER_ENV]: "1" },
		stdio: "inherit",
	});
	// Give the callee time to register and seed its policy row.
	await sleep(3_000);

	// Look up callee identity from DB and grant rpc.call/handle in both
	// directions (callee → caller acceptance not needed; we need caller egress
	// + callee acceptance).
	const { sql } = await import("bun");
	const prev = process.env.DATABASE_URL;
	process.env.DATABASE_URL =
		process.env.TEST_DATABASE_URL ??
		"postgresql://servicebridge:servicebridge@localhost:5433/servicebridge";
	let calleeId: string;
	try {
		const rows = (await sql`
			SELECT id::text AS id
			FROM services
			WHERE name = 'abandoned-demo-service' AND status = 'active'
			ORDER BY created_at DESC LIMIT 1
		`) as Array<{ id: string }>;
		if (!rows[0]) throw new Error("abandoned-demo-service row not found in DB");
		calleeId = rows[0].id;
	} finally {
		if (prev === undefined) delete process.env.DATABASE_URL;
		else process.env.DATABASE_URL = prev;
	}
	await grantRpcWildcard(callerId, calleeId, ABANDONED_METHOD);
	await sleep(1_500); // NOTIFY propagation

	// Issue the long call; schedule SIGKILL of the callee 3 s in.
	const killTimer = setTimeout(() => {
		console.error(
			`abandoned-scenario: sending SIGKILL to callee pid=${child.pid}`,
		);
		try {
			child.kill("SIGKILL");
		} catch {
			// ignore
		}
	}, 3_000);

	let lastError: unknown = null;
	try {
		await caller.rpc.call<{ note: string }, { note: string }>(
			"abandoned-demo-service",
			ABANDONED_METHOD,
			{ note: "showcase-abandoned" },
		);
	} catch (err) {
		lastError = err;
		console.error(
			`abandoned-scenario: caller saw expected failure: ${(err as Error).message}`,
		);
	} finally {
		clearTimeout(killTimer);
	}

	// Wait long enough for the runtime grace+sweep to promote RPC.HANDLE → ABANDONED.
	console.error(
		"abandoned-scenario: waiting 30s for runtime grace+sweep to flip RPC.HANDLE → ABANDONED ...",
	);
	await sleep(30_000);

	// Look up the trace_id of the latest RPC.HANDLE op for the dead callee.
	const { sql: sql2 } = await import("bun");
	process.env.DATABASE_URL =
		process.env.TEST_DATABASE_URL ??
		"postgresql://servicebridge:servicebridge@localhost:5433/servicebridge";
	let traceId: string;
	try {
		const rows = (await sql2`
			SELECT trace_id::text AS trace_id
			FROM operations
			WHERE channel = 2 AND kind = 3 AND actor_service_id = ${calleeId}::uuid
			ORDER BY started_at DESC LIMIT 1
		`) as Array<{ trace_id: string }>;
		traceId = rows[0]?.trace_id ?? "";
	} finally {
		if (prev === undefined) delete process.env.DATABASE_URL;
		else process.env.DATABASE_URL = prev;
	}

	await caller.stop();
	void lastError;
	return { traceId };
}

async function main(): Promise<void> {
	if (process.env[CHILD_MARKER_ENV]) {
		await runCallee();
		return;
	}
	const { traceId } = await runCaller();
	console.log("");
	console.log("Abandoned scenario trace ready:");
	if (traceId) {
		console.log(`  http://localhost:14444/traces/${traceId}`);
	} else {
		console.log("  (no RPC.HANDLE op found yet — re-check the DB in a minute)");
	}
}

if (import.meta.main) {
	main().catch((err) => {
		console.error("abandoned-scenario: fatal:", err);
		process.exit(1);
	});
}
