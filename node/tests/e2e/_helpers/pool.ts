// pool.ts — service clients for e2e tests, keyed per domain.
//
// Each domain runs as its own `bun test` process against its own three service
// identities (e2e-<domain>-1/2/3), selected by SB_E2E_DOMAIN. That makes the
// domain the unit of both isolation and parallelism: domains run concurrently
// without sharing any identity, so their registrations and deliveries never
// cross. There is no shared/global fallback — a run with no SB_E2E_DOMAIN or no
// keys for it hard-fails.
//
// shared(role): warm client, started once per (process, role) and reused. Most
// tests use this; they isolate their work with unique names (namespace.ts), so
// handlers accumulating on a shared client never collide.
//   NOT for: jobs (a registered job fires for the whole process), workflow
//   OWNERS (the runtime resolves a run against definitions present at the
//   owner's registration, so the definition must exist BEFORE start()),
//   access-policy (mutates rules/caps on its identity), and connect/restart
//   tests. Those call dedicated(role) and own the client's lifecycle.
//
// dedicated(role): a fresh, UNSTARTED client under the same per-domain key. The
// test registers handlers, calls connect(), and stop()s it itself (afterEach).
//
// The warm pool is closed once at process exit via the global afterAll in
// setup.ts (a top-level afterAll in a Bun preload fires once per run).

import { ServiceBridge } from "../../../src/connection/service-bridge";

export type Role = "primary" | "second" | "third";

const CONNECT_TIMEOUT_MS = 10_000;

const POOL_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	// Tests never run long enough to rotate certs; keep the lead huge so the
	// refresh timer never fires and keeps the process alive past the suite.
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

const ROLE_INDEX: Record<Role, number> = { primary: 1, second: 2, third: 3 };

function domain(): string {
	const d = process.env.SB_E2E_DOMAIN;
	if (!d) {
		throw new Error(
			"pool: SB_E2E_DOMAIN not set — run a domain file via the e2e runner, or `SB_E2E_DOMAIN=<domain> bun test <domain>.test.ts`",
		);
	}
	return d;
}

function keyForRole(role: Role): { url: string; key: string } {
	const url = process.env.SERVICEBRIDGE_URL;
	if (!url) {
		throw new Error(
			"pool: SERVICEBRIDGE_URL not set — run `bash scripts/bootstrap-e2e-keys.sh`",
		);
	}
	const envName = `SB_E2E_${domain().toUpperCase().replace(/-/g, "_")}_${ROLE_INDEX[role]}`;
	const key = process.env[envName];
	if (!key) {
		throw new Error(
			`pool: ${envName} not set — run \`bash scripts/bootstrap-e2e-keys.sh\` to provision domain "${domain()}"`,
		);
	}
	return { url, key };
}

function buildClient(role: Role, tag: string): ServiceBridge {
	const { url, key } = keyForRole(role);
	return new ServiceBridge(url, key, {
		...POOL_OPTS,
		advertise: { host: "127.0.0.1", port: 0 },
		dataDir: `./.servicebridge-e2e/${domain()}-${tag}`,
	});
}

// The owner-side workflow subscriber only boots on Welcome, and only if a
// workflow handler is registered at that moment. A warm client is started bare,
// so without this sentinel its subscriber would never start and runs dispatched
// to it would hang. The sentinel is never started by anyone (fixed name, no
// policy permits it); real per-test handlers added later are dispatched because
// the runner looks handlers up dynamically by name.
const SENTINEL_WF = "__sb_pool_sentinel__";

function registerWorkflowSentinel(sb: ServiceBridge): void {
	sb.workflow.handle(SENTINEL_WF, {
		steps: [{ type: "local", id: "noop", fn: async () => ({}) }],
	});
}

// connect starts a client and resolves once it is connected (identity() set).
// Used by the pool and by dedicated clients a test owns and stop()s itself.
export async function connect(sb: ServiceBridge): Promise<ServiceBridge> {
	await new Promise<void>((resolve, reject) => {
		const timer = setTimeout(() => {
			reject(
				new Error(`pool: connect timed out after ${CONNECT_TIMEOUT_MS}ms`),
			);
		}, CONNECT_TIMEOUT_MS);
		sb.on("connected", () => {
			clearTimeout(timer);
			resolve();
		});
		sb.on("disconnected", (evt) => {
			clearTimeout(timer);
			reject(evt.error ?? new Error(evt.reason));
		});
		sb.start().catch((err: unknown) => {
			clearTimeout(timer);
			reject(err instanceof Error ? err : new Error(String(err)));
		});
	});
	return sb;
}

interface Entry {
	sb: ServiceBridge;
	ready: Promise<ServiceBridge>;
}

const pool = new Map<Role, Entry>();

// shared returns the warm client for the given role, started and connected
// (identity() is non-null on resolve). First call per role starts it.
export function shared(role: Role = "primary"): Promise<ServiceBridge> {
	let entry = pool.get(role);
	if (!entry) {
		const sb = buildClient(role, role);
		registerWorkflowSentinel(sb);
		entry = { sb, ready: connect(sb) };
		pool.set(role, entry);
	}
	return entry.ready;
}

let dedicatedCount = 0;

// dedicated returns a fresh UNSTARTED client under the role's per-domain key.
// The caller registers handlers, then connect()s and stop()s it. Each gets its
// own SQLite outbox so concurrent dedicated clients never share a file.
export function dedicated(role: Role = "primary"): ServiceBridge {
	return buildClient(role, `ded-${++dedicatedCount}`);
}

// closeAll stops every warm client. Called once from the global afterAll.
export async function closeAll(): Promise<void> {
	const entries = [...pool.values()];
	pool.clear();
	await Promise.all(entries.map((e) => e.sb.stop().catch(() => {})));
}
