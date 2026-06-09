// showcase-keys.ts — provision + cache bootstrap keys for all showcase services.
//
// The showcase needs 8 distinct service identities (showcase-service,
// billing-service, inventory-service, external-api-service, audit-subscriber,
// notification-subscriber-1..3, abandoned-demo-service). Each must have its
// own row in `services` so policy rules can target distinct peers. We shell out
// to `go run ./cmd/sbkey-gen` — same provisioning path as bootstrap-e2e-keys.sh
// — and cache the resulting bootstrap keys in <repo>/.env.showcase so re-runs
// don't keep churning rows.
//
// Each key is name-stable: re-provisioning revokes the prior `active` row for
// that name (same idempotency strategy as the e2e bootstrap) and writes a fresh
// key, so a CA rotation or stale cache simply requires re-running the script.

import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..", "..");
const CACHE_FILE = resolve(REPO_ROOT, ".env.showcase");
const CA_CERT = resolve(REPO_ROOT, "runtime", "certs", "ca.crt");
const CA_KEY = resolve(REPO_ROOT, "runtime", "certs", "ca.key");
const POSTGRES_DSN =
	process.env.POSTGRES_DSN ??
	"postgres://servicebridge:servicebridge@localhost:5433/servicebridge?sslmode=disable";
const PG_CONTAINER = process.env.PG_CONTAINER ?? "servicebridge2-pg";

export type ShowcaseRole =
	| "showcase-service"
	| "billing-service"
	| "inventory-service"
	| "external-api-service"
	| "audit-subscriber"
	| "notification-subscriber-1"
	| "notification-subscriber-2"
	| "notification-subscriber-3"
	| "abandoned-demo-service";

export const ALL_ROLES: readonly ShowcaseRole[] = [
	"showcase-service",
	"billing-service",
	"inventory-service",
	"external-api-service",
	"audit-subscriber",
	"notification-subscriber-1",
	"notification-subscriber-2",
	"notification-subscriber-3",
	"abandoned-demo-service",
] as const;

export interface ShowcaseKeys {
	runtimeUrl: string;
	keys: Record<ShowcaseRole, string>;
}

function parseDotEnv(content: string): Record<string, string> {
	const out: Record<string, string> = {};
	for (const raw of content.split(/\r?\n/)) {
		const line = raw.trim();
		if (!line || line.startsWith("#")) continue;
		const eq = line.indexOf("=");
		if (eq < 0) continue;
		out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
	}
	return out;
}

function envKey(role: ShowcaseRole): string {
	return `SHOWCASE_${role.toUpperCase().replace(/-/g, "_")}_KEY`;
}

function readCache(): ShowcaseKeys | null {
	if (!existsSync(CACHE_FILE)) return null;
	const parsed = parseDotEnv(readFileSync(CACHE_FILE, "utf8"));
	const url = parsed.SHOWCASE_URL ?? parsed.SERVICEBRIDGE_URL;
	if (!url) return null;
	const keys: Partial<Record<ShowcaseRole, string>> = {};
	for (const role of ALL_ROLES) {
		const v = parsed[envKey(role)];
		if (!v) return null;
		keys[role] = v;
	}
	return { runtimeUrl: url, keys: keys as Record<ShowcaseRole, string> };
}

function writeCache(payload: ShowcaseKeys): void {
	const lines = [
		"# Showcase fixture keys — gitignored, regenerate via showcase-workflow.ts.",
		`# Generated: ${new Date().toISOString()}`,
		`SHOWCASE_URL=${payload.runtimeUrl}`,
	];
	for (const role of ALL_ROLES) {
		lines.push(`${envKey(role)}=${payload.keys[role]}`);
	}
	writeFileSync(CACHE_FILE, `${lines.join("\n")}\n`);
}

function quiesceExistingRows(role: ShowcaseRole): void {
	// Match bootstrap-e2e-keys.sh quiesce_service: revoke prior `active` rows
	// for this name and detach their service_instances so the new bootstrap
	// lands cleanly. Without this, calling sbkey-gen on a name with an existing
	// `active` row produces a duplicate `services` row that the runtime then
	// has to disambiguate by created_at — fine, but cleaner to revoke first.
	const sql = `
UPDATE service_instances
   SET status = 'disconnected', call_endpoint = '', http_endpoint = ''
 WHERE service_id IN (SELECT id FROM services WHERE name = '${role}')
   AND status = 'connected';
UPDATE services SET status = 'revoked' WHERE name = '${role}' AND status = 'active';
`;
	const res = spawnSync(
		"docker",
		[
			"exec",
			"-i",
			PG_CONTAINER,
			"psql",
			"-U",
			"servicebridge",
			"-d",
			"servicebridge",
			"-v",
			"ON_ERROR_STOP=1",
			"-q",
		],
		{ input: sql, encoding: "utf8" },
	);
	if (res.status !== 0) {
		throw new Error(
			`quiesce ${role} failed: status=${res.status} stderr=${res.stderr}`,
		);
	}
}

function provisionOne(role: ShowcaseRole): string {
	quiesceExistingRows(role);
	const res = spawnSync(
		"go",
		[
			"run",
			"./cmd/sbkey-gen",
			"-dsn",
			POSTGRES_DSN,
			"-ca-cert",
			CA_CERT,
			"-ca-key",
			CA_KEY,
			"-name",
			role,
		],
		{ cwd: resolve(REPO_ROOT, "runtime"), encoding: "utf8" },
	);
	if (res.status !== 0) {
		throw new Error(
			`sbkey-gen ${role} failed: status=${res.status} stderr=${res.stderr}`,
		);
	}
	// stdout last line is the key (`sb.<base64url...>`) per cmd/sbkey-gen.
	const lines = res.stdout.trim().split(/\r?\n/);
	const key = lines[lines.length - 1];
	if (!key?.startsWith("sb.")) {
		throw new Error(
			`sbkey-gen ${role} produced unexpected stdout: ${res.stdout}`,
		);
	}
	return key;
}

/**
 * Ensure cached keys exist for every ShowcaseRole. On cache miss or `force`,
 * provisions fresh keys via sbkey-gen and writes <repo>/.env.showcase. Returns
 * the full set of keys + the runtime URL the keys are bound to (the CA
 * embedded in the key is tied to a specific runtime instance).
 */
export function ensureShowcaseKeys(opts?: {
	force?: boolean;
	runtimeUrl?: string;
}): ShowcaseKeys {
	const url =
		opts?.runtimeUrl ?? process.env.SERVICEBRIDGE_URL ?? "localhost:14445";
	if (!opts?.force) {
		const cached = readCache();
		if (cached && cached.runtimeUrl === url) return cached;
	}
	if (!existsSync(CA_CERT) || !existsSync(CA_KEY)) {
		throw new Error(
			`showcase: CA material missing at ${CA_CERT} / ${CA_KEY}. ` +
				"Boot the runtime once to auto-generate: go run -C runtime ./cmd/runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/servicebridge?sslmode=disable",
		);
	}
	const keys: Partial<Record<ShowcaseRole, string>> = {};
	for (const role of ALL_ROLES) {
		console.error(`showcase: provisioning ${role} ...`);
		keys[role] = provisionOne(role);
	}
	const payload: ShowcaseKeys = {
		runtimeUrl: url,
		keys: keys as Record<ShowcaseRole, string>,
	};
	writeCache(payload);
	return payload;
}
