// setup.ts — bun test preload that hydrates process.env from <repo>/.env.e2e.
//
// .env.e2e is the single source of truth for e2e bootstrap keys (see
// scripts/bootstrap-e2e-keys.sh). It is gitignored and lives at the repo
// root. This preload runs before any e2e test file.
//
// Precedence:
//   1. SERVICEBRIDGE_URL / _SERVICE_KEY / _SERVICE2_KEY → always overridden
//      by .env.e2e when present. The dedicated e2e file is authoritative
//      for those three vars — otherwise stale values in <repo>/.env (which
//      Bun autoloads) would shadow freshly-provisioned keys and surface as
//      UNAUTHENTICATED on bootstrap, which is exactly the friction we are
//      trying to remove.
//   2. Any other key from .env.e2e → does NOT overwrite an existing
//      process.env entry, so CI / shell exports still win for non-key vars.

import { afterAll } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

// Close the warm SDK pool (pool.ts) once, after the entire run. A top-level
// afterAll in a Bun preload fires once per `bun test` process, not per file —
// so the pool survives across all e2e files and is torn down at the very end.
// Lazy-imported so plain unit runs (which never touch the pool) don't load the
// SDK here.
afterAll(async () => {
	const { closeAll } = await import("./pool");
	await closeAll();
});

const ENV_FILE_NAMES = [".env.e2e"];

// Walk upward from cwd until we find any of the env files or hit fs root.
function locateEnvFile(): string | null {
	let dir = process.cwd();
	for (let i = 0; i < 8; i++) {
		for (const name of ENV_FILE_NAMES) {
			const candidate = resolve(dir, name);
			if (existsSync(candidate)) return candidate;
		}
		const parent = resolve(dir, "..");
		if (parent === dir) return null;
		dir = parent;
	}
	return null;
}

function parseDotEnv(content: string): Record<string, string> {
	const out: Record<string, string> = {};
	for (const rawLine of content.split(/\r?\n/)) {
		const line = rawLine.trim();
		if (!line || line.startsWith("#")) continue;
		const eq = line.indexOf("=");
		if (eq < 0) continue;
		const key = line.slice(0, eq).trim();
		let value = line.slice(eq + 1).trim();
		// Strip surrounding quotes if balanced.
		if (
			(value.startsWith('"') && value.endsWith('"')) ||
			(value.startsWith("'") && value.endsWith("'"))
		) {
			value = value.slice(1, -1);
		}
		out[key] = value;
	}
	return out;
}

// isOverride — vars where .env.e2e is authoritative. Bun's autoloaded .env may
// hold stale values for these from earlier sessions; they must not leak into
// the e2e run. The runtime URL plus every per-domain key (SB_E2E_*) are always
// taken from .env.e2e.
function isOverride(key: string): boolean {
	return key === "SERVICEBRIDGE_URL" || key.startsWith("SB_E2E_");
}

const envPath = locateEnvFile();
if (envPath) {
	const parsed = parseDotEnv(readFileSync(envPath, "utf8"));
	for (const [k, v] of Object.entries(parsed)) {
		if (isOverride(k) || process.env[k] === undefined) {
			process.env[k] = v;
		}
	}
}
