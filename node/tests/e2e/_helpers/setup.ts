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

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

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

// E2E_OVERRIDE_KEYS = vars where .env.e2e is authoritative. Bun's autoloaded
// .env may contain stale values for these (from earlier test sessions); they
// must not leak into the e2e run.
const E2E_OVERRIDE_KEYS = new Set([
	"SERVICEBRIDGE_URL",
	"SERVICEBRIDGE_SERVICE_KEY",
	"SERVICEBRIDGE_SERVICE2_KEY",
]);

// SB_DISABLE_E2E_PRELOAD=1 — set by the Go integration test
// (runtime/tests/integration/connection_test.go::TestSDKE2E) which spawns its
// own throwaway runtime and provisions its own keys directly via cmd.Env.
// In that mode the preload must NOT overwrite SERVICEBRIDGE_URL/_KEY/_KEY2 —
// otherwise the test SDK ends up pointed at the dev runtime with dev keys.
const disablePreload = process.env.SB_DISABLE_E2E_PRELOAD === "1";

const envPath = locateEnvFile();
if (envPath && !disablePreload) {
	const parsed = parseDotEnv(readFileSync(envPath, "utf8"));
	for (const [k, v] of Object.entries(parsed)) {
		if (E2E_OVERRIDE_KEYS.has(k) || process.env[k] === undefined) {
			process.env[k] = v;
		}
	}
}
