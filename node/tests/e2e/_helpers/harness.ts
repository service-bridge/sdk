// harness.ts — env+probe helpers for the SDK e2e suite.
//
// Persistent bootstrap keys live in <repo>/.env.e2e (gitignored, generated
// once by scripts/bootstrap-e2e-keys.sh). The setup preload loads that file
// into process.env. Tests call harnessFromEnv() to read it.
//
// Error policy (matches CLAUDE.md "no silent skips" rule for things that
// should work):
//   - Missing env → throw E2ESetupError with the exact regen command.
//   - Probe fails → throw E2EKeysExpiredError with regen command + cause.
// Both messages are formatted so an LLM agent reading the failure can fix
// the environment without further investigation.

import { ServiceBridge } from "../../../src/connection/service-bridge";

const REGEN_CMD = "bash scripts/bootstrap-e2e-keys.sh";

/**
 * E2ESetupError — required env var is missing, no .env.e2e present.
 * Fixable by provisioning persistent keys once.
 */
export class E2ESetupError extends Error {
	constructor(missing: string) {
		super(
			[
				`e2e setup incomplete: ${missing} not set.`,
				``,
				`The SDK e2e suite expects persistent bootstrap keys in <repo>/.env.e2e.`,
				`Run the provisioning script once (idempotent, does not touch existing rows):`,
				``,
				`    ${REGEN_CMD}`,
				``,
				`Preconditions: runtime must be reachable + runtime/certs/ca.{crt,key} exist.`,
				`Boot the runtime once if the CA material is missing:`,
				``,
				`    go run -C runtime ./cmd/runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/service-bridge?sslmode=disable`,
			].join("\n"),
		);
		this.name = "E2ESetupError";
	}
}

/**
 * E2EKeysExpiredError — env was set, but bootstrap auth against the runtime
 * was rejected. The likely causes (in order of frequency) are:
 *   1. CA was regenerated → old bootstrap keys reference a dead CA.
 *   2. services row revoked or deleted from Postgres.
 *   3. Runtime is reachable but not the same instance that issued the key.
 *
 * Regenerate keys, the script writes a fresh .env.e2e.
 */
export class E2EKeysExpiredError extends Error {
	constructor(cause: unknown) {
		const causeMsg = cause instanceof Error ? cause.message : String(cause);
		super(
			[
				`e2e bootstrap auth failed — keys in .env.e2e are stale or unknown to runtime.`,
				``,
				`Underlying error: ${causeMsg}`,
				``,
				`Regenerate persistent keys (does not delete existing rows):`,
				``,
				`    ${REGEN_CMD}`,
				``,
				`If the runtime is not running, start it first:`,
				``,
				`    go run -C runtime ./cmd/runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/service-bridge?sslmode=disable`,
			].join("\n"),
		);
		this.name = "E2EKeysExpiredError";
	}
}

export interface HarnessKeys {
	url: string;
	serviceKey: string;
	service2Key?: string;
}

/**
 * Reads SERVICEBRIDGE_URL + SERVICEBRIDGE_SERVICE_KEY[2] from process.env
 * (populated either by the shell or by the bun preload from .env.e2e).
 * Throws E2ESetupError on missing values — the error contains the exact
 * provisioning command an LLM should run.
 */
export function harnessFromEnv(opts?: { needSecond?: boolean }): HarnessKeys {
	const url = process.env.SERVICEBRIDGE_URL;
	const serviceKey = process.env.SERVICEBRIDGE_SERVICE_KEY;
	const service2Key = process.env.SERVICEBRIDGE_SERVICE2_KEY;

	if (!url) throw new E2ESetupError("SERVICEBRIDGE_URL");
	if (!serviceKey) throw new E2ESetupError("SERVICEBRIDGE_SERVICE_KEY");
	if (opts?.needSecond && !service2Key) {
		throw new E2ESetupError("SERVICEBRIDGE_SERVICE2_KEY");
	}

	return { url, serviceKey, service2Key };
}

/**
 * True iff the harness env vars are present. Lets per-file `describe.skipIf`
 * patterns keep working — when nobody has provisioned keys at all, the
 * suite stays out of the way instead of failing the whole run.
 *
 * The preferred path for new code is `harnessFromEnv()` which throws with
 * regen instructions, but legacy `describe.skipIf(!e2eEnabled())` blocks
 * are tolerated for backward compatibility.
 */
export function e2eEnabled(needSecond = false): boolean {
	if (!process.env.SERVICEBRIDGE_URL) return false;
	if (!process.env.SERVICEBRIDGE_SERVICE_KEY) return false;
	if (needSecond && !process.env.SERVICEBRIDGE_SERVICE2_KEY) return false;
	return true;
}

/**
 * Probes that the persistent keys still bootstrap successfully against the
 * runtime. Throws E2EKeysExpiredError on failure (with a regen command in
 * the message). Resolves on first `connected` event.
 *
 * Use at the top of a describe-block once per test file — caches result so
 * a single suite run probes at most once per (url, key) pair.
 */
const probeCache = new Map<string, Promise<void>>();

export async function ensureKeysValid(
	keys: HarnessKeys,
	timeoutMs = 5000,
): Promise<void> {
	const cacheKey = `${keys.url}|${keys.serviceKey}`;
	const existing = probeCache.get(cacheKey);
	if (existing) return existing;

	const probe = (async () => {
		const sb = new ServiceBridge(keys.url, keys.serviceKey, {
			reconnectIntervalMs: 200,
			reconnectAttempts: 1,
			certRefreshLeadMs: 60 * 60 * 1000,
		});
		try {
			await new Promise<void>((resolveOk, rejectFail) => {
				const timer = setTimeout(() => {
					rejectFail(
						new Error(`bootstrap probe timed out after ${timeoutMs}ms`),
					);
				}, timeoutMs);
				sb.on("connected", () => {
					clearTimeout(timer);
					resolveOk();
				});
				// ServiceBridge reports failures as `disconnected` with a reason —
				// `exhausted` after reconnect retries, or a transport error string.
				sb.on("disconnected", (evt) => {
					clearTimeout(timer);
					rejectFail(evt.error ?? new Error(evt.reason));
				});
				sb.start().catch((err: unknown) => {
					clearTimeout(timer);
					rejectFail(err instanceof Error ? err : new Error(String(err)));
				});
			});
		} catch (cause) {
			throw new E2EKeysExpiredError(cause);
		} finally {
			await sb.stop().catch(() => {
				/* probe close errors are not interesting */
			});
		}
	})();

	probeCache.set(cacheKey, probe);
	try {
		await probe;
	} catch (err) {
		// Drop the failed promise so a follow-up suite (e.g. after the user
		// regenerated keys mid-run) can retry instead of seeing the cached fail.
		probeCache.delete(cacheKey);
		throw err;
	}
}

/**
 * Legacy class kept so existing event-suite stubs (events-happy, events-fanout)
 * keep compiling. New code should use harnessFromEnv() + ensureKeysValid()
 * directly.
 */
export class TestHarness {
	url = "";
	serviceKey = "";
	service2Key = "";

	async start(opts?: { needSecond?: boolean }): Promise<void> {
		const keys = harnessFromEnv(opts);
		this.url = keys.url;
		this.serviceKey = keys.serviceKey;
		this.service2Key = keys.service2Key ?? "";
		await ensureKeysValid(keys);
	}

	async stop(): Promise<void> {
		// External runtime, not owned by the test.
	}
}

// Backwards-compatible alias — kept so callers still relying on the old
// import don't break. New code: import { E2ESetupError } directly.
export const SkipE2EError = E2ESetupError;
