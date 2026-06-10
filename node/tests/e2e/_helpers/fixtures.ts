// fixtures.ts — the single import surface for e2e domain files.
//
// A domain test imports clients, names, and waits from here:
//   import { shared, dedicated, connect, uniqueName, sleep, waitFor } from "./_helpers/fixtures";
//
// Clients (pool.ts) are keyed per domain via SB_E2E_DOMAIN; names (namespace.ts)
// keep tests collision-free on the shared client. Raw SQL policy helpers live in
// policy-db.ts and an isolated-runtime harness in dedicated-runtime.ts — import
// those directly where a domain needs them.

export { uniqueId, uniqueName } from "./namespace";
export { connect, dedicated, type Role, shared } from "./pool";

export function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

// waitFor polls `predicate` (sync or async) every `intervalMs` until it returns
// truthy or `timeoutMs` elapses, then throws naming `what`. Use it instead of a
// fixed sleep: it returns as soon as the condition holds.
export async function waitFor(
	predicate: () => boolean | Promise<boolean>,
	timeoutMs: number,
	what: string,
	intervalMs = 50,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await predicate()) return;
		await sleep(intervalMs);
	}
	throw new Error(`timeout waiting for: ${what} (${timeoutMs}ms)`);
}
