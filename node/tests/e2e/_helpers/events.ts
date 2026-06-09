// events.ts — shared helpers for events e2e tests.
//
// Each test gets a unique event-name prefix derived from a per-process counter,
// so concurrent tests do not collide on `event_log`, `event_deliveries`, or
// subscription patterns. Tests run against a live ServiceBridge runtime + real
// Postgres; cleanup happens through the regular GC path on retention timers,
// not by hand — events are append-only by design.
//
// Public surface:
//   - `eventsEnv()` — read SERVICEBRIDGE_URL + two service keys, throw with a
//     readable message when anything is missing (no skips per CLAUDE.md).
//   - `uniqueEventName(prefix)` — collision-free name per test invocation.
//   - `waitFor(predicate, timeoutMs, what)` — polling wait, throws on timeout.
//   - `sleep(ms)` — promise-based sleep.
//   - `FAST_OPTS` — tight reconnect timing for fast failures.
//   - `ORDER_EVENT_PROTO` — absolute path to the shared .proto fixture.

import { join } from "node:path";

const REGEN_CMD = "bash scripts/bootstrap-e2e-keys.sh";

// FAST_OPTS — tight timing so timeouts surface in seconds, not minutes.
// Tests that need a longer reconnect window override individual fields.
export const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

// Path to the shared protobuf fixture. Tests load it via SchemaSpec.protoFile.
export const ORDER_EVENT_PROTO = join(
	import.meta.dir,
	"..",
	"..",
	"..",
	"src",
	"events",
	"testdata",
	"order-event.proto",
);

export interface EventsEnv {
	url: string;
	publisherKey: string;
	subscriberKey: string;
}

// eventsEnv reads the three env vars required by every events e2e file. Throws
// with an actionable message — no silent skips. The bun preload reads
// .env.e2e, so the env is normally already populated by the harness.
export function eventsEnv(): EventsEnv {
	const url = process.env.SERVICEBRIDGE_URL;
	const publisherKey = process.env.SERVICEBRIDGE_SERVICE_KEY;
	const subscriberKey = process.env.SERVICEBRIDGE_SERVICE2_KEY;

	const missing: string[] = [];
	if (!url) missing.push("SERVICEBRIDGE_URL");
	if (!publisherKey) missing.push("SERVICEBRIDGE_SERVICE_KEY");
	if (!subscriberKey) missing.push("SERVICEBRIDGE_SERVICE2_KEY");

	if (missing.length > 0) {
		throw new Error(
			[
				`events e2e: missing env: ${missing.join(", ")}.`,
				``,
				`Persistent bootstrap keys live in <repo>/.env.e2e and load via bun preload.`,
				`Run once (idempotent, does not delete existing rows):`,
				``,
				`    ${REGEN_CMD}`,
				``,
				`Runtime must be up at SERVICEBRIDGE_URL — start it if needed:`,
				``,
				`    go run -C runtime ./cmd/runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/service-bridge?sslmode=disable`,
			].join("\n"),
		);
	}

	return {
		url: url as string,
		publisherKey: publisherKey as string,
		subscriberKey: subscriberKey as string,
	};
}

// Per-process counter feeding uniqueEventName(). Reset would defeat the
// purpose; tests rely on monotonicity within a single bun run.
let eventNameCounter = 0;

// uniqueEventName returns a collision-free event name for a single test. The
// prefix is repeated in the name so failure logs are debuggable. Format:
// `<prefix>.t${n}.${random}` — runtime accepts `[a-z0-9_-]+(\.[a-z0-9_-]+)*`.
export function uniqueEventName(prefix: string): string {
	const n = ++eventNameCounter;
	const rand = Math.random().toString(36).slice(2, 8);
	return `${prefix}.t${n}.${rand}`;
}

// uniquePartitionKey returns a collision-free partition key for tests. The
// runtime enforces «only one in_flight per (consumer_service, partition_key)»
// and the dispatcher's FIFO filter blocks newer events when older ones with
// the same partition_key are still pending. Re-using a hardcoded key across
// test runs (e.g. "order-7") therefore pollutes the queue with orphaned
// deliveries from prior failures. Using a unique key per test isolates each
// test's queue.
export function uniquePartitionKey(prefix: string): string {
	const n = eventNameCounter;
	const rand = Math.random().toString(36).slice(2, 8);
	return `${prefix}-${n}-${rand}`;
}

export function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

// waitFor polls predicate at ~20Hz until it returns truthy or the timeout
// elapses. On timeout throws `Error("timeout waiting for: <what>")`. Predicate
// may be sync or async; rejections from async predicates surface immediately.
export async function waitFor(
	predicate: () => boolean | Promise<boolean>,
	timeoutMs: number,
	what: string,
): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await predicate()) return;
		await sleep(50);
	}
	throw new Error(`timeout waiting for: ${what}`);
}
