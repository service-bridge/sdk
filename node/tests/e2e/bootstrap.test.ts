/**
 * SDK end-to-end test against a running runtime.
 *
 * Requires:
 *   - A live ServiceBridge runtime reachable at SERVICEBRIDGE_URL (e.g.
 *     "localhost:14445").
 *   - A bootstrap key string in SERVICEBRIDGE_SERVICE_KEY for a service row
 *     already inserted in the runtime's Postgres.
 *
 * Both env vars must be set; otherwise the suite is skipped (matches the
 * runtime/tests/integration policy of skipping when TEST_DATABASE_URL is unset).
 *
 * Typical invocation:
 *
 *   SERVICEBRIDGE_URL=localhost:14445 \
 *   SERVICEBRIDGE_SERVICE_KEY=sb.<base64url> \
 *     bun test sdk/node/tests/e2e
 *
 * The Go integration test runtime/tests/integration/connection_test.go knows
 * how to launch the runtime, seed a service row, and shell out to this file —
 * see TestSDKE2E in that package.
 */

import { describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";

const URL = process.env.SERVICEBRIDGE_URL;
const KEY = process.env.SERVICEBRIDGE_SERVICE_KEY;

const enabled = Boolean(URL && KEY);

describe.skipIf(!enabled)("SDK e2e against live runtime", () => {
	test("start → connected event → stop", async () => {
		const sb = new ServiceBridge(URL as string, KEY as string, {
			// Make timeouts tight so failures fail fast.
			reconnectIntervalMs: 500,
			reconnectAttempts: 2,
			certRefreshLeadMs: 60 * 60 * 1000,
		});

		const events: { type: string; payload: unknown }[] = [];
		sb.on("connected", (e) => events.push({ type: "connected", payload: e }));
		sb.on("reconnecting", (e) =>
			events.push({ type: "reconnecting", payload: e }),
		);
		sb.on("disconnected", (e) =>
			events.push({ type: "disconnected", payload: e }),
		);

		await sb.start();
		try {
			await waitFor(
				() => events.some((e) => e.type === "connected"),
				5_000,
				"connected event",
			);
		} catch (err) {
			console.error(
				"e2e diagnostic — events so far:",
				JSON.stringify(events, null, 2),
			);
			throw err;
		}

		const connected = events.find((e) => e.type === "connected");
		expect(connected).toBeDefined();
		if (!connected) throw new Error("unreachable");
		const c = connected.payload as {
			sessionId: string;
			serviceId: string;
			serviceName: string;
		};
		expect(c.sessionId.length).toBeGreaterThan(0);
		expect(c.serviceId.length).toBeGreaterThan(0);
		expect(c.serviceName.length).toBeGreaterThan(0);

		// Hold the session long enough for at least one heartbeat round-trip.
		await sleep(2_500);

		await sb.stop();
		// After stop, no reconnect events should fire.
		const before = events.length;
		await sleep(1_500);
		expect(events.length).toBe(before);
	});

	test("invalid key emits reconnecting then disconnected{exhausted}", async () => {
		// Flip a byte in the secret to make auth fail without changing the key
		// shape. Anything in the secret region (8..40) works.
		const badKey = corruptSecret(KEY as string);

		const sb = new ServiceBridge(URL as string, badKey, {
			reconnectIntervalMs: 200,
			reconnectAttempts: 2,
			certRefreshLeadMs: 60 * 60 * 1000,
		});

		const events: { type: string; payload: unknown }[] = [];
		sb.on("reconnecting", (e) =>
			events.push({ type: "reconnecting", payload: e }),
		);
		sb.on("disconnected", (e) =>
			events.push({ type: "disconnected", payload: e }),
		);

		await sb.start();
		await waitFor(
			() => events.some((e) => e.type === "disconnected"),
			10_000,
			"disconnected event",
		);

		const exhausted = events.find(
			(e) =>
				e.type === "disconnected" &&
				(e.payload as { reason: string }).reason === "exhausted",
		);
		expect(exhausted).toBeDefined();
	});
});

// ── helpers ───────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
	predicate: () => boolean,
	timeoutMs: number,
	what: string,
): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (predicate()) return;
		await sleep(50);
	}
	throw new Error(`timeout waiting for ${what}`);
}

function corruptSecret(raw: string): string {
	if (!raw.startsWith("sb.")) {
		throw new Error("invalid key format");
	}
	const payload = Buffer.from(raw.slice(3), "base64url");
	// Flip a bit in the secret region (offset 8..40).
	const b = payload[10];
	if (b === undefined) throw new Error("payload too short");
	payload[10] = b ^ 0xff;
	return `sb.${payload.toString("base64url")}`;
}
