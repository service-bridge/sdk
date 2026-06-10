// misc-lifecycle — connect/reconnect FSM + per-channel payload-capture.
//
// These cannot ride the warm pool: the pool only hands out already-started,
// healthy, connected clients, so the connected-event timing, heartbeat hold,
// post-stop no-event invariant, and the bad-key reconnect-exhaustion FSM are
// unobservable on a pooled identity; the corrupt-secret test deliberately
// authenticates with a broken key. Payload-capture mutates GLOBAL runtime_settings
// rows (every connected SDK process-wide) and needs a FRESH client that connects
// AFTER the settings write so the first snapshot already carries them — every key
// is saved+restored in afterEach and these two tests run serially.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { Channel } from "../../src/pb/servicebridge/v1/telemetry";
import { withDb } from "./_helpers/policy-db";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

function env(): { url: string; key: string } {
	const url = process.env.SERVICEBRIDGE_URL;
	const key = process.env.SB_E2E_MISC_1;
	if (!url || !key) {
		throw new Error(
			"misc-lifecycle: SERVICEBRIDGE_URL / SB_E2E_MISC_1 not set — run `bash scripts/bootstrap-e2e-keys.sh`",
		);
	}
	return { url, key };
}

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
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

// Flip a byte in the secret region (offset 8..40) so auth fails without changing
// the key shape.
function corruptSecret(raw: string): string {
	if (!raw.startsWith("sb.")) throw new Error("invalid key format");
	const payload = Buffer.from(raw.slice(3), "base64url");
	const b = payload[10];
	if (b === undefined) throw new Error("payload too short");
	payload[10] = b ^ 0xff;
	return `sb.${payload.toString("base64url")}`;
}

describe("misc-lifecycle: connect FSM", () => {
	let sb: ServiceBridge | undefined;

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("connect: start emits connected with session/service identity, stop is terminal", async () => {
		const { url, key } = env();
		sb = new ServiceBridge(url, key, FAST_OPTS);

		const events: { type: string; payload: unknown }[] = [];
		sb.on("connected", (e) => events.push({ type: "connected", payload: e }));
		sb.on("reconnecting", (e) =>
			events.push({ type: "reconnecting", payload: e }),
		);
		sb.on("disconnected", (e) =>
			events.push({ type: "disconnected", payload: e }),
		);

		await sb.start();
		await waitFor(
			() => events.some((e) => e.type === "connected"),
			5_000,
			"connected event",
		);

		const connected = events.find((e) => e.type === "connected");
		expect(connected).toBeDefined();
		const c = connected!.payload as {
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
		sb = undefined;
		// After stop, no reconnect/disconnect events should fire.
		const before = events.length;
		await sleep(1_500);
		expect(events.length).toBe(before);
	}, 20_000);

	test("connect: corrupted secret drives reconnect FSM to disconnected{exhausted}", async () => {
		const { url, key } = env();
		const badKey = corruptSecret(key);

		sb = new ServiceBridge(url, badKey, {
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

		expect(events.some((e) => e.type === "reconnecting")).toBe(true);
		const exhausted = events.find(
			(e) =>
				e.type === "disconnected" &&
				(e.payload as { reason: string }).reason === "exhausted",
		);
		expect(exhausted).toBeDefined();
	}, 20_000);
});

const CAPTURE_KEYS = [
	"rpc.payload_capture",
	"http.payload_capture",
	"events.payload_capture",
	"workflows.payload_capture",
] as const;

async function setCapture(key: string, value: string): Promise<void> {
	await withDb(async (sql) => {
		await sql`UPDATE runtime_settings SET value = ${value} WHERE key = ${key}`;
	});
}

async function getCapture(key: string): Promise<string> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT value FROM runtime_settings WHERE key = ${key}
		`) as Array<{ value: string }>;
		return rows[0]?.value ?? "none";
	});
}

describe("misc-lifecycle: payload capture per-channel (live)", () => {
	let sb: ServiceBridge | undefined;
	const saved = new Map<string, string>();

	beforeEach(async () => {
		sb = undefined;
		saved.clear();
		for (const key of CAPTURE_KEYS) {
			saved.set(key, await getCapture(key));
		}
	});

	afterEach(async () => {
		if (sb) {
			await sb.stop();
			sb = undefined;
		}
		for (const [key, value] of saved) {
			await setCapture(key, value);
		}
	});

	test("payload-capture: SDK reads distinct per-channel modes from the first snapshot", async () => {
		const { url, key } = env();
		await setCapture("rpc.payload_capture", "all");
		await setCapture("events.payload_capture", "none");
		await setCapture("workflows.payload_capture", "errors");
		// Let the runtime pick up the NOTIFY before connecting so the first
		// snapshot already carries the new modes.
		await sleep(1500);

		sb = new ServiceBridge(url, key, FAST_OPTS);
		await sb.start();

		await waitFor(
			() => sb?.telemetry.captureModeForChannel(Channel.RPC) === "all",
			5000,
			"rpc capture mode = all",
		);

		expect(sb.telemetry.captureModeForChannel(Channel.RPC)).toBe("all");
		expect(sb.telemetry.captureModeForChannel(Channel.EVENT)).toBe("none");
		expect(sb.telemetry.captureModeForChannel(Channel.WORKFLOW)).toBe("errors");
	}, 20_000);

	test("payload-capture: live settings change re-emits new mode on the same connection, other channels untouched", async () => {
		const { url, key } = env();
		await setCapture("rpc.payload_capture", "none");
		await setCapture("events.payload_capture", "none");
		await sleep(1500);

		sb = new ServiceBridge(url, key, FAST_OPTS);
		await sb.start();

		await waitFor(
			() => sb?.telemetry.captureModeForChannel(Channel.EVENT) === "none",
			5000,
			"events capture mode = none initially",
		);
		expect(sb.telemetry.captureModeForChannel(Channel.RPC)).toBe("none");

		// Operator flips ONLY the events channel to "all" on the live runtime.
		await setCapture("events.payload_capture", "all");

		// The same connection must pick up the new events mode via the live
		// re-emit (NOTIFY → loader OnChanged → registry update), no reconnect.
		await waitFor(
			() => sb?.telemetry.captureModeForChannel(Channel.EVENT) === "all",
			5000,
			"events capture mode flips to all live",
		);
		expect(sb.telemetry.captureModeForChannel(Channel.EVENT)).toBe("all");
		// rpc must be untouched — channels are independent.
		expect(sb.telemetry.captureModeForChannel(Channel.RPC)).toBe("none");
	}, 20_000);
});
