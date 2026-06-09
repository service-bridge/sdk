// payload-capture-per-channel.test.ts — per-channel payload capture propagation.
//
// The runtime resolves each channel's mode from its <channel>.payload_capture
// setting and pushes the whole CaptureModes set to every SDK over the registry
// stream. This e2e proves, over real gRPC against a running runtime:
//   1. Distinct per-channel modes reach the SDK and are read back per channel
//      (rpc vs events differ — no global single mode).
//   2. A live settings change re-emits the fresh modes without reconnect
//      (loader OnChanged → Hub.NotifyCaptureModesChanged → RegistryUpdate).
//
// Settings are toggled directly in runtime_settings; the runtime picks the
// change up via NOTIFY settings_changed (<1s) + 30s poll fallback. Every key
// is restored to its prior value in afterEach.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { Channel } from "../../src/pb/servicebridge/v1/telemetry";
import { eventsEnv, FAST_OPTS } from "./_helpers/events";
import { withDb } from "./_helpers/policy-db";
import { sleep, waitFor } from "./_helpers/sdk";

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

describe("payload capture: per-channel propagation (live)", () => {
	const env = eventsEnv();
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

	test("SDK receives distinct modes per channel from the first snapshot", async () => {
		await setCapture("rpc.payload_capture", "all");
		await setCapture("events.payload_capture", "none");
		await setCapture("workflows.payload_capture", "errors");
		// Give the runtime time to pick up the NOTIFY before connecting so the
		// first snapshot already carries the new modes.
		await sleep(1500);

		sb = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		await sb.start();

		await waitFor(
			() => sb?.telemetry.captureModeForChannel(Channel.RPC) === "all",
			5000,
			"rpc capture mode = all",
		);

		expect(sb.telemetry.captureModeForChannel(Channel.RPC)).toBe("all");
		expect(sb.telemetry.captureModeForChannel(Channel.EVENT)).toBe("none");
		expect(sb.telemetry.captureModeForChannel(Channel.WORKFLOW)).toBe("errors");
	});

	test("a live settings change re-emits the new mode without reconnect", async () => {
		await setCapture("rpc.payload_capture", "none");
		await setCapture("events.payload_capture", "none");
		await sleep(1500);

		sb = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
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
	});
});
