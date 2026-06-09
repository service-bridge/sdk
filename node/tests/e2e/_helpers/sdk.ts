/**
 * Helper to create a connected ServiceBridge SDK instance for e2e tests.
 * Each call creates a fresh ServiceBridge with cleanup attached.
 */

import { ServiceBridge } from "../../../src/connection/service-bridge";
import type { TestHarness } from "./harness";

export interface SDKHandle {
	sb: ServiceBridge;
	stop: () => Promise<void>;
}

export async function makeSDK(
	harness: TestHarness,
	_serviceName: string,
	opts: {
		key?: "primary" | "second";
		handlers?: (sb: ServiceBridge) => void;
	} = {},
): Promise<SDKHandle> {
	const which = opts.key ?? "primary";
	const key = which === "second" ? harness.service2Key : harness.serviceKey;
	if (!key) {
		throw new Error(`makeSDK: harness has no ${which} key`);
	}
	const sb = new ServiceBridge(harness.url, key, {
		reconnectIntervalMs: 500,
		reconnectAttempts: 3,
	});

	if (opts.handlers) {
		opts.handlers(sb);
	}

	await sb.start();

	return {
		sb,
		stop: async () => {
			await sb.stop();
		},
	};
}

export function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

export async function waitFor(
	check: () => boolean,
	timeoutMs: number,
	what: string,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (check()) return;
		await sleep(50);
	}
	throw new Error(`Timeout waiting for: ${what} (${timeoutMs}ms)`);
}
