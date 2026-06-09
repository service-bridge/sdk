// logs-collector — SDK structured logs reach the runtime and persist
// per-service in telemetry_logs (source='sdk').
//
// Proves the full chain: sb.logger.{info,warn,error} → telemetry ring →
// bidi transport → runtime ingest → telemetry_logs row carrying the
// service_id and instance_id of the emitting session. This is the path the
// per-service Logs tab in the UI reads from.

import { afterEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { waitFor } from "./_helpers/events";
import { harnessFromEnv } from "./_helpers/harness";
import { withDb } from "./_helpers/policy-db";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

interface LogRow {
	level: string;
	message: string;
	fields: unknown;
	service_id: string | null;
	instance_id: string | null;
}

async function fetchSdkLog(message: string): Promise<LogRow | undefined> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT level, message, fields, service_id::text, instance_id
			FROM telemetry_logs
			WHERE source = 'sdk' AND message = ${message}
			ORDER BY at DESC LIMIT 1
		`) as LogRow[];
		return rows[0];
	});
}

describe("logs-collector e2e", () => {
	const keys = harnessFromEnv();
	let sb: ServiceBridge | undefined;

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("sdk logger entries persist per-service in telemetry_logs", async () => {
		sb = new ServiceBridge(keys.url, keys.serviceKey, FAST_OPTS);

		const connected: { serviceId?: string; instanceId?: string }[] = [];
		sb.on("connected", (e) => connected.push(e as never));
		await sb.start();
		await waitFor(() => connected.length > 0, 5_000, "connected event");

		const stamp = Date.now();
		const infoMsg = `e2e log probe info ${stamp}`;
		const warnMsg = `e2e log probe warn ${stamp}`;
		const errMsg = `e2e log probe error ${stamp}`;

		sb.logger.info(infoMsg, { phase: "probe", n: 1 });
		sb.logger.warn(warnMsg);
		sb.logger.error(errMsg, { boom: true });

		await waitFor(
			async () => Boolean(await fetchSdkLog(infoMsg)),
			8_000,
			"sdk info log persisted",
		);

		const info = await fetchSdkLog(infoMsg);
		const warn = await fetchSdkLog(warnMsg);
		const err = await fetchSdkLog(errMsg);

		expect(info).toBeDefined();
		expect(warn).toBeDefined();
		expect(err).toBeDefined();

		// Per-service attribution — the row the Logs tab filters on.
		expect(info?.service_id).not.toBeNull();
		expect(info?.instance_id).not.toBeNull();

		expect(info?.level).toBe("info");
		expect(warn?.level).toBe("warn");
		expect(err?.level).toBe("error");

		expect(info?.fields).toMatchObject({ phase: "probe", n: 1 });
		expect(err?.fields).toMatchObject({ boom: true });
	});
});
