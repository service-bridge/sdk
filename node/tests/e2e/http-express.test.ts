/**
 * Express HTTP integration e2e — drives REAL HTTP requests through an
 * Express server attached via `attachExpress`, then asserts that the runtime
 * persisted HTTP.HANDLE operations (SUCCESS + ERROR) for the "http-test"
 * service in Postgres.
 *
 *   SERVICEBRIDGE_URL            — runtime gRPC endpoint
 *   SERVICEBRIDGE_HTTP_TEST_KEY  — provider key, cert maps to service "http-test"
 *   TEST_DATABASE_URL            — runtime Postgres (default localhost:5433)
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import express from "express";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { attachExpress } from "../../src/http/express";
import { Channel } from "../../src/pb/servicebridge/v1/telemetry";
import { freePort } from "./_helpers/http-port";
import { withDb } from "./_helpers/policy-db";

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

const URL = process.env.SERVICEBRIDGE_URL;
const SERVICE_KEY = process.env.SERVICEBRIDGE_HTTP_TEST_KEY;
const enabled = Boolean(URL && SERVICE_KEY);

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
};

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

async function countHttpOps(
	base: string,
	startedAfterMs: number,
): Promise<{ ok: number; err: number }> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT
				COUNT(*) FILTER (WHERE o.status = 2)::int AS ok,
				COUNT(*) FILTER (WHERE o.status = 3)::int AS err
			FROM operations o
			JOIN services s ON s.id = o.actor_service_id
			WHERE o.channel = 1 AND o.kind = 1
			  AND s.name = 'http-test'
			  AND o.subject LIKE ${`%${base}%`}
			  AND o.started_at >= to_timestamp(${startedAfterMs} / 1000.0)
		`) as Array<{ ok: number; err: number }>;
		return { ok: rows[0]?.ok ?? 0, err: rows[0]?.err ?? 0 };
	});
}

async function countHttpPayloads(
	base: string,
	startedAfterMs: number,
): Promise<{ inp: number; outp: number }> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT
				COUNT(*) FILTER (WHERE p.direction = 1)::int AS inp,
				COUNT(*) FILTER (WHERE p.direction = 2)::int AS outp
			FROM op_payloads p
			JOIN operations o ON o.op_id = p.op_id
			JOIN services s ON s.id = o.actor_service_id
			WHERE o.channel = 1
			  AND s.name = 'http-test'
			  AND o.subject LIKE ${`%${base}%`}
			  AND o.started_at >= to_timestamp(${startedAfterMs} / 1000.0)
		`) as Array<{ inp: number; outp: number }>;
		return { inp: rows[0]?.inp ?? 0, outp: rows[0]?.outp ?? 0 };
	});
}

describe.skipIf(!enabled)("HTTP Express integration e2e", () => {
	let provider: ServiceBridge | undefined;
	let server: { close(cb: () => void): void } | undefined;
	let savedHttpCapture: string | undefined;

	beforeEach(async () => {
		savedHttpCapture = await getCapture("http.payload_capture");
		await setCapture("http.payload_capture", "all");
		await sleep(1500);
	});

	afterEach(async () => {
		if (server) {
			await new Promise<void>((r) => server!.close(() => r()));
			server = undefined;
		}
		await provider?.stop();
		provider = undefined;
		if (savedHttpCapture !== undefined) {
			await setCapture("http.payload_capture", savedHttpCapture);
			savedHttpCapture = undefined;
		}
	});

	test("Express drives /express with all methods + an error → SUCCESS and ERROR traces", async () => {
		const base = "/express";
		const app = express();
		app.use(express.json());
		app.get(`${base}/get`, (_req, res) => res.json({ ok: true }));
		app.post(`${base}/post`, (_req, res) => res.json({ ok: true }));
		app.put(`${base}/put`, (_req, res) => res.json({ ok: true }));
		app.delete(`${base}/delete`, (_req, res) => res.json({ ok: true }));
		app.patch(`${base}/patch`, (_req, res) => res.json({ ok: true }));
		app.get(`${base}/error`, (_req, res) =>
			res.status(500).json({ error: true }),
		);

		provider = new ServiceBridge(URL!, SERVICE_KEY!, FAST_OPTS);
		await provider.start();
		await waitFor(
			() => provider!.identity() !== null,
			5_000,
			"provider connected",
		);
		await waitFor(
			() => provider!.telemetry.captureModeForChannel(Channel.HTTP) === "all",
			5_000,
			"http capture mode = all",
		);

		const port = await freePort();
		server = app.listen(port) as unknown as {
			close(cb: () => void): void;
		};
		attachExpress(app, provider, { host: "127.0.0.1", port });
		await sleep(300);

		const baseline = Date.now();

		for (const method of ["GET", "POST", "PUT", "DELETE", "PATCH"]) {
			const hasBody =
				method === "POST" || method === "PUT" || method === "PATCH";
			const code = await fetch(
				`http://127.0.0.1:${port}${base}/${method.toLowerCase()}`,
				hasBody
					? {
							method,
							headers: { "content-type": "application/json" },
							body: JSON.stringify({ framework: base.slice(1), method }),
						}
					: { method },
			).then((r) => r.status);
			expect(code).toBeGreaterThanOrEqual(200);
			expect(code).toBeLessThan(300);
		}
		const errCode = await fetch(`http://127.0.0.1:${port}${base}/error`).then(
			(r) => r.status,
		);
		expect(errCode).toBe(500);

		await waitFor(
			async () => {
				const { ok, err } = await countHttpOps(base, baseline);
				return ok >= 5 && err >= 1;
			},
			12_000,
			`>=5 SUCCESS + >=1 ERROR HTTP.HANDLE ops for http-test ${base}`,
		);

		const { ok, err } = await countHttpOps(base, baseline);
		expect(ok).toBeGreaterThanOrEqual(5);
		expect(err).toBeGreaterThanOrEqual(1);

		await waitFor(
			async () => {
				const { inp, outp } = await countHttpPayloads(base, baseline);
				return inp >= 3 && outp >= 5;
			},
			12_000,
			`>=3 IN + >=5 OUT raw/json payloads for http-test ${base}`,
		);
		const { inp, outp } = await countHttpPayloads(base, baseline);
		expect(inp).toBeGreaterThanOrEqual(3);
		expect(outp).toBeGreaterThanOrEqual(5);
	}, 40_000);
});
