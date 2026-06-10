/**
 * HTTP integration e2e — drives REAL HTTP requests through each supported
 * framework adapter (Express, Fastify, Hono) and asserts that the runtime
 * persisted HTTP.HANDLE operations (SUCCESS + ERROR) for the provider service
 * in Postgres. The Express block also asserts IN/OUT payload capture; that
 * path is adapter-agnostic, so it is asserted once.
 *
 *   SERVICEBRIDGE_URL  — runtime gRPC endpoint
 *   SB_E2E_HTTP_1      — provider key (per-domain identity e2e-http-1)
 *   TEST_DATABASE_URL  — runtime Postgres (default localhost:5433)
 *
 * Uses a DEDICATED ServiceBridge on this domain's "primary" identity, NOT the
 * warm pool: the count SQL attributes ops by the provider's resolved service
 * name (identity().serviceName). The provider is started per test and stopped
 * in afterEach. beforeEach forces the process-wide runtime setting
 * http.payload_capture='all' and restores it in afterEach; the three blocks
 * run serially within this file so they don't race each other on that setting.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import express from "express";
import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import { Hono } from "hono";
import type { ServiceBridge } from "../../src/connection/service-bridge";
import { attachExpress } from "../../src/http/express";
import { sbFastify } from "../../src/http/fastify";
import { attachHono } from "../../src/http/hono";
import { Channel } from "../../src/pb/servicebridge/v1/telemetry";
import { connect, dedicated, uniqueId, waitFor } from "./_helpers/fixtures";
import { freePort } from "./_helpers/http-port";
import { withDb } from "./_helpers/policy-db";

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

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

async function countHttpOps(
	serviceName: string,
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
			  AND s.name = ${serviceName}
			  AND o.subject LIKE ${`%${base}%`}
			  AND o.started_at >= to_timestamp(${startedAfterMs} / 1000.0)
		`) as Array<{ ok: number; err: number }>;
		return { ok: rows[0]?.ok ?? 0, err: rows[0]?.err ?? 0 };
	});
}

async function countHttpPayloads(
	serviceName: string,
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
			  AND s.name = ${serviceName}
			  AND o.subject LIKE ${`%${base}%`}
			  AND o.started_at >= to_timestamp(${startedAfterMs} / 1000.0)
		`) as Array<{ inp: number; outp: number }>;
		return { inp: rows[0]?.inp ?? 0, outp: rows[0]?.outp ?? 0 };
	});
}

const VERBS = ["GET", "POST", "PUT", "DELETE", "PATCH"] as const;

// Drives the 5 happy-path verbs (asserting 2xx) plus a 500 /error route, then
// returns the baseline timestamp captured just before the first request. The
// 2xx/500 checks are a precondition exercising the user's own app; the
// load-bearing assertion is that each handled request produces an HTTP op.
async function driveVerbsAndError(port: number, base: string): Promise<number> {
	const baseline = Date.now();
	for (const method of VERBS) {
		const hasBody = method === "POST" || method === "PUT" || method === "PATCH";
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
	return baseline;
}

async function expectHttpOps(
	serviceName: string,
	base: string,
	baseline: number,
): Promise<void> {
	await waitFor(
		async () => {
			const { ok, err } = await countHttpOps(serviceName, base, baseline);
			return ok >= 5 && err >= 1;
		},
		12_000,
		`>=5 SUCCESS + >=1 ERROR HTTP.HANDLE ops for ${serviceName} ${base}`,
	);
	const { ok, err } = await countHttpOps(serviceName, base, baseline);
	expect(ok).toBeGreaterThanOrEqual(5);
	expect(err).toBeGreaterThanOrEqual(1);
}

describe("HTTP framework integration e2e", () => {
	let provider: ServiceBridge | undefined;
	let providerName: string | undefined;
	let stopServer: (() => void | Promise<void>) | undefined;
	let savedHttpCapture: string | undefined;

	beforeEach(async () => {
		savedHttpCapture = await getCapture("http.payload_capture");
		await setCapture("http.payload_capture", "all");
		await sleep(1500);
	});

	afterEach(async () => {
		if (stopServer) {
			await stopServer();
			stopServer = undefined;
		}
		await provider?.stop();
		provider = undefined;
		providerName = undefined;
		if (savedHttpCapture !== undefined) {
			await setCapture("http.payload_capture", savedHttpCapture);
			savedHttpCapture = undefined;
		}
	});

	async function startProvider(): Promise<ServiceBridge> {
		const sb = dedicated("primary");
		await connect(sb);
		await waitFor(() => sb.identity() !== null, 5_000, "provider connected");
		providerName = sb.identity()?.serviceName;
		await waitFor(
			() => sb.telemetry.captureModeForChannel(Channel.HTTP) === "all",
			5_000,
			"http capture mode = all",
		);
		return sb;
	}

	test("Express: 5 verbs + 500 error → >=5 SUCCESS and >=1 ERROR HTTP.HANDLE ops (and IN/OUT payloads captured)", async () => {
		const base = `/${uniqueId("express")}`;
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

		provider = await startProvider();

		const port = await freePort();
		const server = app.listen(port) as unknown as {
			close(cb: () => void): void;
		};
		stopServer = () => new Promise<void>((r) => server.close(() => r()));
		attachExpress(app, provider, { host: "127.0.0.1", port });
		await sleep(300);

		const baseline = await driveVerbsAndError(port, base);
		const name = providerName!;

		await expectHttpOps(name, base, baseline);

		// Payload capture is adapter-agnostic — assert the IN/OUT path once.
		await waitFor(
			async () => {
				const { inp, outp } = await countHttpPayloads(name, base, baseline);
				return inp >= 3 && outp >= 5;
			},
			12_000,
			`>=3 IN + >=5 OUT raw/json payloads for ${name} ${base}`,
		);
		const { inp, outp } = await countHttpPayloads(name, base, baseline);
		expect(inp).toBeGreaterThanOrEqual(3);
		expect(outp).toBeGreaterThanOrEqual(5);
	}, 40_000);

	test("Fastify: 5 verbs + 500 error → >=5 SUCCESS and >=1 ERROR HTTP.HANDLE ops", async () => {
		const base = `/${uniqueId("fastify")}`;

		provider = await startProvider();

		const app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: provider, host: "127.0.0.1" });
		app.get(`${base}/get`, async () => ({ ok: true }));
		app.post(`${base}/post`, async () => ({ ok: true }));
		app.put(`${base}/put`, async () => ({ ok: true }));
		app.delete(`${base}/delete`, async () => ({ ok: true }));
		app.patch(`${base}/patch`, async () => ({ ok: true }));
		app.get(
			`${base}/error`,
			async (_req: FastifyRequest, reply: FastifyReply) => {
				reply.code(500);
				return { error: true };
			},
		);

		const port = await freePort();
		await app.listen({ port, host: "127.0.0.1" });
		stopServer = () => app.close();
		await sleep(300);

		const baseline = await driveVerbsAndError(port, base);

		await expectHttpOps(providerName!, base, baseline);
	}, 40_000);

	test("Hono: 5 verbs + 500 error → >=5 SUCCESS and >=1 ERROR HTTP.HANDLE ops", async () => {
		const base = `/${uniqueId("hono")}`;
		const app = new Hono();
		app.get(`${base}/get`, (c) => c.json({ ok: true }));
		app.post(`${base}/post`, (c) => c.json({ ok: true }));
		app.put(`${base}/put`, (c) => c.json({ ok: true }));
		app.delete(`${base}/delete`, (c) => c.json({ ok: true }));
		app.patch(`${base}/patch`, (c) => c.json({ ok: true }));
		app.get(`${base}/error`, (c) => c.json({ error: true }, 500));

		provider = await startProvider();

		const port = await freePort();
		attachHono(app, provider, { host: "127.0.0.1", port });
		const server = Bun.serve({ fetch: app.fetch, port }) as unknown as {
			stop(): void;
		};
		stopServer = () => server.stop();
		await sleep(300);

		const baseline = await driveVerbsAndError(port, base);

		await expectHttpOps(providerName!, base, baseline);
	}, 40_000);
});
