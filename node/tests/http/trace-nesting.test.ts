// Поведенческая регрессия: downstream user-code (sb.rpc.call / event.publish),
// вызванный из HTTP-handler, ОБЯЗАН видеть trace-контекст, где:
//   - traceId == traceId стартованного HTTP.HANDLE op (единый trace, не split);
//   - parentOpId == opId этого HTTP.HANDLE op (вложенность, а не соседний корень).
// Покрывает express / fastify / hono одинаково. Источник истины — als из
// telemetry/context (в исходниках он единственный; дублирование лечит сборка,
// см. tests/build/single-als-instance.test.ts).

import { afterEach, describe, expect, it } from "bun:test";
import express from "express";
import Fastify from "fastify";
import { Hono } from "hono";
import { uuidv7 } from "uuidv7";
import type { ServiceBridge } from "../../src/connection/service-bridge";
import { attachExpress } from "../../src/http/express";
import { sbFastify } from "../../src/http/fastify";
import { attachHono } from "../../src/http/hono";
import { RouteCollector } from "../../src/http/route";
import { currentTraceContext } from "../../src/telemetry/context";
import type { TraceContext } from "../../src/telemetry/trace-context";

interface StartedOp {
	traceId: string;
	opId: string;
}

function makeSb(): { sb: ServiceBridge; started: StartedOp[] } {
	const started: StartedOp[] = [];
	const sb = {
		routes: new RouteCollector({ setEndpoint() {}, triggerRestart() {} }),
		telemetry: {
			startOp(p: { traceId?: string }) {
				const op = { traceId: p.traceId ?? "", opId: uuidv7() };
				started.push(op);
				return {
					...op,
					capturing: false,
					captureIn() {},
					captureOut() {},
					end() {},
				};
			},
		},
	} as unknown as ServiceBridge;
	return { sb, started };
}

function assertNested(seen: TraceContext | undefined, httpOp: StartedOp): void {
	expect(seen).toBeDefined();
	expect(seen?.traceId).toBe(httpOp.traceId);
	expect(seen?.parentOpId).toBe(httpOp.opId);
}

describe("HTTP→downstream trace nesting", () => {
	let fastifyApp: ReturnType<typeof Fastify> | undefined;
	let expressServer: ReturnType<express.Express["listen"]> | undefined;

	afterEach(async () => {
		if (fastifyApp) {
			await fastifyApp.close();
			fastifyApp = undefined;
		}
		if (expressServer) {
			await new Promise<void>((r) => expressServer?.close(() => r()));
			expressServer = undefined;
		}
	});

	it("fastify: downstream sees HTTP.HANDLE as parent", async () => {
		const { sb, started } = makeSb();
		fastifyApp = Fastify({ logger: false });
		await fastifyApp.register(sbFastify, { sb });
		let seen: TraceContext | undefined;
		fastifyApp.get("/payments/prices/:currency", async () => {
			await new Promise((r) => setTimeout(r, 1));
			seen = currentTraceContext();
			return { ok: true };
		});
		await fastifyApp.listen({ port: 0, host: "127.0.0.1" });
		const port = (fastifyApp.server.address() as { port: number }).port;
		await (await fetch(`http://127.0.0.1:${port}/payments/prices/RUB`)).text();

		expect(started.length).toBe(1);
		assertNested(seen, started[0]!);
	});

	it("express: downstream sees HTTP.HANDLE as parent", async () => {
		const { sb, started } = makeSb();
		const app = express();
		let seen: TraceContext | undefined;
		app.get("/payments/prices/:currency", async (_req, res) => {
			await new Promise((r) => setTimeout(r, 1));
			seen = currentTraceContext();
			res.json({ ok: true });
		});
		const port = await new Promise<number>((resolve) => {
			expressServer = app.listen(0, "127.0.0.1", () => {
				resolve((expressServer?.address() as { port: number }).port);
			});
		});
		attachExpress(app, sb, { port });
		await (await fetch(`http://127.0.0.1:${port}/payments/prices/RUB`)).text();

		expect(started.length).toBe(1);
		assertNested(seen, started[0]!);
	});

	it("hono: downstream sees HTTP.HANDLE as parent", async () => {
		const { sb, started } = makeSb();
		const app = new Hono();
		let seen: TraceContext | undefined;
		app.get("/payments/prices/:currency", async (c) => {
			await new Promise((r) => setTimeout(r, 1));
			seen = currentTraceContext();
			return c.json({ ok: true });
		});
		attachHono(app, sb, { port: 1 });
		await app.fetch(new Request("http://localhost/payments/prices/RUB"));

		expect(started.length).toBe(1);
		assertNested(seen, started[0]!);
	});
});
