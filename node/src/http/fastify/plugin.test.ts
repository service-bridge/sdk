import { afterEach, describe, expect, it } from "bun:test";
import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import { Status } from "../../telemetry/ops";
import { makeSbStub } from "../_common/sb-stub";
import { sbFastify } from "./plugin";

describe("sbFastify plugin", () => {
	let app: ReturnType<typeof Fastify> | undefined;

	afterEach(async () => {
		if (app) {
			await app.close();
			app = undefined;
		}
	});

	it("collects routes via onRoute hook", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.get("/api/orders/:id", async () => ({ ok: true }));
		app.post("/api/orders", async () => ({ created: true }));
		app.delete("/api/orders/:id", async () => ({}));
		await app.ready();

		const patterns = stub.routes
			.snapshot()
			.map((r) => `${r.method} ${r.pattern}`);
		expect(patterns).toContain("GET /api/orders/:id");
		expect(patterns).toContain("POST /api/orders");
		expect(patterns).toContain("DELETE /api/orders/:id");
	});

	it("handles array method ['GET','POST']", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.route({
			method: ["GET", "POST"],
			url: "/multi",
			handler: async () => ({}),
		});
		await app.ready();

		const patterns = stub.routes
			.snapshot()
			.map((r) => `${r.method} ${r.pattern}`);
		expect(patterns).toContain("GET /multi");
		expect(patterns).toContain("POST /multi");
	});

	it("publishes raw Fastify URL with regex constraints unchanged", async () => {
		// Раньше pattern.ts удалял `(^\\d+$)` — теперь SDK публикует сырой URL.
		// Косметика — забота Service Map UI.
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.get("/users/:id(^\\d+$)", async () => ({}));
		await app.ready();

		const patterns = stub.routes
			.snapshot()
			.map((r) => `${r.method} ${r.pattern}`);
		expect(patterns).toContain("GET /users/:id(^\\d+$)");
	});

	it("skips auto-generated HEAD routes", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.get("/a", async () => ({}));
		await app.ready();

		const methods = stub.routes.snapshot().map((r) => r.method);
		expect(methods).toContain("GET");
		expect(methods).not.toContain("HEAD");
	});

	it("publishes httpEndpoint on listen via onReady", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.get("/health", async () => ({ ok: true }));
		await app.listen({ port: 0, host: "127.0.0.1" });

		expect(stub.endpoint).toMatch(/127\.0\.0\.1:\d+/);
		expect(stub.publishHits).toBe(1);
	});

	it("respects explicit host override", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb, host: "internal.example" });
		app.get("/x", async () => ({}));
		await app.listen({ port: 0, host: "127.0.0.1" });

		expect(stub.endpoint).toMatch(/^internal\.example:\d+$/);
	});

	it("ends HTTP.HANDLE with TIMEOUT when client aborts before response", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		// Handler hangs forever — the only way the request resolves is the abort.
		app.get("/hang", () => new Promise<never>(() => {}));
		await app.listen({ port: 0, host: "127.0.0.1" });
		const addr = app.server.address();
		if (!addr || typeof addr !== "object") throw new Error("no address");
		const port = addr.port;

		const ctrl = new AbortController();
		const inflight = fetch(`http://127.0.0.1:${port}/hang`, {
			signal: ctrl.signal,
		}).catch(() => {
			// aborted — expected
		});
		// Give Fastify a tick to enter the handler, then abort the connection.
		await new Promise((r) => setTimeout(r, 50));
		ctrl.abort();
		await inflight;

		// Allow onRequestAbort to fire.
		await new Promise((r) => setTimeout(r, 50));

		expect(stub.endCalls).toHaveLength(1);
		expect(stub.endCalls[0]?.status).toBe(Status.TIMEOUT);
		expect(stub.endCalls[0]?.message).toBe("client abort");
	});

	it("dedupes when route added twice with same method/pattern", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.get("/dup", async () => ({}));
		await app.ready();
		// можно не симулировать duplicate — Fastify не позволит две одинаковых.
		// Но проверим что RouteCollector внутренний дедуп работает через add()
		stub.routes.add({ method: "GET", pattern: "/dup", source: "fastify" });
		stub.routes.add({ method: "GET", pattern: "/dup", source: "fastify" });
		const count = stub.routes
			.snapshot()
			.filter((r) => r.method === "GET" && r.pattern === "/dup").length;
		expect(count).toBe(1);
	});

	it("maps 4xx and 5xx alike to ERROR", async () => {
		const stub = makeSbStub();
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.get("/missing", async (_req: FastifyRequest, reply: FastifyReply) =>
			reply.code(404).send({}),
		);
		app.get("/boom", async (_req: FastifyRequest, reply: FastifyReply) =>
			reply.code(503).send({}),
		);
		await app.ready();
		await app.inject({ method: "GET", url: "/missing" });
		await app.inject({ method: "GET", url: "/boom" });
		expect(stub.endCalls).toEqual([
			{ status: Status.ERROR, message: "HTTP 404" },
			{ status: Status.ERROR, message: "HTTP 503" },
		]);
	});
});

describe("sbFastify payload capture gating", () => {
	let app: ReturnType<typeof Fastify> | undefined;

	afterEach(async () => {
		if (app) {
			await app.close();
			app = undefined;
		}
	});

	async function roundtrip(mode: "none" | "all") {
		const stub = makeSbStub(mode);
		app = Fastify({ logger: false });
		await app.register(sbFastify, { sb: stub.sb });
		app.post("/echo", async () => ({ ok: true }));
		await app.ready();
		await app.inject({
			method: "POST",
			url: "/echo",
			payload: { hello: "world" },
		});
		return stub;
	}

	it('captures nothing when capture mode is "none"', async () => {
		const stub = await roundtrip("none");
		expect(stub.captures).toHaveLength(0);
		expect(stub.endCalls).toEqual([
			{ status: Status.SUCCESS, message: undefined },
		]);
	});

	it('captures request and response bodies when capture mode is "all"', async () => {
		const stub = await roundtrip("all");
		expect(stub.captures.map((c) => c.direction)).toEqual(["in", "out"]);
	});
});
