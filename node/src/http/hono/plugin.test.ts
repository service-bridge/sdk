import { describe, expect, it } from "bun:test";
import { Hono } from "hono";
import type { ServiceBridge } from "../../connection/service-bridge";
import { Status } from "../../telemetry/ops";
import { makeSbStub } from "../_common/sb-stub";
import { RouteCollector } from "../route";
import { attachHono, collectHonoRoutes } from "./plugin";

describe("collectHonoRoutes", () => {
	it("collects routes after registration", () => {
		const stub = makeSbStub();
		const app = new Hono();
		app.get("/api/orders/:id", (c) => c.json({}));
		app.post("/api/orders", (c) => c.json({}));
		app.delete("/api/orders/:id", (c) => c.body(null, 204));

		collectHonoRoutes(app, stub.sb);
		const patterns = stub.routes
			.snapshot()
			.map((r) => `${r.method} ${r.pattern}`);
		expect(patterns).toContain("GET /api/orders/:id");
		expect(patterns).toContain("POST /api/orders");
		expect(patterns).toContain("DELETE /api/orders/:id");
	});

	it("publishes raw Hono path with regex constraints unchanged", () => {
		// Раньше pattern.ts удалял `{[0-9]+}` — теперь SDK публикует сырой
		// `route.path` из Hono. Косметика — забота Service Map UI.
		const stub = makeSbStub();
		const app = new Hono();
		app.get("/users/:id{[0-9]+}", (c) => c.json({}));
		collectHonoRoutes(app, stub.sb);
		expect(stub.routes.snapshot()[0]!.pattern).toBe("/users/:id{[0-9]+}");
	});

	it("publishes raw path with multiple regex constraints", () => {
		const stub = makeSbStub();
		const app = new Hono();
		app.get("/post/:date{[0-9]+}/:title{[a-z]+}", (c) => c.json({}));
		collectHonoRoutes(app, stub.sb);
		expect(stub.routes.snapshot()[0]!.pattern).toBe(
			"/post/:date{[0-9]+}/:title{[a-z]+}",
		);
	});

	it("keeps wildcard *", () => {
		const stub = makeSbStub();
		const app = new Hono();
		app.get("/files/*", (c) => c.json({}));
		collectHonoRoutes(app, stub.sb);
		expect(stub.routes.snapshot()[0]!.pattern).toBe("/files/*");
	});

	it("skips ALL method", () => {
		const stub = makeSbStub();
		const app = new Hono();
		app.all("/everything", (c) => c.json({}));
		collectHonoRoutes(app, stub.sb);
		const methods = stub.routes.snapshot().map((r) => r.method);
		expect(methods).not.toContain("ALL");
	});
});

describe("attachHono", () => {
	it("registers routes + endpoint atomically", () => {
		const stub = makeSbStub();
		const app = new Hono();
		app.get("/api/a", (c) => c.json({}));
		app.post("/api/b", (c) => c.json({}));

		attachHono(app, stub.sb, { host: "127.0.0.1", port: 8080 });
		expect(stub.routes.size()).toBe(2);
		expect(stub.endpoint).toBe("127.0.0.1:8080");
		expect(stub.publishHits).toBe(1);
	});

	it("falls back to resolveHttpAdvertiseHost when host omitted", () => {
		const stub = makeSbStub();
		const app = new Hono();
		app.get("/a", (c) => c.json({}));
		attachHono(app, stub.sb, { port: 5555 });
		expect(stub.endpoint).toMatch(/:5555$/);
	});

	it("is callable before sb.start (triggerRestart no-op until started)", () => {
		// Симулируем pre-start: triggerRestart игнорируется (no-op).
		const sink = {
			endpoint: null as string | null,
			restartCalls: 0,
			setEndpoint(ep: string) {
				this.endpoint = ep;
			},
			triggerRestart() {
				this.restartCalls++;
			},
		};
		const collector = new RouteCollector(sink);
		const sb = { routes: collector } as unknown as ServiceBridge;
		const app = new Hono();
		app.get("/early", (c) => c.json({}));
		attachHono(app, sb, { host: "h", port: 9090 });
		expect(collector.size()).toBe(1);
		expect(sink.endpoint).toBe("h:9090");
		// triggerRestart called once (но это no-op в реальном ServiceBridge до start).
		expect(sink.restartCalls).toBe(1);
	});
});

describe("attachHono payload capture gating", () => {
	async function roundtrip(mode: "none" | "all") {
		const stub = makeSbStub(mode);
		const app = new Hono();
		app.post("/echo", (c) => c.json({ ok: true }));
		attachHono(app, stub.sb, { port: 1 });

		const req = new Request("http://localhost/echo", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ hello: "world" }),
		});
		// Клон стрима — самая дорогая часть захвата у Hono; считаем его напрямую.
		let clones = 0;
		const origClone = req.clone.bind(req);
		Object.defineProperty(req, "clone", {
			value: () => {
				clones++;
				return origClone();
			},
		});
		await app.fetch(req);
		return { stub, clones: () => clones };
	}

	it('does not clone or serialize bodies when capture mode is "none"', async () => {
		const { stub, clones } = await roundtrip("none");
		expect(clones()).toBe(0);
		expect(stub.captures).toHaveLength(0);
		expect(stub.endCalls).toEqual([
			{ status: Status.SUCCESS, message: undefined },
		]);
	});

	it('captures request and response bodies when capture mode is "all"', async () => {
		const { stub, clones } = await roundtrip("all");
		expect(clones()).toBe(1);
		expect(stub.captures.map((c) => c.direction)).toEqual(["in", "out"]);
	});

	it("maps 4xx and 5xx alike to ERROR", async () => {
		const stub = makeSbStub();
		const app = new Hono();
		app.get("/missing", (c) => c.json({}, 404));
		app.get("/boom", (c) => c.json({}, 503));
		attachHono(app, stub.sb, { port: 1 });
		await app.fetch(new Request("http://localhost/missing"));
		await app.fetch(new Request("http://localhost/boom"));
		expect(stub.endCalls).toEqual([
			{ status: Status.ERROR, message: "HTTP 404" },
			{ status: Status.ERROR, message: "HTTP 503" },
		]);
	});
});
