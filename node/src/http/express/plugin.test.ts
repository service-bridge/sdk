import { afterEach, describe, expect, it } from "bun:test";
import express, { Router } from "express";
import { Status } from "../../telemetry/ops";
import { makeSbStub } from "../_common/sb-stub";
import { attachExpress } from "./plugin";

describe("attachExpress", () => {
	it("collects top-level routes and publishes endpoint", () => {
		const app = express();
		app.get("/api/orders/:id", (_req, res) => {
			res.json({});
		});
		app.post("/api/orders", (_req, res) => {
			res.json({});
		});
		app.delete("/api/orders/:id", (_req, res) => {
			res.status(204).end();
		});

		const stub = makeSbStub();
		attachExpress(app, stub.sb, { host: "h", port: 1234 });
		const patterns = stub.routes
			.snapshot()
			.map((r) => `${r.method} ${r.pattern}`);
		expect(patterns).toContain("GET /api/orders/:id");
		expect(patterns).toContain("POST /api/orders");
		expect(patterns).toContain("DELETE /api/orders/:id");
		expect(stub.endpoint).toBe("h:1234");
		expect(stub.publishHits).toBe(1);
	});

	it("collects routes from mounted sub-router (Express 5: без префикса; Express 4: с префиксом)", () => {
		const app = express();
		const sub = Router();
		sub.get("/list", (_req, res) => {
			res.json([]);
		});
		sub.post("/create", (_req, res) => {
			res.status(201).json({});
		});
		app.use("/v1/users", sub);

		const stub = makeSbStub();
		attachExpress(app, stub.sb, { host: "h", port: 1 });
		const patterns = stub.routes
			.snapshot()
			.map((r) => `${r.method} ${r.pattern}`);
		expect(
			patterns.includes("GET /v1/users/list") || patterns.includes("GET /list"),
		).toBe(true);
		expect(
			patterns.includes("POST /v1/users/create") ||
				patterns.includes("POST /create"),
		).toBe(true);
	});

	it("dedupes when called twice (idempotent route add)", () => {
		const app = express();
		app.get("/a", (_req, res) => {
			res.json({});
		});
		const stub = makeSbStub();
		attachExpress(app, stub.sb, { host: "h", port: 1 });
		attachExpress(app, stub.sb, { host: "h", port: 1 });
		expect(stub.routes.size()).toBe(1);
	});

	it("handles multi-method route via app.route()", () => {
		const app = express();
		app
			.route("/multi")
			.get((_req, res) => {
				res.json({});
			})
			.post((_req, res) => {
				res.json({});
			});
		const stub = makeSbStub();
		attachExpress(app, stub.sb, { host: "h", port: 1 });
		const patterns = stub.routes
			.snapshot()
			.map((r) => `${r.method} ${r.pattern}`);
		expect(patterns).toContain("GET /multi");
		expect(patterns).toContain("POST /multi");
	});

	it("falls back to resolveHttpAdvertiseHost when host omitted", () => {
		const app = express();
		app.get("/a", (_req, res) => {
			res.json({});
		});
		const stub = makeSbStub();
		attachExpress(app, stub.sb, { port: 9999 });
		expect(stub.endpoint).toMatch(/:9999$/);
		expect(stub.publishHits).toBe(1);
	});

	it("publishes endpoint even when app has no router yet (no routes registered)", () => {
		const app = express();
		const stub = makeSbStub();
		attachExpress(app, stub.sb, { host: "h", port: 42 });
		expect(stub.endpoint).toBe("h:42");
		expect(stub.publishHits).toBe(1);
	});

	it("fails loudly when the root router is unreachable", () => {
		// Без root router'а middleware остался бы за роутами и HTTP.HANDLE не
		// эмиттился бы вовсе — молчать здесь нельзя.
		const fake = { use() {} } as unknown as express.Express;
		const stub = makeSbStub();
		expect(() => attachExpress(fake, stub.sb, { host: "h", port: 1 })).toThrow(
			/root router not found/,
		);
	});
});

describe("attachExpress payload capture gating", () => {
	let server: ReturnType<express.Express["listen"]> | undefined;

	afterEach(async () => {
		if (server) {
			await new Promise<void>((r) => server?.close(() => r()));
			server = undefined;
		}
	});

	async function roundtrip(mode: "none" | "all") {
		const stub = makeSbStub(mode);
		const app = express();
		app.use(express.json());
		// toJSON считает КАЖДУЮ сериализацию этого тела: одну делает сам
		// res.json, вторую — payload capture. При mode="none" второй быть не должно.
		let serializations = 0;
		const payload = {
			toJSON() {
				serializations++;
				return { ok: true };
			},
		};
		app.post("/echo", (_req, res) => {
			res.json(payload);
		});
		const port = await new Promise<number>((resolve) => {
			server = app.listen(0, "127.0.0.1", () => {
				resolve((server?.address() as { port: number }).port);
			});
		});
		attachExpress(app, stub.sb, { port });
		await (
			await fetch(`http://127.0.0.1:${port}/echo`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ hello: "world" }),
			})
		).text();
		// res "finish" срабатывает асинхронно относительно ответа клиенту.
		await new Promise((r) => setTimeout(r, 30));
		return { stub, serializations: () => serializations };
	}

	it('does not serialize bodies when capture mode is "none"', async () => {
		const { stub, serializations } = await roundtrip("none");
		expect(serializations()).toBe(1);
		expect(stub.captures).toHaveLength(0);
		expect(stub.endCalls).toEqual([
			{ status: Status.SUCCESS, message: undefined },
		]);
	});

	it('captures request and response bodies when capture mode is "all"', async () => {
		const { stub, serializations } = await roundtrip("all");
		expect(serializations()).toBe(2);
		expect(stub.captures.map((c) => c.direction)).toEqual(["in", "out"]);
	});
});
