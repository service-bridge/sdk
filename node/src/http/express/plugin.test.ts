import { describe, expect, it } from "bun:test";
import express, { Router } from "express";
import type { ServiceBridge } from "../../connection/service-bridge";
import { RouteCollector } from "../route";
import { attachExpress } from "./plugin";

interface SbStub {
	sb: ServiceBridge;
	routes: RouteCollector;
	endpoint: string | null;
	publishHits: number;
}

function makeSbStub(): SbStub {
	const state = {
		endpoint: null as string | null,
		publishHits: 0,
	};
	const collector = new RouteCollector({
		setEndpoint(ep: string) {
			state.endpoint = ep;
		},
		triggerRestart() {
			state.publishHits++;
		},
	});
	const sb = { routes: collector } as unknown as ServiceBridge;
	return {
		sb,
		routes: collector,
		get endpoint() {
			return state.endpoint;
		},
		get publishHits() {
			return state.publishHits;
		},
	} as unknown as SbStub;
}

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
});
