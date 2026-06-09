import { describe, expect, it } from "bun:test";
import { type Route, RouteCollector, type RouteSink } from "./route";

interface FakeSink extends RouteSink {
	last: string | null;
	setCalls: number;
	restartCalls: number;
}

function makeSink(): FakeSink {
	const state: FakeSink = {
		last: null,
		setCalls: 0,
		restartCalls: 0,
		setEndpoint(endpoint: string) {
			this.last = endpoint;
			this.setCalls++;
		},
		triggerRestart() {
			this.restartCalls++;
		},
	};
	return state;
}

const route = (
	method: string,
	pattern: string,
	source: Route["source"] = "express",
): Route => ({
	method,
	pattern,
	source,
});

describe("RouteCollector.add", () => {
	it("preserves insertion order in snapshot", () => {
		const c = new RouteCollector(makeSink());
		c.add(route("GET", "/a"));
		c.add(route("POST", "/b"));
		c.add(route("DELETE", "/c"));
		expect(c.snapshot().map((r) => r.pattern)).toEqual(["/a", "/b", "/c"]);
	});

	it("dedupes by `${method} ${pattern}` key", () => {
		const c = new RouteCollector(makeSink());
		c.add(route("GET", "/users/:id"));
		c.add(route("GET", "/users/:id"));
		c.add(route("POST", "/users/:id"));
		expect(c.size()).toBe(2);
		expect(c.snapshot()).toHaveLength(2);
	});

	it("treats different methods on same path as distinct", () => {
		const c = new RouteCollector(makeSink());
		c.add(route("GET", "/a"));
		c.add(route("POST", "/a"));
		c.add(route("PUT", "/a"));
		expect(c.size()).toBe(3);
	});

	it("last write wins on duplicate key (e.g. source change)", () => {
		const c = new RouteCollector(makeSink());
		c.add(route("GET", "/a", "express"));
		c.add(route("GET", "/a", "fastify"));
		expect(c.snapshot()[0]!.source).toBe("fastify");
	});
});

describe("RouteCollector.publishHttp", () => {
	it("writes endpoint to sink and triggers restart", () => {
		const sink = makeSink();
		const c = new RouteCollector(sink);
		c.publishHttp({ host: "10.0.0.5", port: 8080 });
		expect(sink.last).toBe("10.0.0.5:8080");
		expect(sink.setCalls).toBe(1);
		expect(sink.restartCalls).toBe(1);
	});

	it("overwrites endpoint on repeated call", () => {
		const sink = makeSink();
		const c = new RouteCollector(sink);
		c.publishHttp({ host: "a", port: 1 });
		c.publishHttp({ host: "b", port: 2 });
		expect(sink.last).toBe("b:2");
		expect(sink.setCalls).toBe(2);
		expect(sink.restartCalls).toBe(2);
	});
});

describe("RouteCollector.snapshot", () => {
	it("returns empty array when nothing added", () => {
		const c = new RouteCollector(makeSink());
		expect(c.snapshot()).toEqual([]);
		expect(c.size()).toBe(0);
	});
});
