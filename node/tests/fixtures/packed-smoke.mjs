import assert from "node:assert/strict";
import express from "express";
import { ServiceBridge, Status } from "service-bridge";
import { attachExpress } from "service-bridge/express";
import { sbFastify } from "service-bridge/fastify";
import { attachHono } from "service-bridge/hono";
import { createTestHarness } from "service-bridge/testing";

const Database = globalThis.Bun
	? (await import("bun:sqlite")).Database
	: (await import("better-sqlite3")).default;

assert.equal(typeof ServiceBridge, "function");
assert.equal(typeof sbFastify, "function");
assert.equal(typeof attachHono, "function");
assert.equal(typeof createTestHarness, "function");
const db = new Database(":memory:");
db.exec("create table smoke (id integer primary key, value text not null)");
db.prepare("insert into smoke(value) values (?)").run("native");
assert.equal(db.prepare("select value from smoke").get().value, "native");
db.close();
// Cross-entry AsyncLocalStorage: HTTP adapter scope must parent core telemetry.
const bridge = new ServiceBridge("localhost:50051", "not-used");
let observed;
const app = express();
app.get("/smoke", (_req, res) => {
	const child = bridge.telemetry.startOp({
		subject: "child",
		channel: 0,
		kind: 0,
	});
	observed = child.traceId;
	child.end(Status.SUCCESS);
	res.send("ok");
});
attachExpress(app, bridge, { host: "127.0.0.1", port: 8080 });
const server = app.listen(0, "127.0.0.1");
await new Promise((resolve) => server.once("listening", resolve));
const trace = "0192f000-0000-7000-8000-000000000abc";
const parent = "0192f000-0000-7000-8000-000000000def";
assert.equal(
	(
		await fetch(`http://127.0.0.1:${server.address().port}/smoke`, {
			headers: { "x-sb-trace": `${trace}-${parent}` },
		})
	).status,
	200,
);
assert.equal(observed, trace);
await new Promise((resolve) => server.close(resolve));
