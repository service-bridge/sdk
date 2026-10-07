import assert from "node:assert/strict";
import express from "express";
import { ServiceBridge, Status } from "service-bridge";
import { attachExpress } from "service-bridge/express";
import { sbFastify } from "service-bridge/fastify";
import { attachHono } from "service-bridge/hono";
import { createTestHarness } from "service-bridge/testing";

assert.equal(typeof ServiceBridge, "function");
assert.equal(typeof sbFastify, "function");
assert.equal(typeof attachHono, "function");
assert.equal(typeof createTestHarness, "function");
// Cross-entry AsyncLocalStorage: HTTP adapter scope must parent core telemetry.
// A well-formed bootstrap key (BootstrapKeyPayload{key_id, secret, ca_cert_der});
// this smoke never connects.
const key = `sb.${Buffer.concat([
	Buffer.from([0x0a, 8]),
	Buffer.alloc(8, 1),
	Buffer.from([0x12, 32]),
	Buffer.alloc(32, 2),
	Buffer.from([0x1a, 1, 3]),
]).toString("base64url")}`;
const bridge = new ServiceBridge("localhost:50051", key, {
	advertise: false,
});
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
attachExpress(app, bridge, {
	host: "127.0.0.1",
	port: 8080,
	trustTraceHeader: true,
});
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
