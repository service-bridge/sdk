import { connect } from "node:net";
import Fastify from "fastify";
import { makeSbStub } from "../../src/http/_common/sb-stub";
import { sbFastify } from "../../src/http/fastify/plugin";

const stub = makeSbStub();
const app = Fastify({ logger: false });
await app.register(sbFastify, { sb: stub.sb });
let entered!: () => void;
const ready = new Promise<void>((resolve) => {
	entered = resolve;
});
let release!: () => void;
const waiting = new Promise<void>((resolve) => {
	release = resolve;
});
app.post("/delayed", async () => {
	entered();
	await waiting;
	return { ok: true };
});
await app.listen({ port: 0, host: "127.0.0.1" });
const addr = app.server.address();
if (!addr || typeof addr === "string") throw new Error("missing TCP address");
const socket = connect({ host: "127.0.0.1", port: addr.port });
socket.on("error", () => {});
await new Promise<void>((resolve) => socket.once("connect", resolve));
const body = JSON.stringify({ ids: [1, 2] });
socket.write(
	`POST /delayed HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
);
await ready;
socket.destroy();
const deadline = Date.now() + 1000;
while (!stub.endCalls.length && Date.now() < deadline)
	await new Promise((resolve) => setTimeout(resolve, 2));
release();
await new Promise((resolve) => setTimeout(resolve, 50));
await app.close();
console.log(JSON.stringify(stub.endCalls));
