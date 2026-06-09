/**
 * UX features e2e — exercises the three ergonomic improvements landed in
 * branch `feat/rpc-ux`:
 *   1. advertise defaults to 127.0.0.1 when not configured
 *   2. ProtoFileSpec auto-resolves input/output from `service` block
 *   3. sb.client(svc, .proto) — typed proxy with auto schema + auto deps
 *
 * Requires the same env keys as rpc-proxy.test.ts.
 */

import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";

const URL = process.env.SERVICEBRIDGE_URL;
const PROVIDER_KEY = process.env.SERVICEBRIDGE_SERVICE_KEY;
const CONSUMER_KEY = process.env.SERVICEBRIDGE_SERVICE2_KEY;
const enabled = Boolean(URL && PROVIDER_KEY && CONSUMER_KEY);

const PROVIDER_SVC = "e2e-registry-svc";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
};

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
	p: () => boolean,
	ms: number,
	what: string,
): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < ms) {
		if (p()) return;
		await sleep(50);
	}
	throw new Error(`timeout waiting for: ${what}`);
}

function writeServiceProto(): string {
	const dir = mkdtempSync(join(tmpdir(), "sb-ux-"));
	const p = join(dir, "payment.proto");
	writeFileSync(
		p,
		`syntax = "proto3";

service PaymentService {
  rpc Charge(ChargeRequest) returns (ChargeResponse);
  rpc Stream(StreamRequest) returns (stream StreamChunk);
}

message ChargeRequest {
  string user_id = 1;
  double amount  = 2;
}

message ChargeResponse {
  string transaction_id = 1;
  bool   ok             = 2;
}

message StreamRequest {
  int32 count = 1;
}

message StreamChunk {
  int32 i = 1;
}
`,
	);
	return p;
}

describe.skipIf(!enabled)("RPC UX e2e", () => {
	let provider: ServiceBridge | undefined;
	let consumer: ServiceBridge | undefined;

	afterEach(async () => {
		await consumer?.stop();
		await provider?.stop();
		provider = undefined;
		consumer = undefined;
	});

	test("advertise default (no config) + auto-resolve from service definition", async () => {
		const protoFile = writeServiceProto();

		// Callee: NO `advertise` set → SDK falls back to 127.0.0.1:0
		// Handler schema: NO input/output specified → resolved from service block
		provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
		provider.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			"Charge",
			async (req) => ({
				transactionId: `tx-${req.userId}`,
				ok: req.amount > 0,
			}),
			{ schema: { protoFile } }, // ← no input/output, auto from service
		);
		await provider.start();
		await waitFor(
			() => provider!.identity() !== null,
			5_000,
			"provider connected",
		);

		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["Charge"] });
		await consumer.start();
		await waitFor(
			() => consumer!.identity() !== null,
			5_000,
			"consumer connected",
		);
		await waitFor(
			() => {
				for (const d of [...consumer!.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "Charge") return true;
				}
				return false;
			},
			5_000,
			"Charge visible",
		);
		await consumer.useSchema(PROVIDER_SVC, "Charge", { protoFile });

		const result = await consumer.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(PROVIDER_SVC, "Charge", { userId: "u-1", amount: 50 });

		expect(result.transactionId).toBe("tx-u-1");
		expect(result.ok).toBe(true);
	});

	test("sb.client() — typed proxy registers deps + schemas + makes calls", async () => {
		const protoFile = writeServiceProto();

		// Provider exposes both unary + streaming methods, schema auto-resolved.
		provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
		provider.rpc.handle(
			"Charge",
			async (req: { userId: string }) => ({
				transactionId: `paid-${req.userId}`,
				ok: true,
			}),
			{ schema: { protoFile } },
		);
		provider.rpc.handleStream<{ count: number }, { i: number }>(
			"Stream",
			async function* (req) {
				for (let i = 0; i < req.count; i++) {
					yield { i };
				}
			},
			{ schema: { protoFile } },
		);
		await provider.start();
		await waitFor(
			() => provider!.identity() !== null,
			5_000,
			"provider connected",
		);

		// Consumer: declare typed client BEFORE start — sb.client() registers
		// outgoing deps and they ride the initial RegisterRequest.
		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		const payment = await consumer.client(PROVIDER_SVC, protoFile);
		await consumer.start();
		await waitFor(
			() => consumer!.identity() !== null,
			5_000,
			"consumer connected",
		);

		// Wait until provider's methods land in consumer's snapshot.
		await waitFor(
			() => {
				let charged = false;
				let streamed = false;
				for (const d of [...consumer!.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "Charge") charged = true;
					if (d.name === "Stream") streamed = true;
				}
				return charged && streamed;
			},
			5_000,
			"methods visible in snapshot",
		);

		// Unary call via typed proxy.
		const charge = (await (
			payment.Charge as (req: unknown) => Promise<unknown>
		)({ userId: "alice", amount: 100 })) as { transactionId: string };
		expect(charge.transactionId).toBe("paid-alice");

		// Streaming call via the same proxy (sb.client picks AsyncIterable for
		// methods marked `returns (stream ...)`).
		const stream = (
			payment.Stream as (req: unknown) => AsyncIterable<{ i: number }>
		)({
			count: 3,
		});
		const chunks: number[] = [];
		for await (const c of stream) chunks.push(c.i);
		expect(chunks).toEqual([0, 1, 2]);
	});

	test("sb.client() with methods filter binds only listed methods", async () => {
		const protoFile = writeServiceProto();

		provider = new ServiceBridge(URL!, PROVIDER_KEY!, FAST_OPTS);
		provider.rpc.handle(
			"Charge",
			async () => ({ transactionId: "x", ok: true }),
			{
				schema: { protoFile },
			},
		);
		await provider.start();
		await waitFor(
			() => provider!.identity() !== null,
			5_000,
			"provider connected",
		);

		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		const payment = await consumer.client(PROVIDER_SVC, protoFile, {
			methods: ["Charge"],
		});

		// Only Charge is on the proxy.
		expect(typeof payment.Charge).toBe("function");
		expect(payment.Stream).toBeUndefined();
	});

	test("sb.client() throws if proto has no service block", async () => {
		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		// payment.proto в testdata не имеет service definition.
		const noServiceProto = join(
			import.meta.dir,
			"..",
			"..",
			"src",
			"serde",
			"testdata",
			"payment.proto",
		);
		await expect(consumer.client(PROVIDER_SVC, noServiceProto)).rejects.toThrow(
			/no service block/,
		);
	});
});
