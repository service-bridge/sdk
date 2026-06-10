// rpc e2e — consolidated RPC transport, observability, contract-routing, and
// ergonomics coverage.
//
// Registration model (DECLARE-BEFORE-START): the runtime resolves discovery
// against the handlers/deps present at a client's registration. A handler or
// outgoing dep added AFTER the client is connected does NOT propagate. Every
// party here either registers an rpc handler (callee/provider) OR declares an
// outgoing service() dep (caller/consumer), so EVERY party is a dedicated,
// unstarted client that registers/declares BEFORE connect() and is stop()'d in
// afterEach. No warm-pool client is used — the pool's clients are already
// started, so a post-start registration would never be discovered.
//
// Isolation: every rpc method name is unique (uniqueName), so concurrent
// dedicated clients under the same per-domain identity add methods/instances
// that no sibling test discovers. The operations-row query is scoped by the
// unique method name + a started_at baseline. No TRUNCATE, no policy mutation —
// e2e-rpc-1/2/3 are default-allow for these pure-transport tests.

import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { connect, uniqueId, uniqueName, waitFor } from "./_helpers/fixtures";
import { withDb } from "./_helpers/policy-db";

const PROTO_FILE = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"serde",
	"testdata",
	"payment.proto",
);

const SCHEMA = {
	protoFile: PROTO_FILE,
	input: "ChargeRequest",
	output: "ChargeResponse",
} as const;

const OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

const ADVERTISE = { host: "127.0.0.1", port: 0 } as const;

// Per-domain key/url readers (mirror pool.ts: SB_E2E_<DOMAIN>_<index>).
// Dedicated clients here are built directly so each test controls advertise
// (false / unset / 127.0.0.1:0) and registers BEFORE connect().
function domainKey(index: number): { url: string; key: string } {
	const url = process.env.SERVICEBRIDGE_URL;
	if (!url) throw new Error("rpc e2e: SERVICEBRIDGE_URL not set");
	const d = process.env.SB_E2E_DOMAIN;
	if (!d) throw new Error("rpc e2e: SB_E2E_DOMAIN not set");
	const envName = `SB_E2E_${d.toUpperCase().replace(/-/g, "_")}_${index}`;
	const key = process.env[envName];
	if (!key) throw new Error(`rpc e2e: ${envName} not set`);
	return { url, key };
}

const primaryKey = () => domainKey(1);
const secondKey = () => domainKey(2);
const thirdKey = () => domainKey(3);

// Wait until `caller`'s snapshot lists `method` on `svcName`, optionally only
// once it is marked streaming.
function waitForMethod(
	caller: ServiceBridge,
	method: string,
	opts: { streaming?: boolean } = {},
): Promise<void> {
	return waitFor(
		() => {
			for (const d of [...caller.serviceMap().values()].flatMap(
				(e) => e.methods,
			)) {
				if (d.name === method && (!opts.streaming || d.streaming)) return true;
			}
			return false;
		},
		5_000,
		`${method} discovered`,
	);
}

// writeVersionedProtos emits two .proto files with the SAME message names but a
// DIFFERENT field set on v2 → different contract hashes that cannot decode each
// other's payloads.
function writeVersionedProtos(): { v1: string; v2: string } {
	const dir = mkdtempSync(join(tmpdir(), "sb-contract-"));
	const v1 = join(dir, "v1.proto");
	const v2 = join(dir, "v2.proto");
	writeFileSync(
		v1,
		`syntax = "proto3";
message ChargeRequest {
  string user_id = 1;
  double amount  = 2;
}
message ChargeResponse {
  string transaction_id = 1;
  bool   ok             = 2;
}
`,
	);
	writeFileSync(
		v2,
		`syntax = "proto3";
message ChargeRequest {
  string user_id = 1;
  double amount  = 2;
  string region  = 3;
}
message ChargeResponse {
  string transaction_id = 1;
  bool   ok             = 2;
  string region_used    = 3;
}
`,
	);
	return { v1, v2 };
}

// writeServiceProto emits a .proto with a `service` block so handler schemas
// can auto-resolve input/output and sb.client() can bind a typed proxy.
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

// countRpcOps returns, for the given method window, the OK+finished RPC.CALL
// rows (kind=1), the count of FORWARD/HANDLE rows (kind 2/3) — zero under the
// one-row model — plus the single CALL row's subject / via_proxy / attempt.
async function countRpcOps(
	method: string,
	startedAfterMs: number,
): Promise<{
	callOk: number;
	callTotal: number;
	forwardHandle: number;
	subject: string | null;
	viaProxy: boolean | null;
	attempt: number | null;
}> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT kind, status, finished_at IS NOT NULL AS finished,
			       subject, attempt, meta
			FROM operations
			WHERE channel = 2
			  AND subject LIKE ${`%${method}%`}
			  AND started_at >= to_timestamp(${startedAfterMs} / 1000.0)
		`) as Array<{
			kind: number;
			status: number;
			finished: boolean;
			subject: string;
			attempt: number;
			meta: { via_proxy?: boolean } | null;
		}>;
		const callRows = rows.filter((r) => r.kind === 1);
		const callRow = callRows[0];
		return {
			callOk: callRows.filter((r) => r.status === 2 && r.finished).length,
			callTotal: callRows.length,
			forwardHandle: rows.filter((r) => r.kind === 2 || r.kind === 3).length,
			subject: callRow?.subject ?? null,
			viaProxy: callRow?.meta?.via_proxy ?? null,
			attempt: callRow?.attempt ?? null,
		};
	});
}

describe("rpc", () => {
	// Dedicated clients owned by individual tests; stopped after each.
	let owned: ServiceBridge[] = [];

	function track(sb: ServiceBridge): ServiceBridge {
		owned.push(sb);
		return sb;
	}

	function newClient(
		key: { url: string; key: string },
		advertise: typeof ADVERTISE | false | undefined,
	): ServiceBridge {
		const opts = advertise === undefined ? { ...OPTS } : { ...OPTS, advertise };
		return track(new ServiceBridge(key.url, key.key, opts));
	}

	afterEach(async () => {
		await Promise.all(owned.map((c) => c.stop().catch(() => {})));
		owned = [];
	});

	test("unary proxy call returns round-trip payload", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			method,
			async (req) => ({
				transactionId: `tx-${req.userId}`,
				ok: req.amount > 0,
			}),
			{ schema: SCHEMA },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);
		await caller.useSchema(calleeName, method, SCHEMA);

		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			calleeName,
			method,
			{ userId: "u-42", amount: 100 },
			{ transport: "proxy", timeout: "5s" },
		);

		expect(result.transactionId).toBe("tx-u-42");
		expect(result.ok).toBe(true);
	}, 30_000);

	test("direct unary call bypasses proxy and returns round-trip payload", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			method,
			async (req) => ({
				transactionId: `direct-${req.userId}`,
				ok: req.amount > 0,
			}),
			{ schema: SCHEMA },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);
		await caller.useSchema(calleeName, method, SCHEMA);

		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			calleeName,
			method,
			{ userId: "u-42", amount: 100 },
			{ timeout: "5s", transport: "direct" },
		);

		expect(result.transactionId).toBe("direct-u-42");
		expect(result.ok).toBe(true);
	}, 30_000);

	test("direct call emits exactly one RPC.CALL operations row (via_proxy=false, no FORWARD/HANDLE)", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			method,
			async (req) => ({
				transactionId: `emit-${req.userId}`,
				ok: req.amount > 0,
			}),
			{ schema: SCHEMA },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);
		await caller.useSchema(calleeName, method, SCHEMA);

		const baseline = Date.now();
		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			calleeName,
			method,
			{ userId: "u-emit", amount: 100 },
			{
				timeout: "5s",
				transport: "direct",
				idempotencyKey: uniqueId("k-emit"),
			},
		);
		expect(result.ok).toBe(true);

		await waitFor(
			async () => (await countRpcOps(method, baseline)).callOk >= 1,
			10_000,
			"RPC.CALL row with status=OK in operations",
		);

		const { callOk, callTotal, forwardHandle, subject, viaProxy, attempt } =
			await countRpcOps(method, baseline);
		expect(callOk).toBe(1);
		expect(callTotal).toBe(1);
		expect(forwardHandle).toBe(0);
		expect(subject).toBe(`rpc.call:${calleeName}/${method}`);
		expect(viaProxy).toBe(false);
		expect(attempt).toBe(0);
	}, 30_000);

	test("auto transport selects direct when callee endpoint is known", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handle(
			method,
			async () => ({ transactionId: "auto-routed", ok: true }),
			{ schema: SCHEMA },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);
		await caller.useSchema(calleeName, method, SCHEMA);

		const baseline = Date.now();
		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(calleeName, method, { userId: "u", amount: 1 }, { transport: "auto" });
		expect(result.transactionId).toBe("auto-routed");

		// Pin the auto→direct selection: a direct call writes via_proxy=false.
		await waitFor(
			async () => (await countRpcOps(method, baseline)).callOk >= 1,
			10_000,
			"auto-call RPC.CALL row in operations",
		);
		expect((await countRpcOps(method, baseline)).viaProxy).toBe(false);
	}, 30_000);

	test("direct transport with no advertised endpoint rejects", async () => {
		const method = uniqueName("charge");

		// Callee under the 'third' key with advertise:false → no endpoint. 'third'
		// avoids perturbing primary/second advertised instances of sibling tests.
		const callee = newClient(thirdKey(), false);
		callee.rpc.handle(method, async () => ({}), { schema: SCHEMA });
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), false);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);
		await caller.useSchema(calleeName, method, SCHEMA);

		await expect(
			caller.rpc.call(
				calleeName,
				method,
				{ userId: "u", amount: 1 },
				{ transport: "direct" },
			),
		).rejects.toThrow(/no endpoint|no instance.*matches caller contract/);
	}, 30_000);

	test("rpc.call without useSchema rejects (no SchemaPair)", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handle(method, async () => ({}), { schema: SCHEMA });
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);

		await expect(
			caller.rpc.call(calleeName, method, { userId: "u", amount: 1 }),
		).rejects.toThrow(/no SchemaPair/);
	}, 30_000);

	test("rpc.call before discovery rejects (no descriptor)", async () => {
		const svc = uniqueName("nonexistent");
		const method = uniqueName("missing");

		// Caller declares a dep on a service that never exists → descriptor never
		// appears. Declared BEFORE connect().
		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(svc, { rpc: [method] });
		await connect(caller);
		await caller.useSchema(svc, method, SCHEMA);

		await expect(
			caller.rpc.call(svc, method, { userId: "u", amount: 1 }),
		).rejects.toThrow(/no descriptor/);
	}, 30_000);

	test("server-side streaming over proxy yields ordered chunks", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handleStream<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			method,
			async function* (req) {
				for (let i = 0; i < 5; i++) {
					yield { transactionId: `${req.userId}-${i}`, ok: true };
				}
			},
			{ schema: SCHEMA },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);
		await caller.useSchema(calleeName, method, SCHEMA);

		const chunks: { transactionId: string; ok: boolean }[] = [];
		for await (const chunk of caller.stream<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			calleeName,
			method,
			{ userId: "u", amount: 1 },
			{ transport: "proxy", timeout: "5s" },
		)) {
			chunks.push(chunk);
		}

		expect(chunks).toHaveLength(5);
		expect(chunks[0]!.transactionId).toBe("u-0");
		expect(chunks[4]!.transactionId).toBe("u-4");
	}, 30_000);

	test("server-side streaming over direct yields ordered chunks", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handleStream(
			method,
			async function* () {
				yield { transactionId: "tx-a", ok: true };
				yield { transactionId: "tx-b", ok: true };
			},
			{ schema: SCHEMA },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method);
		await caller.useSchema(calleeName, method, SCHEMA);

		const chunks: { transactionId: string }[] = [];
		for await (const chunk of caller.stream<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			calleeName,
			method,
			{ userId: "u", amount: 1 },
			{ transport: "direct" },
		)) {
			chunks.push(chunk);
		}

		expect(chunks.map((c) => c.transactionId)).toEqual(["tx-a", "tx-b"]);
	}, 30_000);

	test("unary rpc.call on a streaming method rejects", async () => {
		const method = uniqueName("charge");

		const callee = newClient(primaryKey(), ADVERTISE);
		callee.rpc.handleStream(
			method,
			async function* () {
				yield { transactionId: "x", ok: true };
			},
			{ schema: SCHEMA },
		);
		await connect(callee);
		const calleeName = callee.identity()!.serviceName;

		const caller = newClient(secondKey(), ADVERTISE);
		caller.service(calleeName, { rpc: [method] });
		await connect(caller);
		await waitForMethod(caller, method, { streaming: true });
		await caller.useSchema(calleeName, method, SCHEMA);

		await expect(
			caller.rpc.call(calleeName, method, { userId: "u", amount: 1 }),
		).rejects.toThrow(/streaming method/);
	}, 30_000);

	test("contract-hash routing: caller reaches only the instance whose schema matches", async () => {
		const { v1, v2 } = writeVersionedProtos();
		const method = uniqueName("charge");
		const key = primaryKey();

		// Two instances of the SAME identity serving `method` with DIFFERENT
		// schemas (v2 adds a `region` field) → distinct contract hashes. Both
		// register BEFORE connect().
		const v1Provider = newClient(key, ADVERTISE);
		v1Provider.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			method,
			async (req) => ({
				transactionId: `v1-${req.userId}`,
				ok: req.amount > 0,
			}),
			{
				schema: {
					protoFile: v1,
					input: "ChargeRequest",
					output: "ChargeResponse",
				},
			},
		);
		await connect(v1Provider);
		const providerSvc = v1Provider.identity()!.serviceName;

		const v2Provider = newClient(key, ADVERTISE);
		v2Provider.rpc.handle<
			{ userId: string; amount: number; region: string },
			{ transactionId: string; ok: boolean; regionUsed: string }
		>(
			method,
			async (req) => ({
				transactionId: `v2-${req.userId}`,
				ok: req.amount > 0,
				regionUsed: req.region,
			}),
			{
				schema: {
					protoFile: v2,
					input: "ChargeRequest",
					output: "ChargeResponse",
				},
			},
		);
		await connect(v2Provider);

		const consumer = newClient(secondKey(), ADVERTISE);
		consumer.service(providerSvc, { rpc: [method] });
		await connect(consumer);

		// Both provider instances must be visible.
		await waitFor(
			() => {
				let count = 0;
				for (const d of [...consumer.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === method) count++;
				}
				return count >= 2;
			},
			5_000,
			"both v1 and v2 instances visible",
		);

		await consumer.useSchema(providerSvc, method, {
			protoFile: v1,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		// Assert exactly the two expected contract hashes are present (guard
		// against zombie state) — SDK is single source of truth (ADR-0005).
		const { computeContractHash } = await import(
			"../../src/serde/contract-hash"
		);
		const { buildSchemaPair } = await import("../../src/serde/serializer");
		const v1Hash = computeContractHash(
			await buildSchemaPair({
				protoFile: v1,
				input: "ChargeRequest",
				output: "ChargeResponse",
			}),
		);
		const v2Hash = computeContractHash(
			await buildSchemaPair({
				protoFile: v2,
				input: "ChargeRequest",
				output: "ChargeResponse",
			}),
		);
		await waitFor(
			() => {
				const hashes = new Set<string>();
				for (const d of [...consumer.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === method) hashes.add(d.contractHash);
				}
				return hashes.has(v1Hash) && hashes.has(v2Hash);
			},
			5_000,
			"both v1 and v2 contract hashes present",
		);

		// 10 calls — all land on v1 because v2's hash mismatches the caller's.
		for (let i = 0; i < 10; i++) {
			const r = await consumer.rpc.call<
				{ userId: string; amount: number },
				{ transactionId: string; ok: boolean }
			>(
				providerSvc,
				method,
				{ userId: `u-${i}`, amount: 1 },
				{ transport: "proxy" },
			);
			expect(r.transactionId).toBe(`v1-u-${i}`);
		}
	}, 30_000);

	test("contract-hash mismatch fails fast", async () => {
		const { v1, v2 } = writeVersionedProtos();
		const method = uniqueName("charge");

		// Provider serves v1 only; registers BEFORE connect().
		const provider = newClient(primaryKey(), ADVERTISE);
		provider.rpc.handle(
			method,
			async () => ({ transactionId: "x", ok: true }),
			{
				schema: {
					protoFile: v1,
					input: "ChargeRequest",
					output: "ChargeResponse",
				},
			},
		);
		await connect(provider);
		const providerSvc = provider.identity()!.serviceName;

		const consumer = newClient(secondKey(), ADVERTISE);
		consumer.service(providerSvc, { rpc: [method] });
		await connect(consumer);
		await waitForMethod(consumer, method);

		// Consumer loads v2 (mismatched).
		await consumer.useSchema(providerSvc, method, {
			protoFile: v2,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		await expect(
			consumer.rpc.call(
				providerSvc,
				method,
				{ userId: "u", amount: 1, region: "eu" },
				{ transport: "proxy", retry: { maxAttempts: 1 } },
			),
		).rejects.toThrow(/no instance.*matches caller contract/);
	}, 30_000);

	test("advertise defaults to 127.0.0.1 and handler schema auto-resolves from proto service block", async () => {
		const protoFile = writeServiceProto();
		// Auto-resolution matches the registered method name against the proto's
		// service block, so the name must be a real rpc entry ("Charge"). The
		// dedicated provider is identity-isolated, so the fixed name is safe.
		const method = "Charge";

		// NO advertise option → SDK falls back to 127.0.0.1:0.
		// Handler schema has NO input/output → resolved from the service block.
		const provider = newClient(primaryKey(), undefined);
		provider.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			method,
			async (req) => ({
				transactionId: `tx-${req.userId}`,
				ok: req.amount > 0,
			}),
			{ schema: { protoFile } },
		);
		await connect(provider);
		const providerSvc = provider.identity()!.serviceName;

		const consumer = newClient(secondKey(), ADVERTISE);
		consumer.service(providerSvc, { rpc: [method] });
		await connect(consumer);
		await waitForMethod(consumer, method);
		await consumer.useSchema(providerSvc, method, { protoFile });

		const result = await consumer.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(providerSvc, method, { userId: "u-1", amount: 50 });

		expect(result.transactionId).toBe("tx-u-1");
		expect(result.ok).toBe(true);
	}, 30_000);

	test("sb.client() typed proxy registers deps+schemas and drives unary and streaming calls", async () => {
		const protoFile = writeServiceProto();

		// Provider exposes both unary + streaming methods; registers BEFORE connect.
		const provider = newClient(primaryKey(), ADVERTISE);
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
				for (let i = 0; i < req.count; i++) yield { i };
			},
			{ schema: { protoFile } },
		);
		await connect(provider);
		const providerSvc = provider.identity()!.serviceName;

		// Consumer must declare the typed client BEFORE start() so its outgoing
		// deps ride the initial RegisterRequest.
		const consumer = newClient(secondKey(), ADVERTISE);
		const payment = await consumer.client(providerSvc, protoFile);
		await connect(consumer);

		await waitFor(
			() => {
				let charged = false;
				let streamed = false;
				for (const d of [...consumer.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "Charge") charged = true;
					if (d.name === "Stream") streamed = true;
				}
				return charged && streamed;
			},
			5_000,
			"Charge + Stream visible in consumer snapshot",
		);

		const charge = (await (
			payment.Charge as (req: unknown) => Promise<unknown>
		)({ userId: "alice", amount: 100 })) as { transactionId: string };
		expect(charge.transactionId).toBe("paid-alice");

		const stream = (
			payment.Stream as (req: unknown) => AsyncIterable<{ i: number }>
		)({ count: 3 });
		const chunks: number[] = [];
		for await (const c of stream) chunks.push(c.i);
		expect(chunks).toEqual([0, 1, 2]);
	}, 30_000);

	test("sb.client() methods filter binds only listed methods", async () => {
		const protoFile = writeServiceProto();

		const provider = newClient(primaryKey(), ADVERTISE);
		provider.rpc.handle(
			"Charge",
			async () => ({ transactionId: "x", ok: true }),
			{ schema: { protoFile } },
		);
		await connect(provider);
		const providerSvc = provider.identity()!.serviceName;

		// client() resolves the proxy shape before connect — no call needed.
		const consumer = newClient(secondKey(), ADVERTISE);
		const payment = await consumer.client(providerSvc, protoFile, {
			methods: ["Charge"],
		});

		expect(typeof payment.Charge).toBe("function");
		expect(payment.Stream).toBeUndefined();
	}, 30_000);

	test("sb.client() on a proto with no service block rejects", async () => {
		// client() resolves the proto before connecting, so no provider/start is
		// needed. A dedicated consumer avoids registering a bad dep on a pooled id.
		const consumer = newClient(secondKey(), ADVERTISE);
		const providerSvc = uniqueName("no-service");

		// PROTO_FILE (testdata/payment.proto) has no service definition.
		await expect(consumer.client(providerSvc, PROTO_FILE)).rejects.toThrow(
			/no service block/,
		);
	}, 30_000);
});
