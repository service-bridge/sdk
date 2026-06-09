/**
 * Contract-version routing e2e — two callee instances of the same service
 * register different schemas (different contract_hash); caller is routed only
 * to the instance whose hash matches the caller's local schema.
 *
 * Validates ADR 0005 (variant 3: SDK is single source of truth for hash,
 * runtime is opaque store, LB filters by hash).
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

// Two .proto files with the SAME message names but DIFFERENT field sets →
// different contract hashes. They cannot decode each other's payloads.
function writeTwoVersions(): { v1: string; v2: string; dir: string } {
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
  string region  = 3;   // NEW field — different contract
}
message ChargeResponse {
  string transaction_id = 1;
  bool   ok             = 2;
  string region_used    = 3;
}
`,
	);
	return { v1, v2, dir };
}

describe.skipIf(!enabled)("RPC contract-version routing e2e", () => {
	let v1Provider: ServiceBridge | undefined;
	let v2Provider: ServiceBridge | undefined;
	let consumer: ServiceBridge | undefined;

	afterEach(async () => {
		await consumer?.stop();
		await v1Provider?.stop();
		await v2Provider?.stop();
		consumer = undefined;
		v1Provider = undefined;
		v2Provider = undefined;
	});

	test("caller is routed only to instance with matching contract_hash", async () => {
		const { v1, v2 } = writeTwoVersions();

		// Both providers serve the SAME method `charge` but with DIFFERENT
		// schemas (v2 has an extra `region` field).
		v1Provider = new ServiceBridge(URL!, PROVIDER_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		v1Provider.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			"charge",
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
		await v1Provider.start();
		await waitFor(() => v1Provider!.identity() !== null, 5_000, "v1 connected");

		v2Provider = new ServiceBridge(URL!, PROVIDER_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		v2Provider.rpc.handle<
			{ userId: string; amount: number; region: string },
			{ transactionId: string; ok: boolean; regionUsed: string }
		>(
			"charge",
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
		await v2Provider.start();
		await waitFor(() => v2Provider!.identity() !== null, 5_000, "v2 connected");

		// Consumer loads V1 schema → must be routed only to v1Provider.
		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["charge"] });
		await consumer.start();
		await waitFor(
			() => consumer!.identity() !== null,
			5_000,
			"consumer connected",
		);

		// Wait until BOTH provider instances are visible in the snapshot.
		await waitFor(
			() => {
				let count = 0;
				for (const d of [...consumer!.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "charge") count++;
				}
				return count >= 2;
			},
			5_000,
			"both v1 and v2 visible",
		);

		await consumer.useSchema(PROVIDER_SVC, "charge", {
			protoFile: v1,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		// Determine which contract hashes are present in the snapshot — guard
		// against zombie state from prior tests by asserting we see exactly the
		// two we expect (v1 and v2 hashes computed locally).
		const { computeContractHash } = await import(
			"../../src/serde/contract-hash"
		);
		const { buildSchemaPair } = await import("../../src/serde/serializer");
		const v1Pair = await buildSchemaPair({
			protoFile: v1,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const v2Pair = await buildSchemaPair({
			protoFile: v2,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const v1Hash = computeContractHash(v1Pair);
		const v2Hash = computeContractHash(v2Pair);
		await waitFor(
			() => {
				const hashes = new Set<string>();
				for (const d of [...consumer!.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "charge") hashes.add(d.contractHash);
				}
				return hashes.has(v1Hash) && hashes.has(v2Hash);
			},
			5_000,
			"both v1 and v2 contract hashes present",
		);

		// 10 calls — all must land on v1 (regardless of round-robin) because v2's
		// contract_hash does not match the caller's.
		for (let i = 0; i < 10; i++) {
			const r = await consumer.rpc.call<
				{ userId: string; amount: number },
				{ transactionId: string; ok: boolean }
			>(
				PROVIDER_SVC,
				"charge",
				{ userId: `u-${i}`, amount: 1 },
				{ transport: "proxy" },
			);
			expect(r.transactionId).toBe(`v1-u-${i}`);
		}
	});

	test("caller with mismatched schema fails fast", async () => {
		const { v1, v2 } = writeTwoVersions();

		// Provider is v1 only.
		v1Provider = new ServiceBridge(URL!, PROVIDER_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		v1Provider.rpc.handle(
			"charge",
			async () => ({ transactionId: "x", ok: true }),
			{
				schema: {
					protoFile: v1,
					input: "ChargeRequest",
					output: "ChargeResponse",
				},
			},
		);
		await v1Provider.start();
		await waitFor(() => v1Provider!.identity() !== null, 5_000, "v1 connected");

		// Consumer loads V2 (mismatched).
		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["charge"] });
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
					if (d.name === "charge") return true;
				}
				return false;
			},
			5_000,
			"charge visible",
		);

		await consumer.useSchema(PROVIDER_SVC, "charge", {
			protoFile: v2,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		await expect(
			consumer.rpc.call(
				PROVIDER_SVC,
				"charge",
				{ userId: "u", amount: 1, region: "eu" },
				{ transport: "proxy", retry: { maxAttempts: 1 } },
			),
		).rejects.toThrow(/no instance.*matches caller contract/);
	});

	test("caller with matching schema (v1↔v1) succeeds", async () => {
		const { v1 } = writeTwoVersions();

		v1Provider = new ServiceBridge(URL!, PROVIDER_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		v1Provider.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			"charge",
			async (req) => ({ transactionId: `tx-${req.userId}`, ok: true }),
			{
				schema: {
					protoFile: v1,
					input: "ChargeRequest",
					output: "ChargeResponse",
				},
			},
		);
		await v1Provider.start();
		await waitFor(() => v1Provider!.identity() !== null, 5_000, "v1 connected");

		consumer = new ServiceBridge(URL!, CONSUMER_KEY!, FAST_OPTS);
		consumer.service(PROVIDER_SVC, { rpc: ["charge"] });
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
					if (d.name === "charge") return true;
				}
				return false;
			},
			5_000,
			"charge visible",
		);
		await consumer.useSchema(PROVIDER_SVC, "charge", {
			protoFile: v1,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		const r = await consumer.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(PROVIDER_SVC, "charge", { userId: "u-1", amount: 1 });
		expect(r.transactionId).toBe("tx-u-1");
	});
});
