/**
 * RPC proxy e2e — caller SDK invokes a method on a callee SDK through the
 * runtime's Invoke proxy. Requires a running runtime and two bootstrap keys:
 *   SERVICEBRIDGE_SERVICE_KEY  — callee service "e2e-registry-svc"
 *   SERVICEBRIDGE_SERVICE2_KEY — caller service "e2e-registry-consumer"
 */

import { afterEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";

const URL = process.env.SERVICEBRIDGE_URL;
const CALLEE_KEY = process.env.SERVICEBRIDGE_SERVICE_KEY;
const CALLER_KEY = process.env.SERVICEBRIDGE_SERVICE2_KEY;
const enabled = Boolean(URL && CALLEE_KEY && CALLER_KEY);

const CALLEE_SVC = "e2e-registry-svc";

const protoFile = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"serde",
	"testdata",
	"payment.proto",
);

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

describe.skipIf(!enabled)("RPC proxy e2e", () => {
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		caller = undefined;
		callee = undefined;
	});

	test("unary proxy call: caller → runtime → callee, payload round-trip", async () => {
		callee = new ServiceBridge(URL!, CALLEE_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		callee.rpc.handle<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			"charge",
			async (req) => ({
				transactionId: `tx-${req.userId}`,
				ok: req.amount > 0,
			}),
			{
				schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			},
		);
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = new ServiceBridge(URL!, CALLER_KEY!, FAST_OPTS);
		caller.service(CALLEE_SVC, { rpc: ["charge"] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		// Wait until caller has discovered the callee instance with non-empty endpoint.
		await waitFor(
			() => {
				for (const d of [...caller!.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "charge") return true;
				}
				return false;
			},
			5_000,
			"charge discovered",
		);

		await caller.useSchema(CALLEE_SVC, "charge", {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(CALLEE_SVC, "charge", { userId: "u-42", amount: 100 }, { timeout: "5s" });

		expect(result.transactionId).toBe("tx-u-42");
		expect(result.ok).toBe(true);
	});

	test("call without useSchema: throws no-SchemaPair error", async () => {
		callee = new ServiceBridge(URL!, CALLEE_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		callee.rpc.handle("charge", async () => ({}), {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
		});
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = new ServiceBridge(URL!, CALLER_KEY!, FAST_OPTS);
		caller.service(CALLEE_SVC, { rpc: ["charge"] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		await waitFor(
			() => {
				for (const d of [...caller!.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "charge") return true;
				}
				return false;
			},
			5_000,
			"charge discovered",
		);

		await expect(
			caller.rpc.call(CALLEE_SVC, "charge", { userId: "u", amount: 1 }),
		).rejects.toThrow(/no SchemaPair/);
	});

	test("call before discovery: throws no-descriptor error", async () => {
		caller = new ServiceBridge(URL!, CALLER_KEY!, FAST_OPTS);
		// Subscribe to a service that does not exist — descriptor never appears.
		caller.service("nonexistent-svc", { rpc: ["missing"] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

		await caller.useSchema("nonexistent-svc", "missing", {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		await expect(
			caller.rpc.call("nonexistent-svc", "missing", { userId: "u", amount: 1 }),
		).rejects.toThrow(/no descriptor/);
	});
});
