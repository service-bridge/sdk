/**
 * RPC direct e2e — caller bypasses the runtime Invoke proxy and dials the
 * callee CallServer directly via mTLS + SPIFFE-validated TLS. Requires the
 * same keys as rpc-proxy: SERVICEBRIDGE_SERVICE_KEY (callee, advertise) and
 * SERVICEBRIDGE_SERVICE2_KEY (caller).
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

describe.skipIf(!enabled)("RPC direct e2e", () => {
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		caller = undefined;
		callee = undefined;
	});

	test('transport: "direct" — call bypasses runtime Invoke, hits callee directly', async () => {
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
				transactionId: `direct-${req.userId}`,
				ok: req.amount > 0,
			}),
			{
				schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			},
		);
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = new ServiceBridge(URL!, CALLER_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
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

		await caller.useSchema(CALLEE_SVC, "charge", {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			CALLEE_SVC,
			"charge",
			{ userId: "u-42", amount: 100 },
			{ timeout: "5s", transport: "direct" },
		);

		expect(result.transactionId).toBe("direct-u-42");
		expect(result.ok).toBe(true);
	});

	test('transport: "auto" picks direct when endpoint known', async () => {
		callee = new ServiceBridge(URL!, CALLEE_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		callee.rpc.handle(
			"charge",
			async () => ({ transactionId: "auto-routed", ok: true }),
			{
				schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			},
		);
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = new ServiceBridge(URL!, CALLER_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
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
		await caller.useSchema(CALLEE_SVC, "charge", {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		// transport: "auto" is default — endpoint known → direct path.
		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(CALLEE_SVC, "charge", { userId: "u", amount: 1 }, { transport: "auto" });
		expect(result.transactionId).toBe("auto-routed");
	});

	test('transport: "direct" with no endpoint — throws', async () => {
		// Explicitly disable advertise so callee is caller-only — the new
		// auto-detect default would otherwise bind 127.0.0.1 and provide an
		// endpoint, defeating the test intent.
		callee = new ServiceBridge(URL!, CALLEE_KEY!, {
			...FAST_OPTS,
			advertise: false,
		});
		callee.rpc.handle("charge", async () => ({}), {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
		});
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = new ServiceBridge(URL!, CALLER_KEY!, {
			...FAST_OPTS,
			advertise: false,
		});
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
		await caller.useSchema(CALLEE_SVC, "charge", {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		await expect(
			caller.rpc.call(
				CALLEE_SVC,
				"charge",
				{ userId: "u", amount: 1 },
				{ transport: "direct" },
			),
		).rejects.toThrow(/no endpoint|no instance.*matches caller contract/);
	});
});
