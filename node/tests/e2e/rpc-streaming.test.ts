/**
 * RPC streaming e2e — server-side streaming through both proxy and direct
 * transports. Requires the same keys as rpc-proxy.
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

describe.skipIf(!enabled)("RPC streaming e2e", () => {
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		caller = undefined;
		callee = undefined;
	});

	test("server-side streaming: 5 chunks via proxy", async () => {
		callee = new ServiceBridge(URL!, CALLEE_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		callee.rpc.handleStream<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			"charge",
			async function* (req) {
				for (let i = 0; i < 5; i++) {
					yield { transactionId: `${req.userId}-${i}`, ok: true };
				}
			},
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

		const chunks: { transactionId: string; ok: boolean }[] = [];
		for await (const chunk of caller.stream<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			CALLEE_SVC,
			"charge",
			{ userId: "u", amount: 1 },
			{ transport: "proxy", timeout: "5s" },
		)) {
			chunks.push(chunk);
		}

		expect(chunks).toHaveLength(5);
		expect(chunks[0]!.transactionId).toBe("u-0");
		expect(chunks[4]!.transactionId).toBe("u-4");
	});

	test("server-side streaming: direct mode bypasses runtime", async () => {
		callee = new ServiceBridge(URL!, CALLEE_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		callee.rpc.handleStream(
			"charge",
			async function* () {
				yield { transactionId: "tx-a", ok: true };
				yield { transactionId: "tx-b", ok: true };
			},
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

		const chunks: { transactionId: string }[] = [];
		for await (const chunk of caller.stream<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			CALLEE_SVC,
			"charge",
			{ userId: "u", amount: 1 },
			{ transport: "direct" },
		)) {
			chunks.push(chunk);
		}

		expect(chunks.map((c) => c.transactionId)).toEqual(["tx-a", "tx-b"]);
	});

	test("sb.call on streaming method throws", async () => {
		callee = new ServiceBridge(URL!, CALLEE_KEY!, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		callee.rpc.handleStream(
			"charge",
			async function* () {
				yield { transactionId: "x", ok: true };
			},
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
		await waitFor(
			() => {
				for (const d of [...caller!.serviceMap().values()].flatMap(
					(e) => e.methods,
				)) {
					if (d.name === "charge" && d.streaming) return true;
				}
				return false;
			},
			5_000,
			"streaming method discovered",
		);
		await caller.useSchema(CALLEE_SVC, "charge", {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

		await expect(
			caller.rpc.call(CALLEE_SVC, "charge", { userId: "u", amount: 1 }),
		).rejects.toThrow(/streaming method/);
	});
});
