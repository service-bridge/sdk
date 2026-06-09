/**
 * RPC.CALL op emission e2e (ADR-0001) — поднимает callee + caller SDK,
 * делает direct RPC, проверяет, что в `operations` появилась РОВНО ОДНА строка
 * RPC.CALL (channel=2, kind=1) с не-null finished_at и status=OK (2), и НЕТ
 * строк RPC.FORWARD/HANDLE (kind 2/3). Прокси-факт — via_proxy=false (direct).
 *
 *   SERVICEBRIDGE_URL          — runtime gRPC endpoint
 *   SERVICEBRIDGE_SERVICE_KEY  — callee key
 *   SERVICEBRIDGE_SERVICE2_KEY — caller key
 *   TEST_DATABASE_URL          — runtime Postgres (default localhost:5433)
 */

import { afterEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { withDb } from "./_helpers/policy-db";

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
	p: () => boolean | Promise<boolean>,
	ms: number,
	what: string,
): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < ms) {
		if (await p()) return;
		await sleep(50);
	}
	throw new Error(`timeout waiting for: ${what}`);
}

// countRpcOps returns, for the given method window, the OK+finished RPC.CALL
// rows (kind=1), the count of legacy FORWARD/HANDLE rows (kind 2/3) — which
// MUST be zero under the one-row model — and the single CALL row's canonical
// subject / via_proxy / attempt so the proxy-fact and retry counter can be
// asserted directly (ADR-0001).
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

describe.skipIf(!enabled)("RPC.CALL op emit e2e", () => {
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		caller = undefined;
		callee = undefined;
	});

	test("direct charge call emits exactly one RPC.CALL row, no FORWARD/HANDLE", async () => {
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
				transactionId: `emit-${req.userId}`,
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

		const baseline = Date.now();
		const result = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			CALLEE_SVC,
			"charge",
			{ userId: "u-emit", amount: 100 },
			{ timeout: "5s", transport: "direct", idempotencyKey: "k-emit-1" },
		);
		expect(result.ok).toBe(true);

		await waitFor(
			async () => {
				const { callOk } = await countRpcOps("charge", baseline);
				return callOk >= 1;
			},
			10_000,
			"RPC.CALL row with status=OK in operations",
		);

		// Exactly one CALL row, zero FORWARD/HANDLE rows, canonical subject,
		// via_proxy=false (direct transport), attempt=0 (no retries) — the full
		// one-row model contract (ADR-0001).
		const { callOk, callTotal, forwardHandle, subject, viaProxy, attempt } =
			await countRpcOps("charge", baseline);
		expect(callOk).toBe(1);
		expect(callTotal).toBe(1);
		expect(forwardHandle).toBe(0);
		expect(subject).toBe(`rpc.call:${CALLEE_SVC}/charge`);
		expect(viaProxy).toBe(false);
		expect(attempt).toBe(0);
	}, 30_000);
});
