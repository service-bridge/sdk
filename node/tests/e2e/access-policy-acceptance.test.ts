// access-policy-acceptance.test.ts — ADR-0004 gate #3, callee side.
//
// Callee has an acceptance rule allowing only `<allowed>` from the caller.
// A call to `<allowed>` is accepted; a call to `<forbidden>` is rejected by
// callee acceptance with PermissionDenied (bilateral check).

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep, waitFor } from "./_helpers/events";
import { addRule, clearRules, serviceID } from "./_helpers/policy-db";

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
const SCHEMA = { protoFile, input: "ChargeRequest", output: "ChargeResponse" };

describe("access-policy: acceptance gate (rpc.handle narrow allow-list)", () => {
	const env = eventsEnv();
	let calleeID: string;
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	const allowed = `acc-allowed-${Date.now()}`;
	const forbidden = `acc-forbidden-${Date.now()}`;

	beforeEach(async () => {
		callee = caller = undefined;
		calleeID = await serviceID(CALLEE_SVC);
		await clearRules(calleeID);
	});

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		await clearRules(calleeID);
		await sleep(300);
	});

	test("acceptance allow-list: allowed succeeds, forbidden denied by callee", async () => {
		caller = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		caller.service(CALLEE_SVC, { rpc: [allowed, forbidden] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");
		const callerID = caller.identity()!.serviceId;

		// Acceptance: callee accepts only `allowed` from this caller.
		await addRule(calleeID, "A", "rpc.handle", callerID, allowed);
		await sleep(800);

		callee = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		callee.rpc.handle(
			allowed,
			async () => ({ transactionId: "ok", ok: true }),
			{ schema: SCHEMA },
		);
		callee.rpc.handle(
			forbidden,
			async () => ({ transactionId: "no", ok: false }),
			{ schema: SCHEMA },
		);
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		await waitFor(
			() => {
				for (const e of caller!.serviceMap().values()) {
					for (const m of e.methods) {
						if (m.name === allowed) return true;
					}
				}
				return false;
			},
			5_000,
			"allowed method discovered",
		);

		await caller.useSchema(CALLEE_SVC, allowed, SCHEMA);
		await caller.useSchema(CALLEE_SVC, forbidden, SCHEMA);

		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			CALLEE_SVC,
			allowed,
			{ userId: "u", amount: 1 },
			{ timeout: "5s", transport: "proxy" },
		);
		expect(ok.transactionId).toBe("ok");

		let denied = false;
		try {
			await caller.rpc.call(
				CALLEE_SVC,
				forbidden,
				{ userId: "u", amount: 1 },
				{ timeout: "5s", transport: "proxy" },
			);
		} catch (e) {
			denied = true;
			expect(String(e)).toMatch(/PermissionDenied|denied|acceptance/i);
		}
		expect(denied).toBe(true);
	}, 30_000);
});
