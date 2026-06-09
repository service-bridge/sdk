// access-policy-actions.test.ts — ADR-0004 gate #3 (caller egress on rpc.call).
//
// Caller has an egress rule allowing only `<allowed>` on the callee. A call
// to `<allowed>` succeeds via runtime proxy; a call to `<forbidden>` is
// rejected by gate #3 with PermissionDenied.

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

describe("access-policy: egress gate (rpc.call narrow allow-list)", () => {
	const env = eventsEnv();
	let callerID: string;
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	const allowed = `act-allowed-${Date.now()}`;
	const forbidden = `act-forbidden-${Date.now()}`;

	beforeEach(async () => {
		callee = caller = undefined;
		callerID = await serviceID("e2e-registry-consumer");
		await clearRules(callerID);
	});

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		await clearRules(callerID);
		await sleep(300);
	});

	test("egress allow-list: allowed succeeds, forbidden denied", async () => {
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
		const calleeID = callee.identity()!.serviceId;

		// Seed the rule against the actual UUID used by the runtime for this
		// session — necessary because multiple rows with the same service
		// name may exist (bootstrap appends).
		await addRule(callerID, "E", "rpc.call", calleeID, allowed);
		await sleep(800); // NOTIFY → snapshot

		caller = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		caller.service(CALLEE_SVC, { rpc: [allowed, forbidden] });
		await caller.start();
		await waitFor(() => caller!.identity() !== null, 5_000, "caller connected");

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
			"methods discovered",
		);

		await caller.useSchema(CALLEE_SVC, allowed, SCHEMA);
		await caller.useSchema(CALLEE_SVC, forbidden, SCHEMA);

		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(CALLEE_SVC, allowed, { userId: "u", amount: 1 }, { timeout: "5s" });
		expect(ok.transactionId).toBe("ok");

		// Default (direct) transport — scope-filter (Layer 1) hides the
		// forbidden method from the caller's snapshot, so the call cannot
		// resolve. Either way the call must be rejected — no scope visibility
		// or explicit permission denial from runtime/callee.
		let denied = false;
		try {
			await caller.rpc.call(
				CALLEE_SVC,
				forbidden,
				{ userId: "u", amount: 1 },
				{ timeout: "5s" },
			);
		} catch (e) {
			denied = true;
			expect(String(e)).toMatch(
				/PermissionDenied|denied|egress|no descriptor|not subscribed|Unavailable/i,
			);
		}
		expect(denied).toBe(true);
	}, 30_000);
});
