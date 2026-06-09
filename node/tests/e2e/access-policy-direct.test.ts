/**
 * access-policy-direct.test.ts — ADR-0004 Layer 2 (direct transport).
 *
 * When the caller and callee talk directly (SDK→SDK mTLS, bypassing the
 * runtime Invoke proxy), the callee enforces its own rpc.handle acceptance
 * rules using the SPIFFE identity extracted from the peer TLS cert. If the
 * caller's service UUID is not in the callee's acceptance allow-list, the
 * direct call is rejected with PermissionDenied.
 *
 * The proxy-only acceptance behaviour lives in access-policy-acceptance.test.ts.
 */

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

describe("access-policy: direct transport (Layer 2 mTLS acceptance)", () => {
	const env = eventsEnv();
	let calleeID: string;
	let callerID: string;
	let callee: ServiceBridge | undefined;
	let caller: ServiceBridge | undefined;
	const allowed = `direct-allowed-${Date.now()}`;
	const forbidden = `direct-forbidden-${Date.now()}`;

	beforeEach(async () => {
		callee = caller = undefined;
		calleeID = await serviceID(CALLEE_SVC);
		callerID = await serviceID("e2e-registry-consumer");
		await clearRules(calleeID);
		await clearRules(callerID);
	});

	afterEach(async () => {
		await caller?.stop();
		await callee?.stop();
		await clearRules(calleeID);
		await clearRules(callerID);
		await sleep(300);
	});

	test("direct: allowed method succeeds, forbidden method denied by callee mTLS check", async () => {
		// Step 1: connect callee briefly to obtain its runtime-assigned UUID,
		// then disconnect — we need the UUID before inserting rules so the
		// rules target the correct ID, but we want the callee to re-register
		// with rules already in DB.
		const calleeProbe = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		await calleeProbe.start();
		await waitFor(
			() => calleeProbe.identity() !== null,
			5_000,
			"callee probe connected",
		);
		const runtimeCalleeID = calleeProbe.identity()!.serviceId;
		await calleeProbe.stop();
		await sleep(200);

		const callerProbe = new ServiceBridge(
			env.url,
			env.subscriberKey,
			FAST_OPTS,
		);
		await callerProbe.start();
		await waitFor(
			() => callerProbe.identity() !== null,
			5_000,
			"caller probe connected",
		);
		const runtimeCallerID = callerProbe.identity()!.serviceId;
		await callerProbe.stop();
		await sleep(200);

		// Step 2: insert rules with correct UUIDs.
		await addRule(runtimeCallerID, "E", "rpc.call", runtimeCalleeID, allowed);
		await addRule(runtimeCallerID, "E", "rpc.call", runtimeCalleeID, forbidden);
		await addRule(runtimeCalleeID, "A", "rpc.handle", runtimeCallerID, allowed);
		await sleep(1500);

		// Step 3: reconnect with handlers / declarations — rules are in DB so
		// the first PolicyEvaluation snapshot has them.
		callee = new ServiceBridge(env.url, env.publisherKey, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
		callee.rpc.handle(
			allowed,
			async () => ({ transactionId: "ok", ok: true }),
			{
				schema: SCHEMA,
			},
		);
		callee.rpc.handle(
			forbidden,
			async () => ({ transactionId: "no", ok: false }),
			{ schema: SCHEMA },
		);
		await callee.start();
		await waitFor(() => callee!.identity() !== null, 5_000, "callee connected");

		caller = new ServiceBridge(env.url, env.subscriberKey, {
			...FAST_OPTS,
			advertise: { host: "127.0.0.1", port: 0 },
		});
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
			"allowed method discovered",
		);

		await caller.useSchema(CALLEE_SVC, allowed, SCHEMA);
		await caller.useSchema(CALLEE_SVC, forbidden, SCHEMA);

		// Allowed direct call → success.
		const ok = await caller.rpc.call<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			CALLEE_SVC,
			allowed,
			{ userId: "u", amount: 1 },
			{ timeout: "5s", transport: "direct" },
		);
		expect(ok.transactionId).toBe("ok");

		// Forbidden direct call → callee SPIFFE acceptance fails closed.
		let denial: unknown;
		try {
			await caller.rpc.call(
				CALLEE_SVC,
				forbidden,
				{ userId: "u", amount: 1 },
				{ timeout: "5s", transport: "direct" },
			);
		} catch (e) {
			denial = e;
		}
		expect(denial).toBeDefined();
		expect(String(denial)).toMatch(/PermissionDenied|acceptance denied/i);
		// The denial message must include the caller's mTLS-derived UUID — proof
		// the identity came from SPIFFE, not from CallRequest.callerService.
		expect(String(denial)).toContain(runtimeCallerID);
	}, 30_000);
});
