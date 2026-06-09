// access-policy-capabilities.test.ts — ADR-0004 gate #1.
//
// When a service's `cap_rpc_handle` is FALSE, the runtime hard-rejects the
// registration of an rpc handler with PermissionDenied — the registration
// stream errors before any service_methods row is written.
//
// Test signal: after start() + brief wait, the method does NOT appear in the
// service_methods table. (The SDK swallows the watch-stream error today; the
// canonical verifier is the persistence layer.)

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep } from "./_helpers/events";
import { serviceID, setCapability, withDb } from "./_helpers/policy-db";

const protoFile = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"serde",
	"testdata",
	"payment.proto",
);

async function methodExists(svcID: string, name: string): Promise<boolean> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT 1 FROM service_methods
			WHERE service_id = ${svcID} AND method_name = ${name}
		`) as unknown[];
		return rows.length > 0;
	});
}

describe("access-policy: capability gate (rpc.handle disabled)", () => {
	const env = eventsEnv();
	let calleeID: string;
	let callee: ServiceBridge | undefined;
	const handlerName = `cap-test-${Date.now()}`;

	beforeEach(async () => {
		callee = undefined;
		calleeID = await serviceID("e2e-registry-svc");
		// Make sure the prior test's row isn't sitting around.
		await withDb(async (sql) => {
			await sql`DELETE FROM service_methods WHERE service_id = ${calleeID} AND method_name = ${handlerName}`;
		});
		await setCapability(calleeID, "cap_rpc_handle", false);
		await sleep(700);
	});

	afterEach(async () => {
		await callee?.stop();
		await setCapability(calleeID, "cap_rpc_handle", true);
		await sleep(300);
	});

	test("rpc handler with cap_rpc_handle=false is not registered in DB", async () => {
		callee = new ServiceBridge(env.url, env.publisherKey, FAST_OPTS);
		callee.rpc.handle(
			handlerName,
			async () => ({ transactionId: "x", ok: true }),
			{
				schema: {
					protoFile,
					input: "ChargeRequest",
					output: "ChargeResponse",
				},
			},
		);

		// start() may resolve (connection auth succeeds) even though the
		// registry stream is rejected. Wait long enough for the registration
		// attempt + rejection to complete server-side.
		await callee.start().catch(() => {});
		await sleep(1_500);

		// Canonical assertion: gate #1 fires BEFORE UpsertMethods, so the row
		// must NOT be there.
		expect(await methodExists(calleeID, handlerName)).toBe(false);
	}, 15_000);
});
