// access-policy-warnings.test.ts — ADR-0004 SDK-side smoke for the
// policy_violation event surface.
//
// Verifies that when a service declares something its policy disallows, the
// runtime returns PolicyEvaluation in the first registry snapshot, and the
// SDK surfaces it via `policy_violation` event + `sb.policyEvaluation()`
// getter. The runtime gates themselves are tested in Go (internal/access).

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep, waitFor } from "./_helpers/events";

const PG_URL =
	process.env.TEST_DATABASE_URL ??
	"postgresql://servicebridge:servicebridge@localhost:5433/service-bridge";

async function withDb<T>(
	fn: (sql: typeof import("bun").sql) => Promise<T>,
): Promise<T> {
	const prev = process.env.DATABASE_URL;
	process.env.DATABASE_URL = PG_URL;
	try {
		const { sql } = await import("bun");
		return await fn(sql);
	} finally {
		if (prev === undefined) {
			delete process.env.DATABASE_URL;
		} else {
			process.env.DATABASE_URL = prev;
		}
	}
}

describe("access-policy: SDK warnings on declared-but-denied outgoing dep", () => {
	const env = eventsEnv();
	let subscriber: ServiceBridge | undefined;
	let subscriberID: string;

	beforeEach(async () => {
		subscriber = undefined;
		subscriberID = await withDb(async (sql) => {
			const rows = (await sql`
				SELECT id FROM services
				WHERE name = 'e2e-registry-consumer'
				ORDER BY created_at DESC LIMIT 1
			`) as Array<{ id: string }>;
			if (!rows[0]?.id) {
				throw new Error("service e2e-registry-consumer not in DB");
			}
			return String(rows[0].id);
		});
		// Seed an egress rule that does NOT cover the dep we are about to
		// declare. Any non-empty egress rule disables default-allow for that
		// (service, action) — so a non-matching dep becomes a warning.
		await withDb(async (sql) => {
			await sql`
				INSERT INTO service_policy_rules
					(service_id, direction, action, peer_service, target_name)
				VALUES
					(${subscriberID}, 'E', 'rpc.call', NULL, 'allowed.method')
				ON CONFLICT DO NOTHING
			`;
		});
		// Let runtime pick up the NOTIFY before connect.
		await sleep(500);
	});

	afterEach(async () => {
		await subscriber?.stop();
		await withDb(async (sql) => {
			await sql`
				DELETE FROM service_policy_rules
				WHERE service_id = ${subscriberID}
				  AND direction = 'E'
				  AND action = 'rpc.call'
				  AND target_name = 'allowed.method'
			`;
		});
		await sleep(500);
	});

	test("declaring forbidden outgoing dep fires policy_violation event", async () => {
		const violations: Array<{
			declaration: string;
			value: string;
			denySide: string;
			reason: string;
		}> = [];

		subscriber = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		subscriber.on("policy_violation", (v) => {
			violations.push(v);
		});
		// Declare an outgoing dep NOT covered by our egress rule. Runtime
		// accepts the declaration but reports a violation.
		subscriber.service("payments", { rpc: ["restricted-method"] });

		await subscriber.start();
		await waitFor(
			() => subscriber!.identity() !== null,
			5_000,
			"subscriber connected",
		);
		// Snapshot frame arrives shortly after identity is set.
		await waitFor(
			() => subscriber!.policyEvaluation() !== null,
			3_000,
			"policy evaluation surfaced",
		);

		expect(violations.length).toBeGreaterThan(0);
		const target = violations.find(
			(v) =>
				v.declaration === "rpc.call" && v.value.includes("restricted-method"),
		);
		expect(target).toBeDefined();
		expect(target!.denySide).toBe("self_egress");

		const eval_ = subscriber.policyEvaluation();
		expect(eval_).not.toBeNull();
		expect(eval_!.warnings.length).toBeGreaterThan(0);
	}, 20_000);
});
