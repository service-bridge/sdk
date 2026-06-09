// access-policy-wildcard.test.ts — ADR-0004 gate #2 (event.handle subscription
// declarations vs acceptance rule's pattern via PatternContains).
//
// Acceptance rule for the subscriber: pattern `orders.*` (one segment after
// `orders.`). Subscribing to:
//   - `orders.created`         → allowed (contained in `orders.*`)
//   - `payments.created`       → denied (different root)
//   - `orders.a.b`             → denied (`*` is exactly one segment)
//   - `orders.#`               → denied (subscriber pattern wider than rule)
//
// All four subscriptions register (soft gate); the violators show up in
// PolicyEvaluation.warnings.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { eventsEnv, FAST_OPTS, sleep, waitFor } from "./_helpers/events";
import { addRule, clearRules, serviceID } from "./_helpers/policy-db";

const protoFile = join(
	import.meta.dir,
	"..",
	"..",
	"src",
	"events",
	"testdata",
	"order-event.proto",
);
const PROTO = { protoFile, method: "orders_created" } as const;

describe("access-policy: event.handle pattern containment", () => {
	const env = eventsEnv();
	let subID: string;
	let sub: ServiceBridge | undefined;

	beforeEach(async () => {
		sub = undefined;
		subID = await serviceID("e2e-registry-consumer");
		await clearRules(subID);
		await addRule(subID, "A", "event.handle", null, "orders.*");
		await sleep(700);
	});

	afterEach(async () => {
		await sub?.stop();
		await clearRules(subID);
		await sleep(300);
	});

	test("only orders.created is allowed by acceptance pattern orders.*", async () => {
		const violations: Array<{ declaration: string; value: string }> = [];
		sub = new ServiceBridge(env.url, env.subscriberKey, FAST_OPTS);
		sub.on("policy_violation", (v) => {
			violations.push({ declaration: v.declaration, value: v.value });
		});

		sub.event.handle("orders.created", async () => {});
		sub.event.define("orders.created", PROTO);

		sub.event.handle("payments.created", async () => {});
		sub.event.define("payments.created", PROTO);

		sub.event.handle("orders.a.b", async () => {});
		sub.event.define("orders.a.b", PROTO);

		sub.event.handle("orders.#", async () => {});
		sub.event.define("orders.#", PROTO);

		await sub.start();
		await waitFor(() => sub!.identity() !== null, 5_000, "sub connected");
		await waitFor(
			() => sub!.policyEvaluation() !== null,
			3_000,
			"policy evaluation surfaced",
		);

		const warnedValues = violations
			.filter((v) => v.declaration === "event.handle")
			.map((v) => v.value);

		expect(warnedValues).toContain("payments.created");
		expect(warnedValues).toContain("orders.a.b");
		expect(warnedValues).toContain("orders.#");
		// Must NOT warn for orders.created — it's contained in orders.*.
		expect(warnedValues).not.toContain("orders.created");
	}, 20_000);
});
