import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import { HandlerError } from "../errors";
import { createTestHarness } from "./harness";

// Usage example referenced from userDocs/testing.md and
// skill/reference/testing.md — keep the three in sync.

const SHOP = join(import.meta.dir, "testdata", "shop.proto");

interface ChargeRequest {
	userId: string;
	amount: number;
}

describe("charge RPC handler (example)", () => {
	async function setup() {
		const h = createTestHarness();
		const { sb } = h;
		// The production wiring, unchanged: outgoing dependency, published event,
		// handler.
		await sb.client("fraud-svc", SHOP, { methods: ["Check"] });
		sb.event.define("payment.charged", {
			protoFile: SHOP,
			input: "PaymentCharged",
			output: "PaymentCharged",
		});
		sb.rpc.handle(
			"Charge",
			async (req: ChargeRequest) => {
				const verdict = await sb.rpc.call<
					{ userId: string },
					{ blocked: boolean }
				>("fraud-svc", "Check", { userId: req.userId });
				if (verdict.blocked)
					throw new HandlerError("BLOCKED", `user ${req.userId} is blocked`);
				const transactionId = `tx-${req.userId}`;
				await sb.event.publish("payment.charged", {
					transactionId,
					amount: req.amount,
				});
				return { transactionId, ok: true };
			},
			{ schema: { protoFile: SHOP, method: "Charge" } },
		);
		await h.start();
		return h;
	}

	it("checks fraud, publishes payment.charged and returns the transaction", async () => {
		const h = await setup();
		h.respond("fraud-svc", "Check", () => ({ blocked: false }));

		const res = await h.invoke("Charge", { userId: "u-1", amount: 42 });

		expect(res).toEqual({ transactionId: "tx-u-1", ok: true });
		expect(h.calls().map((c) => [c.service, c.method, c.payload])).toEqual([
			["fraud-svc", "Check", { userId: "u-1" }],
		]);
		expect(h.published().map((p) => [p.name, p.payload])).toEqual([
			["payment.charged", { transactionId: "tx-u-1", amount: 42 }],
		]);
		await h.stop();
	});

	it("answers with the business code when fraud blocks the user", async () => {
		const h = await setup();
		h.respond("fraud-svc", "Check", () => ({ blocked: true }));

		const err = await h
			.invoke("Charge", { userId: "u-2", amount: 1 })
			.catch((e) => e);

		expect(err).toBeInstanceOf(HandlerError);
		expect((err as HandlerError).handlerCode).toBe("BLOCKED");
		expect(h.published()).toHaveLength(0);
		await h.stop();
	});
});
