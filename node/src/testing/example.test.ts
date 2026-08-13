import { describe, expect, it } from "bun:test";
import type { TestEventDomain } from "./event-harness";
import { createTestHarness } from "./harness";
import type { TestRpcDomain } from "./rpc-harness";

// Usage example referenced from userDocs/testing.md and
// skill/reference/testing.md — keep the three in sync.
//
// Handler factories take the narrow slice of the SDK they actually use
// (`Pick<RpcDomain, "call">`, `Pick<EventDomain, "publish">`) instead of the
// full `ServiceBridge` — the same "accept interfaces" rule the project
// already applies everywhere else. That narrow interface is what lets
// `harness.rpc` / `harness.event` (TestRpcDomain / TestEventDomain) satisfy
// the parameter type without a cast: TestRpcDomain.call and
// RpcDomain.call/EventDomain.publish share the exact same method signatures.

interface ChargeRequest {
	userId: string;
	amount: number;
}
interface ChargeResponse {
	transactionId: string;
	ok: boolean;
}

function makeChargeHandler(deps: {
	rpc: Pick<TestRpcDomain, "call">;
	event: Pick<TestEventDomain, "publish">;
}) {
	return async (req: ChargeRequest): Promise<ChargeResponse> => {
		const fraud = await deps.rpc.call<{ userId: string }, { blocked: boolean }>(
			"fraud-svc",
			"Check",
			{ userId: req.userId },
		);
		if (fraud.blocked) {
			throw new Error(`user ${req.userId} blocked by fraud check`);
		}

		const transactionId = `tx-${req.userId}`;
		await deps.event.publish("payment.charged", {
			transactionId,
			amount: req.amount,
		});
		return { transactionId, ok: true };
	};
}

describe("charge RPC handler (example)", () => {
	it("checks fraud, publishes payment.charged, and returns the transaction", async () => {
		const harness = createTestHarness();
		harness.rpc.mockResponse("fraud-svc", "Check", { blocked: false });
		harness.rpc.handle("Charge", makeChargeHandler(harness));

		const res = await harness.rpc.invoke<ChargeRequest, ChargeResponse>(
			"Charge",
			{ userId: "u-1", amount: 42 },
		);

		expect(res).toEqual({ transactionId: "tx-u-1", ok: true });
		expect(harness.rpc.calls()).toEqual([
			{
				serviceName: "fraud-svc",
				methodName: "Check",
				payload: { userId: "u-1" },
			},
		]);
		expect(harness.event.published()).toEqual([
			{
				name: "payment.charged",
				payload: { transactionId: "tx-u-1", amount: 42 },
			},
		]);
	});

	it("rejects and publishes nothing when the fraud check blocks the user", async () => {
		const harness = createTestHarness();
		harness.rpc.mockResponse("fraud-svc", "Check", { blocked: true });
		harness.rpc.handle("Charge", makeChargeHandler(harness));

		await expect(
			harness.rpc.invoke("Charge", { userId: "u-2", amount: 5 }),
		).rejects.toThrow(/blocked by fraud check/);
		expect(harness.event.published()).toEqual([]);
	});
});

describe("payment.charged event handler (example)", () => {
	it("acks after sending the receipt; nacks and sends nothing when the mailer fails", async () => {
		const harness = createTestHarness();
		const sent: string[] = [];
		let mailerDown = false;

		harness.event.handle("payment.charged", async (payload) => {
			if (mailerDown) throw new Error("mailer unavailable");
			sent.push((payload as { transactionId: string }).transactionId);
		});

		const first = await harness.event.deliver("payment.charged", {
			transactionId: "tx-u-1",
		});
		expect(first).toEqual({ outcome: "ack" });
		expect(sent).toEqual(["tx-u-1"]);

		mailerDown = true;
		const retried = await harness.event.deliver("payment.charged", {
			transactionId: "tx-u-2",
		});
		expect(retried).toEqual({
			outcome: "nack",
			reason: "Error: mailer unavailable",
		});
		expect(sent).toEqual(["tx-u-1"]);
	});
});
