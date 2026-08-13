import { describe, expect, it } from "bun:test";
import { createTestHarness } from "./harness";

describe("createTestHarness", () => {
	it("wires rpc and event handles that can be registered and invoked independently", async () => {
		const harness = createTestHarness();

		harness.rpc.handle("Charge", (req: { amount: number }) => ({
			ok: req.amount > 0,
		}));
		harness.event.handle("payment.charged", () => {});

		const rpcRes = await harness.rpc.invoke("Charge", { amount: 1 });
		expect(rpcRes).toEqual({ ok: true });

		const eventRes = await harness.event.deliver("payment.charged", {});
		expect(eventRes).toEqual({ outcome: "ack" });
	});

	it("reset() clears both rpc and event state", async () => {
		const harness = createTestHarness();
		harness.rpc.handle("Charge", () => ({}));
		harness.event.handle("payment.charged", () => {});
		await harness.event.publish("payment.charged", {});

		harness.reset();

		await expect(harness.rpc.invoke("Charge", {})).rejects.toThrow();
		expect(harness.event.published()).toEqual([]);
	});
});
