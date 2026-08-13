import { describe, expect, it } from "bun:test";
import { TestEventDomain } from "./event-harness";

describe("TestEventDomain.handle + deliver", () => {
	it("invokes the registered handler and acks on success", async () => {
		const event = new TestEventDomain();
		const seen: unknown[] = [];
		event.handle("payment.charged", (payload) => {
			seen.push(payload);
		});

		const result = await event.deliver("payment.charged", { txId: "tx-1" });
		expect(result).toEqual({ outcome: "ack" });
		expect(seen).toEqual([{ txId: "tx-1" }]);
	});

	it("acks when no handler is registered — matches subscriber.ts (server-side routing)", async () => {
		const event = new TestEventDomain();
		const result = await event.deliver("nobody.listens", {});
		expect(result).toEqual({ outcome: "ack" });
	});

	it("nacks with String(error) when the handler throws", async () => {
		const event = new TestEventDomain();
		event.handle("payment.charged", () => {
			throw new Error("db unavailable");
		});

		const result = await event.deliver("payment.charged", {});
		expect(result).toEqual({
			outcome: "nack",
			reason: "Error: db unavailable",
		});
	});

	it("invokes every handler registered under the same exact pattern, in order", async () => {
		const event = new TestEventDomain();
		const calls: string[] = [];
		event.handle("payment.charged", () => {
			calls.push("first");
		});
		event.handle("payment.charged", () => {
			calls.push("second");
		});

		const result = await event.deliver("payment.charged", {});
		expect(result).toEqual({ outcome: "ack" });
		expect(calls).toEqual(["first", "second"]);
	});

	it("stops at the first throwing handler and nacks (matches subscriber.ts)", async () => {
		const event = new TestEventDomain();
		const calls: string[] = [];
		event.handle("payment.charged", () => {
			calls.push("first");
			throw new Error("boom");
		});
		event.handle("payment.charged", () => {
			calls.push("second");
		});

		const result = await event.deliver("payment.charged", {});
		expect(result).toEqual({ outcome: "nack", reason: "Error: boom" });
		expect(calls).toEqual(["first"]);
	});

	it("dispatches by exact name — a wildcard pattern registration does not match", async () => {
		const event = new TestEventDomain();
		const calls: string[] = [];
		event.handle("payment.*", () => {
			calls.push("wildcard");
		});

		const result = await event.deliver("payment.charged", {});
		expect(result).toEqual({ outcome: "ack" });
		expect(calls).toEqual([]);
	});

	it("supports async handlers", async () => {
		const event = new TestEventDomain();
		let seen: unknown;
		event.handle("payment.charged", async (payload) => {
			await Promise.resolve();
			seen = payload;
		});

		await event.deliver("payment.charged", { amount: 5 });
		expect(seen).toEqual({ amount: 5 });
	});
});

describe("TestEventDomain.publish", () => {
	it("records the published event and returns a generated eventId", async () => {
		const event = new TestEventDomain();
		const { eventId } = await event.publish("payment.charged", {
			amount: 10,
		});

		expect(typeof eventId).toBe("string");
		expect(eventId.length).toBeGreaterThan(0);
		expect(event.published()).toEqual([
			{ name: "payment.charged", payload: { amount: 10 } },
		]);
	});

	it("records opts alongside the published event", async () => {
		const event = new TestEventDomain();
		await event.publish("payment.charged", {}, { partitionKey: "user-1" });
		expect(event.published()[0]?.opts).toEqual({ partitionKey: "user-1" });
	});

	it("generates a distinct eventId per publish call", async () => {
		const event = new TestEventDomain();
		const first = await event.publish("payment.charged", {});
		const second = await event.publish("payment.charged", {});
		expect(first.eventId).not.toBe(second.eventId);
	});
});

describe("TestEventDomain.reset", () => {
	it("clears registered handlers and published records", async () => {
		const event = new TestEventDomain();
		event.handle("payment.charged", () => {});
		await event.publish("payment.charged", {});

		event.reset();

		expect(event.published()).toEqual([]);
		// No handler registered anymore → deliver falls back to ack, not a call.
		const calls: string[] = [];
		event.handle("payment.charged", () => {
			calls.push("x");
		});
		await event.deliver("other.name", {});
		expect(calls).toEqual([]);
	});
});
