import { describe, expect, it } from "bun:test";
import { TestRpcDomain } from "./rpc-harness";

describe("TestRpcDomain.handle + invoke", () => {
	it("invokes the registered handler and returns its result", async () => {
		const rpc = new TestRpcDomain();
		rpc.handle("Charge", (req: { amount: number }) => ({
			ok: req.amount > 0,
		}));

		const res = await rpc.invoke("Charge", { amount: 10 });
		expect(res).toEqual({ ok: true });
	});

	it("rejects when no handler is registered for the method", async () => {
		const rpc = new TestRpcDomain();
		await expect(rpc.invoke("Missing", {})).rejects.toThrow(
			/no RPC handler registered for "Missing"/,
		);
	});

	it("propagates the handler's thrown error unchanged", async () => {
		const rpc = new TestRpcDomain();
		rpc.handle("Charge", () => {
			throw new Error("insufficient funds");
		});

		await expect(rpc.invoke("Charge", {})).rejects.toThrow(
			"insufficient funds",
		);
	});

	it("supports async handlers", async () => {
		const rpc = new TestRpcDomain();
		rpc.handle("Charge", async (req: { amount: number }) => {
			await Promise.resolve();
			return { doubled: req.amount * 2 };
		});

		const res = await rpc.invoke("Charge", { amount: 5 });
		expect(res).toEqual({ doubled: 10 });
	});
});

describe("TestRpcDomain.call", () => {
	it("records the call and throws when no mock response is configured", async () => {
		const rpc = new TestRpcDomain();
		await expect(rpc.call("other-svc", "Method", { a: 1 })).rejects.toThrow(
			/no mock response configured for rpc\.call\("other-svc", "Method"\)/,
		);
		expect(rpc.calls()).toEqual([
			{ serviceName: "other-svc", methodName: "Method", payload: { a: 1 } },
		]);
	});

	it("returns a static mocked response and records the call", async () => {
		const rpc = new TestRpcDomain();
		rpc.mockResponse("other-svc", "Method", { transactionId: "tx-1" });

		const res = await rpc.call("other-svc", "Method", { amount: 42 });
		expect(res).toEqual({ transactionId: "tx-1" });
		expect(rpc.calls()).toHaveLength(1);
		expect(rpc.calls()[0]).toEqual({
			serviceName: "other-svc",
			methodName: "Method",
			payload: { amount: 42 },
		});
	});

	it("computes a mocked response from the payload via a responder function", async () => {
		const rpc = new TestRpcDomain();
		rpc.mockResponse("other-svc", "Method", (payload: { amount: number }) => ({
			doubled: payload.amount * 2,
		}));

		const res = await rpc.call("other-svc", "Method", { amount: 21 });
		expect(res).toEqual({ doubled: 42 });
	});

	it("records opts alongside the call", async () => {
		const rpc = new TestRpcDomain();
		rpc.mockResponse("other-svc", "Method", {});
		await rpc.call("other-svc", "Method", {}, { timeout: "5s" });
		expect(rpc.calls()[0]?.opts).toEqual({ timeout: "5s" });
	});
});

describe("TestRpcDomain.reset", () => {
	it("clears registered handlers, mocks, and recorded calls", async () => {
		const rpc = new TestRpcDomain();
		rpc.handle("Charge", () => ({}));
		rpc.mockResponse("svc", "Method", {});
		await rpc.call("svc", "Method", {});

		rpc.reset();

		expect(rpc.calls()).toEqual([]);
		await expect(rpc.invoke("Charge", {})).rejects.toThrow();
		await expect(rpc.call("svc", "Method", {})).rejects.toThrow(
			/no mock response configured/,
		);
	});
});
