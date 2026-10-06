import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import {
	ConfigurationError,
	HandlerError,
	type ServiceBridgeError,
	ValidationError,
} from "../errors";
import type { EventHandlerContext } from "../registry/registry";
import type { RpcHandlerContext } from "../rpc/dispatch-port";
import { createTestHarness, matchPattern } from "./harness";

const SHOP = join(import.meta.dir, "testdata", "shop.proto");
const CHARGE = { protoFile: SHOP, method: "Charge" };
const TICK = { protoFile: SHOP, input: "Tick", output: "Tick" };

describe("invoke", () => {
	it("round-trips the request and response through the schema", async () => {
		const h = createTestHarness();
		let seen: unknown;
		h.sb.rpc.handle(
			"Charge",
			(req) => {
				seen = req;
				return {
					transactionId: "t",
					ok: true,
					ignored: "dropped by the schema",
				};
			},
			{ schema: CHARGE },
		);
		await h.start();
		expect(
			await h.invoke<unknown, unknown>("Charge", { userId: "u", amount: 2.5 }),
		).toEqual({
			transactionId: "t",
			ok: true,
		});
		expect(seen).toEqual({ userId: "u", amount: 2.5 });
	});

	it("hands the handler ctx with the given caller, ids and deadline", async () => {
		const h = createTestHarness();
		let ctx: RpcHandlerContext | undefined;
		h.sb.rpc.handle(
			"Charge",
			(_req, c) => {
				ctx = c;
				return {};
			},
			{ schema: CHARGE },
		);
		await h.start();
		await h.invoke(
			"Charge",
			{},
			{
				caller: { serviceId: "s", instanceId: "i" },
				requestId: "r-1",
				idempotencyKey: "k",
				deadline: 123,
			},
		);
		expect(ctx).toMatchObject({
			caller: { serviceId: "s", instanceId: "i" },
			requestId: "r-1",
			idempotencyKey: "k",
			deadline: 123,
		});
	});

	it("errors come back in the caller's form", async () => {
		const h = createTestHarness();
		h.sb.rpc.handle(
			"Charge",
			(req: { userId: string }) => {
				if (req.userId === "biz") throw new HandlerError("NOPE", "no");
				throw new Error("boom");
			},
			{ schema: CHARGE },
		);
		await h.start();
		const biz = await h.invoke("Charge", { userId: "biz" }).catch((e) => e);
		expect((biz as HandlerError).handlerCode).toBe("NOPE");
		const internal = await h.invoke("Charge", { userId: "x" }).catch((e) => e);
		expect(internal).toBeInstanceOf(HandlerError);
		expect((internal as HandlerError).handlerCode).toBe("INTERNAL");
		expect((internal as HandlerError).message).toBe("boom");
		const missing = await h.invoke("Nope", {}).catch((e) => e);
		expect((missing as ServiceBridgeError).code).toBe("NOT_FOUND");
	});

	it("invokeStream collects the chunks", async () => {
		const h = createTestHarness();
		h.sb.rpc.handleStream(
			"Countdown",
			async function* (req: { n: number }) {
				for (let i = req.n; i > 0; i--) yield { n: i };
			},
			{ schema: TICK },
		);
		await h.start();
		expect(await h.invokeStream("Countdown", { n: 3 })).toEqual([
			{ n: 3 },
			{ n: 2 },
			{ n: 1 },
		]);
	});
});

describe("outbound calls", () => {
	it("an unanswered call is recorded and fails loudly", async () => {
		const h = createTestHarness();
		await h.sb.client("fraud-svc", SHOP, { methods: ["Check"] });
		await h.start();
		const err = await h.sb.rpc
			.call("fraud-svc", "Check", { userId: "u" }, { idempotencyKey: "k" })
			.catch((e) => e);
		expect((err as ServiceBridgeError).code).toBe("NO_LIVE_INSTANCE");
		expect(h.calls()[0]).toMatchObject({
			service: "fraud-svc",
			method: "Check",
			payload: { userId: "u" },
			opts: { idempotencyKey: "k" },
		});
	});

	it("a call without a declared schema is a configuration error, as in production", async () => {
		const h = createTestHarness();
		await h.start();
		await expect(h.sb.rpc.call("x", "y", {})).rejects.toBeInstanceOf(
			ConfigurationError,
		);
	});

	it("a responder's HandlerError reaches the caller with its code", async () => {
		const h = createTestHarness();
		await h.sb.client("fraud-svc", SHOP, { methods: ["Check"] });
		await h.start();
		h.respond("fraud-svc", "Check", () => {
			throw new HandlerError("DOWN", "fraud is down");
		});
		const err = await h.sb.rpc
			.call("fraud-svc", "Check", { userId: "u" })
			.catch((e) => e);
		expect((err as HandlerError).handlerCode).toBe("DOWN");
	});

	it("respondStream answers an outbound stream", async () => {
		const h = createTestHarness();
		await h.sb.useSchema("payments", "Countdown", TICK);
		await h.start();
		h.respondStream("payments", "Countdown", function* (req: { n: number }) {
			yield { n: req.n };
			yield { n: req.n - 1 };
		});
		const got: unknown[] = [];
		for await (const chunk of h.sb.stream("payments", "Countdown", { n: 5 }))
			got.push(chunk);
		expect(got).toEqual([{ n: 5 }, { n: 4 }]);
	});
});

describe("events", () => {
	const ORDER = {
		protoFile: join(
			import.meta.dir,
			"..",
			"events",
			"testdata",
			"order-event.proto",
		),
		method: "orders_created",
	};

	it("publish goes through the real publisher and is recorded decoded", async () => {
		const h = createTestHarness();
		h.sb.event.define("orders.created", ORDER);
		await h.start();
		const { eventId } = await h.sb.event.publish(
			"orders.created",
			{ orderId: "o-1", amount: 3, currency: "EUR" },
			{ partitionKey: "o-1", idempotencyKey: "k", headers: { a: "b" } },
		);
		expect(h.published()[0]).toMatchObject({
			id: eventId,
			name: "orders.created",
			payload: { orderId: "o-1", amount: 3, currency: "EUR" },
			payloadJson: { orderId: "o-1", amount: 3, currency: "EUR" },
			partitionKey: "o-1",
			idempotencyKey: "k",
			headers: { a: "b" },
		});
	});

	it("deliver routes by the runtime's matching rules and reports ack/nack", async () => {
		const h = createTestHarness();
		const seen: string[] = [];
		let ctx: EventHandlerContext | undefined;
		h.sb.event.handle(
			"orders.*",
			(p: unknown, c) => {
				ctx = c;
				seen.push(`wild:${(p as { orderId: string }).orderId}`);
			},
			{ schema: ORDER },
		);
		h.sb.event.handle(
			"orders.created",
			(p: unknown) => {
				if ((p as { orderId: string }).orderId === "bad")
					throw new Error("bad order");
				seen.push("exact");
			},
			{ schema: ORDER },
		);
		await h.start();

		const ok = await h.deliver(
			"orders.created",
			{ orderId: "o-1" },
			{ partitionKey: "o-1", attempt: 2 },
		);
		expect(ok).toEqual({
			acked: true,
			reason: "",
			matchedPatterns: ["orders.*", "orders.created"],
		});
		expect(seen).toEqual(["wild:o-1", "exact"]);
		expect(ctx).toMatchObject({
			eventName: "orders.created",
			attempt: 2,
			partitionKey: "o-1",
		});

		const bad = await h.deliver("orders.created", { orderId: "bad" });
		expect(bad.acked).toBe(false);
		expect(bad.reason).toBe("bad order");

		const none = await h.deliver("invoices.created", new Uint8Array([1]));
		expect(none).toEqual({
			acked: false,
			reason: "no handler for matched patterns []",
			matchedPatterns: [],
		});
		await h.stop();
	});

	it("deliver without a schema needs raw bytes", async () => {
		const h = createTestHarness();
		h.sb.event.handle("raw.thing", () => {});
		await h.start();
		await expect(h.deliver("raw.thing", { a: 1 })).rejects.toBeInstanceOf(
			ValidationError,
		);
		expect((await h.deliver("raw.thing", new Uint8Array([1]))).acked).toBe(
			true,
		);
		await h.stop();
	});
});

describe("matchPattern", () => {
	it.each([
		["a.b", "a.b", true],
		["a.*", "a.b", true],
		["a.*", "a.b.c", false],
		["a.#", "a", true],
		["a.#", "a.b.c", true],
		["#.c", "a.b.c", true],
		["*.c", "a.b.c", false],
	] as const)("%s ~ %s → %s", (p, n, want) => {
		expect(matchPattern(p, n)).toBe(want);
	});
});
