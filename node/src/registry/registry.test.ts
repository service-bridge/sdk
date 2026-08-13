import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import { MethodType } from "../pb/servicebridge/v1/registry";
import { Handle, Registry } from "./registry";

const protoFile = join(
	import.meta.dir,
	"..",
	"serde",
	"testdata",
	"payment.proto",
);

// ── Handle ──────────────────────────────────────────────────────────────────

describe("Handle.rpc", () => {
	it("schema: loads SchemaPair and sets both schema-json fields", async () => {
		const h = new Handle();
		h.rpc("charge", () => ({}), {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
		});
		await h.finalize();

		const methods = h.incomingMethods();
		expect(methods).toHaveLength(1);
		expect(methods[0]!.type).toBe(MethodType.METHOD_TYPE_RPC);
		expect(methods[0]!.name).toBe("charge");
		expect(methods[0]!.inputSchemaJson.length).toBeGreaterThan(0);
		expect(methods[0]!.outputSchemaJson.length).toBeGreaterThan(0);
		expect(methods[0]!.streaming).toBe(false);
	});

	it("dispatchUnary encodes/decodes via SchemaPair", async () => {
		const h = new Handle();
		h.rpc<
			{ userId: string; amount: number },
			{ transactionId: string; ok: boolean }
		>(
			"charge",
			(req) => ({ transactionId: `tx-${req.userId}`, ok: req.amount > 0 }),
			{
				schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			},
		);
		await h.finalize();

		// Encode a request through the same SchemaPair the handler will use.
		const { buildSchemaPair } = await import("../serde/serializer");
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const reqBytes = pair.input.encode({ userId: "u-1", amount: 5 });

		const port = h.asDispatchPort();
		const result = await port.dispatchUnary("charge", reqBytes);
		expect(result.errorCode ?? "").toBe("");
		const decoded = pair.output.decode(result.payload) as Record<
			string,
			unknown
		>;
		expect(decoded.transactionId).toBe("tx-u-1");
		expect(decoded.ok).toBe(true);
	});

	it("dispatchUnary: unknown method returns NOT_FOUND", async () => {
		const h = new Handle();
		await h.finalize();
		const result = await h
			.asDispatchPort()
			.dispatchUnary("missing", new Uint8Array());
		expect(result.errorCode).toBe("NOT_FOUND");
	});

	it("dispatchUnary: handler throw maps to INTERNAL", async () => {
		const h = new Handle();
		h.rpc(
			"oops",
			() => {
				throw new Error("boom");
			},
			{
				schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			},
		);
		await h.finalize();

		const { buildSchemaPair } = await import("../serde/serializer");
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const reqBytes = pair.input.encode({ userId: "u", amount: 1 });

		const result = await h.asDispatchPort().dispatchUnary("oops", reqBytes);
		expect(result.errorCode).toBe("INTERNAL");
		expect(result.errorMessage).toContain("boom");
	});

	it("captureMode lookup: returns per-handler override or undefined", async () => {
		const h = new Handle();
		h.rpc("with-capture", () => ({}), {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			captureMode: "errors",
		});
		h.rpc("no-capture", () => ({}), {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
		});
		await h.finalize();

		const port = h.asDispatchPort();
		expect(port.captureMode("with-capture")).toBe("errors");
		expect(port.captureMode("no-capture")).toBeUndefined();
		expect(port.captureMode("unknown-method")).toBeUndefined();
	});
});

describe("Handle — hot-path lookups are indexed, not scanned", () => {
	// `_entries` / `_published` mix every declaration type, and dispatch runs per
	// inbound call. Shadowing `find` proves the lookup never walks the array:
	// a scan would both flip the flag and lose the handler.
	function trapScan(arr: unknown[]): () => boolean {
		let scanned = false;
		(arr as unknown as { find: () => undefined }).find = () => {
			scanned = true;
			return undefined;
		};
		return () => scanned;
	}

	it("dispatchUnary + captureMode resolve the RPC entry without scanning _entries", async () => {
		const h = new Handle();
		for (let i = 0; i < 20; i++) h.event(`noise.${i}`, () => {});
		h.rpc("charge", () => ({ transactionId: "tx", ok: true }), {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			captureMode: "errors",
		});
		await h.finalize();

		const { buildSchemaPair } = await import("../serde/serializer");
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const reqBytes = pair.input.encode({ userId: "u", amount: 1 });

		const port = h.asDispatchPort();
		const didScan = trapScan(h._entries);
		const result = await port.dispatchUnary("charge", reqBytes);
		expect(result.errorCode ?? "").toBe("");
		expect(port.captureMode("charge")).toBe("errors");
		expect(didScan()).toBe(false);
	});

	it("getPublishedEvent resolves without scanning _published", async () => {
		const h = new Handle();
		for (let i = 0; i < 20; i++) h._declarePublishedEventForTests(`noise.${i}`);
		h.publishEvent("payments.failed", {
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		await h.finalize();

		const didScan = trapScan(h._published);
		const found = h.getPublishedEvent("payments.failed");
		expect(found?.contractHash.length).toBeGreaterThan(0);
		expect(h.getPublishedEvent("nope")).toBeUndefined();
		expect(didScan()).toBe(false);
	});

	it("eventHandlers returns the fan-out set for a pattern in registration order", () => {
		const h = new Handle();
		const seen: string[] = [];
		h.event("order.created", () => {
			seen.push("first");
		});
		h.event("order.created", () => {
			seen.push("second");
		});
		h.event("order.shipped", () => {});

		const handlers = h.eventHandlers("order.created");
		expect(handlers).toHaveLength(2);
		for (const fn of handlers) fn({});
		expect(seen).toEqual(["first", "second"]);
		expect(h.eventHandlers("order.shipped")).toHaveLength(1);
		expect(h.eventHandlers("unknown")).toHaveLength(0);
	});
});

describe("Handle.finalize — schema load failures", () => {
	it("bad .proto is reported by finalize(), not as an unhandled rejection", async () => {
		const h = new Handle();
		h.rpc("broken", () => ({}), {
			schema: {
				protoFile: join(import.meta.dir, "no-such-file.proto"),
				input: "A",
				output: "B",
			},
		});
		// Let the failing load settle before anything awaits it — that is the
		// window where an unhandled rejection would fire.
		await new Promise((r) => setTimeout(r, 10));
		await expect(h.finalize()).rejects.toThrow();
	});
});

describe("Handle.event", () => {
	it("EVENT entries NOT emitted via incomingMethods (event_subscriptions only)", () => {
		const h = new Handle();
		h.event("payments.success", () => {});
		expect(h.incomingMethods()).toHaveLength(0);
	});

	it("non-EVENT handlers still appear in incomingMethods alongside events", () => {
		const h = new Handle();
		h.event("payments.success", () => {});
		h.workflow("processPayment", []);
		const methods = h.incomingMethods();
		expect(methods).toHaveLength(1);
		expect(methods[0]!.type).toBe(MethodType.METHOD_TYPE_WORKFLOW);
	});
});

describe("Handle.workflow", () => {
	it("with input: inputSchemaJson set, outputSchemaJson empty (workflows have no top-level output)", () => {
		const h = new Handle();
		h.workflow("processPayment", [], { input: { orderId: "string" } });
		const m = h.incomingMethods()[0]!;
		expect(m.type).toBe(MethodType.METHOD_TYPE_WORKFLOW);
		expect(m.inputSchemaJson.length).toBeGreaterThan(0);
		expect(m.outputSchemaJson.length).toBe(0);
	});

	it("no opts: both fields empty", () => {
		const h = new Handle();
		h.workflow("noop", []);
		const m = h.incomingMethods()[0]!;
		expect(m.inputSchemaJson.length).toBe(0);
		expect(m.outputSchemaJson.length).toBe(0);
	});
});

// Handle.http removed (ADR 0001): HTTP routes live in user app, published via
// integrations in src/http/{express,fastify,hono}/.

// ── Handle.publishEvent (registry surface for sb.event.define) ───────────────
//
// _declarePublishedEventForTests bypasses async .proto loading — schemaJson
// and contractHash stay empty. Tests that exercise the full Protobuf path
// live in src/events/domain.test.ts with a real .proto fixture.

describe("Handle.publishEvent (test-only declarations)", () => {
	it("declared event appears in published list with empty schemaJson", () => {
		const r = new Registry();
		r._handle._declarePublishedEventForTests("payments.failed");
		const req = r.buildRegisterRequest();
		expect(req.published).toHaveLength(1);
		expect(req.published[0]!.name).toBe("payments.failed");
		expect(req.published[0]!.schemaJson.length).toBe(0);
		expect(req.published[0]!.contractHash).toBe("");
	});

	it("duplicate test declaration is a no-op", () => {
		const r = new Registry();
		r._handle._declarePublishedEventForTests("payments.success");
		r._handle._declarePublishedEventForTests("payments.success");
		const req = r.buildRegisterRequest();
		expect(req.published).toHaveLength(1);
	});
});

// ── Registry.service ─────────────────────────────────────────────────────────

describe("Registry.service", () => {
	it("exact rpc dep", () => {
		const r = new Registry();
		r.service("payments", { rpc: ["charge"] });
		const req = r.buildRegisterRequest();
		expect(req.outgoing).toHaveLength(1);
		expect(req.outgoing[0]!.serviceName).toBe("payments");
		expect(req.outgoing[0]!.methodName).toBe("charge");
		expect(req.outgoing[0]!.type).toBe(MethodType.METHOD_TYPE_RPC);
	});

	it("wildcard rpc dep", () => {
		const r = new Registry();
		r.service("billing", { rpc: ["*"] });
		const req = r.buildRegisterRequest();
		expect(req.outgoing[0]!.methodName).toBe("*");
	});

	it("duplicate outgoing deps are collapsed", () => {
		const r = new Registry();
		r.service("payments", { rpc: ["charge", "charge"] });
		r.service("payments", { rpc: ["charge"] });
		const req = r.buildRegisterRequest();
		expect(req.outgoing).toHaveLength(1);
	});

	it("same method name under different types stays separate", () => {
		const r = new Registry();
		r.service("svc", { rpc: ["op"], workflows: ["op"] });
		expect(r.buildRegisterRequest().outgoing).toHaveLength(2);
	});

	it("multiple dep types", () => {
		const r = new Registry();
		r.service("svc", { rpc: ["op1"], workflows: ["flow1"], http: ["GET /"] });
		const req = r.buildRegisterRequest();
		expect(req.outgoing).toHaveLength(3);
		const types = req.outgoing.map((o) => o.type);
		expect(types).toContain(MethodType.METHOD_TYPE_RPC);
		expect(types).toContain(MethodType.METHOD_TYPE_WORKFLOW);
		expect(types).toContain(MethodType.METHOD_TYPE_HTTP);
	});
});

// ── Registry.buildRegisterRequest — eventSubscriptions ───────────────────────

describe("Registry.buildRegisterRequest eventSubscriptions", () => {
	it("_handle.event maps to eventSubscriptions with durable=true", () => {
		const r = new Registry();
		r._handle.event("payment.charged", () => {});
		const req = r.buildRegisterRequest();
		expect(req.eventSubscriptions).toHaveLength(1);
		expect(req.eventSubscriptions[0]!.pattern).toBe("payment.charged");
		expect(req.eventSubscriptions[0]!.durable).toBe(true);
	});

	it("multiple event handlers produce separate subscriptions", () => {
		const r = new Registry();
		r._handle.event("order.created", () => {});
		r._handle.event("order.shipped", () => {});
		const req = r.buildRegisterRequest();
		expect(req.eventSubscriptions).toHaveLength(2);
		const patterns = req.eventSubscriptions.map((s) => s.pattern);
		expect(patterns).toContain("order.created");
		expect(patterns).toContain("order.shipped");
	});

	it("no event handlers — eventSubscriptions is empty", () => {
		const r = new Registry();
		r._handle._declareForTests("charge");
		const req = r.buildRegisterRequest();
		expect(req.eventSubscriptions).toHaveLength(0);
	});
});
