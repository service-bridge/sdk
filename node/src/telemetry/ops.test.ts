import { beforeEach, describe, expect, test } from "bun:test";
import type {
	OpReport,
	PayloadAttachment,
} from "../pb/servicebridge/v1/telemetry";
import { runWithTrace } from "./context";
import { Channel, HttpHandle, OpHandle, RpcCall, Status } from "./ops";
import { TelemetryRing } from "./ring";
import { ZERO_OP_ID } from "./trace-context";

describe("OpHandle", () => {
	let ring: TelemetryRing;

	beforeEach(() => {
		ring = new TelemetryRing();
	});

	const drainOps = (): OpReport[] =>
		ring
			.peek(100)
			.filter((i) => i.kind === "ops")
			.map((i) => i.message as OpReport);

	const makeParams = () => ({
		traceId: "01900000-0000-7000-8000-000000000001",
		opId: "01900000-0000-7000-8000-000000000002",
		channel: Channel.RPC,
		kind: RpcCall,
		subject: "rpc.call:svc/method",
	});

	test("start enqueues a START frame (no finishedAtMs)", () => {
		const handle = OpHandle.start(ring, makeParams());
		const ops = drainOps();
		expect(ops).toHaveLength(1);

		const decoded = ops[0]!;
		expect(decoded.traceId).toBe("01900000-0000-7000-8000-000000000001");
		expect(decoded.opId).toBe("01900000-0000-7000-8000-000000000002");
		expect(decoded.finishedAtMs).toBeUndefined();
		expect(decoded.status).toBe(Status.PENDING);
		void handle;
	});

	test("end enqueues an END frame with finishedAtMs", () => {
		const handle = OpHandle.start(ring, makeParams());
		ring.commit(ring.peek(100)); // consume start frame

		handle.end(Status.SUCCESS);
		const ops = drainOps();
		expect(ops).toHaveLength(1);

		const decoded = ops[0]!;
		expect(decoded.finishedAtMs).toBeDefined();
		expect(Number(decoded.finishedAtMs) > 0).toBe(true);
		expect(decoded.status).toBe(Status.SUCCESS);
	});

	test("end is idempotent", () => {
		const handle = OpHandle.start(ring, makeParams());
		ring.commit(ring.peek(100));

		handle.end(Status.ERROR, "oops");
		handle.end(Status.SUCCESS); // should be no-op

		const ops = drainOps();
		expect(ops).toHaveLength(1);
		const decoded = ops[0]!;
		expect(decoded.status).toBe(Status.ERROR);
		expect(decoded.statusMessage).toBe("oops");
	});

	test("auto-mints opId and inherits trace context from ALS", () => {
		const ctx = {
			traceId: "01900000-0000-7000-8000-0000000000aa",
			parentOpId: "01900000-0000-7000-8000-0000000000bb",
		};
		runWithTrace(ctx, () => {
			OpHandle.start(ring, {
				channel: Channel.HTTP,
				kind: HttpHandle,
				subject: "http.handle:GET//api",
				businessKey: "req-1",
			});
		});
		const ops = drainOps();
		expect(ops).toHaveLength(1);
		const decoded = ops[0]!;
		expect(decoded.traceId).toBe(ctx.traceId);
		expect(decoded.parentOpId).toBe(ctx.parentOpId);
		expect(decoded.opId).toMatch(/^[0-9a-f-]{36}$/);
	});

	test("auto-mints opId and uses ZERO parent when no ALS context", () => {
		OpHandle.start(ring, {
			channel: Channel.HTTP,
			kind: HttpHandle,
			subject: "http.handle:GET//api",
			businessKey: "req-2",
		});
		const decoded = drainOps()[0]!;
		expect(decoded.parentOpId).toBe(ZERO_OP_ID);
		expect(decoded.traceId).toMatch(/^[0-9a-f-]{36}$/);
		expect(decoded.opId).toMatch(/^[0-9a-f-]{36}$/);
	});

	test("opId and traceId getters expose minted ids", () => {
		const handle = OpHandle.start(ring, {
			channel: Channel.RPC,
			kind: RpcCall,
			subject: "rpc.call:svc/m",
		});
		expect(handle.opId).toMatch(/^[0-9a-f-]{36}$/);
		expect(handle.traceId).toMatch(/^[0-9a-f-]{36}$/);
	});
});

describe("OpHandle payload capture", () => {
	let ring: TelemetryRing;

	beforeEach(() => {
		ring = new TelemetryRing();
	});

	const drainPayloads = (): PayloadAttachment[] =>
		ring
			.peek(100)
			.filter((i) => i.kind === "payloads")
			.map((i) => i.message as PayloadAttachment);

	// effectiveCaptureMode is the runtime-pushed mode (the authority);
	// captureMode is the optional per-handler narrowing override.
	const params = (
		effectiveCaptureMode?: "all" | "errors" | "none",
		captureMode?: "all" | "errors" | "none",
	) => ({
		traceId: "01900000-0000-7000-8000-000000000001",
		opId: "01900000-0000-7000-8000-000000000002",
		channel: Channel.RPC,
		kind: RpcCall,
		subject: "rpc.call:svc/m",
		effectiveCaptureMode,
		captureMode,
	});

	test("pushed none: captureIn/Out emit nothing", () => {
		const h = OpHandle.start(ring, params("none"));
		h.captureIn(new Uint8Array([1]), "hash");
		h.captureOut(new Uint8Array([2]), "hash");
		h.end(Status.ERROR, "x");
		expect(drainPayloads()).toHaveLength(0);
	});

	test("no pushed mode defaults to none (capture nothing)", () => {
		const h = OpHandle.start(ring, params());
		h.captureIn(new Uint8Array([1]), "hash");
		h.end(Status.ERROR, "x");
		expect(drainPayloads()).toHaveLength(0);
	});

	test("pushed all: captureIn/Out emit immediately on success", () => {
		const h = OpHandle.start(ring, params("all"));
		h.captureIn(new Uint8Array([1]), "hash");
		h.captureOut(new Uint8Array([2]), "hash");
		const payloads = drainPayloads();
		expect(payloads).toHaveLength(2);
		const a = payloads[0]!;
		expect(a.opId).toBe("01900000-0000-7000-8000-000000000002");
		expect([1, 2]).toContain(a.direction);
	});

	test("pushed errors: buffers and emits on ERROR end", () => {
		const h = OpHandle.start(ring, params("errors"));
		h.captureIn(new Uint8Array([1]), "hash");
		h.captureOut(new Uint8Array([2]), "hash");
		expect(drainPayloads()).toHaveLength(0); // buffered, nothing yet
		h.end(Status.ERROR, "boom");
		const payloads = drainPayloads();
		expect(payloads).toHaveLength(2);
		expect(payloads.map((p) => p.direction).sort()).toEqual([1, 2]);
	});

	test("pushed errors: re-capturing a direction keeps only the last (last-wins)", () => {
		const h = OpHandle.start(ring, params("errors"));
		h.captureOut(new Uint8Array([1]), "hash");
		h.captureOut(new Uint8Array([2]), "hash"); // retry re-capture, overwrites
		h.end(Status.ERROR, "boom");
		const payloads = drainPayloads();
		expect(payloads).toHaveLength(1);
		expect(payloads[0]?.direction).toBe(2);
		expect(Array.from(payloads[0]?.bytes ?? [])).toEqual([2]);
	});

	test("pushed errors: drops on OK end", () => {
		const h = OpHandle.start(ring, params("errors"));
		h.captureIn(new Uint8Array([1]), "hash");
		h.end(Status.SUCCESS);
		expect(drainPayloads()).toHaveLength(0);
	});

	test("per-handler override narrows pushed all to none (no capture)", () => {
		const h = OpHandle.start(ring, params("all", "none"));
		h.captureIn(new Uint8Array([1]), "hash");
		expect(drainPayloads()).toHaveLength(0);
	});

	test("per-handler override cannot widen pushed none to all", () => {
		const h = OpHandle.start(ring, params("none", "all"));
		h.captureIn(new Uint8Array([1]), "hash");
		expect(drainPayloads()).toHaveLength(0);
	});

	test("attachment carries originalSize for truncated payloads", () => {
		const h = OpHandle.start(ring, { ...params("all"), payloadMaxBytes: 2 });
		h.captureIn(new Uint8Array([1, 2, 3, 4, 5]), "hash");
		const a = drainPayloads()[0]!;
		expect(a.bytes.byteLength).toBe(2);
		expect(a.originalSize).toBe(5);
	});
});
