import { beforeEach, describe, expect, test } from "bun:test";
import type {
	OpReport,
	PayloadAttachment,
} from "../pb/servicebridge/v1/telemetry";
import { currentTraceContext, runWithTrace } from "./context";
import {
	Channel,
	HttpHandle,
	OpHandle,
	RpcCall,
	Status,
	UserSubOp,
} from "./ops";
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

describe("OpHandle.run", () => {
	let ring: TelemetryRing;

	beforeEach(() => {
		ring = new TelemetryRing();
	});

	const frames = (): OpReport[] =>
		ring
			.peek(200)
			.filter((i) => i.kind === "ops")
			.map((i) => i.message as OpReport);

	const startFrames = (): OpReport[] =>
		frames().filter((f) => f.finishedAtMs === undefined);

	const endFrame = (opId: string): OpReport | undefined =>
		frames().find((f) => f.opId === opId && f.finishedAtMs !== undefined);

	const startFrameOf = (opId: string): OpReport | undefined =>
		startFrames().find((f) => f.opId === opId);

	const userOp = (subject: string) =>
		OpHandle.start(ring, {
			channel: Channel.USER,
			kind: UserSubOp,
			subject,
		});

	test("work inside the op becomes its child", async () => {
		const outer = userOp("reconcile");
		let child: OpHandle | undefined;

		await outer.run(async () => {
			await Promise.resolve();
			child = OpHandle.start(ring, {
				channel: Channel.RPC,
				kind: RpcCall,
				subject: "rpc.call:billing/Charge",
			});
		});

		const childFrame = startFrameOf(child?.opId ?? "");
		expect(childFrame?.parentOpId).toBe(outer.opId);
		expect(childFrame?.traceId).toBe(outer.traceId);
		expect(endFrame(outer.opId)?.status).toBe(Status.SUCCESS);
	});

	test("passes the handle to fn and returns its value", async () => {
		const op = userOp("reconcile");
		const got = await op.run((handle) => {
			expect(handle).toBe(op);
			return 42;
		});
		expect(got).toBe(42);
	});

	test("a throw closes the op with ERROR and re-throws", async () => {
		const op = userOp("reconcile");
		await expect(
			op.run(async () => {
				throw new Error("boom");
			}),
		).rejects.toThrow("boom");

		const ended = endFrame(op.opId);
		expect(ended?.status).toBe(Status.ERROR);
		expect(ended?.statusMessage).toBe("boom");
	});

	test("a non-Error throw is stringified into the status message", async () => {
		const op = userOp("reconcile");
		await expect(op.run(() => Promise.reject("nope"))).rejects.toBe("nope");
		expect(endFrame(op.opId)?.statusMessage).toBe("nope");
	});

	test("nested runs build a chain, not a flat list", async () => {
		const outer = userOp("outer");
		let inner: OpHandle | undefined;
		let leaf: OpHandle | undefined;

		await outer.run(async () => {
			inner = userOp("inner");
			await inner.run(async () => {
				await Promise.resolve();
				leaf = userOp("leaf");
			});
		});

		expect(startFrameOf(inner?.opId ?? "")?.parentOpId).toBe(outer.opId);
		expect(startFrameOf(leaf?.opId ?? "")?.parentOpId).toBe(inner?.opId);
		expect(endFrame(inner?.opId ?? "")?.status).toBe(Status.SUCCESS);
		expect(endFrame(outer.opId)?.status).toBe(Status.SUCCESS);
	});

	test("parallel runs do not steal each other's children", async () => {
		const root = {
			traceId: "01900000-0000-7000-8000-0000000000cc",
			parentOpId: "01900000-0000-7000-8000-0000000000dd",
		};

		const spawn = async (subject: string, delayMs: number) => {
			const op = userOp(subject);
			let child: OpHandle | undefined;
			await op.run(async () => {
				await new Promise((r) => setTimeout(r, delayMs));
				child = userOp(`${subject}-child`);
			});
			return { op, child };
		};

		const [a, b] = await runWithTrace(root, () =>
			Promise.all([spawn("a", 5), spawn("b", 1)]),
		);

		expect(startFrameOf(a.child?.opId ?? "")?.parentOpId).toBe(a.op.opId);
		expect(startFrameOf(b.child?.opId ?? "")?.parentOpId).toBe(b.op.opId);
		expect(a.op.opId).not.toBe(b.op.opId);
		// Both spans stay siblings under the ambient root, not under each other.
		expect(startFrameOf(a.op.opId)?.parentOpId).toBe(root.parentOpId);
		expect(startFrameOf(b.op.opId)?.parentOpId).toBe(root.parentOpId);
	});

	test("the ambient scope is restored after run", async () => {
		const root = {
			traceId: "01900000-0000-7000-8000-0000000000ee",
			parentOpId: "01900000-0000-7000-8000-0000000000ff",
		};

		await runWithTrace(root, async () => {
			const first = userOp("first");
			await first.run(async () => {
				await Promise.resolve();
			});
			expect(currentTraceContext()).toEqual(root);
			const second = userOp("second");
			expect(startFrameOf(second.opId)?.parentOpId).toBe(root.parentOpId);
		});
	});

	test("a status set inside fn wins over the automatic close", async () => {
		const op = userOp("reconcile");
		await op.run((handle) => {
			handle.end(Status.TIMEOUT, "took too long");
		});

		const ends = frames().filter(
			(f) => f.opId === op.opId && f.finishedAtMs !== undefined,
		);
		expect(ends).toHaveLength(1);
		expect(ends[0]?.status).toBe(Status.TIMEOUT);
		expect(ends[0]?.statusMessage).toBe("took too long");
	});

	test("scope exposes this op as the parent for children", () => {
		const op = userOp("reconcile");
		expect(op.scope).toEqual({ traceId: op.traceId, parentOpId: op.opId });
	});
});
