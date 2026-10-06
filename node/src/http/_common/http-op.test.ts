import { describe, expect, it } from "bun:test";
import { Status } from "../../telemetry/ops";
import { ZERO_OP_ID } from "../../telemetry/trace-context";
import { firstHeader, type HttpOpRequest, startHttpOp } from "./http-op";
import { makeSbStub } from "./sb-stub";

const REQ: HttpOpRequest = {
	method: "post",
	route: "/orders/:id",
	traceHeader: undefined,
	idempotencyKey: undefined,
};

const TRACE = "0198f0f1-0000-7000-8000-000000000001";
const PARENT = "0198f0f1-0000-7000-8000-000000000002";

describe("firstHeader", () => {
	it("passes a single string through and takes the first of a repeated header", () => {
		expect(firstHeader("a")).toBe("a");
		expect(firstHeader(["a", "b"])).toBe("a");
		expect(firstHeader(null)).toBeUndefined();
		expect(firstHeader(undefined)).toBeUndefined();
	});
});

describe("startHttpOp", () => {
	it("subject and meta carry the route template, never the raw path", () => {
		const stub = makeSbStub();
		startHttpOp(stub.sb, REQ, {});
		expect(stub.started[0]?.subject).toBe("http.handle:POST//orders/:id");
		expect(stub.started[0]?.meta).toEqual({
			method: "POST",
			route: "/orders/:id",
		});
	});

	it("business key falls back to '<METHOD> <route>'", () => {
		const stub = makeSbStub();
		startHttpOp(stub.sb, REQ, {});
		expect(stub.started[0]?.businessKey).toBe("POST /orders/:id");
	});

	it("prefers the Idempotency-Key header as the business key", () => {
		const stub = makeSbStub();
		startHttpOp(stub.sb, { ...REQ, idempotencyKey: ["key-1", "key-2"] }, {});
		expect(stub.started[0]?.businessKey).toBe("key-1");
	});

	it("ignores an incoming X-SB-Trace by default — a public edge mints its own root", () => {
		const stub = makeSbStub();
		const op = startHttpOp(
			stub.sb,
			{ ...REQ, traceHeader: `${TRACE}-${PARENT}` },
			{},
		);
		expect(stub.started[0]?.traceId).not.toBe(TRACE);
		expect(stub.started[0]?.parentOpId).toBe(ZERO_OP_ID);
		expect(op.scope.parentOpId).toBe(op.handle.opId);
	});

	it("joins the caller's trace with trustTraceHeader", () => {
		const stub = makeSbStub();
		startHttpOp(
			stub.sb,
			{ ...REQ, traceHeader: `${TRACE}-${PARENT}` },
			{ trustTraceHeader: true },
		);
		expect(stub.started[0]?.traceId).toBe(TRACE);
		expect(stub.started[0]?.parentOpId).toBe(PARENT);
	});

	it("a malformed trusted header still yields a fresh root", () => {
		const stub = makeSbStub();
		startHttpOp(
			stub.sb,
			{ ...REQ, traceHeader: "garbage" },
			{ trustTraceHeader: true },
		);
		expect(stub.started[0]?.parentOpId).toBe(ZERO_OP_ID);
	});

	it("finish maps the status code and records it in meta", () => {
		const stub = makeSbStub();
		const ok = startHttpOp(stub.sb, REQ, {});
		ok.finish(204);
		const failed = startHttpOp(stub.sb, REQ, {});
		failed.finish(404);
		expect(stub.endCalls[0]).toEqual({
			status: Status.SUCCESS,
			message: undefined,
			meta: { status: 204 },
		});
		expect(stub.endCalls[1]).toEqual({
			status: Status.ERROR,
			message: "HTTP 404",
			meta: { status: 404 },
		});
	});

	it("abort and fail end the op as TIMEOUT and ERROR", () => {
		const stub = makeSbStub();
		startHttpOp(stub.sb, REQ, {}).abort();
		startHttpOp(stub.sb, REQ, {}).fail("boom");
		expect(stub.endCalls.map((e) => e.status)).toEqual([
			Status.TIMEOUT,
			Status.ERROR,
		]);
	});

	it("reports capturing from the runtime-pushed mode", () => {
		expect(startHttpOp(makeSbStub("none").sb, REQ, {}).capturing).toBe(false);
		expect(startHttpOp(makeSbStub("errors").sb, REQ, {}).capturing).toBe(true);
	});
});
