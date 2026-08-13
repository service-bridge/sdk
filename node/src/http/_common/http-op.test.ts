import { describe, expect, it } from "bun:test";
import { Status } from "../../telemetry/ops";
import { ZERO_OP_ID } from "../../telemetry/trace-context";
import { firstHeader, startHttpOp, statusForHttpCode } from "./http-op";
import { makeSbStub } from "./sb-stub";

const REQ = {
	method: "POST",
	subjectPath: "/orders/:id",
	keyPath: "/orders/42",
	traceHeader: undefined,
	idempotencyKey: undefined,
};

describe("firstHeader", () => {
	it("passes a single string through", () => {
		expect(firstHeader("a")).toBe("a");
	});

	it("takes the first entry of a repeated header", () => {
		expect(firstHeader(["a", "b"])).toBe("a");
	});

	it("maps null and undefined to undefined", () => {
		expect(firstHeader(null)).toBeUndefined();
		expect(firstHeader(undefined)).toBeUndefined();
	});
});

describe("statusForHttpCode", () => {
	it("maps <400 to SUCCESS without a message", () => {
		expect(statusForHttpCode(204)).toEqual({ status: Status.SUCCESS });
		expect(statusForHttpCode(399)).toEqual({ status: Status.SUCCESS });
	});

	it("maps 4xx and 5xx to the same ERROR shape", () => {
		expect(statusForHttpCode(404)).toEqual({
			status: Status.ERROR,
			message: "HTTP 404",
		});
		expect(statusForHttpCode(500)).toEqual({
			status: Status.ERROR,
			message: "HTTP 500",
		});
	});
});

describe("startHttpOp", () => {
	it("mints a root op when X-SB-Trace is absent", () => {
		const stub = makeSbStub();
		const op = startHttpOp(stub.sb, REQ);
		expect(stub.started).toHaveLength(1);
		expect(stub.started[0]?.parentOpId).toBe(ZERO_OP_ID);
		expect(stub.started[0]?.subject).toBe("http.handle:POST//orders/:id");
		expect(op.scope.traceId).toBe(op.incoming.traceId);
		expect(op.scope.parentOpId).toBe(op.handle.opId);
	});

	it("inherits traceId and parent from a propagated X-SB-Trace", () => {
		const stub = makeSbStub();
		const traceId = "0198f0f1-0000-7000-8000-000000000001";
		const parentOpId = "0198f0f1-0000-7000-8000-000000000002";
		const op = startHttpOp(stub.sb, {
			...REQ,
			traceHeader: `${traceId}-${parentOpId}`,
		});
		expect(op.incoming.traceId).toBe(traceId);
		expect(stub.started[0]?.traceId).toBe(traceId);
		expect(stub.started[0]?.parentOpId).toBe(parentOpId);
	});

	it("falls back to '<METHOD> <path>' as the business key", () => {
		const stub = makeSbStub();
		startHttpOp(stub.sb, REQ);
		expect(stub.started[0]?.businessKey).toBe("POST /orders/42");
	});

	it("prefers the Idempotency-Key header as the business key", () => {
		const stub = makeSbStub();
		startHttpOp(stub.sb, { ...REQ, idempotencyKey: ["key-1", "key-2"] });
		expect(stub.started[0]?.businessKey).toBe("key-1");
	});

	it("reports capturing=false while the runtime pushes 'none'", () => {
		const stub = makeSbStub("none");
		expect(startHttpOp(stub.sb, REQ).capturing).toBe(false);
	});

	it("reports capturing=true for 'all' and 'errors'", () => {
		const all = makeSbStub("all");
		expect(startHttpOp(all.sb, REQ).capturing).toBe(true);
		const errors = makeSbStub("errors");
		expect(startHttpOp(errors.sb, REQ).capturing).toBe(true);
	});
});
