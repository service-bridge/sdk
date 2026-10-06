import { describe, expect, it } from "bun:test";
import { Metadata } from "@grpc/grpc-js";
import { AccessDeniedError, HandlerError, TimeoutError } from "../errors";
import {
	CallFailure,
	grpcFailure,
	handlerFailure,
	NOT_DISPATCHED_TRAILER,
	notDispatchedTrailer,
} from "./wire";

function grpcError(code: number, details: string, metadata = new Metadata()) {
	return Object.assign(new Error(`${code} X: ${details}`), {
		code,
		details,
		metadata,
	});
}

describe("grpcFailure", () => {
	it("a status with the not-dispatched trailer is pre-dispatch", () => {
		const f = grpcFailure(
			"rpc",
			grpcError(8, "overloaded", notDispatchedTrailer()),
		);
		expect(f.preDispatch).toBe(true);
		expect(f.error.code).toBe("OVERLOADED");
		expect(f.transport).toBe(true);
	});

	it("the same status without the trailer is not", () => {
		const f = grpcFailure("rpc", grpcError(14, "reset"));
		expect(f.preDispatch).toBe(false);
		expect(f.error.code).toBe("CONNECTION");
	});

	it("a channel that never became ready is a pre-dispatch CONNECTION failure", () => {
		const f = grpcFailure(
			"rpc",
			new Error("Failed to connect before the deadline"),
			true,
		);
		expect(f.preDispatch).toBe(true);
		expect(f.error.code).toBe("CONNECTION");
	});

	it("classifies by status and keeps details as the message", () => {
		const denied = grpcFailure("rpc m", grpcError(7, "acceptance denied"));
		expect(denied.error).toBeInstanceOf(AccessDeniedError);
		expect(denied.error.message).toBe("rpc m: acceptance denied");
		expect(denied.transport).toBe(false);
		const late = grpcFailure("rpc m", grpcError(4, "deadline"));
		expect(late.error).toBeInstanceOf(TimeoutError);
		expect(late.transport).toBe(true);
	});

	it("passes an existing CallFailure through", () => {
		const f = handlerFailure("X", "y");
		expect(grpcFailure("rpc", f)).toBe(f);
	});

	it("the trailer key is the one the Go SDK uses", () => {
		expect(NOT_DISPATCHED_TRAILER).toBe("x-sb-not-dispatched");
	});
});

describe("handlerFailure", () => {
	it("is a HandlerError with the business code, never transport", () => {
		const f = handlerFailure("OUT_OF_STOCK", "none");
		expect(f).toBeInstanceOf(CallFailure);
		expect(f.error).toBeInstanceOf(HandlerError);
		expect((f.error as HandlerError).handlerCode).toBe("OUT_OF_STOCK");
		expect(f.transport).toBe(false);
		expect(f.preDispatch).toBe(false);
	});
});
