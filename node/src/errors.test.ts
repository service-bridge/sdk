import { describe, expect, it } from "bun:test";
import { ConnectionError } from "./connection/service-bridge-error";
import {
	AccessDeniedError,
	ConfigurationError,
	codeForGrpcStatus,
	type ErrorCode,
	HandlerError,
	NoLiveInstanceError,
	ServiceBridgeError,
	StateError,
	TimeoutError,
	toServiceBridgeError,
	ValidationError,
} from "./errors";
import { InvalidEventNameError } from "./events/errors";
import {
	WorkflowAccessDeniedError,
	WorkflowNotFoundError,
	WorkflowTerminalError,
} from "./workflow/errors";
import { JsonPathError } from "./workflow/jsonpath";
import { WorkflowValidationError } from "./workflow/validate";

describe("error hierarchy", () => {
	const errors: [string, ServiceBridgeError, ErrorCode][] = [
		[
			"ConnectionError",
			new ConnectionError("provision", new Error("boom")),
			"CONNECTION",
		],
		[
			"InvalidEventNameError",
			new InvalidEventNameError("Bad Name"),
			"INVALID_EVENT_NAME",
		],
		["AccessDeniedError", new AccessDeniedError("denied"), "ACCESS_DENIED"],
		[
			"NoLiveInstanceError",
			new NoLiveInstanceError("none"),
			"NO_LIVE_INSTANCE",
		],
		["ConfigurationError", new ConfigurationError("bad"), "CONFIG"],
		["StateError", new StateError("early"), "STATE"],
		["ValidationError", new ValidationError("bad"), "VALIDATION"],
		["TimeoutError", new TimeoutError("late"), "TIMEOUT"],
		["HandlerError", new HandlerError("OUT_OF_STOCK", "none left"), "HANDLER"],
		[
			"WorkflowAccessDeniedError",
			new WorkflowAccessDeniedError("wf", "no"),
			"ACCESS_DENIED",
		],
		["WorkflowNotFoundError", new WorkflowNotFoundError("wf"), "NOT_FOUND"],
		[
			"WorkflowTerminalError",
			new WorkflowTerminalError("run-1", "success"),
			"TERMINAL",
		],
		[
			"WorkflowValidationError",
			new WorkflowValidationError("bad graph"),
			"VALIDATION",
		],
		[
			"JsonPathError",
			new JsonPathError("unexpected token", "$.["),
			"VALIDATION",
		],
	];

	// One predicate catches every SDK failure, and the code is the axis to
	// switch on.
	for (const [name, err, code] of errors) {
		it(`${name} is a ServiceBridgeError with code ${code}`, () => {
			expect(err).toBeInstanceOf(ServiceBridgeError);
			expect(err.name).toBe(name);
			expect(err.code).toBe(code);
		});
	}

	it("only transient codes are retryable", () => {
		expect(new ConnectionError("x", new Error("y")).retryable).toBe(true);
		expect(new NoLiveInstanceError("x").retryable).toBe(true);
		expect(new ServiceBridgeError("OVERLOADED", "x").retryable).toBe(true);
		expect(new ServiceBridgeError("QUEUE_FULL", "x").retryable).toBe(true);
		expect(new TimeoutError("x").retryable).toBe(false);
		expect(new HandlerError("X", "x").retryable).toBe(false);
		expect(new AccessDeniedError("x").retryable).toBe(false);
	});

	it("a HandlerError without a code reports INTERNAL", () => {
		expect(new HandlerError("", "x").handlerCode).toBe("INTERNAL");
	});

	it("a plain Error is not mistaken for an SDK failure", () => {
		expect(new Error("unrelated")).not.toBeInstanceOf(ServiceBridgeError);
	});
});

describe("gRPC status classification", () => {
	it.each([
		[1, "CANCELLED"],
		[2, "INTERNAL"],
		[3, "VALIDATION"],
		[4, "TIMEOUT"],
		[5, "NOT_FOUND"],
		[6, "CONFLICT"],
		[7, "ACCESS_DENIED"],
		[8, "OVERLOADED"],
		[9, "VALIDATION"],
		[10, "INTERNAL"],
		[11, "VALIDATION"],
		[12, "NOT_FOUND"],
		[13, "INTERNAL"],
		[14, "CONNECTION"],
		[15, "INTERNAL"],
		[16, "ACCESS_DENIED"],
		[99, "INTERNAL"],
	] as [number, ErrorCode][])("status %d → %s", (status, code) => {
		expect(codeForGrpcStatus(status)).toBe(code);
	});

	it("wraps a grpc-js error by status and keeps it as the cause", () => {
		const cause = Object.assign(new Error("7 PERMISSION_DENIED: no"), {
			code: 7,
		});
		const err = toServiceBridgeError("rpc", cause);
		expect(err).toBeInstanceOf(AccessDeniedError);
		expect(err.cause).toBe(cause);
		expect(err.message).toBe("rpc: 7 PERMISSION_DENIED: no");
	});

	it("passes an SDK error through unchanged", () => {
		const err = new StateError("early");
		expect(toServiceBridgeError("x", err)).toBe(err);
	});

	it("an error without a status is INTERNAL", () => {
		expect(toServiceBridgeError("x", new Error("boom")).code).toBe("INTERNAL");
		expect(toServiceBridgeError("x", "boom").code).toBe("INTERNAL");
	});

	it("a nested ConnectionError keeps the inner gRPC status", () => {
		const inner = new ConnectionError(
			"provision",
			Object.assign(new Error("x"), { code: 16 }),
		);
		expect(new ConnectionError("connect", inner).grpcCode).toBe(16);
	});
});
