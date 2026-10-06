// Error model of the SDK. Every error the SDK throws is a ServiceBridgeError,
// so one `instanceof` separates SDK failures from application ones, and the
// `code` is the single axis callers switch on. The codes and the retryable set
// are identical in the Go SDK (see ../../README.md, parity table).
//
// @public — см. ../README.md

/** Classification of an SDK failure. Same strings as the Go SDK `Code`. */
export type ErrorCode =
	| "CONFIG"
	| "STATE"
	| "CONNECTION"
	| "TIMEOUT"
	| "CANCELLED"
	| "ACCESS_DENIED"
	| "NOT_FOUND"
	| "VALIDATION"
	| "CONFLICT"
	| "TERMINAL"
	| "NO_LIVE_INSTANCE"
	| "OVERLOADED"
	| "QUEUE_FULL"
	| "INVALID_EVENT_NAME"
	| "HANDLER"
	| "INTERNAL";

// Transient conditions: repeating the same request later may succeed and the
// SDK knows the previous attempt had no effect. TIMEOUT is deliberately absent —
// the outcome is unknown, so a repeat is safe only with an idempotency key.
const RETRYABLE: ReadonlySet<ErrorCode> = new Set<ErrorCode>([
	"CONNECTION",
	"NO_LIVE_INSTANCE",
	"OVERLOADED",
	"QUEUE_FULL",
]);

/** @public — см. ../README.md */
export class ServiceBridgeError extends Error {
	readonly code: ErrorCode;
	readonly retryable: boolean;

	constructor(code: ErrorCode, message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "ServiceBridgeError";
		this.code = code;
		this.retryable = RETRYABLE.has(code);
	}
}

/** An option the SDK refuses to run with. Never retried. */
export class ConfigurationError extends ServiceBridgeError {
	constructor(message: string, options?: ErrorOptions) {
		super("CONFIG", message, options);
		this.name = "ConfigurationError";
	}
}

/** An operation attempted in the wrong lifecycle phase. */
export class StateError extends ServiceBridgeError {
	constructor(message: string, options?: ErrorOptions) {
		super("STATE", message, options);
		this.name = "StateError";
	}
}

/** A declaration or argument the runtime would reject, caught locally. */
export class ValidationError extends ServiceBridgeError {
	constructor(message: string, options?: ErrorOptions) {
		super("VALIDATION", message, options);
		this.name = "ValidationError";
	}
}

/** Refused by the mesh access policy (or the peer is revoked). */
export class AccessDeniedError extends ServiceBridgeError {
	constructor(message: string, options?: ErrorOptions) {
		super("ACCESS_DENIED", message, options);
		this.name = "AccessDeniedError";
	}
}

/** The deadline passed; the remote outcome is unknown. */
export class TimeoutError extends ServiceBridgeError {
	constructor(message: string, options?: ErrorOptions) {
		super("TIMEOUT", message, options);
		this.name = "TimeoutError";
	}
}

/**
 * A failure returned by the callee's handler. Thrown by a handler with a
 * business code, it travels as `error_code` and the caller receives the same
 * class with the same `handlerCode`. Any other thrown error reaches the caller
 * as a HandlerError with handlerCode "INTERNAL".
 */
export class HandlerError extends ServiceBridgeError {
	readonly handlerCode: string;

	constructor(handlerCode: string, message: string, options?: ErrorOptions) {
		super("HANDLER", message, options);
		this.name = "HandlerError";
		this.handlerCode = handlerCode || "INTERNAL";
	}
}

/** Nothing can serve the call: no contract match, no endpoint, all shedding. */
export class NoLiveInstanceError extends ServiceBridgeError {
	constructor(message: string, options?: ErrorOptions) {
		super("NO_LIVE_INSTANCE", message, options);
		this.name = "NoLiveInstanceError";
	}
}

// gRPC status code → SDK code for a remote answer. Same table in Go
// (errors.go). Numeric to keep this module free of grpc-js.
const GRPC_TO_CODE: Record<number, ErrorCode> = {
	1: "CANCELLED",
	2: "INTERNAL",
	3: "VALIDATION",
	4: "TIMEOUT",
	5: "NOT_FOUND",
	6: "CONFLICT",
	7: "ACCESS_DENIED",
	8: "OVERLOADED",
	9: "VALIDATION",
	10: "INTERNAL",
	11: "VALIDATION",
	12: "NOT_FOUND",
	13: "INTERNAL",
	14: "CONNECTION",
	15: "INTERNAL",
	16: "ACCESS_DENIED",
};

/** Maps a numeric gRPC status code onto the SDK error code. */
export function codeForGrpcStatus(status: number): ErrorCode {
	return GRPC_TO_CODE[status] ?? "INTERNAL";
}

/**
 * Wraps any failure as a ServiceBridgeError. An SDK error passes through
 * unchanged; a grpc-js error (numeric `code`) is classified by status; anything
 * else becomes INTERNAL. `scope` prefixes the message.
 */
export function toServiceBridgeError(
	scope: string,
	err: unknown,
): ServiceBridgeError {
	if (err instanceof ServiceBridgeError) return err;
	const message = err instanceof Error ? err.message : String(err);
	const status = (err as { code?: unknown } | null)?.code;
	const code =
		typeof status === "number" ? codeForGrpcStatus(status) : "INTERNAL";
	const wrapped = `${scope}: ${message}`;
	switch (code) {
		case "ACCESS_DENIED":
			return new AccessDeniedError(wrapped, { cause: err });
		case "TIMEOUT":
			return new TimeoutError(wrapped, { cause: err });
		case "VALIDATION":
			return new ValidationError(wrapped, { cause: err });
		default:
			return new ServiceBridgeError(code, wrapped, { cause: err });
	}
}
