import { status as GrpcStatus } from "@grpc/grpc-js";
import { ServiceBridgeError } from "../errors";

const NON_RETRYABLE = new Set([
	GrpcStatus.UNAUTHENTICATED,
	GrpcStatus.PERMISSION_DENIED,
	GrpcStatus.NOT_FOUND,
	GrpcStatus.INVALID_ARGUMENT,
]);

// isRetryable returns true if the gRPC status code is transient (worth a retry).
// Used by the connection lifecycle to decide between scheduling a reconnect
// and emitting a terminal `disconnected` event.
//
// @internal — см. ./README.md
export function isRetryable(grpcCode: number): boolean {
	return grpcCode === -1 || !NON_RETRYABLE.has(grpcCode);
}

/** @public см. ./README.md */
export class ConnectionError extends ServiceBridgeError {
	readonly code: number;

	constructor(scope: string, cause: unknown) {
		const grpcCode =
			cause != null &&
			typeof cause === "object" &&
			"code" in cause &&
			typeof (cause as { code: unknown }).code === "number"
				? (cause as { code: number }).code
				: -1;

		const message = cause instanceof Error ? cause.message : String(cause);
		super(`${scope}: ${message}`, { cause });

		this.name = "ConnectionError";
		this.code = grpcCode;
	}
}
