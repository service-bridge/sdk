import { status as GrpcStatus } from "@grpc/grpc-js";
import { ServiceBridgeError } from "../errors";

// Codes no reconnect can fix: the identity or the key is rejected, the service
// is gone, the declarations are invalid, or the runtime does not speak this
// SDK's protocol. Same set in the Go SDK (internal/connection).
const TERMINAL = new Set<number>([
	GrpcStatus.UNAUTHENTICATED,
	GrpcStatus.PERMISSION_DENIED,
	GrpcStatus.NOT_FOUND,
	GrpcStatus.INVALID_ARGUMENT,
	GrpcStatus.FAILED_PRECONDITION,
]);

// isTerminal reports whether a control-plane failure must stop the bridge
// instead of feeding the reconnect ladder.
// @internal — см. ./README.md
export function isTerminal(grpcCode: number): boolean {
	return TERMINAL.has(grpcCode);
}

/**
 * A control-plane failure: provisioning, the Control session, the registry
 * stream, certificate refresh. `grpcCode` is the status the runtime answered
 * with (-1 when the failure carried none).
 *
 * @public см. ./README.md
 */
export class ConnectionError extends ServiceBridgeError {
	readonly grpcCode: number;

	constructor(scope: string, cause: unknown) {
		const grpcCode =
			cause instanceof ConnectionError
				? cause.grpcCode
				: cause != null &&
						typeof cause === "object" &&
						"code" in cause &&
						typeof (cause as { code: unknown }).code === "number"
					? (cause as { code: number }).code
					: -1;
		const message = cause instanceof Error ? cause.message : String(cause);
		super("CONNECTION", `${scope}: ${message}`, { cause });
		this.name = "ConnectionError";
		this.grpcCode = grpcCode;
	}
}
