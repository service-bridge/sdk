import { ServiceBridgeError } from "../errors";

// RPC-domain error classes surfaced to callers.
//
// @public — см. ./README.md

// RpcAccessDeniedError — gate #3 bilateral check failed on rpc.call
// (caller egress rpc.call + callee acceptance rpc.handle, ADR-0004). Maps from
// the gRPC PERMISSION_DENIED the runtime returns at call time.
export class RpcAccessDeniedError extends ServiceBridgeError {
	constructor(
		public readonly serviceName: string,
		public readonly methodName: string,
		public readonly reason: string,
	) {
		super(`rpc.call ${serviceName}/${methodName}: access denied — ${reason}`);
		this.name = "RpcAccessDeniedError";
	}
}
