// @public — см. ./README.md
import type {
	Registry,
	RpcHandlerFn,
	RpcHandlerOpts,
	RpcStreamHandlerFn,
} from "../registry/registry";
import type { CallOpts, RpcClient } from "./client";
import { RpcAccessDeniedError } from "./errors";

// gRPC PERMISSION_DENIED numeric code (matches @grpc/grpc-js).
const GRPC_PERMISSION_DENIED = 7;

// Sink for call-time policy denials; the owner wires it to emit
// `policy_violation`. Structural type avoids importing from connection.
type PolicyViolationSink = (v: {
	declaration: string;
	value: string;
	denySide: string;
	reason: string;
}) => void;

export class RpcDomain {
	constructor(
		private readonly registry: Registry,
		private readonly getClient: () => RpcClient | null,
		private readonly onPolicyViolation?: PolicyViolationSink,
	) {}

	handle<Req = unknown, Res = unknown>(
		name: string,
		fn: RpcHandlerFn<Req, Res>,
		opts: RpcHandlerOpts,
	): void {
		this.registry._handle.rpc(name, fn, opts);
	}

	handleStream<Req = unknown, Chunk = unknown>(
		name: string,
		fn: RpcStreamHandlerFn<Req, Chunk>,
		opts: RpcHandlerOpts,
	): void {
		this.registry._handle.stream(name, fn, opts);
	}

	// _declareForTests registers an RPC entry without a schema. E2E tests use this
	// to assemble fixtures that exercise registration paths without loading real
	// .proto files. Production code MUST use handle() with an explicit schema.
	//
	// @internal — см. ./README.md
	_declareForTests(name: string, streaming = false): void {
		this.registry._handle._declareForTests(name, streaming);
	}

	async call<Req = unknown, Res = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		opts?: CallOpts,
	): Promise<Res> {
		const client = this.getClient();
		if (!client) {
			throw new Error(
				"ServiceBridge: rpc client not ready — call start() and wait for 'connected' event before calling sb.rpc.call()",
			);
		}
		try {
			return await client.call<Req, Res>(
				serviceName,
				methodName,
				payload,
				opts,
			);
		} catch (err) {
			// Gate #3 denial (ADR-0014): surface as a typed, catchable error and
			// emit a policy_violation so call-time denials are observable on the
			// same channel as registration warnings.
			if ((err as { code?: number }).code === GRPC_PERMISSION_DENIED) {
				const reason =
					(err as { details?: string }).details ||
					(err instanceof Error ? err.message : String(err));
				this.onPolicyViolation?.({
					declaration: "rpc.call",
					value: `${serviceName}/${methodName}`,
					denySide: "self_egress",
					reason,
				});
				throw new RpcAccessDeniedError(serviceName, methodName, reason);
			}
			throw err;
		}
	}
}
