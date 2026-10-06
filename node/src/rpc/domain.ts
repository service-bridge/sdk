// @public — см. ./README.md
import { AccessDeniedError, StateError } from "../errors";
import type {
	Registry,
	RpcHandlerFn,
	RpcHandlerOpts,
	RpcStreamHandlerFn,
} from "../registry/registry";
import type { CallOpts, RpcClient } from "./client";

// Sink for call-time policy denials; the owner wires it to `policy_violation`.
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

	/** Registers a unary handler: `(req, ctx) => res`. Throw HandlerError for a business code. */
	handle<Req = unknown, Res = unknown>(
		name: string,
		fn: RpcHandlerFn<Req, Res>,
		opts: RpcHandlerOpts,
	): void {
		this.registry._handle.rpc(name, fn, opts);
	}

	/** Registers a server-streaming handler: `async function* (req, ctx)`. */
	handleStream<Req = unknown, Chunk = unknown>(
		name: string,
		fn: RpcStreamHandlerFn<Req, Chunk>,
		opts: RpcHandlerOpts,
	): void {
		this.registry._handle.stream(name, fn, opts);
	}

	/**
	 * Calls a method of another service. Every failure is a
	 * ServiceBridgeError: HandlerError (the callee's answer), AccessDeniedError
	 * (policy; also emitted as `policy_violation`), NoLiveInstanceError,
	 * TimeoutError, or CONNECTION/OVERLOADED/CANCELLED/INTERNAL codes.
	 */
	async call<Req = unknown, Res = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		opts?: CallOpts,
	): Promise<Res> {
		const client = this.getClient();
		if (!client)
			throw new StateError("rpc: call before start() — call sb.start() first");
		try {
			return await client.call<Req, Res>(
				serviceName,
				methodName,
				payload,
				opts,
			);
		} catch (err) {
			if (err instanceof AccessDeniedError)
				this.onPolicyViolation?.({
					declaration: "rpc.call",
					value: `${serviceName}/${methodName}`,
					denySide: "self_egress",
					reason: err.message,
				});
			throw err;
		}
	}
}
