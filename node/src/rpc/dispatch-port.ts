import type { CaptureMode } from "../telemetry/payload-capture";

/**
 * What a handler knows about the call it serves. `signal` aborts when the
 * caller cancels or the deadline passes; `caller` is the peer's verified
 * identity (instanceId is empty when the call came through the runtime
 * proxy), null when it could not be established.
 *
 * @public — см. ./README.md
 */
export interface RpcHandlerContext {
	signal: AbortSignal;
	/** Absolute deadline, unix ms; null when the caller set none. */
	deadline: number | null;
	requestId: string;
	idempotencyKey: string;
	caller: { serviceId: string; instanceId: string } | null;
}

// DispatchPort is the boundary between the handler registry (which owns the
// handlers and their schemas) and the CallServer (which owns the wire). It
// lets the server be tested with a stub.
export interface DispatchPort {
	dispatchUnary(
		method: string,
		payload: Uint8Array,
		ctx: RpcHandlerContext,
	): Promise<UnaryResult>;
	// dispatchStream yields one item per chunk. An item with errorCode ends the
	// stream with a handler error; an item with status refuses the call before
	// the handler ran.
	dispatchStream(
		method: string,
		payload: Uint8Array,
		ctx: RpcHandlerContext,
	): AsyncIterable<StreamItem>;
	// captureMode returns the per-handler payload capture override, which may
	// only narrow the runtime-pushed effective mode.
	captureMode(method: string): CaptureMode | undefined;
}

/**
 * One dispatch outcome. `status` (a gRPC code) is a refusal before the
 * handler ran — unknown method, wrong kind, undecodable request. `errorCode`
 * is the handler's own answer under a gRPC OK.
 */
export interface UnaryResult {
	payload?: Uint8Array;
	status?: number;
	errorCode?: string;
	errorMessage?: string;
}

export interface StreamItem {
	payload?: Uint8Array;
	status?: number;
	errorCode?: string;
	errorMessage?: string;
}
