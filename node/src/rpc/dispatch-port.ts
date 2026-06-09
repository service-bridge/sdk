import type { CaptureMode } from "../telemetry/payload-capture";

// DispatchPort is the boundary between Registry/Handle (which owns registered
// handlers and their schema pairs) and the CallServer (which receives wire-level
// CallRequests). It hides Handle._entries from the gRPC layer and lets the
// CallServer be tested with a mock implementation.
export interface DispatchPort {
	dispatchUnary(method: string, payload: Uint8Array): Promise<UnaryResult>;
	// dispatchStream yields one StreamItem per chunk. A terminating item with
	// errorCode set ends the stream with an application error; without errorCode
	// the natural end of the iterable terminates the stream successfully.
	dispatchStream(
		method: string,
		payload: Uint8Array,
	): AsyncIterable<StreamItem>;
	// captureMode returns the per-handler payload capture override (if the
	// handler was registered with `{captureMode: ...}`), or undefined when none.
	// The override may only narrow the runtime-pushed effective mode.
	captureMode(method: string): CaptureMode | undefined;
}

export interface UnaryResult {
	payload: Uint8Array;
	errorCode?: string;
	errorMessage?: string;
}

export interface StreamItem {
	payload?: Uint8Array;
	errorCode?: string;
	errorMessage?: string;
}
