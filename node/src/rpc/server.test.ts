import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import * as grpc from "@grpc/grpc-js";
import type { CallRequest, CallResponse } from "../pb/servicebridge/v1/call";
import type { PolicyEvaluation } from "../pb/servicebridge/v1/registry";
import { currentTraceContext } from "../telemetry/context";
import type { CaptureMode } from "../telemetry/payload-capture";
import { type TraceContext, ZERO_OP_ID } from "../telemetry/trace-context";
import { formatXSbTrace } from "../telemetry/wire-trace";
import type { DispatchPort, StreamItem, UnaryResult } from "./dispatch-port";
import { CallServer, type CallServerLimits } from "./server";

// CallServer.start() needs a bound TLS socket, so the wire path is covered by
// e2e. These tests drive the two call handlers directly with the grpc-js call
// shapes, which is where the status/error-code contract lives.
interface ServerInternals {
	handleUnary(
		call: unknown,
		callback: grpc.sendUnaryData<CallResponse>,
	): Promise<void>;
	handleStream(call: unknown): Promise<void>;
}

const emptyCreds = {
	caChainDer: Buffer.alloc(0),
	leafCertDer: Buffer.alloc(0),
	privateKeyDer: Buffer.alloc(0),
};

const notImplementedStream = (): AsyncIterable<StreamItem> => {
	throw new Error("stream not implemented");
};

function makeServer(
	dispatch: Partial<DispatchPort>,
	getPolicy: () => PolicyEvaluation | null = () => null,
	limits: CallServerLimits = {},
): ServerInternals {
	const port: DispatchPort = {
		dispatchUnary:
			dispatch.dispatchUnary ??
			(() => Promise.reject(new Error("unary not implemented"))),
		dispatchStream: dispatch.dispatchStream ?? notImplementedStream,
		captureMode:
			dispatch.captureMode ?? ((): CaptureMode | undefined => undefined),
	};
	const server = new CallServer(port, emptyCreds, getPolicy, limits);
	return server as unknown as ServerInternals;
}

function makeRequest(over: Partial<CallRequest> = {}): CallRequest {
	return {
		method: "charge",
		payload: new Uint8Array([1, 2, 3]),
		xSbTrace: "",
		...over,
	} as CallRequest;
}

// makeUnaryCall builds a call with no TLS peer. Acceptance only inspects the
// peer when policy carries rpc.handle rules, so this is the shape for every
// test that is not about acceptance.
function makeUnaryCall(request: CallRequest = makeRequest()): unknown {
	return { request };
}

interface CapturedCallback {
	error: grpc.ServiceError | null;
	response: CallResponse | null;
	calls: number;
}

function captureCallback(): {
	callback: grpc.sendUnaryData<CallResponse>;
	captured: CapturedCallback;
	settled: Promise<void>;
} {
	const captured: CapturedCallback = {
		error: null,
		response: null,
		calls: 0,
	};
	let resolve!: () => void;
	const settled = new Promise<void>((r) => {
		resolve = r;
	});
	const callback = ((err: grpc.ServiceError | null, res?: CallResponse) => {
		captured.calls++;
		captured.error = err;
		captured.response = res ?? null;
		resolve();
	}) as unknown as grpc.sendUnaryData<CallResponse>;
	return { callback, captured, settled };
}

// FakeWritable stands in for grpc.ServerWritableStream. It records chunks and
// lets a test force write() to return false so the drain path is exercised.
class FakeWritable extends EventEmitter {
	readonly chunks: unknown[] = [];
	ended = false;
	writableEnded = false;
	destroyed = false;
	acceptWrites = true;
	emittedError: unknown = null;

	constructor(readonly request: CallRequest) {
		super();
		// grpc-js attaches its own 'error' listener; mirror that so emit() here
		// never throws an unhandled 'error'.
		this.on("error", (err) => {
			this.emittedError = err;
		});
	}

	write(chunk: unknown): boolean {
		this.chunks.push(chunk);
		return this.acceptWrites;
	}

	end(): void {
		this.ended = true;
		this.writableEnded = true;
	}
}

// ── unary: dispatch result mapping ───────────────────────────────────────────

describe("CallServer.handleUnary", () => {
	it("returns the handler payload with gRPC status OK", async () => {
		const server = makeServer({
			dispatchUnary: async (): Promise<UnaryResult> => ({
				payload: new Uint8Array([7, 8]),
			}),
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;

		expect(captured.error).toBeNull();
		expect(captured.response?.errorCode).toBe("");
		expect(Buffer.from(captured.response?.payload ?? [])).toEqual(
			Buffer.from([7, 8]),
		);
	});

	it("maps a thrown handler error to errorCode INTERNAL with gRPC status OK", async () => {
		// This split is what the caller's retry decision keys off: a handler
		// failure must not look like a transport failure.
		const server = makeServer({
			dispatchUnary: async () => {
				throw new Error("handler blew up");
			},
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;

		expect(captured.error).toBeNull();
		expect(captured.response?.errorCode).toBe("INTERNAL");
		expect(captured.response?.errorMessage).toBe("handler blew up");
	});

	it("passes a dispatch-reported NOT_FOUND through as an application error", async () => {
		const server = makeServer({
			dispatchUnary: async () => ({
				payload: new Uint8Array(),
				errorCode: "NOT_FOUND",
				errorMessage: "rpc: no handler for method missing",
			}),
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(makeRequest({ method: "missing" })),
			callback,
		);
		await settled;

		expect(captured.error).toBeNull();
		expect(captured.response?.errorCode).toBe("NOT_FOUND");
	});

	it("passes FAILED_PRECONDITION through when a streaming method is called unary", async () => {
		const server = makeServer({
			dispatchUnary: async () => ({
				payload: new Uint8Array(),
				errorCode: "FAILED_PRECONDITION",
				errorMessage: "rpc: method feed is streaming",
			}),
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(makeRequest({ method: "feed" })),
			callback,
		);
		await settled;

		expect(captured.response?.errorCode).toBe("FAILED_PRECONDITION");
		expect(captured.response?.errorMessage).toContain("streaming");
	});

	it("passes INVALID_ARGUMENT through on a decode failure", async () => {
		const server = makeServer({
			dispatchUnary: async () => ({
				payload: new Uint8Array(),
				errorCode: "INVALID_ARGUMENT",
				errorMessage: "rpc: decode request: bad wire type",
			}),
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;

		expect(captured.error).toBeNull();
		expect(captured.response?.errorCode).toBe("INVALID_ARGUMENT");
	});

	it("calls the callback exactly once", async () => {
		const server = makeServer({
			dispatchUnary: async () => ({ payload: new Uint8Array() }),
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;
		expect(captured.calls).toBe(1);
	});
});

// ── unary: acceptance ────────────────────────────────────────────────────────

describe("CallServer acceptance", () => {
	const restrictivePolicy: PolicyEvaluation = {
		capabilities: [],
		egress: [],
		acceptance: [
			{
				action: "rpc.handle",
				peerServiceId: "svc-allowed",
				peerServiceName: "",
				targetName: "charge",
			},
		],
		warnings: [],
	};

	it("rejects with PERMISSION_DENIED when the peer cannot be identified", async () => {
		let dispatched = false;
		const server = makeServer(
			{
				dispatchUnary: async () => {
					dispatched = true;
					return { payload: new Uint8Array() };
				},
			},
			() => restrictivePolicy,
		);
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;

		expect(captured.error?.code).toBe(grpc.status.PERMISSION_DENIED);
		expect(dispatched).toBe(false);
	});

	it("ends a denied stream with a PERMISSION_DENIED chunk", async () => {
		const server = makeServer({}, () => restrictivePolicy);
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);

		expect(call.chunks).toHaveLength(1);
		expect((call.chunks[0] as { errorCode: string }).errorCode).toBe(
			"PERMISSION_DENIED",
		);
		expect(call.ended).toBe(true);
	});

	it("admits the call when policy carries no rpc.handle rules", async () => {
		const server = makeServer(
			{ dispatchUnary: async () => ({ payload: new Uint8Array([1]) }) },
			() => ({ capabilities: [], egress: [], acceptance: [], warnings: [] }),
		);
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;
		expect(captured.error).toBeNull();
	});
});

// ── overload ─────────────────────────────────────────────────────────────────

describe("CallServer overload", () => {
	it("returns RESOURCE_EXHAUSTED instead of queueing past the limit", async () => {
		let started = 0;
		let unblock!: () => void;
		const blocked = new Promise<void>((r) => {
			unblock = r;
		});
		const server = makeServer(
			{
				dispatchUnary: async () => {
					started++;
					await blocked;
					return { payload: new Uint8Array() };
				},
			},
			() => null,
			{ maxConcurrentCalls: 1, maxQueuedCalls: 0 },
		);

		const first = captureCallback();
		const inflight = server.handleUnary(makeUnaryCall(), first.callback);
		// Let the first call reach the handler and occupy the only slot.
		await Promise.resolve();
		await Promise.resolve();
		expect(started).toBe(1);

		const shed = captureCallback();
		await server.handleUnary(makeUnaryCall(), shed.callback);
		await shed.settled;

		expect(shed.captured.error?.code).toBe(grpc.status.RESOURCE_EXHAUSTED);
		expect(shed.captured.error?.message).toContain("overloaded");
		// The shed call never reached the handler — load was dropped, not buffered.
		expect(started).toBe(1);

		unblock();
		await inflight;
		await first.settled;
		expect(first.captured.error).toBeNull();
	});

	it("queues up to the queue depth before shedding", async () => {
		let started = 0;
		let unblock!: () => void;
		const blocked = new Promise<void>((r) => {
			unblock = r;
		});
		const server = makeServer(
			{
				dispatchUnary: async () => {
					started++;
					await blocked;
					return { payload: new Uint8Array() };
				},
			},
			() => null,
			{ maxConcurrentCalls: 1, maxQueuedCalls: 1 },
		);

		const a = captureCallback();
		const b = captureCallback();
		const c = captureCallback();
		const running = server.handleUnary(makeUnaryCall(), a.callback);
		await Promise.resolve();
		await Promise.resolve();
		const queued = server.handleUnary(makeUnaryCall(), b.callback);
		await Promise.resolve();

		await server.handleUnary(makeUnaryCall(), c.callback);
		await c.settled;
		expect(c.captured.error?.code).toBe(grpc.status.RESOURCE_EXHAUSTED);
		expect(started).toBe(1);

		unblock();
		await running;
		await queued;
		await b.settled;
		// The queued call ran once a slot freed up.
		expect(b.captured.error).toBeNull();
		expect(started).toBe(2);
	});

	it("frees the slot after a handler throws", async () => {
		const server = makeServer(
			{
				dispatchUnary: async () => {
					throw new Error("boom");
				},
			},
			() => null,
			{ maxConcurrentCalls: 1, maxQueuedCalls: 0 },
		);

		for (let i = 0; i < 3; i++) {
			const { callback, captured, settled } = captureCallback();
			await server.handleUnary(makeUnaryCall(), callback);
			await settled;
			expect(captured.error).toBeNull();
			expect(captured.response?.errorCode).toBe("INTERNAL");
		}
	});

	it("fails an overloaded stream with a RESOURCE_EXHAUSTED status", async () => {
		let unblock!: () => void;
		const blocked = new Promise<void>((r) => {
			unblock = r;
		});
		const server = makeServer(
			{
				// eslint-disable-next-line require-yield
				dispatchStream: async function* () {
					await blocked;
					yield { payload: new Uint8Array([1]) } as StreamItem;
				},
			},
			() => null,
			{ maxConcurrentCalls: 1, maxQueuedCalls: 0 },
		);

		const first = new FakeWritable(makeRequest());
		const inflight = server.handleStream(first);
		await Promise.resolve();
		await Promise.resolve();

		const shed = new FakeWritable(makeRequest());
		await server.handleStream(shed);

		expect(shed.emittedError).toMatchObject({
			code: grpc.status.RESOURCE_EXHAUSTED,
		});
		expect(shed.chunks).toHaveLength(0);

		unblock();
		await inflight;
	});
});

// ── streaming ────────────────────────────────────────────────────────────────

describe("CallServer.handleStream", () => {
	it("writes each chunk and ends the stream", async () => {
		const server = makeServer({
			dispatchStream: async function* () {
				yield { payload: new Uint8Array([1]) };
				yield { payload: new Uint8Array([2]) };
			},
		});
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);

		expect(call.chunks).toHaveLength(2);
		expect(call.ended).toBe(true);
	});

	it("terminates on an application error chunk", async () => {
		const server = makeServer({
			dispatchStream: async function* () {
				yield { payload: new Uint8Array([1]) };
				yield { errorCode: "INTERNAL", errorMessage: "mid-stream failure" };
				yield { payload: new Uint8Array([3]) };
			},
		});
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);

		expect(call.chunks).toHaveLength(2);
		expect((call.chunks[1] as { errorCode: string }).errorCode).toBe(
			"INTERNAL",
		);
		expect(call.ended).toBe(true);
	});

	it("maps a thrown generator error to an INTERNAL chunk", async () => {
		const server = makeServer({
			dispatchStream: async function* () {
				yield { payload: new Uint8Array([1]) };
				throw new Error("generator blew up");
			},
		});
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);

		const last = call.chunks[call.chunks.length - 1] as {
			errorCode: string;
			errorMessage: string;
		};
		expect(last.errorCode).toBe("INTERNAL");
		expect(last.errorMessage).toBe("generator blew up");
		expect(call.ended).toBe(true);
	});

	it("waits for 'drain' when the consumer is slow", async () => {
		let produced = 0;
		const server = makeServer({
			dispatchStream: async function* () {
				for (let i = 0; i < 3; i++) {
					produced++;
					yield { payload: new Uint8Array([i]) };
				}
			},
		});
		const call = new FakeWritable(makeRequest());
		call.acceptWrites = false; // every write() signals "buffer full"

		const running = server.handleStream(call);
		await Promise.resolve();
		await Promise.resolve();

		// Production is parked on the first drain instead of racing ahead.
		expect(produced).toBe(1);
		expect(call.ended).toBe(false);

		call.acceptWrites = true;
		call.emit("drain");
		await running;

		expect(produced).toBe(3);
		expect(call.chunks).toHaveLength(3);
		expect(call.ended).toBe(true);
	});

	it("stops the generator when the caller cancels", async () => {
		let produced = 0;
		let returned = false;
		const server = makeServer({
			dispatchStream: async function* () {
				try {
					while (true) {
						produced++;
						yield { payload: new Uint8Array([produced % 256]) };
						// A real handler yields to the event loop between chunks.
						await new Promise((r) => setTimeout(r, 1));
					}
				} finally {
					returned = true;
				}
			},
		});
		const call = new FakeWritable(makeRequest());

		const running = server.handleStream(call);
		await new Promise((r) => setTimeout(r, 5));
		expect(produced).toBeGreaterThan(0);

		call.emit("cancelled");
		await running;

		const producedAtCancel = produced;
		// The generator's finally ran — it is not still producing into a dead call.
		expect(returned).toBe(true);
		await new Promise((r) => setTimeout(r, 5));
		expect(produced).toBe(producedAtCancel);
		// A cancelled call is already closed; the server does not end() it again.
		expect(call.ended).toBe(false);
	});

	it("releases the concurrency slot after cancellation", async () => {
		const server = makeServer(
			{
				dispatchStream: async function* () {
					while (true) {
						yield { payload: new Uint8Array([1]) };
						await new Promise((r) => setTimeout(r, 1));
					}
				},
			},
			() => null,
			{ maxConcurrentCalls: 1, maxQueuedCalls: 0 },
		);

		const first = new FakeWritable(makeRequest());
		const running = server.handleStream(first);
		await new Promise((r) => setTimeout(r, 5));
		first.emit("cancelled");
		await running;

		// Slot returned — the next stream is admitted rather than shed.
		const second = new FakeWritable(makeRequest());
		const next = server.handleStream(second);
		await new Promise((r) => setTimeout(r, 5));
		second.emit("cancelled");
		await next;
		expect(second.emittedError).toBeNull();
	});
});

// ── trace context ────────────────────────────────────────────────────────────

const UUID_RE =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

describe("CallServer trace context", () => {
	const traceId = "019ffc00-0000-7000-8000-000000000001";
	const parentOpId = "019ffc00-0000-7000-8000-000000000002";
	const inboundHeader = formatXSbTrace(traceId, parentOpId);

	it("runs the handler inside the inbound trace context", async () => {
		let seen: TraceContext | undefined;
		const server = makeServer({
			dispatchUnary: async () => {
				seen = currentTraceContext();
				return { payload: new Uint8Array() };
			},
		});
		const { callback, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(makeRequest({ xSbTrace: inboundHeader })),
			callback,
		);
		await settled;

		expect(seen?.traceId).toBe(traceId);
		expect(seen?.parentOpId).toBe(parentOpId);
	});

	it("mints a fresh root context when X-SB-Trace is absent", async () => {
		let seen: TraceContext | undefined;
		const server = makeServer({
			dispatchUnary: async () => {
				seen = currentTraceContext();
				return { payload: new Uint8Array() };
			},
		});
		const { callback, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;

		expect(seen).toBeDefined();
		expect(seen?.traceId).toMatch(UUID_RE);
		expect(seen?.parentOpId).toBe(ZERO_OP_ID);
	});

	it("mints a fresh root context on a malformed X-SB-Trace instead of throwing", async () => {
		let seen: TraceContext | undefined;
		const server = makeServer({
			dispatchUnary: async () => {
				seen = currentTraceContext();
				return { payload: new Uint8Array() };
			},
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(makeRequest({ xSbTrace: "not-a-valid-trace-header" })),
			callback,
		);
		await settled;

		// Strict parse, never an exception: a bad header yields a fresh root trace.
		expect(captured.error).toBeNull();
		expect(seen?.traceId).toMatch(UUID_RE);
		expect(seen?.traceId).not.toBe(traceId);
	});

	it("mints a fresh root context when X-SB-Trace is the right length but not UUIDs", async () => {
		let seen: TraceContext | undefined;
		const server = makeServer({
			dispatchUnary: async () => {
				seen = currentTraceContext();
				return { payload: new Uint8Array() };
			},
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(
				makeRequest({ xSbTrace: `${"z".repeat(36)}-${"z".repeat(36)}` }),
			),
			callback,
		);
		await settled;

		expect(captured.error).toBeNull();
		expect(seen?.traceId).toMatch(UUID_RE);
	});

	it("runs a streaming handler inside the inbound trace context", async () => {
		let seen: TraceContext | undefined;
		const server = makeServer({
			dispatchStream: async function* () {
				seen = currentTraceContext();
				yield { payload: new Uint8Array([1]) };
			},
		});
		const call = new FakeWritable(makeRequest({ xSbTrace: inboundHeader }));
		await server.handleStream(call);

		expect(seen?.traceId).toBe(traceId);
		expect(seen?.parentOpId).toBe(parentOpId);
	});
});

// ── telemetry ────────────────────────────────────────────────────────────────

describe("CallServer telemetry", () => {
	it("never reaches the op emitter — the callee emits no op (ADR-0001)", async () => {
		// One logical RPC call is one operations row, owned by the caller. The op
		// emitter lives in telemetry/ops; the callee must not touch it, and the
		// only way to emit is to import it.
		const source = await Bun.file(
			new URL("./server.ts", import.meta.url),
		).text();
		expect(source).not.toContain("telemetry/ops");
	});

	it("dispatches each call exactly once", async () => {
		let unaryCalls = 0;
		let streamCalls = 0;
		const server = makeServer({
			dispatchUnary: async () => {
				unaryCalls++;
				return { payload: new Uint8Array() };
			},
			dispatchStream: async function* () {
				streamCalls++;
				yield { payload: new Uint8Array([1]) };
			},
		});

		const { callback, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;
		await server.handleStream(new FakeWritable(makeRequest()));

		expect(unaryCalls).toBe(1);
		expect(streamCalls).toBe(1);
	});
});
