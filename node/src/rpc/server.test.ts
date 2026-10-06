// server.test.ts — CallServer wire form: refusals are gRPC statuses (the
// transient ones carry x-sb-not-dispatched), a handler failure is error_code
// under OK, the handler gets ctx, cancellation reaches it. The TLS bind path
// is covered by e2e; here the two call handlers are driven directly.

import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import * as grpc from "@grpc/grpc-js";
import type { CertificateStore } from "../connection/tls-material";
import { silentLogger } from "../logger";
import type { CallRequest, CallResponse } from "../pb/servicebridge/v1/call";
import type { PolicyEvaluation } from "../pb/servicebridge/v1/registry";
import { currentTraceContext } from "../telemetry/context";
import type { CaptureMode } from "../telemetry/payload-capture";
import { ZERO_OP_ID } from "../telemetry/trace-context";
import { formatXSbTrace } from "../telemetry/wire-trace";
import type {
	DispatchPort,
	RpcHandlerContext,
	StreamItem,
	UnaryResult,
} from "./dispatch-port";
import { CallServer, type CallServerLimits } from "./server";
import { NOT_DISPATCHED_TRAILER } from "./wire";

interface ServerInternals {
	handleUnary(
		call: unknown,
		callback: grpc.sendUnaryData<CallResponse>,
	): Promise<void>;
	handleStream(call: unknown): Promise<void>;
	beginDrain(): void;
	waitIdle(ms: number): Promise<void>;
}

const OPEN_POLICY: PolicyEvaluation = {
	capabilities: ["rpc.handle"],
	egress: [],
	acceptance: [],
	warnings: [],
};

const CALLER = "11111111-1111-1111-1111-111111111111";

function makeServer(
	dispatch: Partial<DispatchPort>,
	opts: {
		policy?: PolicyEvaluation | null;
		limits?: CallServerLimits;
		revoked?: (s: string, i: string) => boolean;
	} = {},
): ServerInternals {
	const port: DispatchPort = {
		dispatchUnary:
			dispatch.dispatchUnary ??
			(() => Promise.reject(new Error("unary not implemented"))),
		dispatchStream:
			dispatch.dispatchStream ??
			(() => {
				throw new Error("stream not implemented");
			}),
		captureMode:
			dispatch.captureMode ?? ((): CaptureMode | undefined => undefined),
	};
	const policy = opts.policy === undefined ? OPEN_POLICY : opts.policy;
	return new CallServer({
		dispatch: port,
		store: {} as CertificateStore,
		policy: () => policy,
		isRevoked: opts.revoked ?? (() => false),
		limits: opts.limits,
		logger: silentLogger,
	}) as unknown as ServerInternals;
}

function makeRequest(over: Partial<CallRequest> = {}): CallRequest {
	return {
		method: "charge",
		payload: new Uint8Array([1, 2, 3]),
		callerService: "",
		requestId: "req-1",
		idempotencyKey: "",
		xSbTrace: "",
		...over,
	} as CallRequest;
}

// A peer presenting an SDK SPIFFE identity (subjectaltname path).
function sdkPeer(serviceId = CALLER, instanceId = "inst-a") {
	return {
		getAuthContext: () => ({
			sslPeerCertificate: {
				subjectaltname: `URI:spiffe://service-bridge/service/${serviceId}/instance/${instanceId}`,
			},
		}),
	};
}

function makeUnaryCall(
	request: CallRequest = makeRequest(),
	extra: object = {},
	deadline: grpc.Deadline = Number.POSITIVE_INFINITY,
): EventEmitter & { request: CallRequest; cancelled: boolean } {
	return Object.assign(new EventEmitter(), {
		request,
		cancelled: false,
		getDeadline: () => deadline,
		...extra,
	});
}

function captureCallback() {
	const captured = {
		error: null as (grpc.ServiceError & { metadata?: grpc.Metadata }) | null,
		response: null as CallResponse | null,
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

class FakeWritable extends EventEmitter {
	readonly chunks: { errorCode?: string; payload?: Uint8Array }[] = [];
	ended = false;
	writableEnded = false;
	destroyed = false;
	cancelled = false;
	acceptWrites = true;
	emittedError: (grpc.ServiceError & { metadata?: grpc.Metadata }) | null =
		null;

	constructor(
		readonly request: CallRequest,
		extra: object = {},
	) {
		super();
		Object.assign(this, extra);
		this.on("error", (err) => {
			this.emittedError = err;
		});
	}

	getDeadline(): grpc.Deadline {
		return Number.POSITIVE_INFINITY;
	}

	write(chunk: { errorCode?: string; payload?: Uint8Array }): boolean {
		this.chunks.push(chunk);
		return this.acceptWrites;
	}

	end(): void {
		this.ended = true;
		this.writableEnded = true;
	}
}

function notDispatched(err: { metadata?: grpc.Metadata } | null): boolean {
	return err?.metadata?.get(NOT_DISPATCHED_TRAILER)[0] === "1";
}

describe("CallServer unary outcomes", () => {
	it("returns the handler payload under OK", async () => {
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
		expect(captured.calls).toBe(1);
	});

	it("passes the handler's business code under OK", async () => {
		const server = makeServer({
			dispatchUnary: async () => ({
				errorCode: "OUT_OF_STOCK",
				errorMessage: "nothing left",
			}),
		});
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;
		expect(captured.error).toBeNull();
		expect(captured.response?.errorCode).toBe("OUT_OF_STOCK");
		expect(captured.response?.errorMessage).toBe("nothing left");
	});

	it.each([
		[grpc.status.NOT_FOUND],
		[grpc.status.FAILED_PRECONDITION],
		[grpc.status.INVALID_ARGUMENT],
	])(
		"a dispatch refusal %d is a gRPC status without the trailer",
		async (status) => {
			const server = makeServer({
				dispatchUnary: async () => ({ status, errorMessage: "refused" }),
			});
			const { callback, captured, settled } = captureCallback();
			await server.handleUnary(makeUnaryCall(), callback);
			await settled;
			expect(captured.error?.code).toBe(status);
			expect(notDispatched(captured.error)).toBe(false);
		},
	);
});

describe("CallServer refusals before the handler", () => {
	it("refuses with UNAVAILABLE + not-dispatched until a policy arrived", async () => {
		let ran = false;
		const server = makeServer(
			{
				dispatchUnary: async () => {
					ran = true;
					return { payload: new Uint8Array() };
				},
			},
			{ policy: null },
		);
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;
		expect(captured.error?.code).toBe(grpc.status.UNAVAILABLE);
		expect(notDispatched(captured.error)).toBe(true);
		expect(ran).toBe(false);
	});

	it("refuses with UNAVAILABLE + not-dispatched while draining", async () => {
		const server = makeServer({
			dispatchUnary: async () => ({ payload: new Uint8Array() }),
		});
		server.beginDrain();
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(), callback);
		await settled;
		expect(captured.error?.code).toBe(grpc.status.UNAVAILABLE);
		expect(captured.error?.details).toContain("draining");
		expect(notDispatched(captured.error)).toBe(true);
	});

	it("refuses a revoked caller with PERMISSION_DENIED at once", async () => {
		const server = makeServer(
			{ dispatchUnary: async () => ({ payload: new Uint8Array() }) },
			{ revoked: (s, i) => s === CALLER && i === "inst-a" },
		);
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(makeRequest(), sdkPeer()), callback);
		await settled;
		expect(captured.error?.code).toBe(grpc.status.PERMISSION_DENIED);
		expect(captured.error?.details).toContain("revoked");
		expect(notDispatched(captured.error)).toBe(false);
	});

	it("refuses a caller the acceptance rules deny", async () => {
		const policy: PolicyEvaluation = {
			...OPEN_POLICY,
			acceptance: [
				{
					action: "rpc.handle",
					peerServiceId: "22222222-2222-2222-2222-222222222222",
					peerServiceName: "other",
					targetName: "*",
				},
			],
		};
		const server = makeServer(
			{ dispatchUnary: async () => ({ payload: new Uint8Array() }) },
			{ policy },
		);
		const { callback, captured, settled } = captureCallback();
		await server.handleUnary(makeUnaryCall(makeRequest(), sdkPeer()), callback);
		await settled;
		expect(captured.error?.code).toBe(grpc.status.PERMISSION_DENIED);
	});

	it("a denied stream ends with a gRPC status, not an error chunk", async () => {
		const server = makeServer({}, { policy: null });
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);
		expect(call.emittedError?.code).toBe(grpc.status.UNAVAILABLE);
		expect(call.chunks).toHaveLength(0);
	});

	it("sheds load with RESOURCE_EXHAUSTED + not-dispatched past the limits", async () => {
		let unblock!: () => void;
		const blocked = new Promise<void>((r) => {
			unblock = r;
		});
		let started = 0;
		const server = makeServer(
			{
				dispatchUnary: async () => {
					started++;
					await blocked;
					return { payload: new Uint8Array() };
				},
			},
			{ limits: { maxConcurrentCalls: 1, maxQueuedCalls: 0 } },
		);
		const first = captureCallback();
		const inflight = server.handleUnary(makeUnaryCall(), first.callback);
		await new Promise((r) => setTimeout(r, 1));
		expect(started).toBe(1);
		const shed = captureCallback();
		await server.handleUnary(makeUnaryCall(), shed.callback);
		await shed.settled;
		expect(shed.captured.error?.code).toBe(grpc.status.RESOURCE_EXHAUSTED);
		expect(notDispatched(shed.captured.error)).toBe(true);
		expect(started).toBe(1);
		unblock();
		await inflight;
		expect(first.captured.error).toBeNull();
	});
});

describe("CallServer handler context", () => {
	it("hands the handler requestId, idempotencyKey, deadline and the verified caller", async () => {
		let ctx: RpcHandlerContext | undefined;
		const server = makeServer({
			dispatchUnary: async (_m, _p, c) => {
				ctx = c;
				return { payload: new Uint8Array() };
			},
		});
		const deadline = new Date(Date.now() + 5_000);
		const { callback, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(
				makeRequest({ requestId: "r-9", idempotencyKey: "k-1" }),
				sdkPeer(),
				deadline,
			),
			callback,
		);
		await settled;
		expect(ctx?.requestId).toBe("r-9");
		expect(ctx?.idempotencyKey).toBe("k-1");
		expect(ctx?.deadline).toBe(deadline.getTime());
		expect(ctx?.caller).toEqual({ serviceId: CALLER, instanceId: "inst-a" });
		expect(ctx?.signal.aborted).toBe(false);
	});

	it("aborts the handler's signal when the caller cancels", async () => {
		let signal: AbortSignal | undefined;
		let release!: () => void;
		const server = makeServer({
			dispatchUnary: async (_m, _p, c) => {
				signal = c.signal;
				await new Promise<void>((r) => {
					release = r;
				});
				return { payload: new Uint8Array() };
			},
		});
		const call = makeUnaryCall();
		const { callback } = captureCallback();
		const running = server.handleUnary(call, callback);
		await new Promise((r) => setTimeout(r, 1));
		call.cancelled = true;
		call.emit("cancelled");
		expect(signal?.aborted).toBe(true);
		release();
		await running;
	});

	it("aborts the handler's signal at the deadline", async () => {
		let signal: AbortSignal | undefined;
		const server = makeServer({
			dispatchUnary: async (_m, _p, c) => {
				signal = c.signal;
				await new Promise((r) => setTimeout(r, 40));
				return { payload: new Uint8Array() };
			},
		});
		const { callback, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(makeRequest(), {}, new Date(Date.now() + 10)),
			callback,
		);
		await settled;
		expect(signal?.aborted).toBe(true);
	});

	it("waitIdle resolves once the in-flight calls finished", async () => {
		let release!: () => void;
		const server = makeServer({
			dispatchUnary: async () => {
				await new Promise<void>((r) => {
					release = r;
				});
				return { payload: new Uint8Array() };
			},
		});
		const { callback } = captureCallback();
		const running = server.handleUnary(makeUnaryCall(), callback);
		await new Promise((r) => setTimeout(r, 1));
		let idle = false;
		const waiting = server.waitIdle(5_000).then(() => {
			idle = true;
		});
		await new Promise((r) => setTimeout(r, 5));
		expect(idle).toBe(false);
		release();
		await running;
		await waiting;
		expect(idle).toBe(true);
	});
});

describe("CallServer streams", () => {
	it("writes each chunk and ends the stream", async () => {
		const server = makeServer({
			dispatchStream: async function* (): AsyncIterable<StreamItem> {
				yield { payload: new Uint8Array([1]) };
				yield { payload: new Uint8Array([2]) };
			},
		});
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);
		expect(call.chunks).toHaveLength(2);
		expect(call.ended).toBe(true);
	});

	it("ends with an error chunk on a handler failure", async () => {
		const server = makeServer({
			dispatchStream: async function* (): AsyncIterable<StreamItem> {
				yield { payload: new Uint8Array([1]) };
				yield { errorCode: "INTERNAL", errorMessage: "boom" };
			},
		});
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);
		expect(call.chunks.at(-1)?.errorCode).toBe("INTERNAL");
		expect(call.ended).toBe(true);
	});

	it("turns a dispatch refusal into a gRPC status", async () => {
		const server = makeServer({
			dispatchStream: async function* (): AsyncIterable<StreamItem> {
				yield { status: grpc.status.NOT_FOUND, errorMessage: "no such" };
			},
		});
		const call = new FakeWritable(makeRequest());
		await server.handleStream(call);
		expect(call.emittedError?.code).toBe(grpc.status.NOT_FOUND);
	});

	it("waits for 'drain' when the consumer is slow", async () => {
		let produced = 0;
		const server = makeServer({
			dispatchStream: async function* (): AsyncIterable<StreamItem> {
				for (let i = 0; i < 3; i++) {
					produced++;
					yield { payload: new Uint8Array([i]) };
				}
			},
		});
		const call = new FakeWritable(makeRequest());
		call.acceptWrites = false;
		const running = server.handleStream(call);
		await new Promise((r) => setTimeout(r, 5));
		expect(produced).toBe(1);
		call.acceptWrites = true;
		call.emit("drain");
		await running;
		expect(produced).toBe(3);
	});

	it("stops the generator when the caller cancels", async () => {
		let returned = false;
		const server = makeServer({
			dispatchStream: async function* (): AsyncIterable<StreamItem> {
				try {
					while (true) {
						yield { payload: new Uint8Array([1]) };
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
		call.cancelled = true;
		call.emit("cancelled");
		await running;
		expect(returned).toBe(true);
		expect(call.ended).toBe(false);
	});
});

describe("CallServer trace context", () => {
	const traceId = "019ffc00-0000-7000-8000-000000000001";
	const parentOpId = "019ffc00-0000-7000-8000-000000000002";

	it("runs the handler inside the inbound trace context", async () => {
		let seen: { traceId: string; parentOpId: string } | undefined;
		const server = makeServer({
			dispatchUnary: async () => {
				seen = currentTraceContext();
				return { payload: new Uint8Array() };
			},
		});
		const { callback, settled } = captureCallback();
		await server.handleUnary(
			makeUnaryCall(
				makeRequest({ xSbTrace: formatXSbTrace(traceId, parentOpId) }),
			),
			callback,
		);
		await settled;
		expect(seen).toEqual({ traceId, parentOpId });
	});

	it("mints a fresh root on a missing or malformed X-SB-Trace", async () => {
		for (const xSbTrace of ["", "garbage"]) {
			let seen: { traceId: string; parentOpId: string } | undefined;
			const server = makeServer({
				dispatchUnary: async () => {
					seen = currentTraceContext();
					return { payload: new Uint8Array() };
				},
			});
			const { callback, settled } = captureCallback();
			await server.handleUnary(
				makeUnaryCall(makeRequest({ xSbTrace })),
				callback,
			);
			await settled;
			expect(seen?.parentOpId).toBe(ZERO_OP_ID);
			expect(seen?.traceId).not.toBe(traceId);
		}
	});
});
