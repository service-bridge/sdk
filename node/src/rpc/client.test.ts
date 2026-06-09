// client.test.ts — unit tests for RpcClient caller-side RPC.CALL emission (ADR-0036).
// These tests verify that RpcClient.call() and RpcClient.stream() emit the
// correct telemetry frames to the ring on each invocation.

import { beforeEach, describe, expect, it } from "bun:test";
import type { ServiceBridge, TelemetryAPI } from "../connection/service-bridge";
import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { MethodType } from "../pb/servicebridge/v1/registry";
import type {
	OpReport,
	PayloadAttachment,
} from "../pb/servicebridge/v1/telemetry";
import { Status } from "../pb/servicebridge/v1/telemetry";
import { runWithTrace } from "../telemetry/context";
import { Channel, OpHandle, RpcCall } from "../telemetry/ops";
import { TelemetryRing } from "../telemetry/ring";
import { CircuitBreakerRegistry } from "./circuit-breaker";
import { RpcClient } from "./client";
import type { DirectTransport } from "./direct-transport";
import { InstanceCache } from "./instance-cache";
import type { Candidate, LoadBalancer } from "./lb";
import type { ProxyTransport } from "./proxy-transport";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function makeDesc(
	serviceName: string,
	methodName: string,
	streaming = false,
): MethodDescriptor {
	return {
		instanceId: "inst-1",
		serviceId: "svc-id",
		serviceName,
		type: MethodType.METHOD_TYPE_RPC,
		name: methodName,
		published: false,
		contractHash: "hash-1",
		inputSchema: Buffer.alloc(0),
		outputSchema: Buffer.alloc(0),
		streaming,
	};
}

function makeInst(
	id = "inst-1",
	endpoint = "localhost:9000",
): ServiceInstanceInfo {
	return {
		instanceId: id,
		serviceId: "target-svc-id",
		serviceName: "target-svc",
		callEndpoint: endpoint,
		status: "connected",
		httpEndpoint: "",
		isUnhealthySinceUnixMs: 0,
	};
}

function makeCandidate(
	serviceName = "target-svc",
	methodName = "Charge",
	streaming = false,
): Candidate {
	return {
		descriptor: makeDesc(serviceName, methodName, streaming),
		instance: makeInst(),
		isUnhealthyAt: null,
	};
}

// makeInstanceCache creates an InstanceCache-shaped stub for testing.
function makeInstanceCache(
	serviceName: string,
	methodName: string,
	streaming = false,
): InstanceCache {
	const cache = new InstanceCache();
	// Override pickAll / descriptorFor without real WatchStream
	(
		cache as unknown as { pickAll: (s: string, m: string) => Candidate[] }
	).pickAll = (s: string, m: string) => {
		if (s === serviceName && m === methodName)
			return [makeCandidate(serviceName, methodName, streaming)];
		return [];
	};
	(
		cache as unknown as {
			descriptorFor: (s: string, m: string) => MethodDescriptor | null;
		}
	).descriptorFor = (s: string, m: string) => {
		if (s === serviceName && m === methodName)
			return makeDesc(serviceName, methodName, streaming);
		return null;
	};
	return cache;
}

// makeSchemaPair creates a minimal encode/decode pair that passes bytes through.
function makeSchemaPair() {
	// Use unknown cast to avoid fully implementing Serializer in test stubs.
	return {
		pair: {
			input: {
				encode: (_obj: object) => Buffer.from("{}"),
			},
			output: {
				decode: (b: Uint8Array) => JSON.parse(Buffer.from(b).toString()),
			},
			contractHash: "hash-1",
		},
		contractHash: "hash-1",
	} as unknown as import("./client").CallerSchema;
}

// makeLB returns a LoadBalancer-shaped stub that picks the single candidate.
function makeLB(candidate: Candidate): LoadBalancer {
	return {
		pick: (_candidates: Candidate[]) => candidate,
		acquire: (_instanceId: string) => () => {},
		recordHealthHint: (_instanceId: string, _at: Date | null) => {},
	} as unknown as LoadBalancer;
}

// makeRing returns a fresh TelemetryRing for inspection.
function makeRing() {
	return new TelemetryRing();
}

// makeStubSb returns a minimal ServiceBridge stub with telemetry ring.
// captureMode models the runtime-pushed effective capture mode (default none).
function makeStubSb(
	ring: TelemetryRing,
	captureMode: "all" | "errors" | "none" = "none",
): ServiceBridge {
	const telemetry: TelemetryAPI = {
		startOp(params) {
			return OpHandle.start(ring, {
				...params,
				effectiveCaptureMode: captureMode,
			});
		},
		captureModeForChannel: () => captureMode,
		log: {
			debug: () => {},
			info: () => {},
			warn: () => {},
			error: () => {},
		},
		counter: () => ({ inc: () => {} }) as ReturnType<TelemetryAPI["counter"]>,
		gauge: () => ({}) as ReturnType<TelemetryAPI["gauge"]>,
		histogram: () => ({}) as ReturnType<TelemetryAPI["histogram"]>,
	};
	return {
		telemetry,
		instanceIdString: () => "caller-inst-id",
		identity: () => ({
			sessionId: "test-session",
			serviceId: "caller-svc-id",
			serviceName: "caller-svc",
			instanceId: "caller-inst-id",
		}),
	} as unknown as ServiceBridge;
}

// makeProxyTransport returns a proxy stub that resolves with empty payload.
function makeProxyTransport(
	onCall?: (targetServiceId: string, method: string) => void,
): ProxyTransport {
	return {
		callUnary: async (targetServiceId: string, method: string) => {
			onCall?.(targetServiceId, method);
			return Buffer.from("{}");
		},
		callStream: async function* () {
			// empty stream
		},
		close: () => {},
	} as unknown as ProxyTransport;
}

// makeFailingProxyTransport returns a proxy that fails the first N attempts.
function makeFailingProxyTransport(failCount: number): ProxyTransport {
	let calls = 0;
	return {
		callUnary: async () => {
			calls++;
			if (calls <= failCount) {
				const err = Object.assign(new Error("UNAVAILABLE"), { code: 14 });
				throw err;
			}
			return Buffer.from("{}");
		},
		callStream: async function* () {
			// empty stream
		},
		close: () => {},
	} as unknown as ProxyTransport;
}

// drainOpReports peeks the ring and returns all typed OpReport frames.
function drainOpReports(ring: TelemetryRing): OpReport[] {
	return ring
		.peek(200)
		.filter((item) => item.kind === "ops")
		.map((item) => item.message as OpReport);
}

// ─── Tests: T5 — caller emits RPC.CALL on call ────────────────────────────────

describe("RpcClient caller-side CALL emission", () => {
	let ring: TelemetryRing;
	let sb: ServiceBridge;
	let cb: CircuitBreakerRegistry;

	beforeEach(() => {
		ring = makeRing();
		sb = makeStubSb(ring);
		cb = new CircuitBreakerRegistry();
	});

	it("caller emits RPC.CALL on call — START+END frames with correct fields", async () => {
		const candidate = makeCandidate("target-svc", "Charge");
		const proxy = makeProxyTransport();
		const cache = makeInstanceCache("target-svc", "Charge");
		const lb = makeLB(candidate);

		const client = new RpcClient(
			proxy,
			null, // no direct transport — forces proxy path
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			sb,
		);

		const traceId = "01900000-0000-7000-8000-000000000001";
		const parentOpId = "01900000-0000-7000-8000-000000000002";

		await runWithTrace({ traceId, parentOpId }, async () => {
			await client.call("target-svc", "Charge", {});
		});

		const reports = drainOpReports(ring);
		const startFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs === undefined,
		);
		const endFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs !== undefined,
		);

		expect(startFrames).toHaveLength(1);
		expect(endFrames).toHaveLength(1);

		const start = startFrames[0]!;
		expect(start.traceId).toBe(traceId);
		expect(start.parentOpId).toBe(parentOpId);
		expect(start.subject).toBe("rpc.call:target-svc/Charge");
		expect(start.attempt).toBe(0);
		expect(start.status).toBe(Status.PENDING);

		const end = endFrames[0]!;
		expect(end.traceId).toBe(traceId);
		expect(end.status).toBe(Status.SUCCESS);
	});

	it("caller emits RPC.CALL payloads (IN+OUT) when runtime pushes capture=all", async () => {
		// The capture mode is the runtime-pushed effective mode, not SDK env.
		const captureRing = new TelemetryRing();
		const captureSb = makeStubSb(captureRing, "all");
		const candidate = makeCandidate("target-svc", "Charge");
		const proxy = makeProxyTransport();
		const cache = makeInstanceCache("target-svc", "Charge");
		const lb = makeLB(candidate);
		const client = new RpcClient(
			proxy,
			null,
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			captureSb,
		);

		await client.call("target-svc", "Charge", {});

		const payloads = captureRing
			.peek(200)
			.filter((i) => i.kind === "payloads")
			.map((i) => i.message as PayloadAttachment);
		const directions = payloads.map((p) => p.direction).sort();
		expect(directions).toEqual([1, 2]);
		expect(payloads.every((p) => p.contractHash === "hash-1")).toBe(true);
	});

	// ─── T6: retry reuses ONE CALL op, attempt counter on the same row ──────────

	it("retry produces ONE CALL op across attempts — single op_id, attempt counter, ends OK (ADR-0037..0042)", async () => {
		const candidate = makeCandidate("target-svc", "Charge");
		const proxy = makeFailingProxyTransport(2); // first 2 fail, 3rd succeeds
		const cache = makeInstanceCache("target-svc", "Charge");
		const lb = makeLB(candidate);

		const client = new RpcClient(
			proxy,
			null,
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			sb,
		);

		const traceId = "01900000-0000-7000-8000-000000000010";
		const parentOpId = "01900000-0000-7000-8000-000000000011";

		await runWithTrace({ traceId, parentOpId }, async () => {
			await client.call(
				"target-svc",
				"Charge",
				{},
				{
					retry: {
						maxAttempts: 3,
						baseDelayMs: 0,
						factor: 1,
						maxDelayMs: 0,
						jitter: 0,
					},
				},
			);
		});

		const reports = drainOpReports(ring);
		const startFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs === undefined,
		);
		const endFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs !== undefined,
		);

		// Exactly one CALL row for the whole logical call (1 START + 1 END).
		expect(startFrames).toHaveLength(1);
		expect(endFrames).toHaveLength(1);

		const start = startFrames[0]!;
		const end = endFrames[0]!;

		// Same op_id and trace across the lifecycle; parent is the ambient parent.
		expect(start.traceId).toBe(traceId);
		expect(start.parentOpId).toBe(parentOpId);
		expect(end.opId).toBe(start.opId);

		// START minted at attempt 0; END carries the final attempt count (2 = took
		// 3 tries) — the counter lives on the same row, no op per attempt.
		expect(start.attempt).toBe(0);
		expect(end.attempt).toBe(2);

		// The single row ends OK (the 3rd try succeeded).
		expect(end.status).toBe(Status.SUCCESS);
	});

	it("exhausted retries close the single CALL row with ERROR and final attempt", async () => {
		const candidate = makeCandidate("target-svc", "Charge");
		const proxy = makeFailingProxyTransport(5); // always fails within maxAttempts
		const cache = makeInstanceCache("target-svc", "Charge");
		const lb = makeLB(candidate);

		const client = new RpcClient(
			proxy,
			null,
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			sb,
		);

		await expect(
			client.call(
				"target-svc",
				"Charge",
				{},
				{
					retry: {
						maxAttempts: 3,
						baseDelayMs: 0,
						factor: 1,
						maxDelayMs: 0,
						jitter: 0,
					},
				},
			),
		).rejects.toThrow();

		const reports = drainOpReports(ring);
		const callFrames = reports.filter(
			(r) => r.channel === Channel.RPC && r.kind === RpcCall,
		);
		const starts = callFrames.filter((r) => r.finishedAtMs === undefined);
		const ends = callFrames.filter((r) => r.finishedAtMs !== undefined);
		expect(starts).toHaveLength(1);
		expect(ends).toHaveLength(1);
		expect(ends[0]!.opId).toBe(starts[0]!.opId);
		expect(ends[0]!.attempt).toBe(2);
		expect(ends[0]!.status).toBe(Status.ERROR);
	});

	// ─── T7: stream produces single CALL op ─────────────────────────────────────

	it("stream produces single CALL op for entire stream lifetime — not per-chunk", async () => {
		const candidate = makeCandidate("target-svc", "Stream", true);
		// Proxy transport that yields 3 chunks then closes.
		const proxy: ProxyTransport = {
			callStream: async function* () {
				yield Buffer.from("{}");
				yield Buffer.from("{}");
				yield Buffer.from("{}");
			},
			callUnary: async () => Buffer.from("{}"),
			close: () => {},
		} as unknown as ProxyTransport;

		const cache = makeInstanceCache("target-svc", "Stream", true);
		const lb = makeLB(candidate);

		const client = new RpcClient(
			proxy,
			null,
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			sb,
		);

		const traceId = "01900000-0000-7000-8000-000000000020";
		const parentOpId = "01900000-0000-7000-8000-000000000021";

		await runWithTrace({ traceId, parentOpId }, async () => {
			const chunks = [];
			for await (const chunk of client.stream("target-svc", "Stream", {})) {
				chunks.push(chunk);
			}
			expect(chunks).toHaveLength(3);
		});

		const reports = drainOpReports(ring);
		const startFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs === undefined,
		);
		const endFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs !== undefined,
		);

		// Exactly 1 CALL op — not 3 (not per-chunk).
		expect(startFrames).toHaveLength(1);
		expect(endFrames).toHaveLength(1);

		const start = startFrames[0]!;
		expect(start.traceId).toBe(traceId);
		expect(start.parentOpId).toBe(parentOpId);
		expect(start.subject).toBe("rpc.call:target-svc/Stream");
		expect(start.attempt).toBe(0);

		const end = endFrames[0]!;
		expect(end.status).toBe(Status.SUCCESS);
	});

	// ─── peerServiceId and metaJson.via_proxy field ──────────────────────────────

	it("CALL op carries peerServiceId = target.serviceId and metaJson.via_proxy flag", async () => {
		const candidate = makeCandidate("target-svc", "Charge");
		const proxy = makeProxyTransport();
		const cache = makeInstanceCache("target-svc", "Charge");
		const lb = makeLB(candidate);

		const client = new RpcClient(
			proxy,
			null,
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			sb,
		);

		await runWithTrace(
			{
				traceId: "01900000-0000-7000-8000-000000000030",
				parentOpId: "01900000-0000-7000-8000-000000000031",
			},
			async () => {
				await client.call("target-svc", "Charge", {});
			},
		);

		const reports = drainOpReports(ring);
		const start = reports.find(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs === undefined,
		)!;
		expect(start).toBeDefined();

		// peerServiceId must be the target instance's serviceId (actor/peer contract).
		expect(start.peerServiceId).toBe(candidate.instance.serviceId);

		// metaJson must carry via_proxy (true here: null directTransport → proxy path).
		const meta = JSON.parse(Buffer.from(start.metaJson!).toString()) as Record<
			string,
			unknown
		>;
		expect(meta.via_proxy).toBe(true);
		expect(meta.method).toBe("Charge");
	});

	// ─── stream error path → CALL ends with ERROR ────────────────────────────────

	it("stream CALL op ends with ERROR when generator throws", async () => {
		const candidate = makeCandidate("target-svc", "Stream", true);
		const errMsg = "upstream broke";
		const erroringProxy: ProxyTransport = {
			callStream: async function* () {
				yield Buffer.from("{}");
				throw new Error(errMsg);
			},
			callUnary: async () => Buffer.from("{}"),
			close: () => {},
		} as unknown as ProxyTransport;

		const cache = makeInstanceCache("target-svc", "Stream", true);
		const lb = makeLB(candidate);

		const client = new RpcClient(
			erroringProxy,
			null,
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			sb,
		);

		await runWithTrace(
			{
				traceId: "01900000-0000-7000-8000-000000000040",
				parentOpId: "01900000-0000-7000-8000-000000000041",
			},
			async () => {
				try {
					for await (const _ of client.stream("target-svc", "Stream", {})) {
						// consume first chunk
					}
				} catch {
					// expected
				}
			},
		);

		const reports = drainOpReports(ring);
		const endFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs !== undefined,
		);

		expect(endFrames).toHaveLength(1);
		const end = endFrames[0]!;
		expect(end.status).toBe(Status.ERROR);
		expect(end.statusMessage).toBe(errMsg);
	});

	// ─── call() error path → CALL ends with ERROR (non-retryable) ───────────────

	it("call CALL op ends with ERROR when transport throws non-retryable error", async () => {
		const candidate = makeCandidate("target-svc", "Charge");
		const errMsg = "permission denied";
		// gRPC status code 7 = PermissionDenied — non-retryable.
		const failProxy: ProxyTransport = {
			callUnary: async () => {
				const err = Object.assign(new Error(errMsg), { code: 7 });
				throw err;
			},
			callStream: async function* () {},
			close: () => {},
		} as unknown as ProxyTransport;

		const cache = makeInstanceCache("target-svc", "Charge");
		const lb = makeLB(candidate);

		const client = new RpcClient(
			failProxy,
			null,
			cache,
			(_s, _m) => makeSchemaPair(),
			"caller-svc",
			cb,
			lb,
			sb,
		);

		await runWithTrace(
			{
				traceId: "01900000-0000-7000-8000-000000000050",
				parentOpId: "01900000-0000-7000-8000-000000000051",
			},
			async () => {
				try {
					await client.call(
						"target-svc",
						"Charge",
						{},
						{ retry: { maxAttempts: 1 } },
					);
				} catch {
					// expected
				}
			},
		);

		const reports = drainOpReports(ring);
		const endFrames = reports.filter(
			(r) =>
				r.channel === Channel.RPC &&
				r.kind === RpcCall &&
				r.finishedAtMs !== undefined,
		);

		expect(endFrames).toHaveLength(1);
		const end = endFrames[0]!;
		expect(end.status).toBe(Status.ERROR);
		expect(end.statusMessage).toBe(errMsg);
	});
});
