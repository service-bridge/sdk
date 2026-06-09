// client-context.test.ts — tests that RpcClient propagates trace context
// correctly to the transport per ADR-0036:
// - Transport sees callOp.opId as parentOpId (not ambient parentOpId).
// - traceId is inherited from ambient context when present.
// - When no ambient context: fresh traceId minted for the CALL op.
// - Streaming path: callStream transport sees childCtx at first yield.

import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { MethodType } from "../pb/servicebridge/v1/registry";
import type { OpReport } from "../pb/servicebridge/v1/telemetry";
import { computeContractHash } from "../serde/contract-hash";
import { buildSchemaPair } from "../serde/serializer";
import { currentTraceContext, runWithTrace } from "../telemetry/context";
import { RpcCall } from "../telemetry/ops";
import { TelemetryRing } from "../telemetry/ring";
import type { TraceContext } from "../telemetry/trace-context";
import { ZERO_OP_ID } from "../telemetry/trace-context";
import { parseXSbTrace } from "../telemetry/wire-trace";
import { CircuitBreakerRegistry } from "./circuit-breaker";
import { RpcClient, SchemaRegistry } from "./client";
import type { DirectTransport } from "./direct-transport";
import type { InstanceCache } from "./instance-cache";
import type { Candidate } from "./lb";
import { LoadBalancer } from "./lb";
import type { ProxyTransport } from "./proxy-transport";
import { makeStubSb } from "./test-helpers";

const protoFile = join(
	import.meta.dir,
	"..",
	"serde",
	"testdata",
	"payment.proto",
);

function mkInstance(id: string): ServiceInstanceInfo {
	return {
		instanceId: id,
		serviceId: "svc-id",
		serviceName: "payment-svc",
		callEndpoint: "h:1",
		status: "connected",
		httpEndpoint: "",
		isUnhealthySinceUnixMs: 0,
	};
}

function mkDescriptor(instanceId: string, hash: string): MethodDescriptor {
	return {
		instanceId,
		serviceId: "svc-id",
		serviceName: "payment-svc",
		type: MethodType.METHOD_TYPE_RPC,
		name: "charge",
		published: false,
		contractHash: hash,
		inputSchema: Buffer.alloc(0),
		outputSchema: Buffer.alloc(0),
		streaming: false,
	};
}

function mkStreamingDescriptor(
	instanceId: string,
	hash: string,
): MethodDescriptor {
	return { ...mkDescriptor(instanceId, hash), streaming: true };
}

function mkStreamingInstanceCache(
	hash: string,
	endpointOverride = "h:1",
): InstanceCache {
	const candidate: Candidate = {
		descriptor: mkStreamingDescriptor("inst-A", hash),
		instance: { ...mkInstance("inst-A"), callEndpoint: endpointOverride },
		isUnhealthyAt: null,
	};
	return {
		pickAll: () => [candidate],
		descriptorFor: () => candidate.descriptor,
		bind: () => {},
		dispose: () => {},
	} as unknown as InstanceCache;
}

function mkInstanceCache(
	hash: string,
	endpointOverride = "h:1",
): InstanceCache {
	const candidate: Candidate = {
		descriptor: mkDescriptor("inst-A", hash),
		instance: { ...mkInstance("inst-A"), callEndpoint: endpointOverride },
		isUnhealthyAt: null,
	};
	return {
		pickAll: () => [candidate],
		descriptorFor: () => candidate.descriptor,
		bind: () => {},
		dispose: () => {},
	} as unknown as InstanceCache;
}

/**
 * SpyProxy captures the xSbTrace field that the proxy's callUnary sees —
 * which reflects what currentTraceContext().parentOpId is in the ALS at
 * transport invocation time.
 * callStream captures the ALS trace context at the first yield point inside
 * the generator body — proving the child context is active when the transport
 * body runs (not just when the generator function is constructed).
 */
class SpyProxy {
	capturedXSbTrace: string | undefined;
	capturedStreamCtx: TraceContext | undefined;

	async callUnary(
		_service: string,
		_method: string,
		_payload: Uint8Array,
		_reqId: string,
		_idem: string,
		_timeout: number,
		_hash: string,
	): Promise<Uint8Array> {
		const { formatXSbTrace } = await import("../telemetry/wire-trace");
		const ctx = currentTraceContext();
		if (ctx) {
			this.capturedXSbTrace = formatXSbTrace(ctx.traceId, ctx.parentOpId);
		} else {
			this.capturedXSbTrace = undefined;
		}
		return new Uint8Array(0);
	}

	async *callStream(
		_service: string,
		_method: string,
		_payload: Uint8Array,
		_reqId: string,
		_idem: string,
		_timeout: number,
		_hash: string,
	): AsyncIterable<Uint8Array> {
		this.capturedStreamCtx = currentTraceContext();
		yield new Uint8Array(0);
	}

	close(): void {}
}

describe("RpcClient trace-context propagation", () => {
	it("transport sees callOp.opId as parentOpId — traceId inherited from ambient (ADR-0036)", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const callerHash = computeContractHash(pair);
		const instances = mkInstanceCache(callerHash);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const proxy = new SpyProxy();
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);
		const ring = new TelemetryRing();

		const client = new RpcClient(
			proxy as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			"caller-svc-id",
			cb,
			lb,
			makeStubSb({ ring }),
		);

		pair.output.decode = (() => ({ ok: true })) as typeof pair.output.decode;

		const ambientTraceId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
		const ambientParentOpId = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

		await runWithTrace(
			{ traceId: ambientTraceId, parentOpId: ambientParentOpId },
			async () => {
				await client.call(
					"payment-svc",
					"charge",
					{ userId: "u", amount: 1 },
					{ transport: "proxy", retry: { maxAttempts: 1 } },
				);
			},
		);

		const captured = proxy.capturedXSbTrace;
		expect(captured).toBeDefined();

		const parsed = parseXSbTrace(captured!);
		expect(parsed).not.toBeNull();

		// traceId must be inherited from the ambient context.
		expect(parsed!.traceId).toBe(ambientTraceId);

		// parentOpId seen by transport = callOp.opId (NOT ambient parentOpId).
		// This is the ADR-0036 contract: CALL op wraps invoke(), so transport
		// sees call op as its parent, enabling HANDLE.parent = CALL.
		expect(parsed!.parentOpId).not.toBe(ZERO_OP_ID);
		expect(parsed!.parentOpId).not.toBe(ambientParentOpId);

		// Confirm the parentOpId matches the CALL op emitted to the ring.
		const items = ring.peek(200);
		const callStart = items
			.filter((i) => i.kind === "ops")
			.map((i) => i.message as OpReport)
			.find((r) => r.kind === RpcCall && r.finishedAtMs === undefined);
		expect(callStart).toBeDefined();
		expect(parsed!.parentOpId).toBe(callStart!.opId);
	});

	it("sends trace header derived from fresh callOp when no ambient context", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const callerHash = computeContractHash(pair);
		const instances = mkInstanceCache(callerHash);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const proxy = new SpyProxy();
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);
		const ring = new TelemetryRing();

		const client = new RpcClient(
			proxy as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			"caller-svc-id",
			cb,
			lb,
			makeStubSb({ ring }),
		);

		pair.output.decode = (() => ({ ok: true })) as typeof pair.output.decode;

		// No runWithTrace wrapper — no ambient context.
		await client.call(
			"payment-svc",
			"charge",
			{ userId: "u", amount: 1 },
			{ transport: "proxy", retry: { maxAttempts: 1 } },
		);

		// With ADR-0036: even without ambient context, a CALL op is started,
		// and the transport sees the callOp context (fresh traceId + callOp.opId).
		const captured = proxy.capturedXSbTrace;
		expect(captured).toBeDefined();

		const parsed = parseXSbTrace(captured!);
		expect(parsed).not.toBeNull();
		expect(parsed!.parentOpId).not.toBe(ZERO_OP_ID);

		// Confirm it matches the CALL op's opId.
		const items = ring.peek(200);
		const callStart = items
			.filter((i) => i.kind === "ops")
			.map((i) => i.message as OpReport)
			.find((r) => r.kind === RpcCall && r.finishedAtMs === undefined);
		expect(callStart).toBeDefined();
		expect(parsed!.parentOpId).toBe(callStart!.opId);
	});

	it("streaming: callStream body sees childCtx with callOp.opId as parentOpId (ADR-0036)", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const callerHash = computeContractHash(pair);
		const instances = mkStreamingInstanceCache(callerHash);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const proxy = new SpyProxy();
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);
		const ring = new TelemetryRing();

		const client = new RpcClient(
			proxy as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			"caller-svc-id",
			cb,
			lb,
			makeStubSb({ ring }),
		);

		pair.output.decode = (() => ({ ok: true })) as typeof pair.output.decode;

		const ambientTraceId = "cccccccc-cccc-cccc-cccc-cccccccccccc";
		const ambientParentOpId = "dddddddd-dddd-dddd-dddd-dddddddddddd";

		await runWithTrace(
			{ traceId: ambientTraceId, parentOpId: ambientParentOpId },
			async () => {
				for await (const _ of client.stream(
					"payment-svc",
					"charge",
					{ userId: "u", amount: 1 },
					{ transport: "proxy" },
				)) {
					// consume all chunks
				}
			},
		);

		// Drain ring to find the CALL start op.
		const items = ring.peek(200);
		const callStart = items
			.filter((i) => i.kind === "ops")
			.map((i) => i.message as OpReport)
			.find((r) => r.kind === RpcCall && r.finishedAtMs === undefined);
		expect(callStart).toBeDefined();

		// The transport body (callStream generator) must have seen the child context
		// with callOp.opId as parentOpId — not the ambient parentOpId.
		const streamCtx = proxy.capturedStreamCtx;
		expect(streamCtx).toBeDefined();
		expect(streamCtx!.traceId).toBe(ambientTraceId);
		expect(streamCtx!.parentOpId).not.toBe(ZERO_OP_ID);
		expect(streamCtx!.parentOpId).not.toBe(ambientParentOpId);
		expect(streamCtx!.parentOpId).toBe(callStart!.opId);
	});

	it("streaming: early consumer break still ends callOp", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const callerHash = computeContractHash(pair);
		const instances = mkStreamingInstanceCache(callerHash);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const proxy = new SpyProxy();
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);
		const ring = new TelemetryRing();

		const client = new RpcClient(
			proxy as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			"caller-svc-id",
			cb,
			lb,
			makeStubSb({ ring }),
		);

		pair.output.decode = (() => ({ ok: true })) as typeof pair.output.decode;

		// Consumer breaks after first chunk — early termination.
		for await (const _ of client.stream(
			"payment-svc",
			"charge",
			{ userId: "u", amount: 1 },
			{ transport: "proxy" },
		)) {
			break;
		}

		// Both CALL start and CALL end must appear in ring (end written in finally).
		const items = ring.peek(200);
		const reports = items
			.filter((i) => i.kind === "ops")
			.map((i) => i.message as OpReport);
		const callEnd = reports.find(
			(r) => r.kind === RpcCall && r.finishedAtMs !== undefined,
		);
		expect(callEnd).toBeDefined();
	});
});
