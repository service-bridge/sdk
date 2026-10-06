// client.test.ts — RpcClient: one RPC.CALL op per logical call (ADR-0001),
// retries only for proven pre-dispatch failures, auto → proxy fallback,
// error model, circuit-breaker input, callDefaults.

import { beforeEach, describe, expect, it } from "bun:test";
import {
	ConfigurationError,
	HandlerError,
	NoLiveInstanceError,
	ServiceBridgeError,
	ValidationError,
} from "../errors";
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
import { Channel, RpcCall } from "../telemetry/ops";
import { TelemetryRing } from "../telemetry/ring";
import { CircuitBreakerRegistry } from "./circuit-breaker";
import { type CallerSchema, type CallOpts, RpcClient } from "./client";
import type { DirectTransport, WireCall } from "./direct-transport";
import type { InstanceCache } from "./instance-cache";
import { type Candidate, cbKey, LoadBalancer } from "./lb";
import type { ProxyTransport } from "./proxy-transport";
import { makeStubSb } from "./test-helpers";
import { CallFailure, handlerFailure } from "./wire";

const SVC = "target-svc";
const METHOD = "Charge";

function desc(streaming = false): MethodDescriptor {
	return {
		instanceId: "inst-1",
		serviceId: "target-svc-id",
		serviceName: SVC,
		type: MethodType.METHOD_TYPE_RPC,
		name: METHOD,
		published: false,
		contractHash: "hash-1",
		inputSchema: Buffer.alloc(0),
		outputSchema: Buffer.alloc(0),
		streaming,
	};
}

function inst(endpoint = "localhost:9000"): ServiceInstanceInfo {
	return {
		instanceId: "inst-1",
		serviceId: "target-svc-id",
		serviceName: SVC,
		callEndpoint: endpoint,
		status: "connected",
		httpEndpoint: "",
		isUnhealthySinceUnixMs: 0,
	};
}

function cache(
	candidates: Candidate[],
	streaming = false,
	known = true,
): InstanceCache {
	return {
		candidatesFor: (s: string, m: string, h: string) =>
			s === SVC && m === METHOD && h === "hash-1" ? candidates : [],
		descriptorFor: (s: string, m: string) =>
			known && s === SVC && m === METHOD ? desc(streaming) : null,
	} as unknown as InstanceCache;
}

const schema = {
	pair: {
		input: { encode: () => Buffer.from("{}") },
		output: {
			decode: (b: Uint8Array) => JSON.parse(Buffer.from(b).toString()),
		},
	},
	contractHash: "hash-1",
	contractHashBytes: Buffer.from("hash-1"),
} as unknown as CallerSchema;

type UnaryFn = (call: WireCall) => Promise<Uint8Array>;

interface Recorder {
	direct: WireCall[];
	proxy: WireCall[];
}

function transports(
	directFn: UnaryFn,
	proxyFn: UnaryFn,
	streamFn?: () => AsyncIterable<Uint8Array>,
): { direct: DirectTransport; proxy: ProxyTransport; rec: Recorder } {
	const rec: Recorder = { direct: [], proxy: [] };
	const direct = {
		callUnary: (_t: unknown, call: WireCall) => {
			rec.direct.push(call);
			return directFn(call);
		},
		callStream: (_t: unknown, _c: WireCall) =>
			streamFn ? streamFn() : (async function* () {})(),
		retain: () => {},
		close: () => {},
	} as unknown as DirectTransport;
	const proxy = {
		callUnary: (_s: string, _h: Buffer, call: WireCall) => {
			rec.proxy.push(call);
			return proxyFn(call);
		},
		callStream: (_s: string, _h: Buffer, _c: WireCall) =>
			streamFn ? streamFn() : (async function* () {})(),
		close: () => {},
	} as unknown as ProxyTransport;
	return { direct, proxy, rec };
}

const ok: UnaryFn = async () => Buffer.from('{"ok":true}');
const preDispatch = () =>
	new CallFailure(
		new ServiceBridgeError("CONNECTION", "connect refused"),
		true,
	);
const dispatched = () =>
	new CallFailure(new ServiceBridgeError("CONNECTION", "stream reset"), false);

function makeClient(opts: {
	candidates?: Candidate[];
	direct?: UnaryFn;
	proxy?: UnaryFn;
	stream?: () => AsyncIterable<Uint8Array>;
	streaming?: boolean;
	known?: boolean;
	ring?: TelemetryRing;
	capture?: "all" | "none";
	callDefaults?: CallOpts;
	noSchema?: boolean;
	cb?: CircuitBreakerRegistry;
}) {
	const ring = opts.ring ?? new TelemetryRing();
	const cb = opts.cb ?? new CircuitBreakerRegistry();
	const t = transports(opts.direct ?? ok, opts.proxy ?? ok, opts.stream);
	const candidates = opts.candidates ?? [
		{ descriptor: desc(opts.streaming), instance: inst(), isUnhealthyAt: null },
	];
	const client = new RpcClient({
		proxy: t.proxy,
		direct: t.direct,
		instances: cache(candidates, opts.streaming, opts.known ?? true),
		resolveSchema: () => (opts.noSchema ? undefined : schema),
		cb,
		lb: new LoadBalancer(cb),
		callDefaults: () => opts.callDefaults ?? {},
		sb: makeStubSb({ ring, captureMode: opts.capture ?? "none" }),
	});
	return { client, ring, rec: t.rec, cb, candidates };
}

function ops(ring: TelemetryRing): OpReport[] {
	return ring
		.peek(500)
		.filter((i) => i.kind === "ops")
		.map((i) => i.message as OpReport)
		.filter((r) => r.channel === Channel.RPC && r.kind === RpcCall);
}

const fast = { retry: { baseDelayMs: 1, maxDelayMs: 2 } } satisfies CallOpts;

describe("RpcClient.call telemetry", () => {
	let ring: TelemetryRing;
	beforeEach(() => {
		ring = new TelemetryRing();
	});

	it("emits one RPC.CALL op with START and END in the caller's trace", async () => {
		const { client } = makeClient({ ring });
		const traceId = "01900000-0000-7000-8000-000000000001";
		const parentOpId = "01900000-0000-7000-8000-000000000002";
		await runWithTrace({ traceId, parentOpId }, () =>
			client.call(SVC, METHOD, {}),
		);
		const frames = ops(ring);
		expect(frames).toHaveLength(2);
		const [start, end] = frames as [OpReport, OpReport];
		expect(start.traceId).toBe(traceId);
		expect(start.parentOpId).toBe(parentOpId);
		expect(start.subject).toBe("rpc.call:target-svc/Charge");
		expect(start.status).toBe(Status.PENDING);
		expect(end.status).toBe(Status.SUCCESS);
	});

	it("captures request and response when the runtime pushes capture=all", async () => {
		const { client } = makeClient({ ring, capture: "all" });
		await client.call(SVC, METHOD, {});
		const directions = ring
			.peek(500)
			.filter((i) => i.kind === "payloads")
			.map((i) => (i.message as PayloadAttachment).direction)
			.sort();
		expect(directions).toEqual([1, 2]);
	});
});

describe("RpcClient.call retries", () => {
	it("retries a pre-dispatch failure on the same op and succeeds", async () => {
		let n = 0;
		const ring = new TelemetryRing();
		const { client, rec } = makeClient({
			ring,
			direct: async () => {
				if (++n < 3) throw preDispatch();
				return Buffer.from("{}");
			},
			callDefaults: { transport: "direct" },
		});
		await client.call(SVC, METHOD, {}, fast);
		expect(rec.direct).toHaveLength(3);
		const frames = ops(ring);
		expect(new Set(frames.map((f) => f.opId)).size).toBe(1);
		expect(frames.at(-1)?.status).toBe(Status.SUCCESS);
		expect(frames.at(-1)?.attempt).toBe(2);
	});

	it("auto falls back to the runtime proxy after a pre-dispatch direct failure", async () => {
		const { client, rec } = makeClient({
			direct: async () => {
				throw preDispatch();
			},
		});
		await expect(client.call(SVC, METHOD, {}, fast)).resolves.toEqual({
			ok: true,
		});
		expect(rec.direct).toHaveLength(1);
		expect(rec.proxy).toHaveLength(1);
	});

	it("never replays a dispatched failure", async () => {
		const { client, rec } = makeClient({
			direct: async () => {
				throw dispatched();
			},
		});
		const err = await client.call(SVC, METHOD, {}, fast).catch((e) => e);
		expect(err).toBeInstanceOf(ServiceBridgeError);
		expect((err as ServiceBridgeError).code).toBe("CONNECTION");
		expect(rec.direct).toHaveLength(1);
		expect(rec.proxy).toHaveLength(0);
	});

	it("exhausted pre-dispatch attempts close the op with ERROR and throw the last error", async () => {
		const ring = new TelemetryRing();
		const { client } = makeClient({
			ring,
			proxy: async () => {
				throw preDispatch();
			},
			callDefaults: { transport: "proxy" },
		});
		const err = await client.call(SVC, METHOD, {}, fast).catch((e) => e);
		expect((err as ServiceBridgeError).code).toBe("CONNECTION");
		expect(ops(ring).at(-1)?.status).toBe(Status.ERROR);
	});

	it("no candidate is NO_LIVE_INSTANCE after the attempts", async () => {
		const { client } = makeClient({ candidates: [] });
		const err = await client.call(SVC, METHOD, {}, fast).catch((e) => e);
		expect(err).toBeInstanceOf(NoLiveInstanceError);
		expect((err as NoLiveInstanceError).retryable).toBe(true);
	});

	it("transport direct with no advertised endpoint never goes via proxy", async () => {
		const { client, rec } = makeClient({
			candidates: [
				{ descriptor: desc(), instance: inst(""), isUnhealthyAt: null },
			],
			callDefaults: { transport: "direct" },
		});
		const err = await client.call(SVC, METHOD, {}, fast).catch((e) => e);
		expect(err).toBeInstanceOf(NoLiveInstanceError);
		expect(rec.proxy).toHaveLength(0);
	});

	it("a cancelled call is CANCELLED and not retried", async () => {
		const ctl = new AbortController();
		const { client, rec } = makeClient({
			direct: async () => {
				ctl.abort();
				throw preDispatch();
			},
		});
		const err = await client
			.call(SVC, METHOD, {}, { ...fast, signal: ctl.signal })
			.catch((e) => e);
		expect((err as ServiceBridgeError).code).toBe("CANCELLED");
		expect(rec.direct).toHaveLength(1);
	});
});

describe("RpcClient.call errors and options", () => {
	it("a handler error reaches the caller as HandlerError with the business code", async () => {
		const { client } = makeClient({
			direct: async () => {
				throw handlerFailure("OUT_OF_STOCK", "nothing left");
			},
		});
		const err = await client.call(SVC, METHOD, {}).catch((e) => e);
		expect(err).toBeInstanceOf(HandlerError);
		expect((err as HandlerError).handlerCode).toBe("OUT_OF_STOCK");
		expect((err as HandlerError).message).toBe("nothing left");
	});

	it("a missing caller schema is a configuration error", async () => {
		const { client } = makeClient({ noSchema: true });
		await expect(client.call(SVC, METHOD, {})).rejects.toBeInstanceOf(
			ConfigurationError,
		);
	});

	it("calling a streaming method as unary is a validation error", async () => {
		const { client } = makeClient({ streaming: true });
		await expect(client.call(SVC, METHOD, {})).rejects.toBeInstanceOf(
			ValidationError,
		);
	});

	it("callDefaults apply and per-call options win", async () => {
		const { client, rec } = makeClient({
			callDefaults: {
				transport: "proxy",
				idempotencyKey: "default-key",
				timeout: "5s",
			},
		});
		const before = Date.now();
		await client.call(SVC, METHOD, {}, { idempotencyKey: "own-key" });
		expect(rec.proxy).toHaveLength(1);
		const call = rec.proxy[0] as WireCall;
		expect(call.idempotencyKey).toBe("own-key");
		expect(call.deadline.getTime() - before).toBeLessThanOrEqual(5_100);
		expect(call.deadline.getTime() - before).toBeGreaterThan(4_000);
	});

	it("an invalid timeout string is a configuration error", async () => {
		const { client } = makeClient({});
		await expect(
			client.call(SVC, METHOD, {}, { timeout: "soon" }),
		).rejects.toBeInstanceOf(ConfigurationError);
	});
});

describe("RpcClient circuit breaker input", () => {
	const cases: [string, () => CallFailure, "OPEN" | "CLOSED"][] = [
		["handler error", () => handlerFailure("BAD", "bad"), "CLOSED"],
		[
			"access denied",
			() =>
				new CallFailure(new ServiceBridgeError("ACCESS_DENIED", "no"), false),
			"CLOSED",
		],
		[
			"validation",
			() => new CallFailure(new ServiceBridgeError("VALIDATION", "no"), false),
			"CLOSED",
		],
		["connection", dispatched, "OPEN"],
		[
			"timeout",
			() => new CallFailure(new ServiceBridgeError("TIMEOUT", "late"), false),
			"OPEN",
		],
		[
			"internal status",
			() => new CallFailure(new ServiceBridgeError("INTERNAL", "boom"), false),
			"OPEN",
		],
	];
	for (const [name, failure, state] of cases) {
		it(`${name} leaves the breaker ${state}`, async () => {
			const cb = new CircuitBreakerRegistry();
			const { client, candidates } = makeClient({
				cb,
				direct: async () => {
					throw failure();
				},
			});
			for (let i = 0; i < 12; i++)
				await client
					.call(SVC, METHOD, {}, { retry: { maxAttempts: 1 } })
					.catch(() => {});
			expect(cb.state(cbKey((candidates[0] as Candidate).instance))).toBe(
				state,
			);
		});
	}
});

describe("RpcClient.stream", () => {
	it("one op for the whole stream, ERROR when the stream fails", async () => {
		const ring = new TelemetryRing();
		const { client } = makeClient({
			ring,
			streaming: true,
			stream: async function* () {
				yield Buffer.from('{"n":1}');
				throw dispatched();
			},
		});
		const got: unknown[] = [];
		const err = await (async () => {
			for await (const chunk of client.stream(SVC, METHOD, {})) got.push(chunk);
		})().catch((e) => e);
		expect(got).toEqual([{ n: 1 }]);
		expect((err as ServiceBridgeError).code).toBe("CONNECTION");
		const frames = ops(ring);
		expect(frames).toHaveLength(2);
		expect(frames[1]?.status).toBe(Status.ERROR);
	});

	it("streaming a unary method is a validation error", async () => {
		const { client } = makeClient({ streaming: false });
		const err = await (async () => {
			for await (const _ of client.stream(SVC, METHOD, {})) {
			}
		})().catch((e) => e);
		expect(err).toBeInstanceOf(ValidationError);
	});
});
