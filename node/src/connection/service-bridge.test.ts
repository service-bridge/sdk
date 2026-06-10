import { afterEach, describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { status as GrpcStatus } from "@grpc/grpc-js";
import { BootstrapKeyPayload } from "../pb/servicebridge/v1/bootstrap";
import type {
	ControlClient,
	ServerControl,
} from "../pb/servicebridge/v1/control";
import type {
	RegistryClient,
	RegistryEvent,
} from "../pb/servicebridge/v1/registry";
import type { ProvisionResult } from "./provision";
import {
	type DisconnectedEvent,
	type PolicyViolationEvent,
	type ReconnectingEvent,
	ServiceBridge,
} from "./service-bridge";
import { ServiceBridgeError } from "./service-bridge-error";

// Minimal valid bootstrap key for tests using BootstrapKeyPayload proto format.
const VALID_KEY = (() => {
	const bytes = BootstrapKeyPayload.encode({
		keyId: Buffer.alloc(8, 0x01),
		secret: Buffer.alloc(32, 0x02),
		caCertDer: Buffer.alloc(1, 0xff),
	}).finish();
	return `sb.${Buffer.from(bytes).toString("base64url")}`;
})();

// ── fakes ───────────────────────────────────────────────────────────────────

// FakeServerStream mimics a gRPC ClientReadableStream<ServerControl>.
class FakeServerStream extends EventEmitter {
	cancelled = false;
	cancel(): void {
		this.cancelled = true;
		this.emit("end");
	}
	emitData(msg: ServerControl): void {
		this.emit("data", msg);
	}
	emitError(err: Error): void {
		this.emit("error", err);
	}
}

function makeFakeClient(stream: FakeServerStream): ControlClient {
	return {
		open: () => stream,
		close: () => {},
	} as unknown as ControlClient;
}

// FakeRegistryStream mimics the gRPC ClientReadableStream<RegistryEvent> the
// WatchStream attaches "data"/"error" listeners to.
class FakeRegistryStream extends EventEmitter {
	cancelled = false;
	cancel(): void {
		this.cancelled = true;
	}
	emitEvent(evt: RegistryEvent): void {
		this.emit("data", evt);
	}
}

// makeRegistryFactory returns a registryClientFactory that records every stream
// it hands out, so a test can drive the most-recent registry stream after a
// reconnect created a fresh one.
function makeRegistryFactory(
	streams: FakeRegistryStream[],
): () => RegistryClient {
	return () =>
		({
			registerAndWatch: () => {
				const s = new FakeRegistryStream();
				streams.push(s);
				return s as unknown as ReturnType<RegistryClient["registerAndWatch"]>;
			},
			close: () => {},
		}) as unknown as RegistryClient;
}

// makeCountingControlFactory hands out a fresh control client per call and
// records created/closed so a test can assert no channel outlives its session.
function makeCountingControlFactory(streams: FakeServerStream[]) {
	const counts = { created: 0, closed: 0 };
	const factory = (): ControlClient => {
		counts.created++;
		const stream = new FakeServerStream();
		streams.push(stream);
		return {
			open: () => stream,
			close: () => {
				counts.closed++;
			},
		} as unknown as ControlClient;
	};
	return { counts, factory };
}

// makeCountingRegistryFactory mirrors makeRegistryFactory but counts channel
// create/close so the leak regression can assert created === closed.
function makeCountingRegistryFactory(streams: FakeRegistryStream[]) {
	const counts = { created: 0, closed: 0 };
	const factory = (): RegistryClient =>
		({
			registerAndWatch: () => {
				const s = new FakeRegistryStream();
				streams.push(s);
				return s as unknown as ReturnType<RegistryClient["registerAndWatch"]>;
			},
			close: () => {
				counts.closed++;
			},
		}) as unknown as RegistryClient;
	const wrapped = (url: string, creds: unknown): RegistryClient => {
		counts.created++;
		void url;
		void creds;
		return factory();
	};
	return { counts, factory: wrapped };
}

function fakeProvisionResult(): ProvisionResult {
	return {
		certDer: Buffer.alloc(1),
		caChainDer: Buffer.alloc(1),
		serviceId: "svc",
		serviceName: "svc-name",
		instanceId: "inst",
		notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 3600),
		privateKey: {} as CryptoKey,
		privateKeyDer: Buffer.alloc(1),
	};
}

// ── tests ───────────────────────────────────────────────────────────────────

// Telemetry transport is disabled per-bridge via { telemetry: false } in these
// tests — they exercise the connect/reconnect/rotation lifecycle, не телеметрию.
// Иначе ServiceBridge поднял бы реальный TelemetryClient на localhost:0.

let activeBridges: ServiceBridge[] = [];
afterEach(async () => {
	for (const b of activeBridges) {
		await b.stop();
	}
	activeBridges = [];
});

describe("ServiceBridge constructor", () => {
	test("instantiates without throwing", () => {
		const sb = new ServiceBridge("localhost:14445", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
		});
		expect(sb).toBeDefined();
	});

	test("on() registers handlers without throwing", () => {
		const sb = new ServiceBridge("localhost:14445", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
		});
		expect(() => {
			sb.on("connected", () => {});
			sb.on("reconnecting", () => {});
			sb.on("disconnected", () => {});
		}).not.toThrow();
	});
});

describe("ServiceBridge connect lifecycle", () => {
	test("emits connected when provision succeeds and Welcome arrives", async () => {
		const stream = new FakeServerStream();
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => fakeProvisionResult(),
			clientFactory: () => makeFakeClient(stream),
			certRefreshLeadMs: 1_000_000,
		});
		activeBridges.push(sb);

		const events: string[] = [];
		sb.on("connected", () => events.push("connected"));
		const connected = once(sb, "connected");

		await sb.start();
		stream.emitData({
			welcome: {
				sessionId: "s1",
				serviceId: "svc",
				serviceName: "svc-name",
			},
		});
		await connected;
		expect(events).toEqual(["connected"]);
	});

	test("identity() returns null before Welcome, full record after, null after stop", async () => {
		const stream = new FakeServerStream();
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => fakeProvisionResult(),
			clientFactory: () => makeFakeClient(stream),
			certRefreshLeadMs: 1_000_000,
		});
		activeBridges.push(sb);

		expect(sb.identity()).toBeNull();

		await sb.start();
		expect(sb.identity()).toBeNull();

		stream.emitData({
			welcome: {
				sessionId: "sess-42",
				serviceId: "svc-id-7",
				serviceName: "billing",
			},
		});
		await waitFor(
			() => sb.identity() !== null,
			"identity populated after Welcome",
		);

		expect(sb.identity()).toEqual({
			sessionId: "sess-42",
			serviceId: "svc-id-7",
			serviceName: "billing",
			instanceId: "inst",
		});

		await sb.stop();
		expect(sb.identity()).toBeNull();
	});

	test("reuses cached cert on reconnect — no re-Provision (argon2)", async () => {
		let provisionCalls = 0;
		const streams: FakeServerStream[] = [];
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				provisionCalls++;
				return fakeProvisionResult();
			},
			clientFactory: () => {
				const s = new FakeServerStream();
				streams.push(s);
				return makeFakeClient(s);
			},
			certRefreshLeadMs: 1_000_000,
			reconnectIntervalMs: 5,
			reconnectAttempts: 5,
		});
		activeBridges.push(sb);

		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));

		await sb.start();
		await waitFor(() => streams.length >= 1, "first stream opened");
		streams[0]?.emitData({
			welcome: { sessionId: "s1", serviceId: "svc", serviceName: "n" },
		});
		await tick();
		expect(provisionCalls).toBe(1);

		// Drop the stream → reconnect. The cert is still valid, so connect() must
		// reuse the cached provision instead of calling provisionFn again.
		streams[0]?.emitError(new Error("transport drop"));
		await waitFor(() => streams.length >= 2, "reconnect opened a new stream");
		streams[1]?.emitData({
			welcome: { sessionId: "s2", serviceId: "svc", serviceName: "n" },
		});
		await tick();

		expect(reconnects.length).toBeGreaterThanOrEqual(1);
		expect(provisionCalls).toBe(1);
	});

	test("emits reconnecting when provision throws", async () => {
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				throw new Error("network");
			},
			reconnectIntervalMs: 1_000_000,
			reconnectAttempts: 3,
		});
		activeBridges.push(sb);

		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));

		await sb.start();
		await tick();
		expect(reconnects.length).toBe(1);
		const r0 = reconnects[0];
		expect(r0?.reason).toContain("network");
		expect(r0?.attempt).toBe(2);
	});

	test("emits disconnected{reason:'exhausted'} after attempts run out", async () => {
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				throw new Error("nope");
			},
			reconnectIntervalMs: 5,
			reconnectAttempts: 2,
		});
		activeBridges.push(sb);

		const disconnects: DisconnectedEvent[] = [];
		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));
		sb.on("disconnected", (e) => disconnects.push(e));

		await sb.start();
		await new Promise((r) => setTimeout(r, 100));

		expect(disconnects.length).toBe(1);
		const d0 = disconnects[0];
		expect(d0?.reason).toBe("exhausted");
		expect(reconnects.length).toBeGreaterThanOrEqual(1);
	});

	test("emits disconnected on drain", async () => {
		const stream = new FakeServerStream();
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => fakeProvisionResult(),
			clientFactory: () => makeFakeClient(stream),
			certRefreshLeadMs: 1_000_000,
		});
		activeBridges.push(sb);

		const disconnects: DisconnectedEvent[] = [];
		sb.on("disconnected", (e) => disconnects.push(e));
		const disconnected = once(sb, "disconnected");

		await sb.start();
		stream.emitData({
			welcome: {
				sessionId: "s1",
				serviceId: "svc",
				serviceName: "svc-name",
			},
		});
		stream.emitData({ drain: { reason: "maintenance" } });
		await disconnected;
		expect(disconnects.length).toBe(1);
		const drain0 = disconnects[0];
		expect(drain0?.reason).toContain("maintenance");
	});

	test("stop() prevents further reconnects", async () => {
		let provisionCalls = 0;
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				provisionCalls++;
				throw new Error("nope");
			},
			reconnectIntervalMs: 5,
			reconnectAttempts: 5,
		});
		activeBridges.push(sb);

		await sb.start();
		await sb.stop();
		await new Promise((r) => setTimeout(r, 50));
		const after = provisionCalls;
		await new Promise((r) => setTimeout(r, 50));
		expect(provisionCalls).toBe(after);
	});

	test("stop() clears the pending reconnect timer (BUG-17)", async () => {
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				throw new Error("nope");
			},
			// Large interval: the reconnect timer is parked, not fired, so we can
			// assert it is cleared by stop() rather than by elapsing.
			reconnectIntervalMs: 1_000_000,
			reconnectAttempts: 5,
		});
		activeBridges.push(sb);

		await sb.start();
		// First provision throws → scheduleReconnect parks a setTimeout handle.
		await waitFor(
			() =>
				(sb as unknown as { reconnectTimer: unknown }).reconnectTimer !== null,
			"reconnect timer parked after failed provision",
		);

		await sb.stop();
		expect(
			(sb as unknown as { reconnectTimer: unknown }).reconnectTimer,
		).toBeNull();
	});
});

describe("ServiceBridge gRPC channel lifecycle (no leak on reconnect)", () => {
	test("every (re)connect closes its predecessor control + registry channel; stop() closes the last", async () => {
		const controlStreams: FakeServerStream[] = [];
		const registryStreams: FakeRegistryStream[] = [];
		const control = makeCountingControlFactory(controlStreams);
		const registry = makeCountingRegistryFactory(registryStreams);

		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => fakeProvisionResult(),
			clientFactory: control.factory,
			registryClientFactory: registry.factory,
			certRefreshLeadMs: 1_000_000,
			reconnectIntervalMs: 5,
			reconnectAttempts: 20,
		});
		activeBridges.push(sb);

		await sb.start();
		await waitFor(() => registryStreams.length >= 1, "first registry stream");
		controlStreams[0]?.emitData({
			welcome: { sessionId: "s1", serviceId: "svc", serviceName: "n" },
		});
		await tick();

		// Drive several transport-level reconnect cycles. Each one opens a fresh
		// control + registry channel; the predecessor must be closed.
		const cycles = 4;
		for (let i = 0; i < cycles; i++) {
			const idx = controlStreams.length - 1;
			controlStreams[idx]?.emitError(new Error(`drop-${i}`));
			await waitFor(
				() => controlStreams.length >= idx + 2,
				`reconnect ${i} opened a new control stream`,
			);
			controlStreams[controlStreams.length - 1]?.emitData({
				welcome: { sessionId: `s${i + 2}`, serviceId: "svc", serviceName: "n" },
			});
			await tick();
		}

		await sb.stop();

		// 1 initial + `cycles` reconnects = cycles+1 control + registry channels.
		expect(control.counts.created).toBe(cycles + 1);
		expect(registry.counts.created).toBe(cycles + 1);
		// Zero live channels: everything created is closed.
		expect(control.counts.closed).toBe(control.counts.created);
		expect(registry.counts.closed).toBe(registry.counts.created);
	});

	test("successful cert rotation closes the old control + registry channel", async () => {
		const controlStreams: FakeServerStream[] = [];
		const registryStreams: FakeRegistryStream[] = [];
		const control = makeCountingControlFactory(controlStreams);
		const registry = makeCountingRegistryFactory(registryStreams);

		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => ({
				...fakeProvisionResult(),
				notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 1),
			}),
			refreshFn: async () => ({
				...fakeProvisionResult(),
				notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 3600),
			}),
			clientFactory: control.factory,
			registryClientFactory: registry.factory,
			certRefreshLeadMs: 0,
			certRefreshJitterMs: 0,
		});
		activeBridges.push(sb);

		await sb.start();
		controlStreams[0]?.emitData({
			welcome: { sessionId: "s1", serviceId: "svc", serviceName: "n" },
		});
		// Rotation timer fires (TTL=1s, lead=0) → new channels opened.
		await waitFor(() => control.counts.created >= 2, "rotation opened channel");
		controlStreams[controlStreams.length - 1]?.emitData({
			welcome: { sessionId: "s2", serviceId: "svc", serviceName: "n" },
		});
		// After Welcome on the new stream, the old pair must be closed.
		await waitFor(
			() => control.counts.closed >= 1 && registry.counts.closed >= 1,
			"old channels closed after rotation Welcome",
		);

		await sb.stop();
		expect(control.counts.closed).toBe(control.counts.created);
		expect(registry.counts.closed).toBe(registry.counts.created);
	});

	test("cert rotation rollback (new stream ends before Welcome) closes the new channels it opened", async () => {
		const controlStreams: FakeServerStream[] = [];
		const registryStreams: FakeRegistryStream[] = [];
		const control = makeCountingControlFactory(controlStreams);
		const registry = makeCountingRegistryFactory(registryStreams);

		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => ({
				...fakeProvisionResult(),
				notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 1),
			}),
			// refresh succeeds → rotation opens new channels and waits for Welcome.
			refreshFn: async () => ({
				...fakeProvisionResult(),
				notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 3600),
			}),
			clientFactory: control.factory,
			registryClientFactory: registry.factory,
			certRefreshLeadMs: 0,
			certRefreshJitterMs: 0,
			reconnectIntervalMs: 1_000_000,
			reconnectAttempts: 3,
		});
		activeBridges.push(sb);

		await sb.start();
		controlStreams[0]?.emitData({
			welcome: { sessionId: "s1", serviceId: "svc", serviceName: "n" },
		});

		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));

		// Wait for rotation to open the new pair, then end the new stream before
		// any Welcome → swap rejects, rollback path must close the new channels.
		await waitFor(
			() => control.counts.created >= 2,
			"rotation opened new channel",
		);
		const newControl = controlStreams[controlStreams.length - 1]!;
		newControl.emit("end");

		await waitFor(
			() => reconnects.length > 0,
			"rotation rollback scheduled reconnect",
		);

		await sb.stop();
		expect(control.counts.closed).toBe(control.counts.created);
		expect(registry.counts.closed).toBe(registry.counts.created);
	});

	test("failed reconnect cycles (provision throws) leak no channels", async () => {
		const controlStreams: FakeServerStream[] = [];
		const registryStreams: FakeRegistryStream[] = [];
		const control = makeCountingControlFactory(controlStreams);
		const registry = makeCountingRegistryFactory(registryStreams);
		let firstConnect = true;

		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				// First connect succeeds (opens channels); subsequent reconnect
				// provisions reuse the cached cert, so to force fresh provisions we
				// invalidate the cache by throwing after the first session drops.
				if (firstConnect) {
					firstConnect = false;
					return fakeProvisionResult();
				}
				throw new Error("runtime down");
			},
			clientFactory: control.factory,
			registryClientFactory: registry.factory,
			certRefreshLeadMs: 1_000_000,
			reconnectIntervalMs: 5,
			reconnectAttempts: 3,
		});
		activeBridges.push(sb);

		await sb.start();
		await waitFor(() => controlStreams.length >= 1, "first control stream");
		controlStreams[0]?.emitData({
			welcome: { sessionId: "s1", serviceId: "svc", serviceName: "n" },
		});
		await tick();

		// Drop → reconnect attempts now hit the throwing provision (cached cert is
		// still valid, so the channels stay reused; the failing provisions open no
		// new channels). Run the attempts to exhaustion.
		controlStreams[0]?.emitError(new Error("drop"));
		await new Promise((r) => setTimeout(r, 120));
		await sb.stop();

		expect(control.counts.closed).toBe(control.counts.created);
		expect(registry.counts.closed).toBe(registry.counts.created);
	});
});

describe("ServiceBridge reconnect backoff", () => {
	test("default (no reconnectIntervalMs) uses the jittered ladder, not a fixed 3s tick", async () => {
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				throw new Error("network");
			},
			// reconnectIntervalMs intentionally unset → ladder.
			reconnectAttempts: 3,
		});
		activeBridges.push(sb);

		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));
		sb.on("disconnected", () => {});

		await sb.start();
		await waitFor(() => reconnects.length >= 1, "first reconnecting event");

		// The first reconnect is attempt #2 → ladder rung index 1 (~5000ms) ±20%
		// jitter. The exact value is jittered (never a fixed flat number), which is
		// the whole point: a fleet does not reconnect in lockstep. Assert it lands
		// in the jittered 5000-rung band and is not the old flat 3000ms tick.
		const first = reconnects[0]!;
		expect(first.delayMs).toBeGreaterThan(3900);
		expect(first.delayMs).toBeLessThan(6100);
		expect(first.delayMs).not.toBe(3000);
	});

	test("explicit reconnectIntervalMs overrides the ladder with a flat delay", async () => {
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				throw new Error("network");
			},
			reconnectIntervalMs: 42,
			reconnectAttempts: 3,
		});
		activeBridges.push(sb);

		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));
		sb.on("disconnected", () => {});

		await sb.start();
		await waitFor(() => reconnects.length >= 1, "first reconnecting event");
		expect(reconnects[0]?.delayMs).toBe(42);
	});
});

describe("ServiceBridge policy listener registration (BUG-18)", () => {
	function snapshotWithWarnings(declarations: string[]): RegistryEvent {
		return {
			snapshot: {
				methods: [],
				instances: [],
				eventSubscriptions: [],
				outgoingCalls: [],
				policy: {
					capabilities: [],
					egress: [],
					acceptance: [],
					warnings: declarations.map((d) => ({
						declaration: d,
						value: "svc",
						denySide: "self_egress",
						reason: "not declared",
					})),
				},
				captureModes: undefined,
			},
			update: undefined,
		} as unknown as RegistryEvent;
	}

	test("policy_violation fires once per warning after two reconnect cycles", async () => {
		const controlStreams: FakeServerStream[] = [];
		const registryStreams: FakeRegistryStream[] = [];
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => fakeProvisionResult(),
			clientFactory: () => {
				const s = new FakeServerStream();
				controlStreams.push(s);
				return makeFakeClient(s);
			},
			registryClientFactory: makeRegistryFactory(registryStreams),
			certRefreshLeadMs: 1_000_000,
			reconnectIntervalMs: 5,
			reconnectAttempts: 10,
		});
		activeBridges.push(sb);

		const violations: PolicyViolationEvent[] = [];
		sb.on("policy_violation", (v) => violations.push(v));

		await sb.start();
		await waitFor(() => registryStreams.length >= 1, "first registry stream");
		controlStreams[0]?.emitData({
			welcome: { sessionId: "s1", serviceId: "svc", serviceName: "n" },
		});
		await tick();

		// Two transport drops → two reconnects → three openSession calls total.
		controlStreams[0]?.emitError(new Error("drop-1"));
		await waitFor(
			() => registryStreams.length >= 2,
			"second registry stream after reconnect 1",
		);
		controlStreams[1]?.emitData({
			welcome: { sessionId: "s2", serviceId: "svc", serviceName: "n" },
		});
		await tick();

		controlStreams[1]?.emitError(new Error("drop-2"));
		await waitFor(
			() => registryStreams.length >= 3,
			"third registry stream after reconnect 2",
		);
		controlStreams[2]?.emitData({
			welcome: { sessionId: "s3", serviceId: "svc", serviceName: "n" },
		});
		await tick();

		// Emit a single PolicyEvaluation carrying one warning on the latest stream.
		// With the listener registered once (not per openSession), exactly one
		// policy_violation must fire — not three.
		const latest = registryStreams[registryStreams.length - 1]!;
		latest.emitEvent(snapshotWithWarnings(["rpc.call"]));
		await tick();

		expect(violations.length).toBe(1);
		expect(violations[0]?.declaration).toBe("rpc.call");
	});
});

describe("ServiceBridge cert rotation (overlap)", () => {
	test("waits for Welcome on new session before closing old", async () => {
		const oldStream = new FakeServerStream();
		const newStream = new FakeServerStream();
		const streams = [oldStream, newStream];
		let streamIdx = 0;

		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => ({
				...fakeProvisionResult(),
				notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 1),
			}),
			refreshFn: async () => ({
				...fakeProvisionResult(),
				notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 3600),
			}),
			clientFactory: () => {
				const s = streams[streamIdx++];
				if (!s) throw new Error("test: stream index out of range");
				return makeFakeClient(s);
			},
			certRefreshLeadMs: 0,
			certRefreshJitterMs: 0,
		});
		activeBridges.push(sb);

		const reconnects: ReconnectingEvent[] = [];
		const disconnects: DisconnectedEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));
		sb.on("disconnected", (e) => disconnects.push(e));

		await sb.start();
		oldStream.emitData({
			welcome: {
				sessionId: "s1",
				serviceId: "svc",
				serviceName: "svc-name",
			},
		});

		// Wait for the cert-refresh timer to fire (TTL=1s, lead=0) and the new
		// session to be opened. The second clientFactory call (streamIdx===2) is
		// the deterministic signal that rotation started; the fixed sleep alone is
		// a real production timer, but on a slow runner the rotation may not have
		// progressed past openSession yet.
		await waitFor(() => streamIdx === 2, "rotation opened the new session");

		// New session opened — old NOT closed yet (still no Welcome on new).
		expect(oldStream.cancelled).toBe(false);

		// Deliver Welcome on new stream — now old must be closed.
		newStream.emitData({
			welcome: {
				sessionId: "s2",
				serviceId: "svc",
				serviceName: "svc-name",
			},
		});
		await waitFor(
			() => oldStream.cancelled,
			"old session closed after new Welcome",
		);

		expect(oldStream.cancelled).toBe(true);
		expect(reconnects.length).toBe(0);
		expect(disconnects.filter((d) => !d.reason.startsWith("drain:"))).toEqual(
			[],
		);
	});

	test("rotation failure emits reconnecting (does NOT silently swallow)", async () => {
		const oldStream = new FakeServerStream();

		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => ({
				...fakeProvisionResult(),
				notAfterUnix: BigInt(Math.floor(Date.now() / 1000) + 1),
			}),
			refreshFn: async () => {
				throw new Error("rotate-fail");
			},
			clientFactory: () => makeFakeClient(oldStream),
			certRefreshLeadMs: 0,
			certRefreshJitterMs: 0,
			reconnectIntervalMs: 1_000_000,
			reconnectAttempts: 3,
		});
		activeBridges.push(sb);

		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));

		await sb.start();
		oldStream.emitData({
			welcome: {
				sessionId: "s1",
				serviceId: "svc",
				serviceName: "svc-name",
			},
		});

		await waitFor(
			() => reconnects.length > 0,
			"rotation failure surfaced a reconnecting event",
		);
		expect(reconnects.length).toBeGreaterThan(0);
		const rot0 = reconnects[0];
		expect(rot0?.reason).toContain("rotate-fail");
	});
});

describe("ServiceBridge non-retryable errors (H11)", () => {
	test("UNAUTHENTICATED provision error emits disconnected with ServiceBridgeError and does NOT reconnect", async () => {
		const unauthErr = Object.assign(new Error("invalid key"), {
			code: GrpcStatus.UNAUTHENTICATED,
		});
		let provisionCalls = 0;
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				provisionCalls++;
				throw unauthErr;
			},
			reconnectIntervalMs: 5,
			reconnectAttempts: 3,
		});
		activeBridges.push(sb);

		const disconnects: DisconnectedEvent[] = [];
		const reconnects: ReconnectingEvent[] = [];
		sb.on("disconnected", (e) => disconnects.push(e));
		sb.on("reconnecting", (e) => reconnects.push(e));

		await sb.start();
		await new Promise((r) => setTimeout(r, 50));

		expect(disconnects.length).toBe(1);
		const unauth0 = disconnects[0];
		expect(unauth0?.error).toBeInstanceOf(ServiceBridgeError);
		expect(unauth0?.error?.code).toBe(GrpcStatus.UNAUTHENTICATED);
		expect(reconnects.length).toBe(0);
		expect(provisionCalls).toBe(1);
	});

	test("UNAVAILABLE provision error retries normally", async () => {
		const unavailErr = Object.assign(new Error("service unavailable"), {
			code: GrpcStatus.UNAVAILABLE,
		});
		const sb = new ServiceBridge("localhost:0", VALID_KEY, {
			advertise: false,
			_disableTelemetryTransport: true,
			provisionFn: async () => {
				throw unavailErr;
			},
			reconnectIntervalMs: 5,
			reconnectAttempts: 2,
		});
		activeBridges.push(sb);

		const reconnects: ReconnectingEvent[] = [];
		sb.on("reconnecting", (e) => reconnects.push(e));
		sb.on("disconnected", () => {});

		await sb.start();
		await new Promise((r) => setTimeout(r, 80));

		expect(reconnects.length).toBeGreaterThan(0);
	});
});

function tick(): Promise<void> {
	return new Promise((r) => setImmediate(r));
}

// waitFor polls `predicate` until it returns true or the timeout elapses.
// Replaces fixed `await tick()` timing assumptions on the async
// provision/Welcome/rotation chain so the tests stay deterministic on a slow
// CI runner where a single microtask turn is not enough to let the chain
// settle. On timeout it throws with `label` so the failure points at the exact
// condition that never became true rather than at a stale assertion.
async function waitFor(
	predicate: () => boolean,
	label: string,
	timeoutMs = 3000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() > deadline) {
			throw new Error(`waitFor timed out after ${timeoutMs}ms: ${label}`);
		}
		await new Promise((r) => setTimeout(r, 5));
	}
}

// once resolves with the first event of the given kind emitted by the bridge,
// or rejects on timeout with `event` named so a missed emission is obvious.
// ServiceBridge exposes a one-shot-friendly `on(event, handler)`; the handler
// fires for every emission but we only resolve once.
function once<E extends "connected" | "reconnecting" | "disconnected">(
	sb: ServiceBridge,
	event: E,
	timeoutMs = 3000,
): Promise<void> {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => {
			reject(new Error(`once timed out after ${timeoutMs}ms: '${event}'`));
		}, timeoutMs);
		let fired = false;
		sb.on(event, () => {
			if (fired) return;
			fired = true;
			clearTimeout(timer);
			resolve();
		});
	});
}
