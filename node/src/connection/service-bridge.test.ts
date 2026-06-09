import { afterEach, describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { status as GrpcStatus } from "@grpc/grpc-js";
import { BootstrapKeyPayload } from "../pb/servicebridge/v1/bootstrap";
import type {
	ControlClient,
	ServerControl,
} from "../pb/servicebridge/v1/control";
import type { ProvisionResult } from "./provision";
import {
	type DisconnectedEvent,
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
