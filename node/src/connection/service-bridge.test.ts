// service-bridge.test.ts — the connection lifecycle against fake Control and
// Registry streams: start() waits for Welcome + snapshot, reconnects count
// consecutive failures, terminal errors stop, rotation swaps TLS material
// without reopening anything, stop() is ordered and idempotent.

import { afterEach, describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { status as GrpcStatus } from "@grpc/grpc-js";
import { testTls } from "../../tests/helpers/tls";
import {
	ConfigurationError,
	StateError,
	TimeoutError,
	ValidationError,
} from "../errors";
import { silentLogger } from "../logger";
import { BootstrapKeyPayload } from "../pb/servicebridge/v1/bootstrap";
import type {
	ControlClient,
	OpenRequest,
	ServerControl,
} from "../pb/servicebridge/v1/control";
import type {
	RegisterRequest,
	RegistryClient,
	RegistryEvent,
} from "../pb/servicebridge/v1/registry";
import type { MetricPoint } from "../pb/servicebridge/v1/telemetry";
import { PROTOCOL_VERSION } from "./handshake";
import type { ProvisionResult } from "./provision";
import {
	type DisconnectedEvent,
	type ReconnectingEvent,
	ServiceBridge,
	type ServiceBridgeOptions,
} from "./service-bridge";
import { ConnectionError } from "./service-bridge-error";

const VALID_KEY = (() => {
	const bytes = BootstrapKeyPayload.encode({
		keyId: Buffer.alloc(8, 0x01),
		secret: Buffer.alloc(32, 0x02),
		caCertDer: Buffer.alloc(1, 0xff),
	}).finish();
	return `sb.${Buffer.from(bytes).toString("base64url")}`;
})();

class FakeStream extends EventEmitter {
	cancelled = false;
	cancel(): void {
		if (this.cancelled) return;
		this.cancelled = true;
		this.emit("error", Object.assign(new Error("cancelled"), { code: 1 }));
	}
}

function grpcError(code: number, message = "x"): Error {
	return Object.assign(new Error(`${code} ${message}`), {
		code,
		details: message,
	});
}

function provision(over: Partial<ProvisionResult> = {}): ProvisionResult {
	return {
		certDer: testTls.certDer,
		caChainDer: testTls.certDer,
		serviceId: "svc",
		serviceName: "svc-name",
		instanceId: "inst",
		notAfterUnixMs: Date.now() + 3_600_000,
		privateKey: {} as CryptoKey,
		privateKeyDer: testTls.privateKeyDer,
		...over,
	};
}

interface Harness {
	sb: ServiceBridge;
	control: FakeStream[];
	opens: OpenRequest[];
	registry: FakeStream[];
	registers: RegisterRequest[];
	clients: { control: number; registry: number; closed: number };
	provisions: () => number;
	welcome(protocolVersion?: number): void;
	snapshot(): void;
}

const bridges: ServiceBridge[] = [];
afterEach(async () => {
	for (const b of bridges.splice(0)) await b.stop();
});

function harness(
	opts: Partial<ServiceBridgeOptions> & Record<string, unknown> = {},
): Harness {
	const control: FakeStream[] = [];
	const opens: OpenRequest[] = [];
	const registry: FakeStream[] = [];
	const registers: RegisterRequest[] = [];
	const clients = { control: 0, registry: 0, closed: 0 };
	let provisions = 0;
	const sb = new ServiceBridge("localhost:1", VALID_KEY, {
		advertise: false,
		logger: silentLogger,
		_disableTelemetryTransport: true,
		reconnectIntervalMs: 5,
		startTimeoutMs: 2_000,
		stopTimeoutMs: 200,
		provisionFn: async () => {
			provisions++;
			return provision();
		},
		controlClientFactory: () => {
			clients.control++;
			return {
				open: (req: OpenRequest) => {
					opens.push(req);
					const s = new FakeStream();
					control.push(s);
					return s;
				},
				close: () => {
					clients.closed++;
				},
			} as unknown as ControlClient;
		},
		registryClientFactory: () => {
			clients.registry++;
			return {
				registerAndWatch: (req: RegisterRequest) => {
					registers.push(req);
					const s = new FakeStream();
					registry.push(s);
					return s;
				},
				close: () => {
					clients.closed++;
				},
			} as unknown as RegistryClient;
		},
		...opts,
	} as ServiceBridgeOptions);
	bridges.push(sb);
	return {
		sb,
		control,
		opens,
		registry,
		registers,
		clients,
		provisions: () => provisions,
		welcome(protocolVersion = PROTOCOL_VERSION) {
			const s = control[control.length - 1];
			s?.emit("data", {
				welcome: {
					sessionId: `s${control.length}`,
					serviceId: "svc",
					serviceName: "svc-name",
					runtimeVersion: "test",
					protocolVersion,
				},
			} satisfies ServerControl);
		},
		snapshot() {
			const s = registry[registry.length - 1];
			s?.emit("data", {
				snapshot: {
					methods: [],
					instances: [],
					eventSubscriptions: [],
					outgoingCalls: [],
					policy: {
						capabilities: [],
						egress: [],
						acceptance: [],
						warnings: [],
					},
				},
			} satisfies RegistryEvent);
		},
	};
}

async function waitFor(
	predicate: () => boolean,
	label: string,
	timeoutMs = 2000,
) {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() > deadline) throw new Error(`waitFor timed out: ${label}`);
		await new Promise((r) => setTimeout(r, 2));
	}
}

// startLive runs start() and plays the runtime's part until it resolves.
async function startLive(h: Harness): Promise<void> {
	const started = h.sb.start();
	await waitFor(() => h.control.length > 0, "Control.Open");
	h.welcome();
	await waitFor(() => h.registry.length > 0, "RegisterAndWatch");
	h.snapshot();
	await started;
}

describe("start()", () => {
	test("resolves only after Welcome AND the first registry snapshot", async () => {
		const h = harness();
		let done = false;
		const started = h.sb.start().then(() => {
			done = true;
		});
		await waitFor(() => h.control.length > 0, "open");
		h.welcome();
		await waitFor(() => h.registry.length > 0, "watch");
		await new Promise((r) => setTimeout(r, 10));
		expect(done).toBe(false);
		h.snapshot();
		await started;
		expect(h.sb.identity()).toEqual({
			sessionId: "s1",
			serviceId: "svc",
			serviceName: "svc-name",
			instanceId: "inst",
		});
	});

	test("sends the handshake identity on Control.Open and RegisterAndWatch", async () => {
		const h = harness();
		await startLive(h);
		expect(h.opens[0]).toMatchObject({
			protocolVersion: PROTOCOL_VERSION,
			sdkLanguage: "node",
		});
		expect(h.registers[0]?.protocolVersion).toBe(PROTOCOL_VERSION);
	});

	test("emits connected with the runtime version", async () => {
		const h = harness();
		const connected: string[] = [];
		h.sb.on("connected", (e) => connected.push(e.runtimeVersion));
		await startLive(h);
		expect(connected).toEqual(["test"]);
	});

	test("rejects with TimeoutError and stops when nothing arrives in time", async () => {
		const h = harness({ startTimeoutMs: 30 });
		await expect(h.sb.start()).rejects.toBeInstanceOf(TimeoutError);
		await expect(h.sb.ready()).rejects.toBeInstanceOf(StateError);
	});

	test("an incompatible protocol on Welcome is a terminal configuration error", async () => {
		const h = harness();
		const started = h.sb.start();
		await waitFor(() => h.control.length > 0, "open");
		h.welcome(99);
		await expect(started).rejects.toBeInstanceOf(ConfigurationError);
	});

	test("FAILED_PRECONDITION on Control.Open (protocol refused) stops without reconnect", async () => {
		const h = harness();
		const started = h.sb.start();
		await waitFor(() => h.control.length > 0, "open");
		h.control[0]?.emit(
			"error",
			grpcError(
				GrpcStatus.FAILED_PRECONDITION,
				"protocol version 1 is not supported",
			),
		);
		await expect(started).rejects.toBeInstanceOf(ConnectionError);
		await new Promise((r) => setTimeout(r, 20));
		expect(h.control).toHaveLength(1);
	});

	test("a rejected key (UNAUTHENTICATED provision) stops at once", async () => {
		const disconnected: DisconnectedEvent[] = [];
		const h = harness({
			provisionFn: async () => {
				throw new ConnectionError(
					"provision",
					grpcError(GrpcStatus.UNAUTHENTICATED),
				);
			},
		});
		h.sb.on("disconnected", (e) => disconnected.push(e));
		const err = await h.sb.start().catch((e) => e);
		expect(err).toBeInstanceOf(ConnectionError);
		expect((err as ConnectionError).grpcCode).toBe(GrpcStatus.UNAUTHENTICATED);
		expect(disconnected).toHaveLength(1);
	});

	test("a second start() is a StateError", async () => {
		const h = harness();
		await startLive(h);
		await expect(h.sb.start()).rejects.toBeInstanceOf(StateError);
	});

	test("an invalid option is rejected at construction", () => {
		expect(
			() =>
				new ServiceBridge("localhost:1", VALID_KEY, { maxPendingPublishes: 0 }),
		).toThrow(ConfigurationError);
		expect(
			() =>
				new ServiceBridge("localhost:1", VALID_KEY, {
					callDefaults: { timeout: "soon" },
				}),
		).toThrow(ConfigurationError);
		expect(() => new ServiceBridge("localhost", VALID_KEY)).toThrow(
			ConfigurationError,
		);
	});
});

describe("reconnect", () => {
	test("counts consecutive failures and resets on Welcome; reuses the cached cert", async () => {
		const failing = false;
		let provisions = 0;
		const h = harness({
			provisionFn: async () => {
				provisions++;
				return provision();
			},
		});
		const attempts: ReconnectingEvent[] = [];
		h.sb.on("reconnecting", (e) => attempts.push(e));
		await startLive(h);

		// Session lost: one failed attempt, then Welcome on the next stream.
		h.control[0]?.emit("end");
		await waitFor(() => h.control.length === 2, "second open");
		h.welcome();
		await waitFor(() => h.sb.identity()?.sessionId === "s2", "second session");
		// Lost again: the count starts over at 1.
		h.control[1]?.emit("error", grpcError(GrpcStatus.UNAVAILABLE));
		await waitFor(() => attempts.length === 2, "second reconnecting");
		expect(attempts.map((a) => a.attempt)).toEqual([1, 1]);
		expect(provisions).toBe(1);
		void failing;
	});

	test("unlimited by default; reconnectAttempts gives up after that many consecutive failures", async () => {
		const disconnected: DisconnectedEvent[] = [];
		let calls = 0;
		const h = harness({
			reconnectAttempts: 2,
			provisionFn: async () => {
				calls++;
				throw grpcError(GrpcStatus.UNAVAILABLE);
			},
			startTimeoutMs: 1_000,
		});
		h.sb.on("disconnected", (e) => disconnected.push(e));
		await expect(h.sb.start()).rejects.toBeInstanceOf(ConnectionError);
		expect(calls).toBe(3);
		expect(disconnected[0]?.reason).toContain("gave up after 2");
	});

	test("a channel is built once and survives every reconnect", async () => {
		const h = harness();
		await startLive(h);
		for (let i = 0; i < 3; i++) {
			const last = h.control[h.control.length - 1];
			last?.emit("end");
			await waitFor(() => h.control.length === i + 2, "reopen");
			h.welcome();
			await waitFor(() => h.sb.identity()?.sessionId === `s${i + 2}`, "live");
		}
		expect(h.clients.control).toBe(1);
		expect(h.clients.registry).toBe(1);
	});

	test("Drain emits draining; the following stream end reconnects", async () => {
		const h = harness();
		const drains: string[] = [];
		h.sb.on("draining", (e) => drains.push(e.reason));
		await startLive(h);
		h.control[0]?.emit("data", { drain: { reason: "runtime shutting down" } });
		h.control[0]?.emit("end");
		await waitFor(() => h.control.length === 2, "reconnect after drain");
		expect(drains).toEqual(["runtime shutting down"]);
	});

	test("ready() waits for the next session after a loss", async () => {
		const h = harness();
		await startLive(h);
		await h.sb.ready();
		h.control[0]?.emit("end");
		let ready = false;
		const waiting = h.sb.ready().then(() => {
			ready = true;
		});
		await waitFor(() => h.control.length === 2, "reopen");
		await new Promise((r) => setTimeout(r, 5));
		expect(ready).toBe(false);
		h.welcome();
		await new Promise((r) => setTimeout(r, 5));
		// Welcome alone is not enough: the new session's snapshot is.
		expect(ready).toBe(false);
		h.snapshot();
		await waiting;
		expect(ready).toBe(true);
	});

	test("a registry stream rejected with INVALID_ARGUMENT is terminal", async () => {
		const h = harness();
		const disconnected: DisconnectedEvent[] = [];
		h.sb.on("disconnected", (e) => disconnected.push(e));
		await startLive(h);
		h.registry[h.registry.length - 1]?.emit(
			"error",
			grpcError(
				GrpcStatus.INVALID_ARGUMENT,
				"event subscription a.b: invalid filter",
			),
		);
		await waitFor(() => disconnected.length === 1, "terminal");
		expect(disconnected[0]?.error).toBeInstanceOf(ValidationError);
	});
});

describe("certificate rotation", () => {
	test("swaps the leaf without reopening Control, channels or the registry stream", async () => {
		const refreshed: ProvisionResult[] = [];
		const h = harness({
			certRefreshLeadMs: 3_600_000 - 50,
			certRefreshJitterMs: 0,
			refreshFn: async (_c: unknown, prev: ProvisionResult) => {
				const next = provision({ notAfterUnixMs: Date.now() + 3_600_000 });
				refreshed.push(next);
				expect(prev.instanceId).toBe("inst");
				return next;
			},
		});
		await startLive(h);
		const registryStreams = h.registry.length;
		await waitFor(() => refreshed.length >= 1, "refresh");
		await new Promise((r) => setTimeout(r, 20));
		expect(h.control).toHaveLength(1);
		expect(h.registry).toHaveLength(registryStreams);
		expect(h.clients.control).toBe(1);
		expect(h.sb.identity()?.instanceId).toBe("inst");
		expect(h.sb.identity()?.sessionId).toBe("s1");
	});

	test("a rate-limited refresh is retried later, the bridge keeps running", async () => {
		let calls = 0;
		const h = harness({
			certRefreshLeadMs: 3_600_000 - 20,
			certRefreshJitterMs: 0,
			refreshFn: async () => {
				calls++;
				throw grpcError(GrpcStatus.RESOURCE_EXHAUSTED, "refresh rate");
			},
		});
		await startLive(h);
		await waitFor(() => calls === 1, "first refresh");
		await new Promise((r) => setTimeout(r, 20));
		expect(h.sb.identity()).not.toBeNull();
	});
});

describe("listeners and telemetry identity", () => {
	test("a throwing listener does not break the bridge or other listeners", async () => {
		const h = harness();
		const seen: string[] = [];
		h.sb.on("connected", () => {
			throw new Error("listener bug");
		});
		h.sb.on("connected", () => seen.push("second"));
		await startLive(h);
		expect(seen).toEqual(["second"]);
	});

	test("a metric handle taken before Welcome rebinds to the real instance_id", async () => {
		const h = harness();
		const hits = h.sb.telemetry.counter("requests_total");
		hits.inc(2);
		await startLive(h);
		hits.inc(5);
		const ring = (
			h.sb as unknown as {
				telemetryRing: {
					metrics: { drain(): MetricPoint[] };
					peek(n: number): Array<{ kind: string; message: unknown }>;
				};
			}
		).telemetryRing;
		const points = [
			...ring
				.peek(1000)
				.filter((i) => i.kind === "metrics")
				.map((i) => i.message as MetricPoint),
			...ring.metrics.drain(),
		]
			.filter((p) => p.name === "requests_total")
			.map((p) => [p.instanceId, p.value])
			.sort();
		expect(points).toEqual([
			["", 2],
			["inst", 5],
		]);
	});
});

describe("stop()", () => {
	test("is idempotent, closes every channel and rejects later ready()", async () => {
		const h = harness();
		await startLive(h);
		await h.sb.stop();
		await h.sb.stop();
		expect(h.clients.closed).toBe(2);
		await expect(h.sb.ready()).rejects.toBeInstanceOf(StateError);
		expect(h.sb.identity()).toBeNull();
	});

	test("stop() during provision builds nothing behind it", async () => {
		let release!: (p: ProvisionResult) => void;
		const h = harness({
			provisionFn: () =>
				new Promise<ProvisionResult>((r) => {
					release = r;
				}),
		});
		const started = h.sb.start().catch((e) => e);
		await waitFor(() => release !== undefined, "provision pending");
		await h.sb.stop();
		release(provision());
		expect(await started).toBeInstanceOf(StateError);
		await new Promise((r) => setTimeout(r, 10));
		expect(h.clients.control).toBe(0);
	});

	test("publish after stop is a StateError", async () => {
		const h = harness();
		await startLive(h);
		await h.sb.stop();
		await expect(h.sb.event.publish("a.b", {})).rejects.toBeInstanceOf(
			StateError,
		);
	});
});
