import * as grpc from "@grpc/grpc-js";
import {
	AccessDeniedError,
	ConfigurationError,
	type ServiceBridgeError,
	StateError,
	TimeoutError,
	ValidationError,
} from "../errors";
import { EventDomain } from "../events/domain";
import {
	DEFAULT_MAX_PENDING_PUBLISHES,
	DEFAULT_PUBLISH_TIMEOUT_MS,
	Publisher,
	type SchemaIndex,
} from "../events/publisher";
import { DEFAULT_EVENTS_MAX_IN_FLIGHT, Subscriber } from "../events/subscriber";
import { JobDomain } from "../job/domain";
import { JobSubscriber } from "../job/subscriber";
import { consoleLogger, type Logger } from "../logger";
import { ControlClient } from "../pb/servicebridge/v1/control";
import { EventsClient } from "../pb/servicebridge/v1/events";
import { JobsClient } from "../pb/servicebridge/v1/jobs";
import type {
	EventSubscriptionDescriptor,
	OutgoingCallDescriptor,
	PolicyEvaluation,
	RegistryClient,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { RegistryClient as RegistryClientImpl } from "../pb/servicebridge/v1/registry";
import { TelemetryClient } from "../pb/servicebridge/v1/telemetry";
import { WorkflowsClient } from "../pb/servicebridge/v1/workflows";
import type { MethodDescriptor, ServiceDeps } from "../registry/registry";
import { MethodType, Registry } from "../registry/registry";
import { WatchStream } from "../registry/watch";
import { CircuitBreakerRegistry } from "../rpc/circuit-breaker";
import type { CallOpts } from "../rpc/client";
import {
	type RpcCaller,
	RpcClient,
	SchemaRegistry,
	timeoutMs,
} from "../rpc/client";
import { DirectTransport } from "../rpc/direct-transport";
import { RpcDomain } from "../rpc/domain";
import { InstanceCache } from "../rpc/instance-cache";
import { LoadBalancer } from "../rpc/lb";
import { ProxyTransport } from "../rpc/proxy-transport";
import { type AdvertiseConfig, CallServer } from "../rpc/server";
import { extractServiceMethods, type TypedClient } from "../rpc/typed-client";
import { buildSchemaPair, type SchemaSpec } from "../serde/serializer";
import { currentTraceContext, runWithTrace } from "../telemetry/context";
import { type LogFields, makeLogger } from "../telemetry/logs";
import {
	type Labels,
	makeCounter,
	makeGauge,
	makeHistogram,
} from "../telemetry/metrics";
import {
	Channel,
	OpHandle,
	type StartOpParams,
	UserSubOp,
} from "../telemetry/ops";
import type { CaptureMode } from "../telemetry/payload-capture";
import { ProcessSampler } from "../telemetry/process-sampler";
import { TelemetryRing } from "../telemetry/ring";
import {
	adaptTelemetryClient,
	type DropObserver,
	TelemetryTransport,
} from "../telemetry/transport";
import { formatXSbTrace, parseXSbTrace } from "../telemetry/wire-trace";
import { reconnectDelay } from "../utils/reconnect-ladder";
import { WorkflowDomain } from "../workflow/domain";
import { makeRuntimeOps } from "../workflow/runtime-ops";
import { WorkflowSubscriber } from "../workflow/subscriber";
import type { Step } from "../workflow/types";
import { PROTOCOL_VERSION } from "./handshake";
import { parseBootstrapKey } from "./key";
import type { ProvisionResult } from "./provision";
import { provision as defaultProvision, refresh } from "./provision";
import { ConnectionError, isTerminal } from "./service-bridge-error";
import { openControlStream, Session } from "./session";
import { makeSpiffeCheck, RUNTIME_SPIFFE_URI } from "./spiffe";
import { CertificateStore, CLIENT_CHANNEL_OPTIONS } from "./tls-material";

export type { ServiceInstanceInfo } from "../pb/servicebridge/v1/registry";
export type {
	MethodDescriptor,
	MethodType,
	RpcHandlerOpts,
	ServiceDeps,
	WorkflowHandlerOpts,
} from "../registry/registry";
export type { CallOpts } from "../rpc/client";
export type { AdvertiseConfig } from "../rpc/server";
export type { SchemaSpec } from "../serde/serializer";

/**
 * Entry of `sb.serviceMap()`: the methods of a service visible to this caller
 * and its live instances with their endpoints (`httpEndpoint` is the user's
 * own HTTP server, ADR 0001).
 */
export interface ServiceMapEntry {
	methods: MethodDescriptor[];
	instances: ServiceInstanceInfo[];
	eventSubscriptions: EventSubscriptionDescriptor[];
	outgoingCalls: OutgoingCallDescriptor[];
}

// Defaults. The same values in the Go SDK — see ../../../README.md (parity).
const DEFAULT_START_TIMEOUT_MS = 30_000;
const DEFAULT_STOP_TIMEOUT_MS = 10_000;
// Telemetry ops-ring byte budget, sized for dense workflow step spans.
const DEFAULT_TELEMETRY_RING_SIZE = 256 * 1024;
// Certificate renewal: 30 min before expiry, spread over a 5 min window so a
// fleet started together does not refresh in lockstep.
const CERT_REFRESH_LEAD_MS = 30 * 60 * 1000;
const CERT_REFRESH_JITTER_MS = 5 * 60 * 1000;
// A refresh refused for rate or an outage is retried this much later.
const CERT_REFRESH_RETRY_MS = 60_000;
// How long a stopping bridge waits for the final telemetry acknowledgement.
const TELEMETRY_STOP_ACK_MS = 2_000;

function randomJitter(maxMs: number): number {
	if (maxMs <= 0) return 0;
	return Math.floor(Math.random() * maxMs);
}

/**
 * TelemetryAPI is the public surface for emitting telemetry from user code
 * and from the SDK's own subsystems.
 *
 * @public — см. ../telemetry/README.md
 */
export interface TelemetryAPI {
	/** Start an op; returns a handle to `.end(status, message?)`. */
	startOp(params: StartOpParams): OpHandle;
	/**
	 * Whether the runtime currently wants telemetry at all. `true` until the
	 * first registry snapshot; ask per emission, the operator can flip it live.
	 */
	enabled(): boolean;
	/** Runtime-pushed payload capture mode of a channel ("none" before the first snapshot). */
	captureModeForChannel(channel: Channel): CaptureMode;
	log: ReturnType<typeof makeLogger>;
	counter(name: string, labels?: Labels): ReturnType<typeof makeCounter>;
	gauge(name: string, labels?: Labels): ReturnType<typeof makeGauge>;
	histogram(
		name: string,
		unit?: string,
		labels?: Labels,
	): ReturnType<typeof makeHistogram>;
}

// makeTelemetryAPI builds the telemetry surface. Every runtime-pushed input is
// a getter: identity appears at the first Welcome, `enabled` and the payload
// cap change whenever the operator edits the settings.
function makeTelemetryAPI(
	ring: TelemetryRing,
	getInstanceId: () => string,
	getEnabled: () => boolean,
	getCaptureModeForChannel: (channel: Channel) => CaptureMode,
	getPayloadMaxBytes: () => number,
): TelemetryAPI {
	const emit = (level: "debug" | "info" | "warn" | "error") => {
		return (message: string, fields?: LogFields) => {
			makeLogger(ring, getInstanceId())[level](message, fields);
		};
	};
	return {
		startOp(params) {
			return OpHandle.start(ring, {
				...params,
				effectiveCaptureMode: getCaptureModeForChannel(params.channel),
				payloadMaxBytes: getPayloadMaxBytes(),
			});
		},
		enabled: getEnabled,
		captureModeForChannel: getCaptureModeForChannel,
		log: {
			debug: emit("debug"),
			info: emit("info"),
			warn: emit("warn"),
			error: emit("error"),
		},
		counter(name, labels) {
			const series = lazySeries(getInstanceId, (id) =>
				makeCounter(ring, id, name, labels),
			);
			return { inc: (amount) => series().inc(amount) };
		},
		gauge(name, labels) {
			const series = lazySeries(getInstanceId, (id) =>
				makeGauge(ring, id, name, labels),
			);
			return { set: (value) => series().set(value) };
		},
		histogram(name, unit, labels) {
			const series = lazySeries(getInstanceId, (id) =>
				makeHistogram(ring, id, name, unit, labels),
			);
			return { observe: (value) => series().observe(value) };
		},
	};
}

// lazySeries binds a metric handle to the series of the current instance_id
// and rebinds when it changes, so a counter taken before start() does not stay
// pinned to the empty identity.
function lazySeries<T>(
	getInstanceId: () => string,
	build: (instanceId: string) => T,
): () => T {
	let boundId: string | null = null;
	let handle: T | null = null;
	return () => {
		const id = getInstanceId();
		if (handle === null || boundId !== id) {
			boundId = id;
			handle = build(id);
		}
		return handle;
	};
}

export interface ConnectedEvent {
	sessionId: string;
	serviceId: string;
	serviceName: string;
	runtimeVersion: string;
}

/** Identity of the current live session (`sb.identity()`). */
export interface Identity {
	sessionId: string;
	serviceId: string;
	serviceName: string;
	instanceId: string;
}

export interface ReconnectingEvent {
	/** Consecutive failed attempts so far (1 on the first retry). */
	attempt: number;
	delayMs: number;
	reason: string;
}

export interface DisconnectedEvent {
	reason: string;
	error?: ServiceBridgeError;
}

export interface DrainingEvent {
	reason: string;
}

/**
 * One declaration the runtime's access policy refused (registration warning)
 * or one call-time denial.
 */
export interface PolicyViolationEvent {
	declaration: string;
	value: string;
	denySide: string;
	reason: string;
}

type EventMap = {
	connected: ConnectedEvent;
	reconnecting: ReconnectingEvent;
	/** The bridge stopped for good (terminal error or attempts exhausted). */
	disconnected: DisconnectedEvent;
	/** The runtime announced a shutdown; the bridge reconnects afterwards. */
	draining: DrainingEvent;
	policy_violation: PolicyViolationEvent;
};

type Handler<K extends keyof EventMap> = (event: EventMap[K]) => void;

/** Public configuration. Documented in `./README.md` (Public contract). */
export interface ServiceBridgeOptions {
	/** Flat reconnect delay; unset → jittered ladder 1s,5s,15s,30s,60s. */
	reconnectIntervalMs?: number;
	/** Consecutive failed attempts before giving up; 0 (default) = never. */
	reconnectAttempts?: number;
	/**
	 * Inbound Call server address. `{host, port}` explicit (required in k8s);
	 * undefined → 127.0.0.1 on a free port with a warning; `false` → caller-only.
	 */
	advertise?: AdvertiseConfig | false;
	/** Defaults for every outbound call (sb.rpc.call, sb.stream, typed clients). */
	callDefaults?: CallOpts;
	/** Stop when the runtime reports a policy violation. Default false. */
	failOnPolicyViolation?: boolean;
	/** How long publish() may wait for the runtime's acknowledgement. Default 30000. */
	publishTimeoutMs?: number;
	/** Publishes waiting for the runtime before publish() fails with QUEUE_FULL. Default 10000. */
	maxPendingPublishes?: number;
	/** Concurrently handled event deliveries. Default 32. */
	eventsMaxInFlight?: number;
	/** Inbound handlers running at once. Default 256. */
	rpcMaxConcurrentCalls?: number;
	/** Inbound calls waiting for a slot before RESOURCE_EXHAUSTED. Default = rpcMaxConcurrentCalls. */
	rpcMaxQueuedCalls?: number;
	/** start() deadline: Welcome and the first registry snapshot. Default 30000. */
	startTimeoutMs?: number;
	/** stop() drain deadline for in-flight work and queued publishes. Default 10000. */
	stopTimeoutMs?: number;
	/** Sink of the SDK's own diagnostics. Default: warn/error to the console. */
	logger?: Logger;
	/** Telemetry options. */
	telemetry?: {
		/** Called when the ring or the runtime dropped telemetry. */
		onDrop?: DropObserver;
	};
}

/** @internal см. ./README.md */
interface ServiceBridgeInternalHooks extends ServiceBridgeOptions {
	certRefreshLeadMs?: number;
	certRefreshJitterMs?: number;
	provisionFn?: typeof defaultProvision;
	refreshFn?: typeof refresh;
	controlClientFactory?: (
		url: string,
		creds: grpc.ChannelCredentials,
	) => ControlClient;
	registryClientFactory?: (
		url: string,
		creds: grpc.ChannelCredentials,
	) => RegistryClient;
	_disableTelemetryTransport?: boolean;
}

interface ResolvedOptions {
	reconnectIntervalMs: number | undefined;
	reconnectAttempts: number;
	advertise: AdvertiseConfig | null;
	callDefaults: CallOpts;
	failOnPolicyViolation: boolean;
	publishTimeoutMs: number;
	maxPendingPublishes: number;
	eventsMaxInFlight: number;
	rpcMaxConcurrentCalls: number | undefined;
	rpcMaxQueuedCalls: number | undefined;
	startTimeoutMs: number;
	stopTimeoutMs: number;
	onDrop: DropObserver | undefined;
	certRefreshLeadMs: number;
	certRefreshJitterMs: number;
	provisionFn: typeof defaultProvision;
	refreshFn: typeof refresh;
	controlClientFactory: (
		url: string,
		creds: grpc.ChannelCredentials,
	) => ControlClient;
	registryClientFactory: (
		url: string,
		creds: grpc.ChannelCredentials,
	) => RegistryClient;
	disableTelemetryTransport: boolean;
}

// Channels to the runtime. Built once from the first certificate and kept for
// the life of the bridge: their credentials follow the CertificateStore, so a
// rotation reaches their next handshake and a reconnect is grpc-js's own.
interface RuntimeChannels {
	control: ControlClient;
	registry: RegistryClient;
	events: EventsClient;
	jobs: JobsClient;
	workflows: WorkflowsClient;
	telemetry: TelemetryClient | null;
	proxy: ProxyTransport;
	direct: DirectTransport;
}

type Waiter = { resolve: () => void; reject: (err: Error) => void };

/**
 * ServiceBridge is the SDK root: it owns the connection to the runtime, the
 * registry view, the inbound Call server, every domain (rpc, event, workflow,
 * job) and the telemetry pipeline.
 *
 * Lifecycle: start() provisions a leaf certificate, binds the Call server,
 * opens Control.Open and the registry stream, and resolves once the runtime
 * has welcomed the session AND sent the first registry snapshot. A lost
 * session reconnects on the jittered ladder (consecutive failures; reset on
 * Welcome) until a terminal error. The certificate is renewed before expiry
 * without rebuilding a single stream: the instance_id stays the same.
 */
export class ServiceBridge {
	private readonly url: string;
	private readonly rawKey: string;
	private readonly opts: ResolvedOptions;
	private readonly log: Logger;
	private readonly handlers = new Map<
		keyof EventMap,
		Handler<keyof EventMap>[]
	>();

	private started = false;
	private stopped = false;
	// Bumped by stop(): async paths re-check it after every await so a stop
	// landing mid-flight does not let them rebuild behind it.
	private generation = 0;
	private live = false;
	private failures = 0;
	private session: Session | null = null;
	private lastProvision: ProvisionResult | null = null;
	private store: CertificateStore | null = null;
	private channels: RuntimeChannels | null = null;
	private certRefreshTimer: ReturnType<typeof setTimeout> | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private currentIdentity: Identity | null = null;
	private telemetryInstanceId = "";
	private readyWaiters: Waiter[] = [];
	private terminalError: ServiceBridgeError | null = null;

	private readonly _registry: Registry = new Registry(() =>
		this.onRegistrationChanged(),
	);
	private readonly watch = new WatchStream();
	private readonly instances = new InstanceCache();
	private readonly schemas = new SchemaRegistry();
	private readonly cb = new CircuitBreakerRegistry();
	private readonly lb = new LoadBalancer(this.cb);
	private callServer: CallServer | null = null;
	private rpcClient: RpcCaller | null = null;
	private publisher: Publisher | null = null;
	private subscriber: Subscriber | null = null;
	private jobSubscriber: JobSubscriber | null = null;
	private workflowSubscriber: WorkflowSubscriber | null = null;
	private telemetryTransport: TelemetryTransport | null = null;
	private processSampler: ProcessSampler | null = null;

	private readonly telemetryRing: TelemetryRing;
	private readonly telemetryApi: TelemetryAPI;
	private readonly schemaIndex: SchemaIndex = {
		get: (name: string) => this._registry._handle.getPublishedEvent(name),
	};

	/** RPC domain — incoming handlers and outgoing calls. */
	readonly rpc: RpcDomain;
	/** Event domain — define published events, subscribe, publish. */
	readonly event: EventDomain;
	/** Workflow domain — define workflows, register step handlers, start/execute runs. */
	readonly workflow: WorkflowDomain;
	/** Job domain — register scheduled job handlers via `.handle(name, opts, fn)`. */
	readonly job: JobDomain;

	constructor(
		url: string,
		key: string,
		options: ServiceBridgeOptions | ServiceBridgeInternalHooks = {},
	) {
		const hooks = options as ServiceBridgeInternalHooks;
		assertInt("rpcMaxConcurrentCalls", options.rpcMaxConcurrentCalls, 1);
		assertInt("rpcMaxQueuedCalls", options.rpcMaxQueuedCalls, 0);
		assertInt("maxPendingPublishes", options.maxPendingPublishes, 1);
		assertInt("publishTimeoutMs", options.publishTimeoutMs, 1);
		assertInt("eventsMaxInFlight", options.eventsMaxInFlight, 1);
		assertInt("reconnectAttempts", options.reconnectAttempts, 0);
		assertInt("reconnectIntervalMs", options.reconnectIntervalMs, 0);
		assertInt("startTimeoutMs", options.startTimeoutMs, 1);
		assertInt("stopTimeoutMs", options.stopTimeoutMs, 0);
		if (options.callDefaults?.timeout !== undefined)
			timeoutMs(options.callDefaults.timeout);

		this.url = url;
		this.rawKey = key;
		this.log = options.logger ?? consoleLogger;
		this.opts = {
			reconnectIntervalMs: options.reconnectIntervalMs,
			reconnectAttempts: options.reconnectAttempts ?? 0,
			advertise: this.resolveAdvertise(options.advertise),
			callDefaults: options.callDefaults ?? {},
			failOnPolicyViolation: options.failOnPolicyViolation ?? false,
			publishTimeoutMs: options.publishTimeoutMs ?? DEFAULT_PUBLISH_TIMEOUT_MS,
			maxPendingPublishes:
				options.maxPendingPublishes ?? DEFAULT_MAX_PENDING_PUBLISHES,
			eventsMaxInFlight:
				options.eventsMaxInFlight ?? DEFAULT_EVENTS_MAX_IN_FLIGHT,
			rpcMaxConcurrentCalls: options.rpcMaxConcurrentCalls,
			rpcMaxQueuedCalls: options.rpcMaxQueuedCalls,
			startTimeoutMs: options.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS,
			stopTimeoutMs: options.stopTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS,
			onDrop: options.telemetry?.onDrop,
			certRefreshLeadMs: hooks.certRefreshLeadMs ?? CERT_REFRESH_LEAD_MS,
			certRefreshJitterMs: hooks.certRefreshJitterMs ?? CERT_REFRESH_JITTER_MS,
			provisionFn: hooks.provisionFn ?? defaultProvision,
			refreshFn: hooks.refreshFn ?? refresh,
			controlClientFactory:
				hooks.controlClientFactory ??
				((u, c) => new ControlClient(u, c, CLIENT_CHANNEL_OPTIONS)),
			registryClientFactory:
				hooks.registryClientFactory ??
				((u, c) => new RegistryClientImpl(u, c, CLIENT_CHANNEL_OPTIONS)),
			disableTelemetryTransport: hooks._disableTelemetryTransport ?? false,
		};

		// User code may emit logs/ops/metrics before start(); they buffer in the
		// ring until the transport drains it.
		this.telemetryRing = new TelemetryRing({
			ops: DEFAULT_TELEMETRY_RING_SIZE,
		});
		this.telemetryApi = makeTelemetryAPI(
			this.telemetryRing,
			() => this.telemetryInstanceId,
			() => this.watch.pushedTelemetryConfig().enabled,
			(channel) => this.watch.captureModeForChannel(channel),
			() => this.watch.pushedTelemetryConfig().payloadMaxBytes,
		);

		const onPolicyViolation = (v: PolicyViolationEvent) =>
			this.emitPolicyViolation(v);
		this.rpc = new RpcDomain(
			this._registry,
			() => this.rpcClient,
			onPolicyViolation,
		);
		this.event = new EventDomain(this._registry, () => this.publisher);
		this.workflow = new WorkflowDomain(this._registry, onPolicyViolation);
		this.job = new JobDomain(this._registry);
	}

	/** Declares outgoing dependencies (rpc/workflows/http). Call before start(). */
	service(serviceName: string, deps: ServiceDeps): void {
		this._registry.service(serviceName, deps);
	}

	/**
	 * Route collector for the HTTP integrations (`service-bridge/express`,
	 * `/fastify`, `/hono`). Application code declares routes in its framework.
	 */
	get routes() {
		return this._registry.routes;
	}

	/** Telemetry API surface — ops, logs, metrics. @public — см. ../telemetry/README.md */
	get telemetry(): TelemetryAPI {
		return this.telemetryApi;
	}

	/** Structured logs shipped to the runtime; same as `sb.telemetry.log`. */
	get logger() {
		return this.telemetryApi.log;
	}

	/**
	 * The SDK's own diagnostics sink (ServiceBridgeOptions.logger), for the HTTP
	 * integrations. @internal
	 */
	get diagnostics(): Logger {
		return this.log;
	}

	/** instance_id of this process ("" before the first Welcome). */
	instanceIdString(): string {
		return this.telemetryInstanceId;
	}

	/**
	 * Subscribes to a lifecycle event. A throwing listener is logged and does
	 * not affect the bridge or the other listeners.
	 */
	on<K extends keyof EventMap>(event: K, handler: Handler<K>): this {
		const list = this.handlers.get(event) ?? [];
		list.push(handler as Handler<keyof EventMap>);
		this.handlers.set(event, list);
		return this;
	}

	/**
	 * Connects and resolves once the runtime has welcomed the session and sent
	 * the first registry snapshot (the mesh view and the access policy), within
	 * `startTimeoutMs`. On failure the bridge is stopped and the error thrown.
	 */
	async start(): Promise<void> {
		if (this.started) throw new StateError("ServiceBridge is already started");
		if (this.stopped)
			throw new StateError("ServiceBridge has been stopped; create a new one");
		this.started = true;
		const gen = this.generation;
		try {
			await this._registry._handle.finalize();
			if (this.stale(gen))
				throw new StateError("ServiceBridge stopped during start()");
			this.instances.bind(this.watch, this.cb);
			this.registerWatchListeners();
			const ready = this.waitReady(this.opts.startTimeoutMs);
			void this.connect();
			await ready;
		} catch (err) {
			await this.stop();
			throw err;
		}
	}

	/**
	 * Resolves when the session is live and the registry snapshot of the
	 * current session is applied — immediately when it already is. Rejects when
	 * the bridge stopped.
	 */
	ready(): Promise<void> {
		return this.waitReady(Number.POSITIVE_INFINITY);
	}

	/**
	 * Graceful shutdown, in this order: stop announcing the inbound endpoint,
	 * refuse new inbound calls and deliveries, wait for in-flight calls, event
	 * handlers and jobs, send what publish() still holds, flush telemetry, then
	 * close streams, channels and the server. Bounded by `stopTimeoutMs`.
	 * Idempotent.
	 */
	async stop(): Promise<void> {
		if (this.stopped) return;
		this.stopped = true;
		this.generation++;
		this.clearTimers();
		const deadline = Date.now() + this.opts.stopTimeoutMs;
		const remaining = () => Math.max(0, deadline - Date.now());

		if (this.live && this.callServer && this.channels) {
			// Unannounce first: peers stop picking this instance while it still
			// answers the calls already on their way.
			this._registry.setCallEndpoint("");
			this.restartWatch();
		}
		this.callServer?.beginDrain();
		await Promise.all([
			this.callServer?.waitIdle(remaining()),
			this.subscriber?.drain(remaining()),
			this.jobSubscriber?.drain(remaining()),
		]);
		await this.publisher?.close(remaining());
		this.processSampler?.close();
		await this.telemetryTransport?.stop(
			Math.min(TELEMETRY_STOP_ACK_MS, Math.max(remaining(), 100)),
		);

		this.subscriber?.stop();
		await this.jobSubscriber?.stop();
		this.workflowSubscriber?.close();
		this.session?.close();
		this.session = null;
		this.watch.stop();
		this.instances.dispose();
		await this.callServer?.stop(remaining());
		const ch = this.channels;
		this.channels = null;
		if (ch) {
			ch.proxy.close();
			ch.direct.close();
			ch.events.close();
			ch.jobs.close();
			ch.workflows.close();
			ch.telemetry?.close();
			ch.registry.close();
			ch.control.close();
		}
		this.live = false;
		this.currentIdentity = null;
		this.rejectWaiters(
			this.terminalError ?? new StateError("ServiceBridge stopped"),
		);
	}

	/**
	 * Registers a caller-side schema for one (service, method). Must happen
	 * before the first call; `sb.client()` does it for a whole .proto service.
	 */
	async useSchema(
		serviceName: string,
		methodName: string,
		spec: SchemaSpec,
	): Promise<void> {
		const pair = await buildSchemaPair({
			...spec,
			...("protoFile" in spec && !spec.method ? { method: methodName } : {}),
		} as SchemaSpec);
		this.schemas.set(serviceName, methodName, pair);
	}

	/**
	 * Typed caller: reads the .proto once, declares every method of its service
	 * block as an outgoing dependency, loads the schemas and returns a proxy
	 * with one method per RPC (streaming methods return AsyncIterable). Call
	 * before start(). `opts.callDefaults` sit between the bridge's callDefaults
	 * and the per-call options.
	 */
	async client(
		serviceName: string,
		protoFile: string,
		opts?: { methods?: string[]; callDefaults?: CallOpts },
	): Promise<TypedClient> {
		const all = await extractServiceMethods(protoFile);
		const allowed = opts?.methods ? new Set(opts.methods) : null;
		const selected = allowed ? all.filter((m) => allowed.has(m.name)) : all;
		if (selected.length === 0) {
			throw new ConfigurationError(
				`rpc: client(${serviceName}, ${protoFile}): no methods to bind`,
			);
		}
		this._registry.service(serviceName, { rpc: selected.map((m) => m.name) });
		for (const m of selected) {
			const pair = await buildSchemaPair({
				protoFile,
				input: m.requestType,
				output: m.responseType,
			});
			this.schemas.set(serviceName, m.name, pair);
		}
		const proxy = {} as Record<string, unknown>;
		for (const m of selected) {
			proxy[m.name] = m.responseStream
				? (req: unknown, callOpts?: CallOpts) =>
						this.stream(serviceName, m.name, req, {
							...opts?.callDefaults,
							...callOpts,
						})
				: (req: unknown, callOpts?: CallOpts) =>
						this.rpc.call(serviceName, m.name, req, {
							...opts?.callDefaults,
							...callOpts,
						});
		}
		return proxy as TypedClient;
	}

	/**
	 * Server-side streaming RPC: an AsyncIterable of decoded chunks. Leaving
	 * the loop cancels the gRPC stream and the callee's handler. Never retried.
	 */
	stream<Req = unknown, Chunk = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		opts?: CallOpts,
	): AsyncIterable<Chunk> {
		const client = this.rpcClient;
		if (!client)
			throw new StateError(
				"rpc: stream before start() — call sb.start() first",
			);
		return client.stream<Req, Chunk>(serviceName, methodName, payload, opts);
	}

	/** Identity of the live session, or null when not connected. */
	identity(): Identity | null {
		return this.currentIdentity;
	}

	/** Live registry view grouped by service name. */
	serviceMap(): ReadonlyMap<string, ServiceMapEntry> {
		const blank = (): ServiceMapEntry => ({
			methods: [],
			instances: [],
			eventSubscriptions: [],
			outgoingCalls: [],
		});
		const result = new Map<string, ServiceMapEntry>();
		for (const m of this.watch.snapshot().values()) {
			const entry = result.get(m.serviceName) ?? blank();
			entry.methods.push(m);
			result.set(m.serviceName, entry);
		}
		for (const i of this.watch.instancesSnapshot().values()) {
			const entry = result.get(i.serviceName) ?? blank();
			entry.instances.push(i);
			result.set(i.serviceName, entry);
		}
		const byServiceId = new Map<string, ServiceMapEntry>();
		for (const entry of result.values()) {
			for (const m of entry.methods) byServiceId.set(m.serviceId, entry);
			for (const i of entry.instances) byServiceId.set(i.serviceId, entry);
		}
		for (const es of this.watch.eventSubscriptionsSnapshot().values())
			byServiceId.get(es.serviceId)?.eventSubscriptions.push(es);
		for (const oc of this.watch.outgoingCallsSnapshot().values())
			byServiceId.get(oc.callerServiceId)?.outgoingCalls.push(oc);
		return result;
	}

	/** The access policy the runtime last pushed; null before the first snapshot. */
	policyEvaluation(): PolicyEvaluation | null {
		return this.watch.policyEvaluation();
	}

	/**
	 * Starts the bridge against an in-memory runtime: outbound calls go to
	 * `rpc`, publishes to `events`, no network is opened. Used by
	 * `service-bridge/testing` only. @internal
	 */
	async _startInMemory(rt: {
		rpc: RpcCaller;
		events: EventsClient;
		identity: Identity;
	}): Promise<{ handle: Registry["_handle"]; schemas: SchemaRegistry }> {
		if (this.started) throw new StateError("ServiceBridge is already started");
		this.started = true;
		await this._registry._handle.finalize();
		this.rpcClient = rt.rpc;
		this.publisher = new Publisher({
			client: () => rt.events,
			schemaIndex: this.schemaIndex,
			logger: this.log,
			timeoutMs: this.opts.publishTimeoutMs,
			maxPending: this.opts.maxPendingPublishes,
			xSbTraceFn: () => {
				const ctx = currentTraceContext();
				return ctx ? formatXSbTrace(ctx.traceId, ctx.parentOpId) : "";
			},
			onPolicyViolation: (v) => this.emitPolicyViolation(v),
		});
		this.currentIdentity = rt.identity;
		this.telemetryInstanceId = rt.identity.instanceId;
		this.live = true;
		return { handle: this._registry._handle, schemas: this.schemas };
	}

	// ── internals ──────────────────────────────────────────────────────────────

	private resolveAdvertise(
		provided: AdvertiseConfig | false | undefined,
	): AdvertiseConfig | null {
		if (provided === false) return null;
		if (provided) return provided;
		this.log?.warn(
			"advertise not configured — the Call server binds 127.0.0.1; pass { advertise: { host, port } } for cross-host reachability",
		);
		return { host: "127.0.0.1", port: 0 };
	}

	private emit<K extends keyof EventMap>(event: K, data: EventMap[K]): void {
		for (const h of this.handlers.get(event) ?? []) {
			try {
				(h as Handler<K>)(data);
			} catch (err) {
				this.log.error(`listener of "${event}" threw`, {
					error: err instanceof Error ? err.message : String(err),
				});
			}
		}
	}

	private emitPolicyViolation(v: PolicyViolationEvent): void {
		this.log.warn("policy violation", { ...v });
		this.emit("policy_violation", v);
	}

	private stale(gen: number): boolean {
		return this.stopped || gen !== this.generation;
	}

	private waitReady(timeoutMs: number): Promise<void> {
		if (this.terminalError) return Promise.reject(this.terminalError);
		if (this.stopped)
			return Promise.reject(new StateError("ServiceBridge stopped"));
		if (this.live && this.watch.hasSnapshot()) return Promise.resolve();
		return new Promise<void>((resolve, reject) => {
			let timer: ReturnType<typeof setTimeout> | undefined;
			const waiter: Waiter = {
				resolve: () => {
					if (timer) clearTimeout(timer);
					resolve();
				},
				reject: (err) => {
					if (timer) clearTimeout(timer);
					reject(err);
				},
			};
			if (Number.isFinite(timeoutMs))
				timer = setTimeout(() => {
					this.readyWaiters = this.readyWaiters.filter((w) => w !== waiter);
					reject(
						new TimeoutError(
							`start: no Welcome and registry snapshot from ${this.url} within ${timeoutMs} ms`,
						),
					);
				}, timeoutMs);
			this.readyWaiters.push(waiter);
		});
	}

	private checkReady(): void {
		if (!this.live || !this.watch.hasSnapshot()) return;
		const waiters = this.readyWaiters;
		this.readyWaiters = [];
		for (const w of waiters) w.resolve();
	}

	private rejectWaiters(err: Error): void {
		const waiters = this.readyWaiters;
		this.readyWaiters = [];
		for (const w of waiters) w.reject(err);
	}

	// The watch listeners are registered once: the WatchStream outlives every
	// session and only its gRPC stream is reopened.
	private registerWatchListeners(): void {
		this.watch.onSnapshot(() => this.checkReady());
		this.watch.onPolicyEvaluation((policy) => {
			for (const w of policy.warnings)
				this.emitPolicyViolation({
					declaration: w.declaration,
					value: w.value,
					denySide: w.denySide,
					reason: w.reason,
				});
			if (this.opts.failOnPolicyViolation && policy.warnings.length > 0) {
				const message = policy.warnings
					.map((w) => `${w.declaration} ${w.value}: ${w.reason}`)
					.join("; ");
				void this.terminate(
					new AccessDeniedError(`policy violations on start: ${message}`),
				);
			}
		});
		this.watch.onRevoked(() => {
			this.channels?.direct.retain(
				(serviceId, instanceId) => !this.watch.isRevoked(serviceId, instanceId),
			);
		});
		this.watch.onInstancesChange((_added, removed) => {
			if (removed.length === 0) return;
			const gone = new Set(removed.map((i) => i.instanceId));
			this.channels?.direct.retain((_s, instanceId) => !gone.has(instanceId));
		});
	}

	private onRegistrationChanged(): void {
		if (!this.started || this.stopped || !this.live) return;
		this.restartWatch();
	}

	private restartWatch(): void {
		const ch = this.channels;
		if (!ch) return;
		this.watch.restart(
			this._registry.buildRegisterRequest(),
			ch.registry,
			(err) => this.onWatchError(err),
		);
	}

	// A registry stream rejected with a terminal status (invalid subscription
	// filter, unsupported protocol, revoked identity) is a configuration or
	// access problem no reconnect can fix.
	private onWatchError(err: Error): void {
		const code = (err as { code?: unknown }).code;
		if (typeof code === "number" && isTerminal(code)) {
			const scope = "registry";
			void this.terminate(
				code === grpc.status.INVALID_ARGUMENT
					? new ValidationError(`${scope}: ${err.message}`, { cause: err })
					: new ConnectionError(scope, err),
			);
			return;
		}
		this.log.warn("registry stream failed, restarting", { error: err.message });
	}

	// connect runs one attempt: certificate (cached or fresh), channels on
	// first use, the Call server on first use, then Control.Open. The session
	// becomes live on Welcome.
	private async connect(): Promise<void> {
		if (this.stopped) return;
		const gen = this.generation;
		try {
			const prov =
				this.reusableProvision() ??
				(await this.opts.provisionFn(this.url, parseBootstrapKey(this.rawKey)));
			if (this.stale(gen)) return;
			this.adoptProvision(prov);
			await this.ensureCallServer();
			if (this.stale(gen)) return;
			this.openSession();
		} catch (err) {
			if (this.stale(gen)) return;
			this.onConnectFailure("connect", err);
		}
	}

	private onConnectFailure(scope: string, err: unknown): void {
		if (err instanceof ConfigurationError || err instanceof ValidationError) {
			void this.terminate(err);
			return;
		}
		const sbErr =
			err instanceof ConnectionError ? err : new ConnectionError(scope, err);
		if (isTerminal(sbErr.grpcCode)) {
			void this.terminate(sbErr);
			return;
		}
		this.scheduleReconnect(sbErr.message);
	}

	// reusableProvision returns the cached leaf while it is comfortably valid,
	// so a reconnect does not cost the runtime an argon2 Provision.
	private reusableProvision(): ProvisionResult | null {
		const prov = this.lastProvision;
		if (!prov) return null;
		return prov.notAfterUnixMs - Date.now() > this.opts.certRefreshLeadMs
			? prov
			: null;
	}

	private adoptProvision(prov: ProvisionResult): void {
		if (prov === this.lastProvision) return;
		this.lastProvision = prov;
		const material = {
			caChainDer: prov.caChainDer,
			certDer: prov.certDer,
			privateKeyDer: prov.privateKeyDer,
		};
		if (this.store) {
			this.store.update(material);
			void this.callServer?.rotate();
			return;
		}
		this.store = new CertificateStore(material);
		this.buildChannels(this.store);
	}

	private buildChannels(store: CertificateStore): void {
		const creds = store.channelCredentials(makeSpiffeCheck(RUNTIME_SPIFFE_URI));
		const telemetry = this.opts.disableTelemetryTransport
			? null
			: new TelemetryClient(this.url, creds, CLIENT_CHANNEL_OPTIONS);
		const ch: RuntimeChannels = {
			control: this.opts.controlClientFactory(this.url, creds),
			registry: this.opts.registryClientFactory(this.url, creds),
			events: new EventsClient(this.url, creds, CLIENT_CHANNEL_OPTIONS),
			jobs: new JobsClient(this.url, creds, CLIENT_CHANNEL_OPTIONS),
			workflows: new WorkflowsClient(this.url, creds, CLIENT_CHANNEL_OPTIONS),
			telemetry,
			proxy: new ProxyTransport(this.url, creds),
			direct: new DirectTransport(
				store,
				() => this.currentIdentity?.serviceId ?? "",
			),
		};
		this.channels = ch;
		this.workflow._attachRpc(ch.workflows);
		this.rpcClient = new RpcClient({
			proxy: ch.proxy,
			direct: ch.direct,
			instances: this.instances,
			resolveSchema: this.schemas.asResolver(),
			cb: this.cb,
			lb: this.lb,
			callDefaults: () => this.opts.callDefaults,
			sb: this,
		});
		this.publisher = new Publisher({
			client: () => this.channels?.events ?? null,
			schemaIndex: this.schemaIndex,
			logger: this.log,
			timeoutMs: this.opts.publishTimeoutMs,
			maxPending: this.opts.maxPendingPublishes,
			// Events published inside a traced scope inherit its trace.
			xSbTraceFn: () => {
				const ctx = currentTraceContext();
				return ctx ? formatXSbTrace(ctx.traceId, ctx.parentOpId) : "";
			},
			onPolicyViolation: (v) => this.emitPolicyViolation(v),
		});
		if (telemetry) {
			const dropped = {
				ring: this.telemetryApi.counter("sb_sdk_telemetry_dropped_total", {
					source: "ring",
				}),
				server: this.telemetryApi.counter("sb_sdk_telemetry_dropped_total", {
					source: "server",
				}),
			};
			const seen = { ring: 0, server: 0 };
			this.telemetryTransport = new TelemetryTransport({
				client: adaptTelemetryClient(telemetry),
				ring: this.telemetryRing,
				onDrop: (info) => {
					// Drops are themselves telemetry: the runtime sees them as a
					// counter, the host through its onDrop hook.
					if (info.ringDrops > seen.ring)
						dropped.ring.inc(info.ringDrops - seen.ring);
					if (info.serverDrops > seen.server)
						dropped.server.inc(info.serverDrops - seen.server);
					seen.ring = info.ringDrops;
					seen.server = info.serverDrops;
					try {
						this.opts.onDrop?.(info);
					} catch (err) {
						this.log.error("telemetry onDrop hook threw", {
							error: (err as Error).message,
						});
					}
				},
			});
		}
	}

	// ensureCallServer binds the inbound server once; its address must be in
	// the very first RegisterRequest. A bind failure (port taken) is a
	// configuration error: no retry frees the port.
	private async ensureCallServer(): Promise<void> {
		if (!this.opts.advertise || this.callServer || !this.store) return;
		const server = new CallServer({
			dispatch: this._registry._handle.asDispatchPort(),
			store: this.store,
			policy: () => this.watch.policyEvaluation(),
			isRevoked: (s, i) => this.watch.isRevoked(s, i),
			limits: {
				maxConcurrentCalls: this.opts.rpcMaxConcurrentCalls,
				maxQueuedCalls: this.opts.rpcMaxQueuedCalls,
			},
			logger: this.log,
		});
		try {
			const endpoint = await server.start(this.opts.advertise);
			this.callServer = server;
			this._registry.setCallEndpoint(endpoint);
		} catch (err) {
			throw new ConfigurationError(
				`call server: cannot bind ${this.opts.advertise.host}:${this.opts.advertise.port} — ${(err as Error).message}`,
				{ cause: err },
			);
		}
	}

	private openSession(): void {
		const ch = this.channels;
		if (!ch) return;
		this.session?.close();
		const gen = this.generation;
		// Opened and handed to Session in one synchronous run: an 'error' on an
		// unlistened stream would be an uncaught exception.
		const session: Session = new Session(openControlStream(ch.control), {
			onWelcome: (welcome) => {
				if (this.stale(gen) || this.session !== session) return;
				if (
					welcome.protocolVersion !== 0 &&
					welcome.protocolVersion !== PROTOCOL_VERSION
				) {
					void this.terminate(
						new ConfigurationError(
							`runtime ${welcome.runtimeVersion} speaks protocol ${welcome.protocolVersion}, this SDK speaks ${PROTOCOL_VERSION} — upgrade the SDK or the runtime`,
						),
					);
					return;
				}
				this.onWelcome(welcome, session);
			},
			onDrain: (reason) => {
				if (this.stale(gen) || this.session !== session) return;
				this.log.info("runtime is draining; reconnecting when it closes", {
					reason,
				});
				this.emit("draining", { reason });
			},
			onError: (err) => {
				if (this.stale(gen) || this.session !== session) return;
				this.onSessionLost(err);
			},
			onEnd: () => {
				if (this.stale(gen) || this.session !== session) return;
				this.onSessionLost(null);
			},
		});
		this.session = session;
	}

	private onWelcome(
		welcome: {
			sessionId: string;
			serviceId: string;
			serviceName: string;
			runtimeVersion: string;
		},
		session: Session,
	): void {
		const prov = this.lastProvision;
		if (!prov || this.session !== session) return;
		this.failures = 0;
		this.live = true;
		this.currentIdentity = {
			sessionId: welcome.sessionId,
			serviceId: welcome.serviceId,
			serviceName: welcome.serviceName,
			instanceId: prov.instanceId,
		};
		if (this.telemetryInstanceId !== prov.instanceId) {
			this.telemetryRing.metrics.retireInstance(this.telemetryInstanceId);
			this.telemetryInstanceId = prov.instanceId;
		}
		this.scheduleCertRefresh(prov);
		this.restartWatch();
		this.startSubsystems();
		this.publisher?.kick();
		this.emit("connected", {
			sessionId: welcome.sessionId,
			serviceId: welcome.serviceId,
			serviceName: welcome.serviceName,
			runtimeVersion: welcome.runtimeVersion,
		});
	}

	private onSessionLost(err: Error | null): void {
		this.live = false;
		this.session = null;
		const code = (err as { code?: unknown } | null)?.code;
		if (err && typeof code === "number" && isTerminal(code)) {
			void this.terminate(new ConnectionError("control stream", err));
			return;
		}
		this.scheduleReconnect(err ? err.message : "control stream ended");
	}

	// startSubsystems starts every long-lived stream once; afterwards they
	// supervise themselves (reconnect on their own ladder) across sessions.
	private startSubsystems(): void {
		const ch = this.channels;
		if (!ch) return;
		if (this.telemetryTransport && !this.processSampler) {
			void this.telemetryTransport.start();
			this.processSampler = new ProcessSampler(
				this.telemetryRing,
				() => this.telemetryInstanceId,
			);
			this.processSampler.start();
		}
		const identity = () => {
			const id = this.currentIdentity;
			return id ? { serviceId: id.serviceId, instanceId: id.instanceId } : null;
		};
		if (!this.subscriber && this._registry._handle.subscriptionCount() > 0) {
			this.subscriber = new Subscriber({
				client: () => this.channels?.events ?? null,
				identity,
				subscription: (p) => this._registry._handle.subscription(p),
				maxInFlight: this.opts.eventsMaxInFlight,
				logger: this.log,
				runWithTrace: this.runHandlerWithTrace,
			});
			this.subscriber.start();
		}
		if (!this.jobSubscriber && this.job.size() > 0) {
			this.jobSubscriber = new JobSubscriber({
				client: () => this.channels?.jobs ?? null,
				identity: () => this.currentIdentity,
				domain: this.job,
				logger: this.log,
				runWithTrace: this.runHandlerWithTrace,
			});
			this.jobSubscriber.start();
		}
		this.maybeStartWorkflowSubscriber(ch.workflows);
	}

	// Owner-side workflow subscriber: started once, only when this service
	// registered a workflow.
	private maybeStartWorkflowSubscriber(rpc: WorkflowsClient): void {
		if (this.workflowSubscriber) return;
		const hasWorkflowHandlers = this._registry._handle._entries.some(
			(e) => e.type === MethodType.METHOD_TYPE_WORKFLOW,
		);
		if (!hasWorkflowHandlers) return;
		const telemetryApi = this.telemetryApi;
		const log = this.log;
		const sub = new WorkflowSubscriber({
			rpc,
			identity: () => this.currentIdentity,
			deps: {
				sb: { rpc: this.rpc, event: this.event, workflow: this.workflow },
				ops: makeRuntimeOps(rpc, () => this.currentIdentity?.instanceId ?? ""),
				// One USER.SUBOP span per executed unit (ADR-0003 nesting).
				wrapStep: async (info, fn) => {
					const meta: Record<string, unknown> = {
						step_id: info.stepId,
						step_name: info.stepName,
						workflow_run_id: info.runId,
					};
					if (info.isCompensation) {
						meta.is_compensation = true;
						meta.compensates_for_step_id = info.compensatesForStepId ?? "";
					}
					const subject =
						info.role === "compensation"
							? `compensate:${info.compensatesForStepId ?? info.stepId}`
							: `${info.role}:${info.stepId}`;
					return telemetryApi
						.startOp({
							channel: Channel.USER,
							kind: UserSubOp,
							subject,
							businessKey: info.runId,
							metaJson: Buffer.from(JSON.stringify(meta)),
						})
						.run(fn);
				},
			},
			logger: {
				warn: (m: string, ...args: unknown[]) => log.warn(m, { args }),
				error: (m: string, ...args: unknown[]) => log.error(m, { args }),
			},
			lookupLocalGraph: (name, fingerprint) => {
				const entry = this._registry._handle._entries.find(
					(e) =>
						e.type === MethodType.METHOD_TYPE_WORKFLOW &&
						e.name === name &&
						e.contractHashOverride === fingerprint,
				);
				return entry ? (entry.fn as unknown as Step[]) : null;
			},
			sb: this,
		});
		sub.start();
		this.workflowSubscriber = sub;
	}

	// Runs a job/event handler inside the inbound trace so nested calls join it.
	private readonly runHandlerWithTrace = (
		xSbTrace: string,
		fn: () => Promise<void>,
	): Promise<void> => {
		const parsed = parseXSbTrace(xSbTrace);
		return parsed ? runWithTrace(parsed, fn) : fn();
	};

	private scheduleCertRefresh(prov: ProvisionResult): void {
		if (this.certRefreshTimer) clearTimeout(this.certRefreshTimer);
		const delay =
			prov.notAfterUnixMs -
			Date.now() -
			this.opts.certRefreshLeadMs +
			randomJitter(this.opts.certRefreshJitterMs);
		this.certRefreshTimer = setTimeout(
			() => {
				this.certRefreshTimer = null;
				void this.rotateCert();
			},
			Math.max(0, delay),
		);
		this.certRefreshTimer.unref?.();
	}

	// rotateCert reissues the leaf over the live session and hands it to every
	// channel's next handshake and to the Call server. No stream, channel or
	// session is rebuilt: in-flight jobs, deliveries and calls continue.
	private async rotateCert(): Promise<void> {
		const ch = this.channels;
		const prev = this.lastProvision;
		if (this.stopped || !ch || !prev) return;
		const gen = this.generation;
		try {
			const next = await this.opts.refreshFn(ch.control, prev);
			if (this.stale(gen)) return;
			this.adoptProvision(next);
			this.scheduleCertRefresh(next);
			this.log.info("certificate renewed", {
				notAfterUnixMs: next.notAfterUnixMs,
			});
		} catch (err) {
			if (this.stale(gen)) return;
			const sbErr = new ConnectionError("certificate refresh", err);
			if (
				isTerminal(sbErr.grpcCode) &&
				sbErr.grpcCode !== grpc.status.FAILED_PRECONDITION
			) {
				void this.terminate(sbErr);
				return;
			}
			// Rate-limited (RESOURCE_EXHAUSTED) or no session right now: try again
			// later. A leaf that expires meanwhile is replaced by a fresh Provision
			// on the next reconnect.
			this.log.warn("certificate refresh failed, retrying", {
				error: sbErr.message,
				retryInMs: CERT_REFRESH_RETRY_MS,
			});
			this.certRefreshTimer = setTimeout(() => {
				this.certRefreshTimer = null;
				void this.rotateCert();
			}, CERT_REFRESH_RETRY_MS);
			this.certRefreshTimer.unref?.();
		}
	}

	private scheduleReconnect(reason: string): void {
		if (this.stopped) return;
		this.failures++;
		if (
			this.opts.reconnectAttempts > 0 &&
			this.failures > this.opts.reconnectAttempts
		) {
			void this.terminate(
				new ConnectionError(
					"reconnect",
					new Error(
						`gave up after ${this.opts.reconnectAttempts} consecutive failed attempts: ${reason}`,
					),
				),
			);
			return;
		}
		const delayMs =
			this.opts.reconnectIntervalMs ?? reconnectDelay(this.failures - 1);
		this.log.warn("connection lost, reconnecting", {
			attempt: this.failures,
			delayMs,
			reason,
		});
		this.emit("reconnecting", { attempt: this.failures, delayMs, reason });
		if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = null;
			void this.connect();
		}, delayMs);
	}

	// terminate stops the bridge for good and reports why.
	private async terminate(err: ServiceBridgeError): Promise<void> {
		if (this.stopped) return;
		this.terminalError = err;
		this.log.error("stopping: unrecoverable error", { error: err.message });
		this.rejectWaiters(err);
		this.emit("disconnected", { reason: err.message, error: err });
		await this.stop();
	}

	private clearTimers(): void {
		if (this.certRefreshTimer) clearTimeout(this.certRefreshTimer);
		if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
		this.certRefreshTimer = null;
		this.reconnectTimer = null;
	}
}

// assertInt rejects an option value that can never be valid, at construction:
// a bad bound must not look like a network condition later.
function assertInt(name: string, value: number | undefined, min: number): void {
	if (value === undefined) return;
	if (!Number.isInteger(value) || value < min) {
		throw new ConfigurationError(
			`ServiceBridge: ${name} must be an integer >= ${min}, got ${value}`,
		);
	}
}
