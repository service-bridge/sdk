// misc.test.ts — runtime/SDK plumbing: connect lifecycle, registry/service-map
// discovery, structured-log ingest, and per-channel payload-capture propagation.
//
// Consolidates the old bootstrap / registry / logs-collector /
// payload-capture-per-channel / service-map-enrichment e2e files. Each test()
// asserts one behavior. Parties that register a handler/subscription/definition
// or declare an outgoing dependency use a DEDICATED client and register BEFORE
// connect() — the runtime resolves discovery against what is present at connect
// time, so post-connect registrations never propagate. Pure callers/readers
// (logger side-channel) reuse the warm pool.
//
// The two payload-capture tests mutate GLOBAL runtime_settings rows and must
// stay on this single-domain process, serialized, save+restore in afterEach.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import {
	type MethodDescriptor,
	ServiceBridge,
	type ServiceMapEntry,
} from "../../src/connection/service-bridge";
import { Channel } from "../../src/pb/servicebridge/v1/telemetry";
import {
	connect,
	dedicated,
	type Role,
	shared,
	sleep,
	uniqueName,
	waitFor,
} from "./_helpers/fixtures";
import { withDb } from "./_helpers/policy-db";

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 2,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

const ROLE_INDEX: Record<Role, number> = { primary: 1, second: 2, third: 3 };

const PROTO_BASE = `${import.meta.dir}/../../src`;
const PAY_SCHEMA = {
	protoFile: `${PROTO_BASE}/serde/testdata/payment.proto`,
	input: "ChargeRequest",
	output: "ChargeResponse",
};
const ORDER_PROTO = `${PROTO_BASE}/events/testdata/order-event.proto`;

function domain(): string {
	const d = process.env.SB_E2E_DOMAIN;
	if (!d) throw new Error("misc e2e: SB_E2E_DOMAIN not set");
	return d;
}

// roleServiceName derives a role's service name without connecting, matching
// scripts/bootstrap-e2e-keys.sh (`e2e-<domain>-<n>`). Lets a test assert a
// peer's name before that peer registers anything.
function roleServiceName(role: Role): string {
	return `e2e-${domain()}-${ROLE_INDEX[role]}`;
}

// rawKey reads the misc-domain bootstrap key for a role directly from the env,
// mirroring pool.ts::keyForRole. Used only by the connect-lifecycle and
// payload-capture tests that build a ServiceBridge by hand (custom listeners, a
// deliberately bad key, or a fresh connection after a settings write) and
// cannot go through the warm pool.
function rawKey(role: Role): { url: string; key: string } {
	const url = process.env.SERVICEBRIDGE_URL;
	const envName = `SB_E2E_${domain().toUpperCase().replace(/-/g, "_")}_${ROLE_INDEX[role]}`;
	const key = process.env[envName];
	if (!url || !key) {
		throw new Error(
			`misc e2e: SERVICEBRIDGE_URL / ${envName} not set — run \`bash scripts/bootstrap-e2e-keys.sh\``,
		);
	}
	return { url, key };
}

// corruptSecret flips a byte in the secret region (offset 8..40) of a bootstrap
// key, keeping the key shape valid so auth fails rather than parsing fails.
function corruptSecret(raw: string): string {
	if (!raw.startsWith("sb.")) throw new Error("invalid key format");
	const payload = Buffer.from(raw.slice(3), "base64url");
	const b = payload[10];
	if (b === undefined) throw new Error("payload too short");
	payload[10] = b ^ 0xff;
	return `sb.${payload.toString("base64url")}`;
}

function allMethods(
	map: ReadonlyMap<string, ServiceMapEntry>,
): MethodDescriptor[] {
	const out: MethodDescriptor[] = [];
	for (const entry of map.values()) out.push(...entry.methods);
	return out;
}

function hasMethod(
	map: ReadonlyMap<string, ServiceMapEntry>,
	name: string,
): boolean {
	return allMethods(map).some((d) => d.name === name);
}

describe("misc: connect lifecycle", () => {
	let sb: ServiceBridge | undefined;

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
	});

	test("start emits connected with session/service identity, stop is terminal", async () => {
		const { url, key } = rawKey("primary");
		sb = new ServiceBridge(url, key, FAST_OPTS);

		const events: { type: string; payload: unknown }[] = [];
		sb.on("connected", (e) => events.push({ type: "connected", payload: e }));
		sb.on("reconnecting", (e) =>
			events.push({ type: "reconnecting", payload: e }),
		);
		sb.on("disconnected", (e) =>
			events.push({ type: "disconnected", payload: e }),
		);

		await sb.start();
		await waitFor(
			() => events.some((e) => e.type === "connected"),
			5_000,
			"connected event",
		);

		const connected = events.find((e) => e.type === "connected");
		expect(connected).toBeDefined();
		const c = connected?.payload as {
			sessionId: string;
			serviceId: string;
			serviceName: string;
		};
		expect(c.sessionId.length).toBeGreaterThan(0);
		expect(c.serviceId.length).toBeGreaterThan(0);
		expect(c.serviceName.length).toBeGreaterThan(0);

		// Hold the session long enough for at least one heartbeat round-trip.
		await sleep(2_500);

		await sb.stop();
		sb = undefined;
		// After stop, no further reconnect/disconnect events fire.
		const before = events.length;
		await sleep(1_500);
		expect(events.length).toBe(before);
	}, 15_000);

	test("corrupted secret drives reconnect FSM to disconnected{exhausted}", async () => {
		const { url, key } = rawKey("primary");
		sb = new ServiceBridge(url, corruptSecret(key), {
			reconnectIntervalMs: 200,
			reconnectAttempts: 2,
			certRefreshLeadMs: 60 * 60 * 1000,
		});

		const events: { type: string; payload: unknown }[] = [];
		sb.on("reconnecting", (e) =>
			events.push({ type: "reconnecting", payload: e }),
		);
		sb.on("disconnected", (e) =>
			events.push({ type: "disconnected", payload: e }),
		);

		await sb.start();
		await waitFor(
			() => events.some((e) => e.type === "disconnected"),
			10_000,
			"disconnected event",
		);

		expect(events.some((e) => e.type === "reconnecting")).toBe(true);
		const exhausted = events.find(
			(e) =>
				e.type === "disconnected" &&
				(e.payload as { reason: string }).reason === "exhausted",
		);
		expect(exhausted).toBeDefined();
	}, 15_000);
});

describe("misc: registry discovery", () => {
	let provider: ServiceBridge | undefined;
	let provider2: ServiceBridge | undefined;
	let consumer: ServiceBridge | undefined;

	afterEach(async () => {
		await provider?.stop().catch(() => {});
		await provider2?.stop().catch(() => {});
		await consumer?.stop().catch(() => {});
		provider = provider2 = consumer = undefined;
	});

	test("method registered before consumer connect appears in initial snapshot", async () => {
		const method = uniqueName("charge");

		provider = dedicated("primary");
		provider.rpc._declareForTests(method);
		await connect(provider);
		const providerName = provider.identity()?.serviceName as string;

		consumer = dedicated("second");
		consumer.service(providerName, { rpc: [method] });
		await connect(consumer);

		await waitFor(
			() => hasMethod(consumer?.serviceMap() ?? new Map(), method),
			5_000,
			"method in initial snapshot",
		);

		const desc = allMethods(consumer.serviceMap()).find(
			(d) => d.name === method,
		);
		expect(desc).toBeDefined();
		expect(desc?.serviceName).toBe(providerName);
	}, 20_000);

	test("method registered after consumer connect arrives via live RegistryUpdate", async () => {
		const method = uniqueName("refund");
		const providerName = roleServiceName("primary");

		consumer = dedicated("second");
		consumer.service(providerName, { rpc: [method] });
		await connect(consumer);
		expect(hasMethod(consumer.serviceMap(), method)).toBe(false);

		provider = dedicated("primary");
		provider.rpc._declareForTests(method);
		await connect(provider);
		expect(provider.identity()?.serviceName).toBe(providerName);

		await waitFor(
			() => hasMethod(consumer?.serviceMap() ?? new Map(), method),
			8_000,
			"method via live update",
		);
	}, 20_000);

	test("provider stop removes its methods from the consumer map", async () => {
		const method = uniqueName("pay");

		provider = dedicated("primary");
		provider.rpc._declareForTests(method);
		await connect(provider);
		const providerName = provider.identity()?.serviceName as string;

		consumer = dedicated("second");
		consumer.service(providerName, { rpc: [method] });
		await connect(consumer);
		await waitFor(
			() => hasMethod(consumer?.serviceMap() ?? new Map(), method),
			5_000,
			"method in snapshot",
		);

		await provider.stop();
		provider = undefined;

		await waitFor(
			() => !hasMethod(consumer?.serviceMap() ?? new Map(), method),
			8_000,
			"method removed after provider stop",
		);
	}, 20_000);

	test("wildcard rpc:['*'] surfaces every provider method", async () => {
		const methods = [
			uniqueName("order-create"),
			uniqueName("order-list"),
			uniqueName("order-cancel"),
		];

		provider = dedicated("primary");
		for (const m of methods) provider.rpc._declareForTests(m);
		await connect(provider);
		const providerName = provider.identity()?.serviceName as string;

		consumer = dedicated("second");
		consumer.service(providerName, { rpc: ["*"] });
		await connect(consumer);

		await waitFor(
			() =>
				methods.every((m) => hasMethod(consumer?.serviceMap() ?? new Map(), m)),
			5_000,
			"all wildcard methods in snapshot",
		);

		for (const m of methods) {
			expect(hasMethod(consumer.serviceMap(), m)).toBe(true);
		}
	}, 20_000);

	test("unknown outgoing dep is skipped, connection succeeds, map stays empty", async () => {
		// Dedicated consumer: declaring a bogus dep mutates the identity's
		// declared deps and the assertion checks the WHOLE map is empty — unsafe
		// on a pooled identity shared with sibling tests.
		const own = dedicated("third");
		const unknown = uniqueName("no-such-svc");
		own.service(unknown, { rpc: ["anything"] });
		try {
			await connect(own);
			expect(own.identity()).not.toBeNull();
			expect(own.serviceMap().has(unknown)).toBe(false);
			expect(allMethods(own.serviceMap())).toHaveLength(0);
		} finally {
			await own.stop().catch(() => {});
		}
	}, 20_000);

	test("two provider instances with distinct methods both visible (keyed by name+instanceId)", async () => {
		const methodA = uniqueName("price-a");
		const methodB = uniqueName("price-b");

		// Two ServiceBridge instances of the SAME key → two instanceIds.
		provider = dedicated("primary");
		provider.rpc._declareForTests(methodA);
		await connect(provider);
		const providerName = provider.identity()?.serviceName as string;

		provider2 = dedicated("primary");
		provider2.rpc._declareForTests(methodB);
		await connect(provider2);

		consumer = dedicated("second");
		consumer.service(providerName, { rpc: ["*"] });
		await connect(consumer);

		await waitFor(
			() => {
				const names = new Set(
					allMethods(consumer?.serviceMap() ?? new Map()).map((d) => d.name),
				);
				return names.has(methodA) && names.has(methodB);
			},
			5_000,
			"both price-a and price-b in snapshot",
		);

		// No schemas → both contract hashes empty (ADR-0005, no fallback).
		// Distinctness comes from method name + instance_id.
		const seen = new Set<string>();
		for (const d of allMethods(consumer.serviceMap())) {
			if (d.name === methodA || d.name === methodB) {
				seen.add(`${d.name}:${d.instanceId}`);
			}
		}
		expect(seen.size).toBe(2);
	}, 20_000);
});

describe("misc: service-map enrichment", () => {
	let callee: ServiceBridge | undefined;
	let consumer: ServiceBridge | undefined;

	afterEach(async () => {
		await callee?.stop().catch(() => {});
		await consumer?.stop().catch(() => {});
		callee = consumer = undefined;
	});

	test("consumer's own entry is enriched with eventSubscriptions + outgoingCalls, peer method visible, event.handle keeps it connected", async () => {
		const method = uniqueName("enrich");
		const subPattern = `${uniqueName("enrich")}.created`;

		callee = dedicated("primary");
		callee.rpc.handle(method, async () => ({ transactionId: "x", ok: true }), {
			schema: PAY_SCHEMA,
		});
		await connect(callee);
		const calleeName = callee.identity()?.serviceName as string;

		consumer = dedicated("second");
		consumer.service(calleeName, { rpc: [method] });
		consumer.event.handle(subPattern, async () => {});
		consumer.event.define(subPattern, {
			protoFile: ORDER_PROTO,
			method: "orders_created",
		});
		await connect(consumer);
		// event.handle registration keeps the consumer connected (absorbs the old
		// registry "event subscription" assertion).
		expect(consumer.identity()).not.toBeNull();
		const consumerName = consumer.identity()?.serviceName as string;

		await waitFor(
			() => {
				const self = consumer!.serviceMap().get(consumerName);
				if (!self) return false;
				return (
					self.eventSubscriptions.some((es) => es.pattern === subPattern) &&
					self.outgoingCalls.some((oc) => oc.targetMethod === method)
				);
			},
			8_000,
			"self enrichment populated",
		);

		const map = consumer.serviceMap();
		const self = map.get(consumerName);
		expect(self?.eventSubscriptions.map((es) => es.pattern)).toContain(
			subPattern,
		);
		expect(self?.outgoingCalls.map((oc) => oc.targetMethod)).toContain(method);

		const peer = map.get(calleeName);
		expect(peer).toBeDefined();
		expect(peer?.methods.map((m) => m.name)).toContain(method);
	}, 25_000);
});

describe("misc: structured-log ingest", () => {
	interface LogRow {
		level: string;
		message: string;
		fields: unknown;
		service_id: string | null;
		instance_id: string | null;
	}

	async function fetchSdkLog(message: string): Promise<LogRow | undefined> {
		return withDb(async (sql) => {
			const rows = (await sql`
				SELECT level, message, fields, service_id::text, instance_id
				FROM telemetry_logs
				WHERE source = 'sdk' AND message = ${message}
				ORDER BY at DESC LIMIT 1
			`) as LogRow[];
			return rows[0];
		});
	}

	test("sdk logger entries persist per-service in telemetry_logs with level/fields/attribution", async () => {
		const sb = await shared("primary");

		const stamp = Date.now();
		const infoMsg = `e2e log probe info ${stamp}`;
		const warnMsg = `e2e log probe warn ${stamp}`;
		const errMsg = `e2e log probe error ${stamp}`;

		sb.logger.info(infoMsg, { phase: "probe", n: 1 });
		sb.logger.warn(warnMsg);
		sb.logger.error(errMsg, { boom: true });

		// All three, not just the first: the ring is flushed in batches, so the
		// entries can be committed by separate ones and the arrival of the first
		// says nothing about the rest.
		await waitFor(
			async () =>
				Boolean(
					(await fetchSdkLog(infoMsg)) &&
						(await fetchSdkLog(warnMsg)) &&
						(await fetchSdkLog(errMsg)),
				),
			15_000,
			"sdk logs persisted",
		);

		const info = await fetchSdkLog(infoMsg);
		const warn = await fetchSdkLog(warnMsg);
		const err = await fetchSdkLog(errMsg);

		expect(info).toBeDefined();
		expect(warn).toBeDefined();
		expect(err).toBeDefined();

		// Per-service attribution — the row the Logs tab filters on.
		expect(info?.service_id).not.toBeNull();
		expect(info?.instance_id).not.toBeNull();

		expect(info?.level).toBe("info");
		expect(warn?.level).toBe("warn");
		expect(err?.level).toBe("error");

		expect(info?.fields).toMatchObject({ phase: "probe", n: 1 });
		expect(err?.fields).toMatchObject({ boom: true });
	}, 20_000);
});

// Both tests mutate GLOBAL runtime_settings rows that affect every connected
// SDK. They save+restore every key in afterEach and run serially in this single
// process — never parallel with each other or any other capture-mode reader.
describe("misc: per-channel payload capture", () => {
	const CAPTURE_KEYS = [
		"rpc.payload_capture",
		"http.payload_capture",
		"events.payload_capture",
		"workflows.payload_capture",
	] as const;

	async function setCapture(key: string, value: string): Promise<void> {
		await withDb(async (sql) => {
			await sql`UPDATE runtime_settings SET value = ${value} WHERE key = ${key}`;
		});
	}

	async function getCapture(key: string): Promise<string> {
		return withDb(async (sql) => {
			const rows = (await sql`
				SELECT value FROM runtime_settings WHERE key = ${key}
			`) as Array<{ value: string }>;
			return rows[0]?.value ?? "none";
		});
	}

	let sb: ServiceBridge | undefined;
	const saved = new Map<string, string>();

	beforeEach(async () => {
		sb = undefined;
		saved.clear();
		for (const key of CAPTURE_KEYS) {
			saved.set(key, await getCapture(key));
		}
	});

	afterEach(async () => {
		await sb?.stop().catch(() => {});
		sb = undefined;
		for (const [key, value] of saved) {
			await setCapture(key, value);
		}
	});

	test("SDK reads distinct per-channel modes from the first snapshot", async () => {
		await setCapture("rpc.payload_capture", "all");
		await setCapture("events.payload_capture", "none");
		await setCapture("workflows.payload_capture", "errors");
		// Let the runtime pick up the NOTIFY before connecting so the first
		// snapshot already carries the new modes.
		await sleep(1_500);

		const { url, key } = rawKey("primary");
		sb = new ServiceBridge(url, key, FAST_OPTS);
		await sb.start();

		await waitFor(
			() => sb?.telemetry.captureModeForChannel(Channel.RPC) === "all",
			5_000,
			"rpc capture mode = all",
		);

		expect(sb.telemetry.captureModeForChannel(Channel.RPC)).toBe("all");
		expect(sb.telemetry.captureModeForChannel(Channel.EVENT)).toBe("none");
		expect(sb.telemetry.captureModeForChannel(Channel.WORKFLOW)).toBe("errors");
	}, 20_000);

	test("live settings change re-emits new mode on the same connection, other channels untouched", async () => {
		await setCapture("rpc.payload_capture", "none");
		await setCapture("events.payload_capture", "none");
		await sleep(1_500);

		const { url, key } = rawKey("primary");
		sb = new ServiceBridge(url, key, FAST_OPTS);
		await sb.start();

		await waitFor(
			() => sb?.telemetry.captureModeForChannel(Channel.EVENT) === "none",
			5_000,
			"events capture mode = none initially",
		);
		expect(sb.telemetry.captureModeForChannel(Channel.RPC)).toBe("none");

		// Operator flips ONLY the events channel on the live runtime.
		await setCapture("events.payload_capture", "all");

		// Same connection picks up the new mode via live re-emit (NOTIFY → loader
		// OnChanged → RegistryUpdate), no reconnect.
		await waitFor(
			() => sb?.telemetry.captureModeForChannel(Channel.EVENT) === "all",
			5_000,
			"events capture mode flips to all live",
		);
		expect(sb.telemetry.captureModeForChannel(Channel.EVENT)).toBe("all");
		// rpc untouched — channels are independent.
		expect(sb.telemetry.captureModeForChannel(Channel.RPC)).toBe("none");
	}, 20_000);
});
