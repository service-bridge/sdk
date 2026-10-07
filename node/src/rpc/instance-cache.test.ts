import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import type {
	MethodDescriptor,
	RegisterRequest,
	RegistryClient,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { WatchStream } from "../registry/watch";
import { InstanceCache, type InstanceRetainer } from "./instance-cache";

const HASH = "hash-1";

// Stub WatchStream that returns prepared snapshots. Tests the mapping logic
// inside InstanceCache.refresh — specifically, that the runtime-supplied
// `is_unhealthy_since_unix_ms` (proto field, decoded to `isUnhealthySinceUnixMs: number`) is
// propagated into the Instance.isUnhealthyAt slot consumed by the LB (ADR-0001).
function stubWatch(
	instances: ServiceInstanceInfo[],
	methods: MethodDescriptor[],
): WatchStream {
	return {
		instancesSnapshot(): Map<string, ServiceInstanceInfo> {
			return new Map(instances.map((i) => [i.instanceId, i]));
		},
		snapshot(): ReadonlyMap<string, MethodDescriptor> {
			return new Map(
				methods.map((m) => [
					`${m.instanceId}:${m.type}:${m.name}:${m.contractHash}`,
					m,
				]),
			);
		},
		onMethodsChange(): () => void {
			return () => {};
		},
		onInstancesChange(): () => void {
			return () => {};
		},
		onRevoked(): () => void {
			return () => {};
		},
		isRevoked: () => false,
	} as unknown as WatchStream;
}

// recordingRetainer captures every liveKeys set InstanceCache pushes down.
function recordingRetainer(): InstanceRetainer & {
	calls: ReadonlySet<string>[];
} {
	const calls: ReadonlySet<string>[] = [];
	return {
		calls,
		retain(liveKeys: ReadonlySet<string>) {
			calls.push(new Set(liveKeys));
		},
	};
}

function mkInstance(
	instanceId: string,
	overrides: Partial<ServiceInstanceInfo> = {},
): ServiceInstanceInfo {
	return {
		instanceId,
		serviceId: "svc",
		serviceName: "svc",
		status: "connected",
		callEndpoint: "host:14446",
		httpEndpoint: "",
		isUnhealthySinceUnixMs: 0,
		...overrides,
	} as ServiceInstanceInfo;
}

function mkMethod(
	instanceId: string,
	name: string,
	contractHash = HASH,
): MethodDescriptor {
	return {
		instanceId,
		serviceId: "svc",
		serviceName: "svc",
		type: "rpc",
		name,
		published: true,
		contractHash,
		inputSchema: new Uint8Array(),
		outputSchema: new Uint8Array(),
	} as unknown as MethodDescriptor;
}

function bound(
	instances: ServiceInstanceInfo[],
	methods: MethodDescriptor[],
): { cache: InstanceCache; retainer: ReturnType<typeof recordingRetainer> } {
	const cache = new InstanceCache();
	const retainer = recordingRetainer();
	cache.bind(stubWatch(instances, methods), retainer);
	return { cache, retainer };
}

describe("InstanceCache health hint propagation (ADR-0001)", () => {
	it("maps runtime-supplied isUnhealthySinceUnixMs → isUnhealthyAt", () => {
		const unhealthyAtMs = new Date("2026-05-20T10:00:00Z").getTime();
		const { cache } = bound(
			[
				mkInstance("inst-A"),
				mkInstance("inst-B", { isUnhealthySinceUnixMs: unhealthyAtMs }),
			],
			[mkMethod("inst-A", "Charge"), mkMethod("inst-B", "Charge")],
		);

		const candidates = cache.candidatesFor("svc", "Charge", HASH);
		expect(candidates).toHaveLength(2);

		const a = candidates.find((c) => c.instance.instanceId === "inst-A");
		const b = candidates.find((c) => c.instance.instanceId === "inst-B");
		expect(a?.isUnhealthyAt).toBeNull();
		expect(b?.isUnhealthyAt?.getTime()).toBe(unhealthyAtMs);
	});

	it("leaves isUnhealthyAt null when proto field is absent", () => {
		const { cache } = bound(
			[mkInstance("inst-A")],
			[mkMethod("inst-A", "Charge")],
		);
		const candidates = cache.candidatesFor("svc", "Charge", HASH);
		expect(candidates).toHaveLength(1);
		expect(candidates[0]?.isUnhealthyAt).toBeNull();
	});
});

describe("InstanceCache contract-version index (ADR-0005)", () => {
	it("returns only candidates whose descriptor carries the caller hash", () => {
		const { cache } = bound(
			[mkInstance("inst-A"), mkInstance("inst-B"), mkInstance("inst-C")],
			[
				mkMethod("inst-A", "Charge", "old-hash"),
				mkMethod("inst-B", "Charge", HASH),
				mkMethod("inst-C", "Charge", "old-hash"),
			],
		);

		const matching = cache.candidatesFor("svc", "Charge", HASH);
		expect(matching.map((c) => c.instance.instanceId)).toEqual(["inst-B"]);
		expect(cache.candidatesFor("svc", "Charge", "old-hash")).toHaveLength(2);
		expect(cache.candidatesFor("svc", "Charge", "unknown")).toHaveLength(0);
	});

	it("skips descriptors whose instance is not in the snapshot", () => {
		const { cache } = bound(
			[mkInstance("inst-A")],
			[mkMethod("inst-A", "Charge"), mkMethod("inst-gone", "Charge")],
		);
		expect(cache.candidatesFor("svc", "Charge", HASH)).toHaveLength(1);
	});

	it("descriptorFor resolves the pair without an endpoint", () => {
		const { cache } = bound(
			[mkInstance("inst-A")],
			[mkMethod("inst-A", "Charge")],
		);
		expect(cache.descriptorFor("svc", "Charge")?.name).toBe("Charge");
		expect(cache.descriptorFor("svc", "Refund")).toBeNull();
	});
});

describe("InstanceCache breaker key + retention", () => {
	it("puts the resolved breaker key on every instance", () => {
		const { cache } = bound(
			[mkInstance("inst-A")],
			[mkMethod("inst-A", "Charge")],
		);
		const candidate = cache.candidatesFor("svc", "Charge", HASH)[0]!;
		expect((candidate.instance as { cbKey?: string }).cbKey).toBe("svc:inst-A");
	});

	it("hands the live breaker keys to the retainer on refresh and clears on dispose", () => {
		const { cache, retainer } = bound(
			[mkInstance("inst-A"), mkInstance("inst-B")],
			[mkMethod("inst-A", "Charge"), mkMethod("inst-B", "Charge")],
		);
		expect(retainer.calls).toHaveLength(1);
		expect([...retainer.calls[0]!].sort()).toEqual([
			"svc:inst-A",
			"svc:inst-B",
		]);

		cache.dispose();
		expect(retainer.calls).toHaveLength(2);
		expect(retainer.calls[1]!.size).toBe(0);
	});
});

describe("InstanceCache lookup cost is independent of fleet size", () => {
	function fleet(instanceCount: number, methodCount: number) {
		const instances: ServiceInstanceInfo[] = [];
		const methods: MethodDescriptor[] = [];
		for (let i = 0; i < instanceCount; i++) {
			const id = `inst-${i}`;
			instances.push(mkInstance(id));
			for (let m = 0; m < methodCount; m++) {
				methods.push(mkMethod(id, `Method${m}`));
			}
		}
		return bound(instances, methods).cache;
	}

	it("resolves the candidate list by lookup, not by scanning descriptors", () => {
		const cache = fleet(200, 30);
		const first = cache.candidatesFor("svc", "Method17", HASH);
		expect(first).toHaveLength(200);
		// Same array instance on every call — proof the list is materialised at
		// refresh time and the call path neither scans nor rebuilds it.
		expect(cache.candidatesFor("svc", "Method17", HASH)).toBe(first);
	});

	it("does not get slower as the callee fleet grows", () => {
		const small = fleet(2, 30);
		const large = fleet(200, 30);
		const iterations = 50_000;

		const measure = (cache: InstanceCache): number => {
			// Warm-up pass so both measurements run against optimised code.
			for (let i = 0; i < iterations; i++) {
				cache.candidatesFor("svc", "Method17", HASH);
			}
			const started = performance.now();
			for (let i = 0; i < iterations; i++) {
				cache.candidatesFor("svc", "Method17", HASH);
			}
			return performance.now() - started;
		};

		const smallMs = Math.max(measure(small), 0.001);
		const largeMs = measure(large);
		// A linear scan over 6000 descriptors would be ~100× the 60-descriptor
		// case; the bound is deliberately loose so timing noise cannot flake it.
		expect(largeMs / smallMs).toBeLessThan(10);
	});
});

// Exercise the real watch reducer: descriptor-only updates do not emit an
// instance delta, but both serviceMap and the RPC index must observe them.
describe("InstanceCache descriptor lifecycle", () => {
	function fixture() {
		const stream = new EventEmitter() as EventEmitter & { cancel(): void };
		stream.cancel = () => {};
		const watch = new WatchStream();
		const client = {
			registerAndWatch: () => stream,
		} as unknown as RegistryClient;
		const cache = new InstanceCache();
		const retainer = recordingRetainer();
		cache.bind(watch, retainer);
		watch.start({} as RegisterRequest, client);
		return { stream, watch, cache, retainer };
	}

	function snapshot(
		stream: EventEmitter,
		methods: MethodDescriptor[],
		instances: ServiceInstanceInfo[],
	) {
		stream.emit("data", {
			snapshot: {
				methods,
				instances,
				eventSubscriptions: [],
				outgoingCalls: [],
			},
		});
	}

	function update(
		stream: EventEmitter,
		added: MethodDescriptor[],
		removed: MethodDescriptor[],
	) {
		stream.emit("data", {
			update: { added, removed, addedInstances: [], removedInstances: [] },
		});
	}

	it("makes a newly registered method callable without another instance event", () => {
		const { stream, watch, cache } = fixture();
		snapshot(stream, [], [mkInstance("inst-A")]);
		const method = mkMethod("inst-A", "Charge");
		update(stream, [method], []);
		expect([...watch.snapshot().values()]).toContain(method);
		expect(cache.descriptorFor("svc", "Charge")).toBe(method);
		expect(cache.candidatesFor("svc", "Charge", HASH)).toHaveLength(1);
		cache.dispose();
		watch.stop();
	});

	it("removes a deregistered method and updates changed contracts without instance churn", () => {
		const { stream, watch, cache } = fixture();
		const method = mkMethod("inst-A", "Charge");
		snapshot(stream, [method], [mkInstance("inst-A")]);
		const replacement = mkMethod("inst-A", "Charge", "new-hash");
		update(stream, [replacement], []);
		expect(cache.candidatesFor("svc", "Charge", HASH)).toHaveLength(0);
		expect(cache.candidatesFor("svc", "Charge", "new-hash")).toHaveLength(1);
		update(stream, [], [replacement]);
		expect(cache.descriptorFor("svc", "Charge")).toBeNull();
		expect(cache.candidatesFor("svc", "Charge", "new-hash")).toHaveLength(0);
		cache.dispose();
		watch.stop();
	});

	it("indexes descriptors from snapshots with no direct endpoint", () => {
		const { stream, watch, cache } = fixture();
		const method = mkMethod("inst-gone", "Charge");
		snapshot(stream, [method], []);
		expect(cache.descriptorFor("svc", "Charge")).toBe(method);
		expect(cache.candidatesFor("svc", "Charge", HASH)).toHaveLength(0);
		cache.dispose();
		watch.stop();
	});
	it("releases both subscriptions when rebinding and disposing", () => {
		const first = fixture();
		const second = fixture();
		first.cache.bind(second.watch, first.retainer);
		const afterBind = first.retainer.calls.length;
		update(first.stream, [mkMethod("inst-A", "Charge")], []);
		expect(first.retainer.calls).toHaveLength(afterBind);
		snapshot(
			second.stream,
			[mkMethod("inst-B", "Refund")],
			[mkInstance("inst-B")],
		);
		expect(first.cache.descriptorFor("svc", "Refund")?.name).toBe("Refund");
		first.cache.dispose();
		const afterDispose = first.retainer.calls.length;
		update(second.stream, [mkMethod("inst-B", "Charge")], []);
		expect(first.retainer.calls).toHaveLength(afterDispose);
		expect(first.cache.descriptorFor("svc", "Charge")).toBeNull();
		first.watch.stop();
		second.cache.dispose();
		second.watch.stop();
	});
});
