import { describe, expect, it } from "bun:test";
import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import type { WatchStream } from "../registry/watch";
import { InstanceCache } from "./instance-cache";

// Stub WatchStream that returns prepared snapshots. Tests the mapping logic
// inside InstanceCache.refresh — specifically, that the runtime-supplied
// `is_unhealthy_since_unix_ms` (proto field, decoded to `isUnhealthySinceUnixMs: number`) is
// propagated into the Instance.isUnhealthyAt slot consumed by the LB (ADR-0009 §3).
function stubWatch(
	instances: ServiceInstanceInfo[],
	methods: MethodDescriptor[],
): WatchStream {
	const onChangeListeners: Array<
		(added: ServiceInstanceInfo[], removed: ServiceInstanceInfo[]) => void
	> = [];
	return {
		instancesSnapshot(): Map<string, ServiceInstanceInfo> {
			return new Map(instances.map((i) => [i.instanceId, i]));
		},
		snapshot(): ReadonlyMap<string, MethodDescriptor> {
			return new Map(
				methods.map((m) => [`${m.instanceId}:${m.type}:${m.name}`, m]),
			);
		},
		onInstancesChange(
			fn: (
				added: ServiceInstanceInfo[],
				removed: ServiceInstanceInfo[],
			) => void,
		): () => void {
			onChangeListeners.push(fn);
			return () => {};
		},
	} as unknown as WatchStream;
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

function mkMethod(instanceId: string, name: string): MethodDescriptor {
	return {
		instanceId,
		serviceId: "svc",
		serviceName: "svc",
		type: "rpc",
		name,
		published: true,
		contractHash: new Uint8Array(),
		inputSchema: new Uint8Array(),
		outputSchema: new Uint8Array(),
	} as unknown as MethodDescriptor;
}

describe("InstanceCache health hint propagation (ADR-0009 §3)", () => {
	it("maps runtime-supplied isUnhealthySinceUnixMs → isUnhealthyAt", () => {
		const unhealthyAtMs = new Date("2026-05-20T10:00:00Z").getTime();
		const cache = new InstanceCache();
		cache.bind(
			stubWatch(
				[
					mkInstance("inst-A"),
					mkInstance("inst-B", { isUnhealthySinceUnixMs: unhealthyAtMs }),
				],
				[mkMethod("inst-A", "Charge"), mkMethod("inst-B", "Charge")],
			),
		);

		const candidates = cache.pickAll("svc", "Charge");
		expect(candidates).toHaveLength(2);

		const a = candidates.find((c) => c.instance.instanceId === "inst-A");
		const b = candidates.find((c) => c.instance.instanceId === "inst-B");
		expect(a?.isUnhealthyAt).toBeNull();
		expect(b?.isUnhealthyAt?.getTime()).toBe(unhealthyAtMs);
	});

	it("leaves isUnhealthyAt null when proto field is absent", () => {
		const cache = new InstanceCache();
		cache.bind(
			stubWatch([mkInstance("inst-A")], [mkMethod("inst-A", "Charge")]),
		);
		const candidates = cache.pickAll("svc", "Charge");
		expect(candidates).toHaveLength(1);
		expect(candidates[0]?.isUnhealthyAt).toBeNull();
	});
});
