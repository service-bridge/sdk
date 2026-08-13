import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { MethodType } from "../pb/servicebridge/v1/registry";
import type { WatchStream } from "../registry/watch";
import { computeContractHash } from "../serde/contract-hash";
import { buildSchemaPair } from "../serde/serializer";
import { CircuitBreakerRegistry } from "./circuit-breaker";
import { RpcClient, SchemaRegistry } from "./client";
import type { DirectTransport } from "./direct-transport";
import { InstanceCache } from "./instance-cache";
import { LoadBalancer, NoLiveInstanceError } from "./lb";
import type { ProxyTransport } from "./proxy-transport";
import { makeStubSb } from "./test-helpers";

const protoFile = join(
	import.meta.dir,
	"..",
	"serde",
	"testdata",
	"payment.proto",
);

function mkInstance(id: string, endpoint: string): ServiceInstanceInfo {
	return {
		instanceId: id,
		serviceId: "svc-id",
		serviceName: "payment-svc",
		callEndpoint: endpoint,
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

// mkInstanceCache binds a real InstanceCache to a stub registry snapshot, so the
// contract-hash index under test is the production one.
function mkInstanceCache(
	entries: { instanceId: string; endpoint: string; hash: string }[],
): InstanceCache {
	const instances = entries.map((e) => mkInstance(e.instanceId, e.endpoint));
	const methods = entries.map((e) => mkDescriptor(e.instanceId, e.hash));
	const watch = {
		instancesSnapshot: () =>
			new Map(instances.map((i) => [i.instanceId, i] as const)),
		snapshot: () =>
			new Map(
				methods.map((m) => [`${m.instanceId}:${m.type}:${m.name}`, m] as const),
			),
		onInstancesChange: () => () => {},
	} as unknown as WatchStream;

	const cache = new InstanceCache();
	cache.bind(watch, { retain: () => {} });
	return cache;
}

// Mock ProxyTransport that records the call for assertions.
class FakeProxy {
	calls: { service: string; method: string }[] = [];
	async callUnary(service: string, method: string): Promise<Uint8Array> {
		this.calls.push({ service, method });
		return new Uint8Array(0);
	}
	async *callStream(): AsyncIterable<Uint8Array> {}
	close(): void {}
}

describe("contract-version routing", () => {
	it("routes only to instances whose contractHash matches the caller", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const callerHash = computeContractHash(pair);
		const otherHash = "f".repeat(64);

		const instances = mkInstanceCache([
			{ instanceId: "inst-A", endpoint: "h:1", hash: otherHash },
			{ instanceId: "inst-B", endpoint: "h:2", hash: callerHash },
			{ instanceId: "inst-C", endpoint: "h:3", hash: otherHash },
		]);

		expect(
			instances
				.candidatesFor("payment-svc", "charge", callerHash)
				.map((c) => c.instance.instanceId),
		).toEqual(["inst-B"]);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const proxy = new FakeProxy();
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);

		const client = new RpcClient(
			proxy as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			() => "caller-svc-id",
			cb,
			lb,
			makeStubSb(),
		);

		// Override output.decode to return an object (avoid real bytes round-trip).
		const origDecode = pair.output.decode.bind(pair.output);
		pair.output.decode = (() => ({
			transactionId: "x",
			ok: true,
		})) as typeof origDecode;

		await client.call(
			"payment-svc",
			"charge",
			{ userId: "u", amount: 1 },
			{ transport: "proxy", retry: { maxAttempts: 1 } },
		);

		// Only the matching instance should have been picked → call went through proxy.
		expect(proxy.calls).toHaveLength(1);
	});

	it("fail-fast when no instance matches caller hash", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const instances = mkInstanceCache([
			{ instanceId: "inst-A", endpoint: "h:1", hash: "e".repeat(64) },
		]);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);

		const client = new RpcClient(
			new FakeProxy() as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			() => "caller-svc-id",
			cb,
			lb,
			makeStubSb(),
		);

		await expect(
			client.call(
				"payment-svc",
				"charge",
				{ userId: "u", amount: 1 },
				{ transport: "proxy", retry: { maxAttempts: 1 } },
			),
		).rejects.toThrow(/no instance.*matches caller contract/);
	});

	it("surfaces an empty callee fleet as NoLiveInstanceError with UNAVAILABLE", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const callerHash = computeContractHash(pair);
		// Matching contract, but no reachable endpoint → the LB filters it out.
		const instances = mkInstanceCache([
			{ instanceId: "inst-A", endpoint: "", hash: callerHash },
		]);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);

		const client = new RpcClient(
			new FakeProxy() as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			() => "caller-svc-id",
			cb,
			lb,
			makeStubSb(),
		);

		const err = await client
			.call(
				"payment-svc",
				"charge",
				{ userId: "u", amount: 1 },
				{ transport: "proxy", retry: { maxAttempts: 1 } },
			)
			.then(
				() => null,
				(e: unknown) => e,
			);

		// The type survives the caller path: "callee fleet is empty" stays
		// distinguishable from "callee answered UNAVAILABLE" without regex on text.
		expect(err).toBeInstanceOf(NoLiveInstanceError);
		expect((err as { code?: number }).code).toBe(14);
	});

	it('transport="direct" with no matching instance throws', async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const instances = mkInstanceCache([
			{ instanceId: "inst-A", endpoint: "h:1", hash: "d".repeat(64) },
		]);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);

		const client = new RpcClient(
			new FakeProxy() as unknown as ProxyTransport,
			{} as unknown as DirectTransport, // direct enabled but never used
			instances,
			schemas.asResolver(),
			() => "caller-svc-id",
			cb,
			lb,
			makeStubSb(),
		);

		await expect(
			client.call(
				"payment-svc",
				"charge",
				{ userId: "u", amount: 1 },
				{ transport: "direct", retry: { maxAttempts: 1 } },
			),
		).rejects.toThrow(
			/no endpoint.*matching contract|no instance.*matches caller contract/,
		);
	});
});
