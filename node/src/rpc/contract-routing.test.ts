import { describe, expect, it } from "bun:test";
import { join } from "node:path";
import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { MethodType } from "../pb/servicebridge/v1/registry";
import { computeContractHash } from "../serde/contract-hash";
import { buildSchemaPair } from "../serde/serializer";
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

// Mock InstanceCache returning fixed Candidates.
function mkInstanceCache(candidates: Candidate[]): InstanceCache {
	return {
		pickAll: () => candidates,
		descriptorFor: () => candidates[0]?.descriptor ?? null,
		bind: () => {},
		dispose: () => {},
	} as unknown as InstanceCache;
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
	it("LB filters out instances whose contractHash differs", async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const callerHash = computeContractHash(pair);
		const otherHash = "f".repeat(64);

		const candidates: Candidate[] = [
			{
				descriptor: mkDescriptor("inst-A", otherHash),
				instance: mkInstance("inst-A", "h:1"),
				isUnhealthyAt: null,
			},
			{
				descriptor: mkDescriptor("inst-B", callerHash),
				instance: mkInstance("inst-B", "h:2"),
				isUnhealthyAt: null,
			},
			{
				descriptor: mkDescriptor("inst-C", otherHash),
				instance: mkInstance("inst-C", "h:3"),
				isUnhealthyAt: null,
			},
		];
		const instances = mkInstanceCache(candidates);

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
			"caller-svc-id",
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
		const otherHash = "e".repeat(64);

		const candidates: Candidate[] = [
			{
				descriptor: mkDescriptor("inst-A", otherHash),
				instance: mkInstance("inst-A", "h:1"),
				isUnhealthyAt: null,
			},
		];
		const instances = mkInstanceCache(candidates);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);

		const client = new RpcClient(
			new FakeProxy() as unknown as ProxyTransport,
			null as unknown as DirectTransport,
			instances,
			schemas.asResolver(),
			"caller-svc-id",
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

	it('transport="direct" with no matching instance throws', async () => {
		const pair = await buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		const otherHash = "d".repeat(64);

		const candidates: Candidate[] = [
			{
				descriptor: mkDescriptor("inst-A", otherHash),
				instance: mkInstance("inst-A", "h:1"),
				isUnhealthyAt: null,
			},
		];
		const instances = mkInstanceCache(candidates);

		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const schemas = new SchemaRegistry();
		schemas.set("payment-svc", "charge", pair);

		const client = new RpcClient(
			new FakeProxy() as unknown as ProxyTransport,
			{} as unknown as DirectTransport, // direct enabled but never used
			instances,
			schemas.asResolver(),
			"caller-svc-id",
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
