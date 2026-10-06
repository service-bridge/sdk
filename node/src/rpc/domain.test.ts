import { describe, expect, it, mock } from "bun:test";
import { join } from "node:path";
import { AccessDeniedError, ServiceBridgeError, StateError } from "../errors";
import { MethodType } from "../pb/servicebridge/v1/registry";
import { Registry } from "../registry/registry";
import type { RpcClient } from "./client";
import { RpcDomain } from "./domain";

const protoFile = join(
	import.meta.dir,
	"..",
	"serde",
	"testdata",
	"payment.proto",
);

function makeRegistry(): Registry {
	return new Registry();
}

function makeDomain(client: RpcClient | null = null): {
	domain: RpcDomain;
	registry: Registry;
} {
	const registry = makeRegistry();
	const domain = new RpcDomain(registry, () => client);
	return { domain, registry };
}

describe("RpcDomain.handle", () => {
	it("registers RPC entry in registry._handle._entries", async () => {
		const { domain, registry } = makeDomain();
		domain.handle("charge", () => ({}), {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
		});
		await registry._handle.finalize();
		const methods = registry._handle.incomingMethods();
		expect(methods).toHaveLength(1);
		expect(methods[0]!.type).toBe(MethodType.METHOD_TYPE_RPC);
		expect(methods[0]!.name).toBe("charge");
		expect(methods[0]!.streaming).toBe(false);
		expect(methods[0]!.inputSchemaJson.length).toBeGreaterThan(0);
		expect(methods[0]!.outputSchemaJson.length).toBeGreaterThan(0);
	});
});

describe("RpcDomain.handleStream", () => {
	it("registers streaming RPC entry with streaming=true", async () => {
		const { domain, registry } = makeDomain();
		domain.handleStream(
			"charge",
			async function* () {
				yield {};
			},
			{
				schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
			},
		);
		await registry._handle.finalize();
		const methods = registry._handle.incomingMethods();
		expect(methods).toHaveLength(1);
		expect(methods[0]!.streaming).toBe(true);
		expect(methods[0]!.name).toBe("charge");
	});
});

describe("RpcDomain.call", () => {
	it("delegates to RpcClient.call with correct arguments", async () => {
		const mockCall = mock(async () => ({ ok: true }));
		const client = { call: mockCall } as unknown as RpcClient;
		const { domain } = makeDomain(client);

		const result = await domain.call("svc", "charge", { amount: 10 });
		expect(result).toEqual({ ok: true });
		expect(mockCall).toHaveBeenCalledTimes(1);
		const args = mockCall.mock.calls[0] as unknown[];
		expect(args[0]).toBe("svc");
		expect(args[1]).toBe("charge");
		expect(args[2]).toEqual({ amount: 10 });
	});

	it("throws when client is not ready", async () => {
		const { domain } = makeDomain(null);
		await expect(domain.call("svc", "method", {})).rejects.toBeInstanceOf(
			StateError,
		);
	});
});

describe("RpcDomain.handle duplicates", () => {
	it("refuses a second handler for the same method", () => {
		const { domain } = makeDomain();
		const opts = {
			schema: { protoFile, input: "ChargeRequest", output: "ChargeResponse" },
		};
		domain.handle("charge", () => ({}), opts);
		expect(() => domain.handle("charge", () => ({}), opts)).toThrow(
			/already has a handler/,
		);
	});
});

describe("RpcDomain.call access denied (gate #3)", () => {
	type Violation = {
		declaration: string;
		value: string;
		denySide: string;
		reason: string;
	};

	it("emits policy_violation for an AccessDeniedError and rethrows it", async () => {
		const violations: Violation[] = [];
		const denied = new AccessDeniedError("no rpc.call rule for billing/charge");
		const client = {
			call: () => Promise.reject(denied),
		} as unknown as RpcClient;
		const domain = new RpcDomain(
			makeRegistry(),
			() => client,
			(v) => violations.push(v),
		);

		let caught: unknown;
		try {
			await domain.call("billing", "charge", {});
		} catch (e) {
			caught = e;
		}
		expect(caught).toBe(denied);
		expect(violations).toHaveLength(1);
		expect(violations[0]).toMatchObject({
			declaration: "rpc.call",
			value: "billing/charge",
			denySide: "self_egress",
		});
	});

	it("rethrows other errors unchanged", async () => {
		const client = {
			call: () =>
				Promise.reject(new ServiceBridgeError("CONNECTION", "unavailable")),
		} as unknown as RpcClient;
		const domain = new RpcDomain(makeRegistry(), () => client);

		let caught: unknown;
		try {
			await domain.call("billing", "charge", {});
		} catch (e) {
			caught = e;
		}
		expect(caught).not.toBeInstanceOf(AccessDeniedError);
		expect((caught as Error).message).toBe("unavailable");
	});
});
