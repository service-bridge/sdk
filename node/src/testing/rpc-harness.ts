import type { RpcHandlerFn } from "../registry/registry";
import type { CallOpts } from "../rpc/client";

// RpcCallRecord captures one outbound `rpc.call()` made through the harness,
// in the order it happened. `opts` is present only when the caller passed it.
// @public — см. ./README.md
export interface RpcCallRecord {
	serviceName: string;
	methodName: string;
	payload: unknown;
	opts?: CallOpts;
}

// RpcMockResponder computes (or is) the response returned by a mocked
// outbound `rpc.call()`. Passing a plain value to `mockResponse()` wraps it
// in a responder that ignores its arguments.
// @public — см. ./README.md
export type RpcMockResponder<Req = unknown, Res = unknown> = (
	payload: Req,
	opts?: CallOpts,
) => Res | Promise<Res>;

// TestRpcDomain doubles `sb.rpc` for unit tests. `handle()` registers
// handlers with the exact same `RpcHandlerFn` contract production code uses
// (src/registry/registry.ts); `invoke()` calls the registered handler
// in-memory — no wire encode/decode, no CallServer error-code mapping, the
// handler's own thrown error propagates unchanged. `call()` doubles the
// outbound direction: every call is recorded, and returns whatever
// `mockResponse()` configured for that (serviceName, methodName) pair, or
// throws if nothing was configured — a forgotten mock fails the test loudly
// instead of resolving to `undefined`.
// @public — см. ./README.md
export class TestRpcDomain {
	private readonly handlers = new Map<string, RpcHandlerFn>();
	private readonly responders = new Map<string, RpcMockResponder>();
	private readonly _calls: RpcCallRecord[] = [];

	handle<Req = unknown, Res = unknown>(
		name: string,
		fn: RpcHandlerFn<Req, Res>,
	): void {
		this.handlers.set(name, fn as RpcHandlerFn);
	}

	async invoke<Req = unknown, Res = unknown>(
		name: string,
		req: Req,
	): Promise<Res> {
		const fn = this.handlers.get(name);
		if (!fn) {
			throw new Error(`testing: no RPC handler registered for "${name}"`);
		}
		return (await fn(req)) as Res;
	}

	async call<Req = unknown, Res = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		opts?: CallOpts,
	): Promise<Res> {
		const record: RpcCallRecord = { serviceName, methodName, payload };
		if (opts !== undefined) record.opts = opts;
		this._calls.push(record);

		const responder = this.responders.get(
			responderKey(serviceName, methodName),
		);
		if (!responder) {
			throw new Error(
				`testing: no mock response configured for rpc.call("${serviceName}", "${methodName}") — ` +
					`call harness.rpc.mockResponse("${serviceName}", "${methodName}", ...) first`,
			);
		}
		return (await responder(payload, opts)) as Res;
	}

	mockResponse<Req = unknown, Res = unknown>(
		serviceName: string,
		methodName: string,
		responder: Res | RpcMockResponder<Req, Res>,
	): void {
		const fn: RpcMockResponder =
			typeof responder === "function"
				? (responder as RpcMockResponder)
				: () => responder;
		this.responders.set(responderKey(serviceName, methodName), fn);
	}

	calls(): readonly RpcCallRecord[] {
		return this._calls;
	}

	reset(): void {
		this.handlers.clear();
		this.responders.clear();
		this._calls.length = 0;
	}
}

function responderKey(serviceName: string, methodName: string): string {
	return `${serviceName}/${methodName}`;
}
