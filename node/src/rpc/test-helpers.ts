// test-helpers.ts — fakes shared across RpcClient unit tests.
// @internal — only used inside *.test.ts files; not re-exported via index.ts.

import type { ServiceBridge, TelemetryAPI } from "../connection/service-bridge";
import { OpHandle } from "../telemetry/ops";
import { TelemetryRing } from "../telemetry/ring";

// makeStubSb returns a minimal ServiceBridge-shaped object that satisfies
// RpcClient: a fixed instance id ("test-instance") + identity probe +
// a real telemetry ring for RPC.CALL emission (ADR-0001).
//
// Returning via `as unknown as` is intentional: we avoid pulling in the full
// bridge (connection, transports, registry) for unit tests.
export function makeStubSb(opts?: {
	instanceId?: string;
	ring?: TelemetryRing;
	captureMode?: "all" | "errors" | "none";
}): ServiceBridge {
	const instanceId = opts?.instanceId ?? "test-instance";
	const ring = opts?.ring ?? new TelemetryRing();
	const effectiveCaptureMode = opts?.captureMode ?? "none";
	const telemetry: TelemetryAPI = {
		startOp(params) {
			return OpHandle.start(ring, {
				...params,
				effectiveCaptureMode,
			});
		},
		captureModeForChannel: () => effectiveCaptureMode,
		log: {
			debug: () => {},
			info: () => {},
			warn: () => {},
			error: () => {},
		},
		counter: () => ({ inc: () => {} }) as ReturnType<TelemetryAPI["counter"]>,
		gauge: () => ({}) as ReturnType<TelemetryAPI["gauge"]>,
		histogram: () => ({}) as ReturnType<TelemetryAPI["histogram"]>,
	};
	const stub = {
		telemetry,
		instanceIdString: () => instanceId,
		identity: () => ({
			sessionId: "test-session",
			serviceId: "test-service-uuid",
			serviceName: "test-service",
			instanceId,
		}),
	};
	return stub as unknown as ServiceBridge;
}
