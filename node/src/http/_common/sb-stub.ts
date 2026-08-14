// sb-stub.ts — ServiceBridge-заглушка для тестов HTTP-интеграций: пишет роуты в
// настоящий RouteCollector, а телеметрию складывает в массивы для ассертов.
// @internal — используется только в *.test.ts, не реэкспортируется.

import type { ServiceBridge } from "../../connection/service-bridge";
import type { Status } from "../../telemetry/ops";
import type { CaptureMode } from "../../telemetry/payload-capture";
import { RouteCollector } from "../route";

export interface StartedOp {
	traceId: string;
	opId: string;
	parentOpId: string;
	subject: string;
	businessKey: string;
}

export interface EndCall {
	status: Status;
	message?: string;
}

export interface CaptureCall {
	direction: "in" | "out";
	bytes: Uint8Array;
}

export interface SbStub {
	sb: ServiceBridge;
	routes: RouteCollector;
	readonly endpoint: string | null;
	readonly publishHits: number;
	readonly started: StartedOp[];
	readonly endCalls: EndCall[];
	readonly captures: CaptureCall[];
}

export function makeSbStub(captureMode: CaptureMode = "none"): SbStub {
	let opCounter = 0;
	const state = {
		endpoint: null as string | null,
		publishHits: 0,
		started: [] as StartedOp[],
		endCalls: [] as EndCall[],
		captures: [] as CaptureCall[],
	};
	const routes = new RouteCollector({
		setEndpoint(ep: string) {
			state.endpoint = ep;
		},
		triggerRestart() {
			state.publishHits++;
		},
	});
	const telemetry = {
		startOp(p: {
			traceId?: string;
			parentOpId?: string;
			subject: string;
			businessKey?: string;
		}) {
			opCounter++;
			const op: StartedOp = {
				traceId: p.traceId ?? "",
				opId: `op-${opCounter}`,
				parentOpId: p.parentOpId ?? "",
				subject: p.subject,
				businessKey: p.businessKey ?? "",
			};
			state.started.push(op);
			return {
				traceId: op.traceId,
				opId: op.opId,
				// Mirrors OpHandle.scope: the plugins nest downstream work under it,
				// so a stub without it would silently un-nest and still pass.
				scope: { traceId: op.traceId, parentOpId: op.opId },
				capturing: captureMode !== "none",
				captureIn(bytes: Uint8Array) {
					state.captures.push({ direction: "in", bytes });
				},
				captureOut(bytes: Uint8Array) {
					state.captures.push({ direction: "out", bytes });
				},
				end(status: Status, message?: string) {
					state.endCalls.push({ status, message });
				},
			};
		},
	};
	const sb = { routes, telemetry } as unknown as ServiceBridge;
	return {
		sb,
		routes,
		get endpoint() {
			return state.endpoint;
		},
		get publishHits() {
			return state.publishHits;
		},
		get started() {
			return state.started;
		},
		get endCalls() {
			return state.endCalls;
		},
		get captures() {
			return state.captures;
		},
	};
}
