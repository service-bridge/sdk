// logs.ts — structured logging into the telemetry ring.
// @public — см. ./README.md

import { type Log, LogLevel } from "../pb/servicebridge/v1/telemetry";
import { currentTraceContext } from "./context";
import type { TelemetryRing } from "./ring";
import { ZERO_OP_ID } from "./trace-context";

export { LogLevel };

export type LogFields = Record<string, unknown>;

// logger pushes a Log entry into the ring.
// @public — см. ./README.md
export function makeLogger(ring: TelemetryRing, instanceId: string) {
	return {
		debug(message: string, fields?: LogFields): void {
			push(ring, instanceId, LogLevel.LOG_LEVEL_DEBUG, message, fields);
		},
		info(message: string, fields?: LogFields): void {
			push(ring, instanceId, LogLevel.LOG_LEVEL_INFO, message, fields);
		},
		warn(message: string, fields?: LogFields): void {
			push(ring, instanceId, LogLevel.LOG_LEVEL_WARN, message, fields);
		},
		error(message: string, fields?: LogFields): void {
			push(ring, instanceId, LogLevel.LOG_LEVEL_ERROR, message, fields);
		},
	};
}

function push(
	ring: TelemetryRing,
	instanceId: string,
	level: LogLevel,
	message: string,
	fields?: LogFields,
): void {
	const ctx = currentTraceContext();
	const entry: Log = {
		atUnixMs: Date.now(),
		level,
		message,
		fieldsJson: fields ? encode(fields) : Buffer.alloc(0),
		traceId: ctx?.traceId ?? "",
		// parentOpId is the operation the log was emitted inside. ZERO_OP_ID is the
		// "no enclosing op" sentinel, not an op that exists — send empty so the
		// runtime stores NULL instead of a dangling id.
		opId: ctx && ctx.parentOpId !== ZERO_OP_ID ? ctx.parentOpId : "",
		instanceId,
		source: "sdk",
	};
	ring.push("logs", entry);
}

function encode(fields: LogFields): Buffer {
	return Buffer.from(JSON.stringify(fields));
}
