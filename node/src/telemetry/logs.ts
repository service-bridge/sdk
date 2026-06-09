// logs.ts — structured logging into the telemetry ring.
// @public — см. ./README.md

import { type Log, LogLevel } from "../pb/servicebridge/v1/telemetry";
import type { TelemetryRing } from "./ring";

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
	const entry: Log = {
		atUnixMs: Date.now(),
		level,
		message,
		fieldsJson: fields ? encode(fields) : Buffer.alloc(0),
		traceId: "",
		opId: "",
		instanceId,
		source: "sdk",
	};
	ring.push("logs", entry);
}

function encode(fields: LogFields): Buffer {
	return Buffer.from(JSON.stringify(fields));
}
