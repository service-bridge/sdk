import { describe, expect, test } from "bun:test";
import type { Log } from "../pb/servicebridge/v1/telemetry";
import { LogLevel, makeLogger } from "./logs";
import { TelemetryRing } from "./ring";

function firstLog(ring: TelemetryRing): Log {
	return ring.peek(10)[0]!.message as Log;
}

describe("logger", () => {
	test("info enqueues a Log entry", () => {
		const ring = new TelemetryRing();
		const logger = makeLogger(ring, "inst-1");
		logger.info("hello", { key: "val" });

		const items = ring.peek(10);
		expect(items).toHaveLength(1);
		expect(items[0]!.kind).toBe("logs");

		const decoded = items[0]!.message as Log;
		expect(decoded.message).toBe("hello");
		expect(decoded.level).toBe(LogLevel.LOG_LEVEL_INFO);
		expect(decoded.instanceId).toBe("inst-1");
		expect(decoded.source).toBe("sdk");
	});

	test("warn enqueues with correct level", () => {
		const ring = new TelemetryRing();
		const logger = makeLogger(ring, "inst-2");
		logger.warn("something wrong");
		expect(firstLog(ring).level).toBe(LogLevel.LOG_LEVEL_WARN);
	});

	test("error enqueues with error level", () => {
		const ring = new TelemetryRing();
		const logger = makeLogger(ring, "inst-3");
		logger.error("boom");
		expect(firstLog(ring).level).toBe(LogLevel.LOG_LEVEL_ERROR);
	});
});
