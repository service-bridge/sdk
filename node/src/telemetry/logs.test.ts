import { describe, expect, test } from "bun:test";
import type { Log } from "../pb/servicebridge/v1/telemetry";
import { runWithTrace } from "./context";
import { LogLevel, makeLogger } from "./logs";
import { TelemetryRing } from "./ring";
import { mintRootContext, ZERO_OP_ID } from "./trace-context";

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

	test("carries traceId and the enclosing opId from the active trace", () => {
		const ring = new TelemetryRing();
		const logger = makeLogger(ring, "inst-4");
		const ctx = {
			traceId: "01900000-0000-7000-8000-0000000000aa",
			parentOpId: "01900000-0000-7000-8000-0000000000bb",
		};
		runWithTrace(ctx, () => logger.info("inside"));

		const entry = firstLog(ring);
		expect(entry.traceId).toBe(ctx.traceId);
		expect(entry.opId).toBe(ctx.parentOpId);
	});

	test("propagates the trace through await chains", async () => {
		const ring = new TelemetryRing();
		const logger = makeLogger(ring, "inst-5");
		const ctx = {
			traceId: "01900000-0000-7000-8000-0000000000cc",
			parentOpId: "01900000-0000-7000-8000-0000000000dd",
		};
		await runWithTrace(ctx, async () => {
			await Promise.resolve();
			logger.warn("deferred");
		});

		expect(firstLog(ring).traceId).toBe(ctx.traceId);
	});

	test("outside any trace both ids are empty", () => {
		const ring = new TelemetryRing();
		makeLogger(ring, "inst-6").info("orphan");

		const entry = firstLog(ring);
		expect(entry.traceId).toBe("");
		expect(entry.opId).toBe("");
	});

	test("a root context contributes traceId but no opId", () => {
		const ring = new TelemetryRing();
		const logger = makeLogger(ring, "inst-7");
		const ctx = mintRootContext();
		expect(ctx.parentOpId).toBe(ZERO_OP_ID);
		runWithTrace(ctx, () => logger.info("root"));

		const entry = firstLog(ring);
		expect(entry.traceId).toBe(ctx.traceId);
		// ZERO_OP_ID means "no enclosing op" — it must not reach the wire as an id.
		expect(entry.opId).toBe("");
	});
});
