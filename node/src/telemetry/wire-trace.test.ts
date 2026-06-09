// wire-trace.test.ts — tests for parseXSbTrace and formatXSbTrace.
// Wire shape "<traceId>-<parentOpId>" matches runtime header parser
// (telemetry.FormatHeader / ParseHeader).
import { describe, expect, it } from "bun:test";
import { formatXSbTrace, parseXSbTrace } from "./wire-trace";

const UUID_A = "01960000-0000-7000-8000-000000000001";
const UUID_B = "01960000-0000-7000-8000-000000000002";

describe("parseXSbTrace", () => {
	it("returns null for null", () => {
		expect(parseXSbTrace(null)).toBeNull();
	});

	it("returns null for undefined", () => {
		expect(parseXSbTrace(undefined)).toBeNull();
	});

	it("returns null for empty string", () => {
		expect(parseXSbTrace("")).toBeNull();
	});

	it("returns null for single uuid (length mismatch)", () => {
		expect(parseXSbTrace(UUID_A)).toBeNull();
	});

	it("returns null when traceId is malformed", () => {
		// Build a 73-char string with '-' at position 36 but invalid traceId chars.
		const garbledTrace = "zzz".padEnd(36, "z");
		expect(parseXSbTrace(`${garbledTrace}-${UUID_B}`)).toBeNull();
	});

	it("returns null when parentOpId is malformed", () => {
		const garbledOp = "zzz".padEnd(36, "z");
		expect(parseXSbTrace(`${UUID_A}-${garbledOp}`)).toBeNull();
	});

	it("returns null when separator is not at position 36", () => {
		// 73 chars total but '-' missing where required.
		const bad = `${UUID_A}X${UUID_B}`;
		expect(parseXSbTrace(bad)).toBeNull();
	});

	it("returns null for length other than 73", () => {
		expect(parseXSbTrace(`${UUID_A}-${UUID_B}x`)).toBeNull();
	});

	it("parses valid trace header", () => {
		const result = parseXSbTrace(`${UUID_A}-${UUID_B}`);
		expect(result).not.toBeNull();
		expect(result!.traceId).toBe(UUID_A);
		expect(result!.parentOpId).toBe(UUID_B);
	});

	it("accepts uppercase UUIDs", () => {
		const upper = UUID_A.toUpperCase();
		const result = parseXSbTrace(`${upper}-${UUID_B}`);
		expect(result).not.toBeNull();
	});

	it("accepts UUIDv4 format (not just v7)", () => {
		const v4 = "550e8400-e29b-41d4-a716-446655440000";
		const result = parseXSbTrace(`${v4}-${UUID_B}`);
		expect(result).not.toBeNull();
	});
});

describe("formatXSbTrace", () => {
	it("formats as traceId-parentOpId", () => {
		expect(formatXSbTrace(UUID_A, UUID_B)).toBe(`${UUID_A}-${UUID_B}`);
	});
});

describe("round-trip", () => {
	it("format → parse produces same values", () => {
		const formatted = formatXSbTrace(UUID_A, UUID_B);
		const parsed = parseXSbTrace(formatted);
		expect(parsed!.traceId).toBe(UUID_A);
		expect(parsed!.parentOpId).toBe(UUID_B);
	});
});
