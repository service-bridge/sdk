// wire-trace.ts — X-SB-Trace header parse and format.
// Format: "<traceId>-<parentOpId>" where both are canonical RFC 9562 UUID
// strings. Matches runtime/internal/telemetry/header.go (ADR 0006 §3 / proto
// contract). UUID itself contains '-', so the separator is identified by
// position (36) rather than indexOf.
// @internal — см. ./README.md

// UUID pattern: 8-4-4-4-12 hex chars, case-insensitive.
const UUID_RE =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Canonical UUID string length: 36 chars. Header total: 36 + 1 + 36 = 73.
const UUID_LEN = 36;
const HEADER_LEN = UUID_LEN + 1 + UUID_LEN;

export interface ParsedXSbTrace {
	traceId: string;
	parentOpId: string;
}

/**
 * Parse the X-SB-Trace header value.
 * Returns null on any malformed input — caller must mint a new root context.
 */
export function parseXSbTrace(
	value: string | null | undefined,
): ParsedXSbTrace | null {
	if (!value) return null;
	if (value.length !== HEADER_LEN) return null;
	if (value[UUID_LEN] !== "-") return null;
	const traceId = value.slice(0, UUID_LEN);
	const parentOpId = value.slice(UUID_LEN + 1);
	if (!UUID_RE.test(traceId) || !UUID_RE.test(parentOpId)) return null;
	return { traceId, parentOpId };
}

/**
 * Format a trace context as the X-SB-Trace header value.
 */
export function formatXSbTrace(traceId: string, parentOpId: string): string {
	return `${traceId}-${parentOpId}`;
}
