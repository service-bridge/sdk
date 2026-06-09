// payload-capture.ts — SDK-side payload capture engine.
//
// Three modes applied per-op: "all" emits captured payloads immediately,
// "errors" buffers them and emits only when the op fails, "none" captures
// nothing. The mode is NOT an SDK setting — the runtime is the single authority
// and pushes a per-channel mode set over the registry stream. An op resolves the
// pushed mode for its own channel; a per-handler override may only NARROW it
// (privacy ordering NONE < ERRORS < ALL), never widen it. Payloads larger than
// the configured cap are truncated while the original byte length is preserved
// for UI badges.
// @public — см. ./README.md

export type CaptureMode = "all" | "errors" | "none";

// Default per-direction payload byte cap, used when the caller passes no
// explicit cap. Overridable via ServiceBridgeOptions.payloadMaxBytes.
// @public — см. ./README.md
export const DEFAULT_PAYLOAD_MAX_BYTES = 65536;

const VALID_MODES: ReadonlySet<string> = new Set(["all", "errors", "none"]);

// Privacy ordering: a higher rank captures strictly more. Narrowing picks the
// lower rank (more private). NONE < ERRORS < ALL.
const PRIVACY_RANK: Record<CaptureMode, number> = {
	none: 0,
	errors: 1,
	all: 2,
};

// resolveCaptureMode computes the effective mode for an op from the
// runtime-pushed mode and an optional per-handler override. The override can
// only narrow (lower the privacy rank), never widen beyond what the runtime
// permits. Invalid pushed modes fail safe to "none"; an invalid override is
// ignored (the pushed mode stands).
// @public — см. ./README.md
export function resolveCaptureMode(
	pushed: CaptureMode,
	perHandler?: CaptureMode,
): CaptureMode {
	const effective: CaptureMode = VALID_MODES.has(pushed) ? pushed : "none";
	if (!perHandler || !VALID_MODES.has(perHandler)) return effective;
	// Narrow only: keep whichever captures less.
	return PRIVACY_RANK[perHandler] < PRIVACY_RANK[effective]
		? perHandler
		: effective;
}

// capPayload truncates bytes to maxBytes and reports the original length so the
// UI can badge "truncated (original N)". maxBytes defaults to
// DEFAULT_PAYLOAD_MAX_BYTES; a non-finite or non-positive value falls back to it.
// @public — см. ./README.md
export function capPayload(
	bytes: Uint8Array,
	maxBytes: number = DEFAULT_PAYLOAD_MAX_BYTES,
): {
	bytes: Uint8Array;
	originalSize: number;
} {
	const cap =
		Number.isFinite(maxBytes) && maxBytes > 0
			? maxBytes
			: DEFAULT_PAYLOAD_MAX_BYTES;
	const originalSize = bytes.byteLength;
	if (originalSize <= cap) return { bytes, originalSize };
	return { bytes: bytes.subarray(0, cap), originalSize };
}

// CapturedAttachment is one direction's captured (already capped) payload.
// @public — см. ./README.md
export interface CapturedAttachment {
	direction: number; // 1=IN, 2=OUT
	bytes: Uint8Array;
	originalSize: number;
	contractHash: string;
}
