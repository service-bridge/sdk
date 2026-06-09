import { describe, expect, test } from "bun:test";
import { capPayload, resolveCaptureMode } from "./payload-capture";

describe("resolveCaptureMode (pushed mode + narrowing override)", () => {
	test("uses the pushed mode when no per-handler override", () => {
		expect(resolveCaptureMode("errors")).toBe("errors");
		expect(resolveCaptureMode("all")).toBe("all");
		expect(resolveCaptureMode("none")).toBe("none");
	});

	test("per-handler override only narrows (never widens) the pushed mode", () => {
		// pushed all + handler errors → errors (narrowed)
		expect(resolveCaptureMode("all", "errors")).toBe("errors");
		// pushed all + handler none → none (narrowed)
		expect(resolveCaptureMode("all", "none")).toBe("none");
		// pushed errors + handler all → errors (handler cannot widen)
		expect(resolveCaptureMode("errors", "all")).toBe("errors");
		// pushed none + handler all → none (handler cannot widen)
		expect(resolveCaptureMode("none", "all")).toBe("none");
		// pushed errors + handler none → none (narrowed)
		expect(resolveCaptureMode("errors", "none")).toBe("none");
	});

	test("invalid per-handler is ignored (pushed mode stands)", () => {
		// @ts-expect-error testing invalid input at runtime
		expect(resolveCaptureMode("all", "bogus")).toBe("all");
	});

	test("invalid pushed mode is treated as none (fail-safe)", () => {
		// @ts-expect-error testing invalid input at runtime
		expect(resolveCaptureMode("bogus")).toBe("none");
	});
});

describe("capPayload", () => {
	test("passes through bytes under the cap, originalSize = length", () => {
		const bytes = new Uint8Array([1, 2, 3]);
		const { bytes: out, originalSize } = capPayload(bytes);
		expect(out).toEqual(bytes);
		expect(originalSize).toBe(3);
	});

	test("truncates over the cap, originalSize is the full length", () => {
		const bytes = new Uint8Array([1, 2, 3, 4, 5, 6]);
		const { bytes: out, originalSize } = capPayload(bytes, 4);
		expect(out.byteLength).toBe(4);
		expect(originalSize).toBe(6);
		expect(Array.from(out)).toEqual([1, 2, 3, 4]);
	});

	test("default cap is 65536", () => {
		const bytes = new Uint8Array(70000);
		const { bytes: out, originalSize } = capPayload(bytes);
		expect(out.byteLength).toBe(65536);
		expect(originalSize).toBe(70000);
	});
});
