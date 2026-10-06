import { afterEach, describe, expect, it } from "bun:test";
import { _resetHostWarn, resolveHttpAdvertiseHost } from "./endpoint";

let sink: string[] = [];
const log = { warn: (m: string) => sink.push(m) };

function captureWarnings(fn: () => void): string[] {
	sink = [];
	fn();
	return sink;
}

describe("resolveHttpAdvertiseHost", () => {
	afterEach(() => {
		// Флаг предупреждения — модульное состояние: без сброса результат зависел
		// бы от порядка тестовых файлов в одном процессе Bun.
		_resetHostWarn();
	});

	it("returns the explicit host as-is and never warns", () => {
		_resetHostWarn();
		const warnings = captureWarnings(() => {
			expect(resolveHttpAdvertiseHost("internal.example", log)).toBe(
				"internal.example",
			);
		});
		expect(warnings).toHaveLength(0);
	});

	it("treats an empty host as absent", () => {
		_resetHostWarn();
		const warnings = captureWarnings(() => {
			expect(resolveHttpAdvertiseHost("", log)).toBe("127.0.0.1");
		});
		expect(warnings).toHaveLength(1);
	});

	it("warns exactly once across repeated fallbacks", () => {
		_resetHostWarn();
		const warnings = captureWarnings(() => {
			resolveHttpAdvertiseHost(undefined, log);
			resolveHttpAdvertiseHost(undefined, log);
			resolveHttpAdvertiseHost(undefined, log);
		});
		expect(warnings).toHaveLength(1);
		expect(warnings[0]).toContain("http advertise host not configured");
	});

	it("warns again after _resetHostWarn", () => {
		_resetHostWarn();
		const first = captureWarnings(() => {
			resolveHttpAdvertiseHost(undefined, log);
		});
		const silent = captureWarnings(() => {
			resolveHttpAdvertiseHost(undefined, log);
		});
		_resetHostWarn();
		const second = captureWarnings(() => {
			resolveHttpAdvertiseHost(undefined, log);
		});
		expect(first).toHaveLength(1);
		expect(silent).toHaveLength(0);
		expect(second).toHaveLength(1);
	});
});
