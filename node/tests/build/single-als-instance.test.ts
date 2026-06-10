// Регрессия на split-trace: HTTP-плагин и ядро ОБЯЗАНЫ делить один инстанс
// AsyncLocalStorage. tsup собирает 4 entry (`.`, `./express`, `./fastify`,
// `./hono`); при splitting:false каждый бандл инлайнит свою копию
// telemetry/context → свой `new AsyncLocalStorage()`. Тогда плагин,
// импортированный как `service-bridge/fastify`, ставит trace-контекст на одном
// als, а `rpc.call` из ядра (`service-bridge`) читает другой → контекст не
// виден → trace расщепляется на два traceId. splitting:true сводит общий код в
// один чанк = один `als`. Тест собирает пакет (без dts, в свой outDir, чтобы не
// трогать боевой dist) и проверяет инвариант на артефактах.

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";

const SDK_ROOT = join(import.meta.dir, "..", "..");
const OUT = join(SDK_ROOT, ".tmp-als-build");
const ENTRY_BUNDLES = [
	"index.js",
	join("http", "express", "index.js"),
	join("http", "fastify", "index.js"),
	join("http", "hono", "index.js"),
];

function allJsFiles(dir: string): string[] {
	const out: string[] = [];
	for (const name of readdirSync(dir)) {
		const full = join(dir, name);
		if (statSync(full).isDirectory()) out.push(...allJsFiles(full));
		else if (name.endsWith(".js")) out.push(full);
	}
	return out;
}

function countOccurrences(text: string, needle: string): number {
	let n = 0;
	let i = text.indexOf(needle);
	while (i !== -1) {
		n++;
		i = text.indexOf(needle, i + needle.length);
	}
	return n;
}

describe("build: single AsyncLocalStorage instance across all entries", () => {
	beforeAll(() => {
		const proc = Bun.spawnSync(
			["./node_modules/.bin/tsup", "--no-dts", "--out-dir", OUT],
			{ cwd: SDK_ROOT, stdout: "pipe", stderr: "pipe" },
		);
		if (proc.exitCode !== 0) {
			throw new Error(
				`tsup build failed: ${proc.stderr.toString() || proc.stdout.toString()}`,
			);
		}
	});

	afterAll(() => {
		rmSync(OUT, { recursive: true, force: true });
	});

	it("emits exactly one `new AsyncLocalStorage` in the whole build output", () => {
		const total = allJsFiles(OUT).reduce(
			(sum, f) =>
				sum +
				countOccurrences(readFileSync(f, "utf8"), "new AsyncLocalStorage"),
			0,
		);
		expect(total).toBe(1);
	});

	it("every entry bundle imports the shared chunk that defines `als`", () => {
		const alsChunk = allJsFiles(OUT).find((f) =>
			readFileSync(f, "utf8").includes("new AsyncLocalStorage"),
		);
		expect(alsChunk).toBeDefined();
		const chunkName = alsChunk?.split("/").pop() ?? "";
		expect(chunkName.startsWith("chunk-")).toBe(true);

		for (const entry of ENTRY_BUNDLES) {
			const src = readFileSync(join(OUT, entry), "utf8");
			expect(src.includes(chunkName)).toBe(true);
		}
	});
});
