import { defineConfig } from "tsup";

export default defineConfig({
	entry: {
		index: "index.ts",
		"http/express/index": "src/http/express/index.ts",
		"http/fastify/index": "src/http/fastify/index.ts",
		"http/hono/index": "src/http/hono/index.ts",
	},
	format: ["esm"],
	target: "node18",
	outDir: "dist",
	dts: true,
	clean: true,
	sourcemap: true,
	// Shared internal modules (telemetry/context → the `als` AsyncLocalStorage
	// singleton) MUST resolve to one instance across every entry. With splitting
	// off, each entry inlines its own copy of `als`, so a plugin imported via
	// `service-bridge/fastify` enters trace context on a different ALS than the
	// core read by `service-bridge` → trace context is invisible to rpc.call and
	// the trace splits. Splitting hoists shared code into one chunk, one `als`.
	splitting: true,
	external: ["better-sqlite3", "bun:sqlite"],
});
