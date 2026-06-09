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
	splitting: false,
	external: ["better-sqlite3", "bun:sqlite"],
});
