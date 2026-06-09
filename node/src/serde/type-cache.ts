import type { SchemaPair, SchemaSpec } from "./serializer";
import { buildSchemaPair } from "./serializer";

// SchemaPairCache deduplicates SchemaPair builds by SchemaSpec identity.
// Loading a .proto file is expensive; both the call server (callee) and the
// call client (caller) reuse the same SchemaPair for every call on a method.
//
// @internal — см. ./README.md
export class SchemaPairCache {
	private inflight = new Map<string, Promise<SchemaPair>>();
	private cache = new Map<string, SchemaPair>();

	async get(spec: SchemaSpec): Promise<SchemaPair> {
		const key = specKey(spec);
		const cached = this.cache.get(key);
		if (cached) return cached;

		let pending = this.inflight.get(key);
		if (!pending) {
			pending = buildSchemaPair(spec).then((pair) => {
				this.cache.set(key, pair);
				this.inflight.delete(key);
				return pair;
			});
			this.inflight.set(key, pending);
		}
		return pending;
	}

	clear(): void {
		this.cache.clear();
		this.inflight.clear();
	}

	size(): number {
		return this.cache.size;
	}
}

function specKey(spec: SchemaSpec): string {
	if ("protoFile" in spec) {
		return `proto::${spec.protoFile}::${spec.input}::${spec.output}`;
	}
	return `json::${spec.schemaFile}`;
}
