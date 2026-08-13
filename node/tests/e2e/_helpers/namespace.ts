// namespace.ts — collision-free names for e2e tests.
//
// Each domain runs as its own process against its own service identities
// (e2e-<domain>-1/2/3), so names never collide across domains. Within a domain
// process, tests share warm pooled clients, so every method / topic / workflow
// name and every partition / business key a test uses must still be unique —
// that is what lets tests share a client without a per-test TRUNCATE.

let counter = 0;

function rand(): string {
	return Math.random().toString(36).slice(2, 8);
}

// uniqueName returns a dotted name valid for topics, rpc methods, and workflow
// names: matches the runtime's `[a-z0-9_-]+(\.[a-z0-9_-]+)*`. The prefix stays
// first so failure logs stay readable.
export function uniqueName(prefix: string): string {
	return `${prefix}.t${++counter}.${rand()}`;
}

// uniqueId returns a dash-joined identifier for partition keys, idempotency
// keys, and business keys where a dotted form is not required.
//
// Partition keys in particular MUST come from here, never from a literal: the
// runtime allows only one in_flight delivery per (consumer_service,
// partition_key), and the dispatcher's FIFO filter holds back newer events
// while an older one on the same key is still pending. A hardcoded key
// ("order-7") therefore inherits orphaned deliveries left by earlier failed
// runs and stalls the test that reuses it.
export function uniqueId(prefix: string): string {
	return `${prefix}-${++counter}-${rand()}`;
}
