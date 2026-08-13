import { TestEventDomain } from "./event-harness";
import { TestRpcDomain } from "./rpc-harness";

// TestHarness bundles the two doubled domains a registered handler closes
// over — `rpc` and `event` — under the same `sb.rpc` / `sb.event` shape used
// in production. It never opens a network connection, never touches
// SQLite, and never requires a live runtime. See ./README.md for exactly
// what this harness reproduces and what it deliberately does not.
// @public — см. ./README.md
export interface TestHarness {
	rpc: TestRpcDomain;
	event: TestEventDomain;
	reset(): void;
}

// createTestHarness builds a fresh, isolated TestHarness. Call once per
// test (or reset() an existing one) so registrations and recorded calls
// don't leak across tests.
// @public — см. ./README.md
export function createTestHarness(): TestHarness {
	const rpc = new TestRpcDomain();
	const event = new TestEventDomain();
	return {
		rpc,
		event,
		reset(): void {
			rpc.reset();
			event.reset();
		},
	};
}
