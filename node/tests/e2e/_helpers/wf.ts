// wf.ts — workflow-domain e2e helpers.
//
// Complements the generic fixtures.ts helpers with the run-lifecycle and
// policy-seeding utilities the workflow e2e files share.

import type { ServiceBridge } from "../../../src/connection/service-bridge";
import { sleep } from "./fixtures";
import { addRule } from "./policy-db";

// FAST_WF_OPTS — tight reconnect for fast test failures.
export const FAST_WF_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

// startWorkflowWhenAllowed — starts a run, retrying while two registrations are
// still propagating to the runtime: the owner's workflow definition
// (WorkflowNotFoundError) and the bilateral policy rule
// (WorkflowAccessDeniedError, gate #5). The runtime rejects on both BEFORE
// creating a run, so retrying never creates duplicate runs — the first
// non-rejected call creates exactly one. This replaces a fixed `sleep(800)`
// after registering the handler + seeding policy: it returns as soon as both
// are live (usually well under the old fixed wait).
const RETRYABLE_START_ERRORS = new Set([
	"WorkflowAccessDeniedError",
	"WorkflowNotFoundError",
]);

export async function startWorkflowWhenAllowed(
	caller: ServiceBridge,
	wfName: string,
	input: unknown,
	timeoutMs = 15_000,
): Promise<{ runId: string }> {
	const deadline = Date.now() + timeoutMs;
	let lastErr: unknown;
	for (;;) {
		try {
			return await caller.workflow.start(wfName, input);
		} catch (err) {
			if (!RETRYABLE_START_ERRORS.has((err as Error)?.name)) throw err;
			lastErr = err;
			if (Date.now() >= deadline) {
				throw new Error(
					`startWorkflowWhenAllowed(${wfName}): not startable after ${timeoutMs}ms: ${(lastErr as Error).message}`,
				);
			}
			await sleep(50);
		}
	}
}

// awaitRunStatus — polls sb.workflow.query(runId) until predicate is true
// or timeoutMs elapses. Throws on timeout.
export async function awaitRunStatus(
	sb: ServiceBridge,
	runId: string,
	predicate: (status: string) => boolean,
	timeoutMs = 15_000,
): Promise<string> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const q = await sb.workflow.query(runId);
		if (predicate(q.status)) return q.status;
		await sleep(200);
	}
	const last = await sb.workflow.query(runId);
	throw new Error(
		`awaitRunStatus(${runId}): timed out after ${timeoutMs}ms, last status="${last.status}"`,
	);
}

// addWorkflowRule seeds the minimal policy rules needed for a workflow run:
//   - caller: egress workflow.run → (ownerID, wfName)
//   - owner:  acceptance workflow.handle ← (callerID, wfName)
//
// Always uses runtime-bound service IDs (from sb.identity()), not DB name
// lookups — a name lookup can resolve to a revoked row and make the seeded
// rule apply to an identity nobody is connected as.
export async function addWorkflowRule(
	callerID: string,
	ownerID: string,
	wfName: string,
): Promise<void> {
	await addRule(callerID, "E", "workflow.run", ownerID, wfName);
	await addRule(ownerID, "A", "workflow.handle", callerID, wfName);
}
