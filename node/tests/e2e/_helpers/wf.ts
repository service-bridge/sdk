// wf.ts — workflow-domain e2e helpers.
//
// Complements the generic fixtures.ts helpers with the run-lifecycle and
// policy-seeding utilities the workflow e2e files share.

import type { ServiceBridge } from "../../../src/connection/service-bridge";
import type { WorkflowsClient } from "../../../src/pb/servicebridge/v1/workflows";
import type { RunSnapshot } from "../../../src/workflow/types";
import { sleep } from "./fixtures";
import { addRule, withDb } from "./policy-db";

// FAST_WF_OPTS — tight reconnect for fast test failures.
export const FAST_WF_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60_000,
	certRefreshJitterMs: 0,
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
	owner: ServiceBridge | string,
	wfName: string,
	input: unknown,
	timeoutMs = 15_000,
): Promise<{ runId: string }> {
	const deadline = Date.now() + timeoutMs;
	const service = typeof owner === "string" ? owner : svcName(owner);
	let lastErr: unknown;
	for (;;) {
		try {
			return await caller.workflow.start(service, wfName, input);
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

// svcName is the registered service name of a connected client.
export function svcName(sb: ServiceBridge): string {
	const id = sb.identity();
	if (!id) throw new Error("svcName: client not connected");
	return id.serviceName;
}

// stateOf rebuilds the step-output map of a run snapshot: the run output on
// success, every step's output otherwise.
export function stateOf(q: RunSnapshot): Record<string, unknown> {
	if (q.output) return q.output;
	return Object.fromEntries(q.steps.map((s) => [s.stepId, s.output]));
}

// isParked reports a run that is waiting on the runtime (sleep, signal,
// event, child) rather than executing.
export function isParked(q: RunSnapshot): boolean {
	return (
		q.status === "active" &&
		["sleep", "signal", "event", "child"].includes(q.waitingReason)
	);
}

// awaitParked waits until the run is parked on the runtime.
export async function awaitParked(
	sb: ServiceBridge,
	runId: string,
	timeoutMs = 10_000,
): Promise<RunSnapshot> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const q = await sb.workflow.query(runId);
		if (isParked(q)) return q;
		if (Date.now() >= deadline)
			throw new Error(
				`awaitParked(${runId}): status=${q.status} waiting=${q.waitingReason}`,
			);
		await sleep(50);
	}
}

// stepStatus returns a workflow_steps row status, or null when the step row
// does not exist yet. Steps the runner never reached have no row.
export async function stepStatus(
	runId: string,
	stepId: string,
): Promise<string | null> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT status FROM workflow_steps
			 WHERE run_id = ${runId} AND step_id = ${stepId}
		`) as Array<{ status: string }>;
		return rows[0]?.status ?? null;
	});
}

// expireTaskLeases expires every leased step of the run in place — what a dead
// holder looks like to the runtime; the dispatcher re-leases the steps.
export async function expireTaskLeases(runId: string): Promise<void> {
	await withDb(async (sql) => {
		await sql`
			UPDATE workflow_steps
			   SET lease_expires_at = now() - interval '1 minute'
			 WHERE run_id = ${runId} AND status = 'leased'
		`;
	});
}

// workflowsWire exposes the WorkflowsClient a connected ServiceBridge already
// owns, for tests that issue task RPCs (CompleteTask/Heartbeat) themselves.
export function workflowsWire(sb: ServiceBridge): WorkflowsClient {
	const client = (sb as unknown as { _workflowsClient: WorkflowsClient | null })
		._workflowsClient;
	if (!client) {
		throw new Error("workflowsWire: client absent — connect the SDK first");
	}
	return client;
}

// GRPC_ABORTED — the code the runtime answers a lost task lease with.
export const GRPC_ABORTED = 10;

// grpcCodeOf runs `call` and returns the gRPC status code it failed with, or
// null when it succeeded.
export async function grpcCodeOf(
	call: () => Promise<unknown>,
): Promise<number | null> {
	try {
		await call();
		return null;
	} catch (err) {
		const code = (err as { code?: unknown }).code;
		if (typeof code !== "number") {
			throw new Error(
				`grpcCodeOf: expected a gRPC ServiceError, got ${String(err)}`,
			);
		}
		return code;
	}
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

// awaitPolicyLive waits until the runtime's own view of a client's rules carries
// the one just written. A rule reaches Postgres synchronously and the snapshot
// asynchronously, so a test that starts work right after addRule races the
// propagation and its steps get denied — a failure that reads like a policy bug
// and is really a missing wait.
export async function awaitPolicyLive(
	sb: ServiceBridge,
	side: "egress" | "acceptance",
	action: string,
	targetName: string,
	timeoutMs = 10_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const rules = sb.policyEvaluation()?.[side] ?? [];
		if (
			rules.some(
				(r) =>
					r.action === action &&
					(r.targetName === targetName || r.targetName === "*"),
			)
		) {
			return;
		}
		if (Date.now() >= deadline) {
			throw new Error(
				`awaitPolicyLive: ${side} ${action} ${targetName} not in the snapshot after ${timeoutMs}ms`,
			);
		}
		await new Promise((r) => setTimeout(r, 100));
	}
}
