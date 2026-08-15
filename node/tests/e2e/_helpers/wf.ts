// wf.ts — workflow-domain e2e helpers.
//
// Complements the generic fixtures.ts helpers with the run-lifecycle and
// policy-seeding utilities the workflow e2e files share.

import type { ServiceBridge } from "../../../src/connection/service-bridge";
import type { WorkflowsClient } from "../../../src/pb/servicebridge/v1/workflows";
import { sleep } from "./fixtures";
import { addRule, withDb } from "./policy-db";

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

// LeaseRow — the lease-bearing columns of a workflow_runs row. Lease state has
// no wire representation (Query returns status + steps only), so the lease
// tests read it straight from Postgres.
export interface LeaseRow {
	status: string;
	leaseEpoch: number;
	leaseHolderInstanceId: string | null;
	leaseExpiresAtMs: number | null;
}

export async function leaseOf(runId: string): Promise<LeaseRow> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT status,
			       lease_epoch,
			       lease_holder_instance_id,
			       lease_expires_at
			  FROM workflow_runs
			 WHERE id = ${runId}
		`) as Array<{
			status: string;
			lease_epoch: string | number;
			lease_holder_instance_id: string | null;
			lease_expires_at: Date | null;
		}>;
		const row = rows[0];
		if (!row) throw new Error(`leaseOf(${runId}): run not in DB`);
		return {
			status: row.status,
			leaseEpoch: Number(row.lease_epoch),
			leaseHolderInstanceId: row.lease_holder_instance_id,
			leaseExpiresAtMs: row.lease_expires_at
				? row.lease_expires_at.getTime()
				: null,
		};
	});
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

// forceLeaseReclaim expires the lease in place, leaving status and holder
// untouched. This is what a dead holder looks like to the runtime: the
// LeaseManager sweep (running/waiting) and the dispatcher's stale-compensating
// sweep both key off lease_expires_at <= now(), so this drives the real reclaim
// paths instead of simulating their outcome.
export async function forceLeaseReclaim(runId: string): Promise<void> {
	await withDb(async (sql) => {
		await sql`
			UPDATE workflow_runs
			   SET lease_expires_at = now() - interval '1 minute'
			 WHERE id = ${runId}
		`;
	});
}

// reclaimLeaseNow applies the LeaseManager's running-run reclaim outcome
// synchronously: bump lease_epoch (fencing the current holder), drop the holder
// and hand the run back to the dispatcher as 'pending'. Used where the test must
// observe the fenced holder BEFORE the reclaim sweep's own interval elapses.
export async function reclaimLeaseNow(runId: string): Promise<void> {
	await withDb(async (sql) => {
		await sql`
			UPDATE workflow_runs
			   SET lease_epoch              = lease_epoch + 1,
			       lease_holder_instance_id = NULL,
			       lease_expires_at         = NULL,
			       status                   = 'pending'
			 WHERE id = ${runId}
			   AND status IN ('running', 'waiting')
		`;
	});
}

// workflowsWire exposes the WorkflowsClient a connected ServiceBridge already
// owns. The checkpoint RPCs (BeginStep/CompleteStep/FailStep/Heartbeat) carry
// lease_epoch and are the surface fencing acts on, but the SDK only ever calls
// them from inside its runner — a fencing test has to issue them itself.
export function workflowsWire(sb: ServiceBridge): WorkflowsClient {
	const client = (sb as unknown as { _workflowsClient: WorkflowsClient | null })
		._workflowsClient;
	if (!client) {
		throw new Error("workflowsWire: client absent — connect the SDK first");
	}
	return client;
}

// GRPC_ABORTED — the code the runtime answers a fenced checkpoint with
// (ErrLeaseFenced → codes.Aborted).
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
