// wf.ts — workflow-specific e2e helpers.
//
// Complements the generic events.ts / harness.ts helpers with
// workflow-domain utilities used across the workflow e2e suite.

import type { ServiceBridge } from "../../../src/connection/service-bridge";
import type { RpcHandlerOpts } from "../../../src/registry/registry";
import { sleep } from "./events";
import { addRule, withDb } from "./policy-db";

// WfEnv mirrors eventsEnv() for workflow tests that need two service keys.
export interface WfEnv {
	url: string;
	ownerKey: string;
	callerKey: string;
}

export function wfEnv(): WfEnv {
	const url = process.env.SERVICEBRIDGE_URL;
	const ownerKey = process.env.SERVICEBRIDGE_SERVICE_KEY;
	const callerKey = process.env.SERVICEBRIDGE_SERVICE2_KEY;

	const missing: string[] = [];
	if (!url) missing.push("SERVICEBRIDGE_URL");
	if (!ownerKey) missing.push("SERVICEBRIDGE_SERVICE_KEY");
	if (!callerKey) missing.push("SERVICEBRIDGE_SERVICE2_KEY");

	if (missing.length) {
		throw new Error(
			[
				`workflow e2e: missing env: ${missing.join(", ")}.`,
				``,
				`Run once: bash scripts/bootstrap-e2e-keys.sh`,
				`Then start runtime: go run -C runtime ./cmd/runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/servicebridge?sslmode=disable`,
			].join("\n"),
		);
	}

	return { url: url!, ownerKey: ownerKey!, callerKey: callerKey! };
}

// FAST_WF_OPTS — tight reconnect for fast test failures.
export const FAST_WF_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

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

// forceLeaseReclaim — sets lease_expires_at to the past so the next dispatcher
// tick will reclaim the run and allow a second SDK instance to pick it up.
// Used exclusively for lease-reclaim and lease-fencing tests.
export async function forceLeaseReclaim(runId: string): Promise<void> {
	await withDb(async (sql) => {
		await sql`
			UPDATE workflow_runs
			SET lease_expires_at = now() - interval '1 minute'
			WHERE id = ${runId}
		`;
	});
}

// cleanupWorkflowState — TRUNCATE workflow run-tracking tables to remove
// orphan rows from prior tests in the same suite invocation. Without this,
// orphan workflow_timers / workflow_signals or runs with stale
// lease_holder_instance_id from torn-down SDK instances can wake the
// dispatcher into wrong instances and cause non-deterministic interference
// when many workflow-* tests run in one `bun test` invocation.
//
// workflow_definitions is intentionally preserved: services re-register
// definitions on connect, and TRUNCATE'ing them on every test would slow the
// suite without changing behavior (definitions are looked up by name+fingerprint).
export async function cleanupWorkflowState(): Promise<void> {
	await withDb(async (sql) => {
		await sql`TRUNCATE workflow_timers, workflow_signals, workflow_steps, workflow_runs CASCADE`;
		// Retire stale service_instances rows left by prior tests. Without this,
		// the registry can resolve a direct-transport target to an offline
		// instance UUID whose cert SAN no longer matches the live cert, surfacing
		// as a SPIFFE mismatch in workflow-compensation-chain (QA finding #2).
		// Heartbeat tracking lives on service_methods.last_seen_at — an instance
		// is stale if all its method rows haven't been refreshed in 5 s (live
		// SDKs refresh on every register/heartbeat tick, well under 10 s).
		await sql`
			UPDATE service_instances si
			   SET status = 'disconnected',
			       call_endpoint = '',
			       http_endpoint = ''
			 WHERE si.status = 'connected'
			   AND EXISTS (
			     SELECT 1 FROM service_methods sm WHERE sm.instance_id = si.id
			   )
			   AND NOT EXISTS (
			     SELECT 1 FROM service_methods sm
			      WHERE sm.instance_id = si.id
			        AND sm.last_seen_at >= now() - interval '5 seconds'
			   )
		`;
	});
}

// reclaimLeaseNow — immediately simulates a full LeaseManager reclaim sweep
// for a single run: increments lease_epoch, clears the holder, resets status
// to 'pending'. The next Dispatcher tick then picks it up for a new holder.
// Used exclusively in lease-fencing tests where we need A's CompleteStep to
// be rejected before the 5 s reclaim sweep fires.
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

// seedFailingCallee — registers an rpc.handle on `sb` for `method` that throws
// `failCount` times then succeeds. The caller supplies `handlerOpts` with the
// schema (required by RpcDomain.handle). Returns a reset function (sets attempt
// counter back to 0 — useful in afterEach for repeated test runs).
export function seedFailingCallee(
	sb: ServiceBridge,
	method: string,
	failCount: number,
	handlerOpts: RpcHandlerOpts,
): () => void {
	let attempts = 0;
	sb.rpc.handle(
		method,
		async () => {
			attempts++;
			if (attempts <= failCount) {
				throw new Error(`simulated failure #${attempts}`);
			}
			return { ok: true, attempt: attempts };
		},
		handlerOpts,
	);
	return () => {
		attempts = 0;
	};
}

// addWorkflowRule seeds the minimal policy rules needed for a workflow run:
//   - caller: egress workflow.run → (ownerID, wfName)
//   - owner:  acceptance workflow.handle ← (callerID, wfName)
//
// Always uses runtime-bound service IDs (from sb.identity()), not DB name
// lookups — prevents the non-determinism that session 7 surfaced.
export async function addWorkflowRule(
	callerID: string,
	ownerID: string,
	wfName: string,
): Promise<void> {
	await addRule(callerID, "E", "workflow.run", ownerID, wfName);
	await addRule(ownerID, "A", "workflow.handle", callerID, wfName);
}

// addRpcSelfRules — owner needs egress + acceptance for internal rpc.call steps
// that call back to itself (common in call-step workflows).
export async function addRpcSelfRules(
	ownerID: string,
	method: string,
): Promise<void> {
	await addRule(ownerID, "E", "rpc.call", ownerID, method);
	await addRule(ownerID, "A", "rpc.handle", ownerID, method);
}
