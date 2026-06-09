/**
 * showcase-workflow.test.ts — automated DB-level smoke test for the showcase
 * fixture against a live runtime + PG18.
 *
 * What it asserts (acceptance gate for the showcase script's SDK emission
 * contract — full trace stitching is verified by the visual checklist in
 * README.md):
 *   - The workflow run reaches a terminal state (any of completed /
 *     failed / failed_compensated / cancelled).
 *   - WORKFLOW.RUN op for the run lands in `operations`.
 *   - ≥ 1 RPC.CALL op lands during the run (one row per logical call, owned by
 *     the caller SDK; no FORWARD/HANDLE rows — ADR-0001).
 *   - ≥ 1 USER.SUBOP with `meta.is_compensation = true` lands — this is the
 *     Phase 5 compensation-marker wire under test.
 *   - The is_compensation meta carries the forward step id under
 *     `compensates_for_step_id` so the UI can draw CompensationArrow.
 *   - Workflow trace nesting: tree depth ≥ 4, forward rpc/event ops nest under
 *     their USER.SUBOP step span, sub-workflow shipping-flow nests under the
 *     `ship` step span in the SAME trace with rpc + event ops inside it, the
 *     compensation op keys to the forward step's step_id, and the recursive
 *     dangling-children close holds the child ≤ parent invariant (no sync op
 *     outlives its parent, nothing stays open after the run terminates).
 *
 * Items the visual checklist covers but this test deliberately does NOT assert:
 *   - JOB.EXEC chains — cron firing is timing-dependent and verified visually.
 */

import { afterAll, describe, expect, test } from "bun:test";
import { runShowcase } from "./showcase-workflow";

const dbUrl =
	process.env.TEST_DATABASE_URL ??
	"postgresql://servicebridge:servicebridge@localhost:5433/service-bridge";
const RUNTIME_URL = process.env.SERVICEBRIDGE_URL ?? "localhost:14445";

async function withDb<T>(fn: (sql: typeof import("bun").sql) => Promise<T>) {
	const prev = process.env.DATABASE_URL;
	process.env.DATABASE_URL = dbUrl;
	try {
		const { sql } = await import("bun");
		return await fn(sql);
	} finally {
		if (prev === undefined) delete process.env.DATABASE_URL;
		else process.env.DATABASE_URL = prev;
	}
}

describe.skipIf(!process.env.SERVICEBRIDGE_URL && !process.env.RUN_SHOWCASE)(
	"showcase fixture — DB-level assertions",
	() => {
		let traceId = "";
		let runId = "";

		test("showcase fixture: workflow terminates + WORKFLOW.RUN + RPC.CALL + compensation USER.SUBOP marker land in operations", async () => {
			const result = await runShowcase({
				skipHttp: true,
				runtimeUrl: RUNTIME_URL,
			});
			expect(result.wfRunId).toMatch(/^[0-9a-f-]{36}$/);
			traceId = result.wfTraceId;
			runId = result.wfRunId;

			// Workflow run reached a terminal state.
			expect([
				"completed",
				"failed",
				"failed_compensated",
				"cancelled",
			]).toContain(result.finalStatus);

			// WORKFLOW.RUN root op materialized for this run id.
			const wfRows = (await withDb(
				(sql) => sql`
						SELECT channel, kind FROM operations
						WHERE channel = 4 AND kind = 1 AND business_key = ${runId}
					`,
			)) as Array<{ channel: number; kind: number }>;
			expect(wfRows.length).toBeGreaterThanOrEqual(1);

			// RPC.CALL for billing/Charge — one row per logical call, owned by the
			// caller SDK (ADR-0001; no FORWARD/HANDLE rows). We check the recent
			// window because trace stitching is covered by the visual checklist,
			// not this DB-level smoke test.
			const rpcCalls = (await withDb(
				(sql) => sql`
						SELECT count(*)::int AS n FROM operations
						WHERE channel = 2 AND kind = 1
						  AND started_at > now() - interval '5 minutes'
					`,
			)) as Array<{ n: number }>;
			expect(rpcCalls[0]?.n ?? 0).toBeGreaterThanOrEqual(1);

			// Compensation marker — USER.SUBOP carrying the meta fields the UI
			// reads to draw the CompensationArrow.
			const compRows = (await withDb(
				(sql) => sql`
						SELECT subject, meta::text AS meta_json
						FROM operations
						WHERE channel = 6 AND kind = 1
						  AND meta::text LIKE '%"is_compensation": true%'
						  AND (meta->>'workflow_run_id') = ${runId}
					`,
			)) as Array<{ subject: string; meta_json: string }>;
			expect(compRows.length).toBeGreaterThanOrEqual(1);
			for (const row of compRows) {
				expect(row.subject).toMatch(/^compensate:/);
				expect(row.meta_json).toContain('"compensates_for_step_id"');
			}

			// --- Workflow trace nesting (fix/workflow-trace-nesting) ---

			// Tree depth ≥ 4 from the run root via parent_op_id within the trace.
			const depthRows = (await withDb(
				(sql) => sql`
						WITH RECURSIVE t AS (
							SELECT op_id, 0 AS depth FROM operations
							WHERE trace_id = ${traceId}::uuid AND parent_op_id IS NULL
							UNION ALL
							SELECT o.op_id, t.depth + 1 FROM operations o
							JOIN t ON o.parent_op_id = t.op_id
							WHERE o.trace_id = ${traceId}::uuid
						)
						SELECT max(depth)::int AS max_depth FROM t
					`,
			)) as Array<{ max_depth: number }>;
			expect(depthRows[0]?.max_depth ?? 0).toBeGreaterThanOrEqual(4);

			// A forward call op nests under its USER.SUBOP step span, not directly
			// under the run root: the rpc.call:.../Reserve op's parent is the
			// step:reserve span (channel=6).
			const reserveParent = (await withDb(
				(sql) => sql`
						SELECT p.channel AS parent_channel,
						       (p.meta->>'step_id') AS parent_step_id
						FROM operations c
						JOIN operations p ON p.op_id = c.parent_op_id
						WHERE c.trace_id = ${traceId}::uuid
						  AND c.channel = 2 AND c.subject LIKE 'rpc.call:%/Reserve'
						LIMIT 1
					`,
			)) as Array<{ parent_channel: number; parent_step_id: string | null }>;
			expect(reserveParent[0]?.parent_channel).toBe(6);
			expect(reserveParent[0]?.parent_step_id).toBe("reserve");

			// Sub-workflow shipping-flow runs in the SAME trace, nested under the
			// `ship` step span, with an rpc + an event op inside it.
			const subWf = (await withDb(
				(sql) => sql`
						SELECT (p.meta->>'step_id') AS parent_step_id
						FROM operations c
						JOIN operations p ON p.op_id = c.parent_op_id
						WHERE c.trace_id = ${traceId}::uuid
						  AND c.channel = 4 AND c.kind = 1
						  AND c.subject = 'workflow.run:shipping-flow'
						LIMIT 1
					`,
			)) as Array<{ parent_step_id: string | null }>;
			expect(subWf[0]?.parent_step_id).toBe("ship");

			// shipping-flow's internal rpc + event ops are present in this trace.
			const subWfOps = (await withDb(
				(sql) => sql`
						SELECT count(*)::int AS n FROM operations
						WHERE trace_id = ${traceId}::uuid
						  AND ((channel = 2 AND subject LIKE 'rpc.call:%/Release')
						       OR subject = 'event.publish:audit_log')
					`,
			)) as Array<{ n: number }>;
			expect(subWfOps[0]?.n ?? 0).toBeGreaterThanOrEqual(2);

			// Recursive close invariant: no synchronous (non event.deliver) child
			// outlives its parent at any depth, and nothing stays open after the
			// run terminates.
			const violations = (await withDb(
				(sql) => sql`
						SELECT count(*)::int AS n
						FROM operations c
						JOIN operations p ON p.op_id = c.parent_op_id
						WHERE c.trace_id = ${traceId}::uuid AND c.channel <> 3
						  AND c.finished_at IS NOT NULL AND p.finished_at IS NOT NULL
						  AND c.finished_at > p.finished_at + interval '5 milliseconds'
					`,
			)) as Array<{ n: number }>;
			expect(violations[0]?.n ?? -1).toBe(0);

			const stillOpen = (await withDb(
				(sql) => sql`
						SELECT count(*)::int AS n FROM operations
						WHERE trace_id = ${traceId}::uuid AND finished_at IS NULL
					`,
			)) as Array<{ n: number }>;
			expect(stillOpen[0]?.n ?? -1).toBe(0);
		}, 180_000);

		afterAll(() => {
			if (traceId) {
				console.log(
					`\nshowcase trace: http://localhost:14444/traces/${traceId}`,
				);
			}
		});
	},
);
