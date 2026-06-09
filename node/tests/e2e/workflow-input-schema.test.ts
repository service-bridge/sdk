// workflow-input-schema — verifies that a workflow's declared INPUT SCHEMA
// (the data contract, distinct from the step pipeline / DAG) is registered
// through the real SDK→runtime path and persisted to the exact column the UI
// Schema Viewer reads: workflow_definitions.input_schema.
//
// This is deliberately separate from the pipeline showcase: the pipeline lives
// in workflow_definitions.graph, the input schema in workflow_definitions.input_schema.
// The test asserts the schema lands in the schema column and NOT the graph.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { sleep, waitFor } from "./_helpers/events";
import { clearRules, withDb } from "./_helpers/policy-db";
import { cleanupWorkflowState, FAST_WF_OPTS, wfEnv } from "./_helpers/wf.ts";

describe("workflow-input-schema", () => {
	const env = wfEnv();
	let owner: ServiceBridge | undefined;
	let ownerID: string | undefined;

	beforeEach(async () => {
		await cleanupWorkflowState();
		owner = undefined;
	});

	afterEach(async () => {
		await owner?.stop();
		if (ownerID) await clearRules(ownerID);
		await sleep(200);
	});

	test("declared input schema persists to workflow_definitions.input_schema, not graph", async () => {
		const wfName = `schema-demo-${Date.now()}`;
		const inputSchema = {
			type: "object",
			required: ["orderId", "amount"],
			properties: {
				orderId: { type: "string" },
				amount: { type: "number", minimum: 0 },
				quantity: { type: "integer", minimum: 1 },
				express: { type: "boolean" },
				address: {
					type: "object",
					required: ["city"],
					properties: {
						city: { type: "string" },
						zip: { type: "string" },
					},
				},
			},
		};

		owner = new ServiceBridge(env.url, env.ownerKey, FAST_WF_OPTS);
		owner.workflow.handle(wfName, {
			input: inputSchema,
			steps: [{ type: "local", id: "noop", fn: async () => ({ ok: true }) }],
		});
		await owner.start();
		await waitFor(() => owner!.identity() !== null, 5_000, "owner connected");
		ownerID = owner.identity()!.serviceId;

		// Registration is async after connect; wait for the definition row.
		await waitFor(
			async () =>
				(
					await withDb(
						(sql) =>
							sql`SELECT 1 FROM workflow_definitions WHERE service_id = ${ownerID} AND name = ${wfName}` as Promise<
								unknown[]
							>,
					)
				).length > 0,
			5_000,
			"workflow definition registered",
		);

		const rows = (await withDb(
			(sql) =>
				sql`
					SELECT input_schema, graph
					FROM workflow_definitions
					WHERE service_id = ${ownerID} AND name = ${wfName}
					ORDER BY registered_at DESC
					LIMIT 1
				` as Promise<Array<{ input_schema: unknown; graph: unknown }>>,
		)) as Array<{ input_schema: Record<string, unknown>; graph: unknown }>;

		expect(rows.length).toBe(1);
		const row = rows[0]!;
		const stored = row.input_schema;

		// The input schema column holds the declared JSON Schema verbatim — this
		// is exactly what GetWorkflowDefDetail.schema_input surfaces to the viewer.
		expect(stored).toEqual(inputSchema);

		// And it must be the schema, not the pipeline: no step/graph keys leaked in.
		expect(stored).not.toHaveProperty("graph");
		expect(stored).not.toHaveProperty("steps");
		expect(stored.type).toBe("object");

		// The pipeline lives in its own column and is a different shape entirely.
		expect(row.graph).not.toBeNull();
	}, 30_000);
});
