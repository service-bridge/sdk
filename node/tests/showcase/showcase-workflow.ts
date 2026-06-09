#!/usr/bin/env bun

/**
 * showcase-workflow.ts — comprehensive workflow + cron job fixture for visual
 * acceptance of the canonical tracing rebuild.
 *
 * Goal: a single command spins up enough services in one process to produce
 * traces that exercise every visual primitive in the UI waterfall (sequence,
 * parallel branches, RPC triples with retries + DLQ, EVENT fan-out, nested
 * workflow, SLEEP, WAIT_SIGNAL timeout, compensation marker, JOB.EXEC with
 * temporal gap). The owner walks the acceptance checklist in `README.md` after
 * the script prints its trace URLs.
 *
 * Architecture:
 *   - 8 ServiceBridge instances in one Node process, each with its own
 *     bootstrap key (provisioned once via showcase-keys.ts → .env.showcase).
 *   - One HTTP server (Express) on port 19999 exposed by showcase-service —
 *     POST /run-showcase starts the workflow run.
 *   - Policy rules (service_policy_rules) seeded once at startup for every
 *     edge the workflow traverses: workflow.run, workflow.handle, rpc.call,
 *     rpc.handle, event.publish, event.handle.
 *   - SIGTERM / SIGINT → graceful shutdown: stop every SDK; DB state is left
 *     intact so the owner can inspect the trace afterwards.
 *
 * Run:
 *   docker ps | grep servicebridge2-pg          # PG18 on :5433
 *   go run -C runtime ./cmd/runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/servicebridge?sslmode=disable & # runtime on :14444 + :14445
 *   cd sdk/node && bun run tests/showcase/showcase-workflow.ts
 */

import { join } from "node:path";
import express, { type Request, type Response } from "express";
import { ServiceBridge } from "../../src/connection/service-bridge";
import { attachExpress } from "../../src/http/express";
import {
	grantEventFlow,
	grantRpcWildcard,
	grantWorkflowFlow,
} from "./policy-helpers";
import {
	ALL_ROLES,
	ensureShowcaseKeys,
	type ShowcaseRole,
} from "./showcase-keys";

const SHOWCASE_PROTO = join(import.meta.dir, "showcase.proto");
const HTTP_PORT = Number(process.env.SHOWCASE_HTTP_PORT ?? 19999);

const FAST_OPTS = {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
} as const;

const WORKFLOW_NAME = "showcase-flow";
const SHIPPING_WORKFLOW_NAME = "shipping-flow";
const ORDER_EVENT = "order_created";
const AUDIT_EVENT = "audit_log";
const CLEANUP_JOB = "cleanup-job";
const DELAYED_JOB = "delayed-cleanup";

interface RunOutput {
	wfTraceId: string;
	cleanupJobTraceId: string | null;
	delayedJobTraceId: string | null;
	wfRunId: string;
}

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
	pred: () => boolean,
	timeoutMs: number,
	what: string,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (pred()) return;
		await sleep(50);
	}
	throw new Error(`waitFor: timed out after ${timeoutMs}ms — ${what}`);
}

interface Bridges {
	showcase: ServiceBridge;
	billing: ServiceBridge;
	inventory: ServiceBridge;
	externalApi: ServiceBridge;
	auditSub: ServiceBridge;
	notif1: ServiceBridge;
	notif2: ServiceBridge;
	notif3: ServiceBridge;
}

interface Identities {
	[k: string]: string; // role → serviceId
}

async function buildBridges(
	url: string,
	keys: Record<ShowcaseRole, string>,
): Promise<Bridges> {
	// SDK sqlite outbox schema evolves outside a migration framework — `x_sb_trace`
	// was added recently and is `NOT NULL DEFAULT ''`. A stale `./.servicebridge/sdk.db`
	// from an earlier session will not have the column and any Publisher write
	// will fail with `table event_outbox has no column named x_sb_trace`. Wipe
	// the SDK-local directory before each showcase run; outbox state is local
	// to a session and not meant to survive between runs.
	const { rmSync } = await import("node:fs");
	const dataDir = process.env.SB_DATA_DIR ?? "./.servicebridge";
	try {
		rmSync(dataDir, { recursive: true, force: true });
	} catch {
		// best effort — surface real failures via the bridge start path
	}
	const make = (role: ShowcaseRole) =>
		new ServiceBridge(url, keys[role], FAST_OPTS);
	return {
		showcase: make("showcase-service"),
		billing: make("billing-service"),
		inventory: make("inventory-service"),
		externalApi: make("external-api-service"),
		auditSub: make("audit-subscriber"),
		notif1: make("notification-subscriber-1"),
		notif2: make("notification-subscriber-2"),
		notif3: make("notification-subscriber-3"),
	};
}

/**
 * Register every handler / publisher / subscriber / workflow definition on
 * the appropriate ServiceBridge. Idempotent: each method/event is registered
 * exactly once on exactly one bridge.
 *
 * Retry counters live in closures so a single Reserve call across attempts
 * sees a monotonically-increasing count; the runtime retry loop calls Reserve
 * multiple times for the same workflow step until success or budget exhaust.
 */
function registerSurfaces(b: Bridges): void {
	// --- Billing: charge always OK, refund is the compensation target. ---
	b.billing.rpc.handle(
		"Charge",
		async (req: { orderId: string; amount: number }) => {
			return { chargeId: `chg-${req.orderId}-${Date.now()}`, ok: true };
		},
		{ schema: { protoFile: SHOWCASE_PROTO } },
	);
	b.billing.rpc.handle(
		"Refund",
		async (_req: { chargeId: string; amount: number }) => ({ ok: true }),
		{ schema: { protoFile: SHOWCASE_PROTO } },
	);

	// --- Inventory: Reserve fails twice then succeeds (retry simulation). ---
	let reserveAttempts = 0;
	b.inventory.rpc.handle(
		"Reserve",
		async (req: { itemId: string; quantity: number }) => {
			reserveAttempts++;
			if (reserveAttempts <= 2) {
				throw new Error(
					`inventory transient #${reserveAttempts} for ${req.itemId}`,
				);
			}
			return {
				reservationId: `rsv-${req.itemId}-${Date.now()}`,
				ok: true,
			};
		},
		{ schema: { protoFile: SHOWCASE_PROTO } },
	);
	b.inventory.rpc.handle(
		"Release",
		async (_req: { reservationId: string }) => ({ ok: true }),
		{ schema: { protoFile: SHOWCASE_PROTO } },
	);

	// --- External API: always fails (DLQ simulation). ---
	b.externalApi.rpc.handle(
		"Notify",
		async (_req: { orderId: string; channel: string }) => {
			throw new Error("external upstream unavailable");
		},
		{ schema: { protoFile: SHOWCASE_PROTO } },
	);

	// --- Events: subscribers for ORDER_EVENT (fan-out × 3) + AUDIT (× 1). ---
	for (const sub of [b.notif1, b.notif2, b.notif3]) {
		sub.event.define(ORDER_EVENT, {
			protoFile: SHOWCASE_PROTO,
			method: ORDER_EVENT,
		});
		sub.event.handle(ORDER_EVENT, async (_payload) => {
			// just absorb — runtime emits EVENT.DELIVER ops for the waterfall
		});
	}
	b.auditSub.event.define(AUDIT_EVENT, {
		protoFile: SHOWCASE_PROTO,
		method: AUDIT_EVENT,
	});
	b.auditSub.event.handle(AUDIT_EVENT, async (_payload) => {
		// absorb
	});

	// Publisher side declares both events.
	b.showcase.event.define(ORDER_EVENT, {
		protoFile: SHOWCASE_PROTO,
		method: ORDER_EVENT,
	});
	b.showcase.event.define(AUDIT_EVENT, {
		protoFile: SHOWCASE_PROTO,
		method: AUDIT_EVENT,
	});

	// --- Workflow definitions on showcase-service. ---
	// Nested workflow: shipping-flow — ONE rpc step + ONE event step. Started
	// from the `ship` step of the main flow, it inherits the main trace via
	// X-SB-Trace (parent = the ship step span) so its WORKFLOW.RUN op and the
	// rpc/event ops inside it nest under `ship` in the same trace tree.
	b.showcase.workflow.handle(SHIPPING_WORKFLOW_NAME, {
		steps: [
			{
				type: "call",
				id: "ship_label",
				service: "inventory-service",
				method: "Release",
				input: { reservationId: "$.input.reservationId" },
			},
			{
				type: "publish",
				id: "ship_notify",
				event: AUDIT_EVENT,
				input: {
					action: "shipment_dispatched",
					actor: "$.input.chargeId",
				},
				waitFor: ["ship_label"],
			},
		],
	});

	// Main workflow.
	b.showcase.workflow.handle(WORKFLOW_NAME, {
		steps: [
			// Phase 1: charge (compensable — wired via compensate{} in the
			// workflow registration, not via a dedicated step).
			{
				type: "call",
				id: "charge",
				service: "billing-service",
				method: "Charge",
				input: { orderId: "$.input.orderId", amount: "$.input.amount" },
				compensate: {
					service: "billing-service",
					method: "Refund",
					input: { chargeId: "$.charge.chargeId", amount: "$.input.amount" },
				},
			},
			// Phase 2: publish order.created — fan-out × 3.
			{
				type: "publish",
				id: "publish_order",
				event: ORDER_EVENT,
				input: {
					orderId: "$.input.orderId",
					amount: "$.input.amount",
				},
				waitFor: ["charge"],
				// Marked as compensable via a publish-publish reversal — the
				// runner re-emits an event with the "compensation:" prefix at
				// compensation time, which the UI renders as a CompensationArrow
				// back to this forward publish.
				compensate: {
					type: "publish",
					event: ORDER_EVENT,
					input: { orderId: "$.input.orderId", amount: 0 },
				},
			},
			// Phase 3: nested sub-workflow. Runs after the order is published and
			// before the demo fanout. shipping-flow inherits this trace via
			// X-SB-Trace (parent = the `ship` step span), so its WORKFLOW.RUN op
			// and the rpc + event ops inside it nest under `ship` in the same
			// trace tree. Kept out of the racing fanout so it reliably executes
			// regardless of the intentionally-failing demo branches below.
			{
				type: "workflow",
				id: "ship",
				workflow: SHIPPING_WORKFLOW_NAME,
				waitFor: ["publish_order"],
				input: {
					chargeId: "$.charge.chargeId",
					reservationId: "rsv-showcase",
				},
			},
			// Phase 4: 3 parallel branches.
			{
				type: "parallel",
				id: "fanout",
				waitFor: ["ship"],
				steps: [
					// Branch A: inventory reserve (retry × 3) — retry demo.
					{
						type: "sequence",
						id: "branch_a",
						steps: [
							{
								type: "call",
								id: "reserve",
								service: "inventory-service",
								method: "Reserve",
								input: {
									itemId: "$.input.itemId",
									quantity: "$.input.quantity",
								},
								retry: {
									maxAttempts: 3,
									baseDelayMs: 1000,
									maxDelayMs: 3000,
									factor: 2,
									jitter: 0,
								},
							},
						],
					},
					// Branch B: sleep then audit publish.
					{
						type: "sequence",
						id: "branch_b",
						steps: [
							{
								type: "sleep",
								id: "wait_15s",
								durationSec: 15,
							},
							{
								type: "publish",
								id: "publish_audit",
								event: AUDIT_EVENT,
								input: {
									action: "order_processed",
									actor: "$.input.orderId",
								},
								waitFor: ["wait_15s"],
							},
						],
					},
					// Branch C: wait_signal (times out) → external Notify (DLQ).
					{
						type: "sequence",
						id: "branch_c",
						steps: [
							{
								type: "wait_signal",
								id: "manager_approval",
								signal: "manager_approval",
								timeoutSec: 10,
							},
							{
								type: "call",
								id: "notify",
								service: "external-api-service",
								method: "Notify",
								input: { orderId: "$.input.orderId", channel: "email" },
								waitFor: ["manager_approval"],
								retry: {
									maxAttempts: 3,
									baseDelayMs: 500,
									maxDelayMs: 2000,
									factor: 2,
									jitter: 0,
								},
							},
						],
					},
				],
			},
		],
	});

	// --- Jobs: cleanup-job (cron immediate, always fails → DLQ on 4th).
	// Cron 5-field syntax: every minute → runtime fires it on first tick,
	// retry policy keeps re-running on failure until DLQ.
	b.showcase.job.handle(
		CLEANUP_JOB,
		{
			trigger: { cron: "* * * * *" },
			maxAttempts: 4,
			retry: { initialMs: 200, maxMs: 1000, multiplier: 2, jitter: 0 },
		},
		async () => {
			throw new Error("cleanup-job: always fails for showcase DLQ demo");
		},
	);
	// delayed-cleanup: one-shot, fires 5s after the showcase starts → temporal
	// gap in the waterfall under the workflow root.
	b.showcase.job.handle(
		DELAYED_JOB,
		{
			trigger: { delayed: { at: new Date(Date.now() + 5_000) } },
		},
		async () => {
			// succeed
		},
	);

	// --- HTTP routes — declare outgoing deps so policy + service map agree. ---
	b.showcase.service("billing-service", { rpc: ["Charge", "Refund"] });
	b.showcase.service("inventory-service", { rpc: ["Reserve", "Release"] });
	b.showcase.service("external-api-service", { rpc: ["Notify"] });
}

async function loadSchemas(b: Bridges): Promise<void> {
	// Schemas the workflow runner needs to encode call inputs.
	await b.showcase.useSchema("billing-service", "Charge", {
		protoFile: SHOWCASE_PROTO,
	});
	await b.showcase.useSchema("billing-service", "Refund", {
		protoFile: SHOWCASE_PROTO,
	});
	await b.showcase.useSchema("inventory-service", "Reserve", {
		protoFile: SHOWCASE_PROTO,
	});
	await b.showcase.useSchema("inventory-service", "Release", {
		protoFile: SHOWCASE_PROTO,
	});
	await b.showcase.useSchema("external-api-service", "Notify", {
		protoFile: SHOWCASE_PROTO,
	});
}

async function seedPolicies(_b: Bridges, ids: Identities): Promise<void> {
	const sc = ids["showcase-service"];
	if (!sc) throw new Error("policy: showcase-service identity missing");
	// Workflow self-loop (showcase starts the workflow it owns).
	await grantWorkflowFlow(sc, sc, WORKFLOW_NAME);
	await grantWorkflowFlow(sc, sc, SHIPPING_WORKFLOW_NAME);
	// RPC fanout from showcase to provider services.
	for (const [role, methods] of [
		["billing-service", ["Charge", "Refund"]],
		["inventory-service", ["Reserve", "Release"]],
		["external-api-service", ["Notify"]],
	] as const) {
		const peer = ids[role];
		if (!peer) throw new Error(`policy: ${role} identity missing`);
		for (const m of methods) await grantRpcWildcard(sc, peer, m);
	}
	// Event flow: showcase → 3 notif subscribers + 1 audit subscriber.
	for (const sub of [
		"notification-subscriber-1",
		"notification-subscriber-2",
		"notification-subscriber-3",
	]) {
		const subId = ids[sub];
		if (!subId) throw new Error(`policy: ${sub} identity missing`);
		await grantEventFlow(sc, subId, ORDER_EVENT);
	}
	const auditId = ids["audit-subscriber"];
	if (!auditId) throw new Error("policy: audit-subscriber identity missing");
	await grantEventFlow(sc, auditId, AUDIT_EVENT);
	// Wait for the runtime NOTIFY → snapshot propagation.
	await sleep(2_000);
}

async function startAll(b: Bridges): Promise<Identities> {
	await Promise.all(
		Object.values(b).map((sb) => (sb as ServiceBridge).start()),
	);
	await waitFor(
		() =>
			Object.values(b).every((sb) => (sb as ServiceBridge).identity() !== null),
		15_000,
		"all SDK instances connected",
	);
	const ids: Identities = {};
	for (const [role, sb] of Object.entries(b)) {
		const id = (sb as ServiceBridge).identity();
		if (!id) throw new Error(`identity null for ${role} after wait`);
		ids[id.serviceName] = id.serviceId;
	}
	return ids;
}

async function stopAll(b: Bridges): Promise<void> {
	for (const sb of Object.values(b)) {
		await (sb as ServiceBridge).stop().catch(() => undefined);
	}
}

function buildHttpApp(showcase: ServiceBridge): {
	app: express.Express;
	listen: () => Promise<{ close: () => Promise<void> }>;
	lastRunId: { value: string | null };
} {
	const app = express();
	app.use(express.json());
	const lastRunId = { value: null as string | null };

	app.post("/run-showcase", async (_req: Request, res: Response) => {
		try {
			const { runId } = await showcase.workflow.start(WORKFLOW_NAME, {
				orderId: `ord-${Date.now()}`,
				amount: 49.99,
				itemId: "widget-7",
				quantity: 1,
			});
			lastRunId.value = runId;
			res.json({ runId });
		} catch (err) {
			res.status(500).json({
				error: err instanceof Error ? err.message : String(err),
			});
		}
	});

	attachExpress(app, showcase, { port: HTTP_PORT });

	const listen = async () =>
		new Promise<{ close: () => Promise<void> }>((resolve) => {
			const server = app.listen(HTTP_PORT, () =>
				resolve({
					close: () =>
						new Promise<void>((r) => {
							server.close(() => r());
						}),
				}),
			);
		});

	return { app, listen, lastRunId };
}

async function queryTraceIds(wfRunId: string): Promise<RunOutput> {
	const { sql } = await import("bun");
	const dbUrl =
		process.env.TEST_DATABASE_URL ??
		"postgresql://servicebridge:servicebridge@localhost:5433/servicebridge";
	const prev = process.env.DATABASE_URL;
	process.env.DATABASE_URL = dbUrl;
	try {
		// Workflow root op: WORKFLOW.RUN (channel=4, kind=1) with business_key=runId
		const wfRow = (await sql`
			SELECT trace_id::text AS trace_id
			FROM operations
			WHERE channel = 4 AND kind = 1 AND business_key = ${wfRunId}
			LIMIT 1
		`) as Array<{ trace_id: string }>;
		const cleanupRow = (await sql`
			SELECT trace_id::text AS trace_id
			FROM operations
			WHERE channel = 5 AND kind = 1 AND subject LIKE ${`%${CLEANUP_JOB}%`}
			ORDER BY started_at DESC
			LIMIT 1
		`) as Array<{ trace_id: string }>;
		const delayedRow = (await sql`
			SELECT trace_id::text AS trace_id
			FROM operations
			WHERE channel = 5 AND kind = 1 AND subject LIKE ${`%${DELAYED_JOB}%`}
			ORDER BY started_at DESC
			LIMIT 1
		`) as Array<{ trace_id: string }>;
		return {
			wfTraceId: wfRow[0]?.trace_id ?? "",
			cleanupJobTraceId: cleanupRow[0]?.trace_id ?? null,
			delayedJobTraceId: delayedRow[0]?.trace_id ?? null,
			wfRunId,
		};
	} finally {
		if (prev === undefined) delete process.env.DATABASE_URL;
		else process.env.DATABASE_URL = prev;
	}
}

export interface ShowcaseRunResult extends RunOutput {
	finalStatus: string;
}

/**
 * Run the showcase end-to-end. Returns trace ids + final workflow status.
 * Exported so the automated integration test (showcase-workflow.test.ts) can
 * call the same code path without spinning up an HTTP server.
 */
export async function runShowcase(opts?: {
	skipHttp?: boolean;
	runtimeUrl?: string;
}): Promise<ShowcaseRunResult> {
	const showcase = ensureShowcaseKeys({ runtimeUrl: opts?.runtimeUrl });
	const bridges = await buildBridges(showcase.runtimeUrl, showcase.keys);

	registerSurfaces(bridges);
	await loadSchemas(bridges);

	const ids = await startAll(bridges);
	await seedPolicies(bridges, ids);

	let serverClose: (() => Promise<void>) | null = null;
	let runId: string;

	try {
		if (opts?.skipHttp) {
			const r = await bridges.showcase.workflow.start(WORKFLOW_NAME, {
				orderId: `ord-${Date.now()}`,
				amount: 49.99,
				itemId: "widget-7",
				quantity: 1,
			});
			runId = r.runId;
		} else {
			const { listen } = buildHttpApp(bridges.showcase);
			const server = await listen();
			serverClose = server.close;
			// Trigger the run via the HTTP route so the trace has an HTTP.HANDLE
			// root linked to the WORKFLOW.RUN root (same trace_id).
			const resp = await fetch(`http://localhost:${HTTP_PORT}/run-showcase`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: "{}",
			});
			const json = (await resp.json()) as { runId: string };
			runId = json.runId;
		}

		// Wait for the workflow to terminate (or up to 90s — sleep is 15s + wait
		// is 10s + DLQ retry chain + nested workflow).
		let finalStatus = "running";
		const deadline = Date.now() + 90_000;
		while (Date.now() < deadline) {
			const q = await bridges.showcase.workflow.query(runId);
			finalStatus = q.status;
			if (
				finalStatus === "completed" ||
				finalStatus === "failed" ||
				finalStatus === "failed_compensated" ||
				finalStatus === "cancelled"
			) {
				break;
			}
			await sleep(1_000);
		}

		// Give telemetry a moment to flush END frames.
		await sleep(2_000);

		const traces = await queryTraceIds(runId);
		return { ...traces, finalStatus };
	} finally {
		if (serverClose) await serverClose();
		await stopAll(bridges);
	}
}

async function main(): Promise<void> {
	console.error("showcase: provisioning keys (cached in .env.showcase) ...");
	const result = await runShowcase();
	console.log("");
	console.log("Showcase trace ready:");
	console.log(
		`  workflow trace:    http://localhost:14444/traces/${result.wfTraceId}`,
	);
	if (result.cleanupJobTraceId) {
		console.log(
			`  cleanup-job trace: http://localhost:14444/traces/${result.cleanupJobTraceId}`,
		);
	}
	if (result.delayedJobTraceId) {
		console.log(
			`  delayed-cleanup:   http://localhost:14444/traces/${result.delayedJobTraceId}`,
		);
	}
	console.log(`  final wf status:   ${result.finalStatus}`);
	console.log(
		"Open the workflow URL and walk through tests/showcase/README.md acceptance checklist.",
	);
}

// Use a sentinel so the script can be imported (showcase-workflow.test.ts)
// without auto-running. Bun honors import.meta.main for the entrypoint check.
if (import.meta.main) {
	const bridges: Bridges | null = null;
	const cleanup = async (signal: string): Promise<void> => {
		console.error(`showcase: received ${signal}, shutting down ...`);
		if (bridges) await stopAll(bridges);
		process.exit(0);
	};
	process.on("SIGINT", () => void cleanup("SIGINT"));
	process.on("SIGTERM", () => void cleanup("SIGTERM"));

	main().catch((err) => {
		console.error("showcase: fatal:", err);
		process.exit(1);
	});
}

void ALL_ROLES; // satisfy unused-import lint
