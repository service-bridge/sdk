// Caller-side WorkflowDomain ops — exercised against a mock WorkflowsClient.

import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import type { WorkflowsClient } from "../pb/servicebridge/v1/workflows";
import { Registry } from "../registry/registry";
import { WorkflowDomain } from "./domain";
import {
	WorkflowAccessDeniedError,
	WorkflowNotFoundError,
	WorkflowTerminalError,
} from "./errors";

interface RecordedCalls {
	start: unknown[];
	cancel: unknown[];
	signal: unknown[];
	query: unknown[];
	replay: unknown[];
	await: unknown[];
}

function makeMock(
	overrides: Partial<Record<keyof RecordedCalls, unknown>> = {},
): { rpc: WorkflowsClient; rec: RecordedCalls } {
	const rec: RecordedCalls = {
		start: [],
		cancel: [],
		signal: [],
		query: [],
		replay: [],
		await: [],
	};
	const rpc = {
		start(req: unknown, cb: (e: unknown, r: unknown) => void) {
			rec.start.push(req);
			const result = overrides.start as
				| { err?: unknown; resp?: unknown }
				| undefined;
			if (result?.err) return cb(result.err, undefined);
			cb(null, result?.resp ?? { runId: "r-new" });
		},
		cancel(req: unknown, cb: (e: unknown, r: unknown) => void) {
			rec.cancel.push(req);
			const result = overrides.cancel as { err?: unknown } | undefined;
			cb(result?.err ?? null, undefined);
		},
		signal(req: unknown, cb: (e: unknown, r: unknown) => void) {
			rec.signal.push(req);
			const result = overrides.signal as { err?: unknown } | undefined;
			cb(result?.err ?? null, undefined);
		},
		query(req: unknown, cb: (e: unknown, r: unknown) => void) {
			rec.query.push(req);
			const result = overrides.query as
				| { err?: unknown; resp?: unknown }
				| undefined;
			if (result?.err) return cb(result.err, undefined);
			cb(
				null,
				result?.resp ?? {
					runId: (req as { runId: string }).runId,
					status: "success",
					state: Buffer.from('{"x":1}', "utf8"),
					steps: [
						{
							stepId: "a",
							status: "success",
							output: Buffer.from('{"ok":true}', "utf8"),
							lastError: "",
						},
					],
				},
			);
		},
		replay(req: unknown, cb: (e: unknown, r: unknown) => void) {
			rec.replay.push(req);
			const result = overrides.replay as
				| { err?: unknown; resp?: unknown }
				| undefined;
			if (result?.err) return cb(result.err, undefined);
			cb(null, result?.resp ?? { runId: "r-replayed" });
		},
		await(req: unknown) {
			rec.await.push(req);
			const stream = new EventEmitter() as EventEmitter & {
				cancel: () => void;
			};
			stream.cancel = () => undefined;
			const result = overrides.await as
				| { updates?: unknown[]; err?: unknown }
				| undefined;
			queueMicrotask(() => {
				if (result?.err) {
					stream.emit("error", result.err);
					return;
				}
				const updates = result?.updates ?? [
					{
						runId: (req as { runId: string }).runId,
						status: "success",
						state: Buffer.from('{"final":true}', "utf8"),
					},
				];
				for (const u of updates) stream.emit("data", u);
				stream.emit("end");
			});
			return stream;
		},
	} as unknown as WorkflowsClient;
	return { rpc, rec };
}

function makeDomain(
	overrides: Partial<Record<keyof RecordedCalls, unknown>> = {},
): { domain: WorkflowDomain; rec: RecordedCalls } {
	const reg = new Registry();
	const domain = new WorkflowDomain(reg);
	const { rpc, rec } = makeMock(overrides);
	domain._attachRpc(rpc);
	return { domain, rec };
}

describe("WorkflowDomain.start", () => {
	it("happy path returns runId; input JSON-encoded", async () => {
		const { domain, rec } = makeDomain();
		const r = await domain.start("wf", { userId: "u-1" });
		expect(r.runId).toBe("r-new");
		const req = rec.start[0] as { workflowName: string; input: Buffer };
		expect(req.workflowName).toBe("wf");
		expect(JSON.parse(req.input.toString("utf8"))).toEqual({ userId: "u-1" });
	});

	it("PERMISSION_DENIED → WorkflowAccessDeniedError", async () => {
		const { domain } = makeDomain({
			start: { err: { code: 7, message: "denied", details: "no rule" } },
		});
		await expect(domain.start("wf", {})).rejects.toBeInstanceOf(
			WorkflowAccessDeniedError,
		);
	});

	it("NOT_FOUND → WorkflowNotFoundError", async () => {
		const { domain } = makeDomain({
			start: { err: { code: 5, message: "not found" } },
		});
		await expect(domain.start("wf", {})).rejects.toBeInstanceOf(
			WorkflowNotFoundError,
		);
	});
});

describe("WorkflowDomain.signal / cancel", () => {
	it("signal serializes payload as JSON", async () => {
		const { domain, rec } = makeDomain();
		await domain.signal("r-1", "admin-approve", { approved: true });
		const req = rec.signal[0] as {
			runId: string;
			signalName: string;
			payload: Buffer;
		};
		expect(req.runId).toBe("r-1");
		expect(req.signalName).toBe("admin-approve");
		expect(JSON.parse(req.payload.toString("utf8"))).toEqual({
			approved: true,
		});
	});

	it("cancel forwards runId", async () => {
		const { domain, rec } = makeDomain();
		await domain.cancel("r-7");
		expect(rec.cancel[0]).toEqual({ runId: "r-7" });
	});

	it("FAILED_PRECONDITION on cancel → WorkflowTerminalError", async () => {
		const { domain } = makeDomain({
			cancel: { err: { code: 9, message: "terminal", details: "success" } },
		});
		await expect(domain.cancel("r-7")).rejects.toBeInstanceOf(
			WorkflowTerminalError,
		);
	});
});

describe("WorkflowDomain.query", () => {
	it("decodes JSON state + step output", async () => {
		const { domain } = makeDomain();
		const q = await domain.query("r-1");
		expect(q.status).toBe("success");
		expect(q.state).toEqual({ x: 1 });
		expect(q.steps[0]).toEqual({
			stepId: "a",
			status: "success",
			output: { ok: true },
			lastError: "",
		});
	});
});

describe("WorkflowDomain.await", () => {
	it("resolves with parsed final state on terminal update", async () => {
		const { domain } = makeDomain();
		const s = await domain.await("r-1");
		expect(s).toEqual({ final: true });
	});

	it("rejects on stream error", async () => {
		const { domain } = makeDomain({
			await: { err: { code: 13, message: "boom" } },
		});
		await expect(domain.await("r-1")).rejects.toBeDefined();
	});
});

describe("WorkflowDomain.replay", () => {
	it("returns new runId; forwards fromStepId", async () => {
		const { domain, rec } = makeDomain();
		const r = await domain.replay("r-1", { fromStepId: "charge" });
		expect(r.runId).toBe("r-replayed");
		expect(rec.replay[0]).toEqual({ runId: "r-1", fromStepId: "charge" });
	});

	it("empty fromStepId allowed (full re-execution)", async () => {
		const { domain, rec } = makeDomain();
		await domain.replay("r-1");
		expect((rec.replay[0] as { fromStepId: string }).fromStepId).toBe("");
	});
});

describe("WorkflowDomain — caller ops require attached rpc", () => {
	it("throws if start() called before _attachRpc", async () => {
		const reg = new Registry();
		const d = new WorkflowDomain(reg);
		await expect(d.start("wf", {})).rejects.toThrow(/RPC channel not yet/);
	});
});
