import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { MethodType } from "../pb/servicebridge/v1/registry";
import type { WorkflowsClient } from "../pb/servicebridge/v1/workflows";
import { Registry } from "../registry/registry";
import { WorkflowDomain } from "./domain";
import {
	WorkflowAccessDeniedError,
	WorkflowNotFoundError,
	WorkflowRunFailedError,
	WorkflowTerminalError,
} from "./errors";

const grpcErr = (code: number) =>
	Object.assign(new Error(`code ${code}`), { code, details: `d${code}` });

function domainWith(client: Partial<Record<string, unknown>>) {
	const violations: string[] = [];
	const d = new WorkflowDomain(new Registry(), (v) => violations.push(v.value));
	d._attachRpc(client as unknown as WorkflowsClient);
	return { d, violations };
}

describe("WorkflowDomain", () => {
	it("handle registers the structured definition and keeps local functions", () => {
		const registry = new Registry();
		const d = new WorkflowDomain(registry);
		const fn = () => 1;
		d.handle("flow", { version: "3", steps: [{ id: "a", type: "local", fn }] });
		const m = registry._handle.incomingMethods()[0]!;
		expect(m.type).toBe(MethodType.METHOD_TYPE_WORKFLOW);
		expect(m.workflow?.steps[0]?.local).toEqual({});
		expect(d._definition("flow")?.locals.get("a")).toBe(fn);
		expect(d._definition("flow")?.version).toBe("3");
		expect(d._size()).toBe(1);
		expect(() => d.handle("flow", { steps: [] })).toThrow();
	});

	it("start sends (service, workflow) and maps errors", async () => {
		let sent: Record<string, unknown> = {};
		const { d, violations } = domainWith({
			start: (
				req: Record<string, unknown>,
				cb: (e: unknown, r?: unknown) => void,
			) => {
				sent = req;
				if (req.workflow === "denied") return cb(grpcErr(7));
				if (req.workflow === "missing") return cb(grpcErr(5));
				cb(null, { runId: "r1" });
			},
		});
		expect(
			await d.start(
				"orders",
				"flow",
				{ a: 1 },
				{ idempotencyKey: "k", timeoutMs: 10 },
			),
		).toEqual({ runId: "r1" });
		expect(sent).toMatchObject({
			service: "orders",
			workflow: "flow",
			idempotencyKey: "k",
			timeoutMs: 10,
		});
		expect(JSON.parse((sent.input as Buffer).toString())).toEqual({ a: 1 });
		await expect(d.start("orders", "denied", null)).rejects.toBeInstanceOf(
			WorkflowAccessDeniedError,
		);
		expect(violations).toEqual(["orders/denied"]);
		await expect(d.start("orders", "missing", null)).rejects.toBeInstanceOf(
			WorkflowNotFoundError,
		);
	});

	it("signal returns the duplicate flag; terminal runs map to WorkflowTerminalError", async () => {
		const { d } = domainWith({
			signal: (
				req: { signalId: string; runId: string },
				cb: (e: unknown, r?: unknown) => void,
			) => {
				if (req.runId === "done") return cb(grpcErr(9));
				cb(null, { duplicate: req.signalId === "dup" });
			},
			cancel: (_: unknown, cb: (e: unknown) => void) => cb(grpcErr(9)),
			retryCompensation: (_: unknown, cb: (e: unknown) => void) => cb(null),
			replay: (_: unknown, cb: (e: unknown, r?: unknown) => void) =>
				cb(null, { runId: "r2" }),
		});
		expect(await d.signal("r", "go", {}, { signalId: "dup" })).toEqual({
			duplicate: true,
		});
		expect(await d.signal("r", "go", {})).toEqual({ duplicate: false });
		await expect(d.signal("done", "go", {})).rejects.toBeInstanceOf(
			WorkflowTerminalError,
		);
		await expect(d.cancel("done")).rejects.toBeInstanceOf(
			WorkflowTerminalError,
		);
		await d.retryCompensation("r");
		expect(await d.replay("r", { fromStepId: "b" })).toEqual({ runId: "r2" });
	});

	it("await resolves the output on success and rejects other terminals", async () => {
		const updates: Record<string, Array<Record<string, unknown>>> = {
			ok: [
				{ status: "active", terminal: false, output: Buffer.alloc(0) },
				{
					status: "success",
					terminal: true,
					output: Buffer.from('{"a":1}'),
					errorCode: "",
					errorMessage: "",
				},
			],
			bad: [
				{
					status: "failed_compensated",
					terminal: true,
					output: Buffer.alloc(0),
					errorCode: "COMPENSATION_FAILED",
					errorMessage: "x",
				},
			],
			cut: [{ status: "active", terminal: false }],
		};
		const { d } = domainWith({
			await: (req: { runId: string }) => {
				const s = new EventEmitter();
				queueMicrotask(() => {
					for (const u of updates[req.runId]!) s.emit("data", u);
					s.emit("end");
				});
				return s;
			},
		});
		expect(await d.await("ok")).toEqual({ a: 1 });
		const err = await d.await("bad").catch((e) => e);
		expect(err).toBeInstanceOf(WorkflowRunFailedError);
		expect(err.status).toBe("failed_compensated");
		await expect(d.await("cut")).rejects.toBeInstanceOf(WorkflowTerminalError);
	});

	it("query maps the snapshot", async () => {
		const { d } = domainWith({
			query: (_: unknown, cb: (e: unknown, r?: unknown) => void) =>
				cb(null, {
					runId: "r",
					service: "s",
					workflow: "w",
					fingerprint: "f",
					status: "active",
					stopReason: "",
					waitingReason: "signal",
					input: Buffer.from("{}"),
					output: Buffer.alloc(0),
					errorCode: "",
					errorMessage: "",
					parentRunId: "",
					startedAtUnixMs: 1,
					endedAtUnixMs: 0,
					steps: [
						{
							stepId: "w",
							parentStepId: "",
							kind: "wait_signal",
							status: "parked",
							attempt: 0,
							output: Buffer.alloc(0),
							errorCode: "",
							errorMessage: "",
							waitingReason: "signal",
							waitKey: "go",
							childRunId: "",
							compensatesStepId: "",
							startedAtUnixMs: 1,
							endedAtUnixMs: 0,
						},
					],
					signals: [
						{
							signalName: "x",
							signalId: "",
							payload: Buffer.from("1"),
							enqueuedAtUnixMs: 5,
						},
					],
				}),
		});
		const snap = await d.query("r");
		expect(snap.waitingReason).toBe("signal");
		expect(snap.output).toBeNull();
		expect(snap.steps[0]).toMatchObject({
			status: "parked",
			waitKey: "go",
			output: null,
		});
		expect(snap.signals[0]).toEqual({
			signalName: "x",
			signalId: "",
			payload: 1,
			enqueuedAtMs: 5,
		});
	});

	it("caller operations need an attached channel", async () => {
		const d = new WorkflowDomain(new Registry());
		await expect(d.query("r")).rejects.toThrow();
	});
});
