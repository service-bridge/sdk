import { describe, expect, test } from "bun:test";
import { Registry } from "../registry/registry";
import { JobDomain } from "./domain";

const noop = async () => {};

function newDomain(): { domain: JobDomain; registry: Registry } {
	const registry = new Registry();
	const domain = new JobDomain(registry);
	return { domain, registry };
}

describe("sb.job.handle — cron validation", () => {
	test("rejects invalid cron expression client-side", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { cron: "not a cron" } }, noop),
		).toThrow(/cron/i);
	});

	test("accepts valid 5-field cron", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { cron: "0 9 * * *" } }, noop),
		).not.toThrow();
	});

	test("rejects six-field cron client-side", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { cron: "* * * * * *" } }, noop),
		).toThrow(/5.field/i);
	});
});

describe("sb.job.handle — duplicate guard", () => {
	test("rejects duplicate name", () => {
		const { domain } = newDomain();
		domain.handle("dup", { trigger: { interval: 1000 } }, noop);
		expect(() =>
			domain.handle("dup", { trigger: { interval: 2000 } }, noop),
		).toThrow(/duplicate/i);
	});
});

describe("sb.job.handle — trigger oneof validation", () => {
	test("rejects both cron and delayed", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				// biome-ignore lint/suspicious/noExplicitAny: malformed input on purpose
				{ trigger: { cron: "0 * * * *", delayed: { at: new Date() } } as any },
				noop,
			),
		).toThrow(/exactly one/i);
	});

	test("rejects all three triggers", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{
					trigger: {
						cron: "0 * * * *",
						delayed: { at: new Date() },
						interval: 1000,
						// biome-ignore lint/suspicious/noExplicitAny: malformed input on purpose
					} as any,
				},
				noop,
			),
		).toThrow(/exactly one/i);
	});

	test("rejects zero triggers", () => {
		const { domain } = newDomain();
		expect(() =>
			// biome-ignore lint/suspicious/noExplicitAny: malformed input on purpose
			domain.handle("j", { trigger: {} as any }, noop),
		).toThrow(/exactly one/i);
	});
});

describe("sb.job.handle — delayed validation", () => {
	test("rejects invalid date string", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { delayed: { at: "not a date" } } }, noop),
		).toThrow(/delayed/i);
	});

	test("accepts Date", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { delayed: { at: new Date() } } }, noop),
		).not.toThrow();
	});

	test("accepts number", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { delayed: { at: Date.now() } } }, noop),
		).not.toThrow();
	});
});

describe("sb.job.handle — interval validation", () => {
	test("rejects 0", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { interval: 0 } }, noop),
		).toThrow(/interval/i);
	});

	test("rejects negative", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { interval: -100 } }, noop),
		).toThrow(/interval/i);
	});

	test("accepts positive", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { trigger: { interval: 100 } }, noop),
		).not.toThrow();
	});
});

describe("sb.job.handle — registry side effects (IncomingMethod{type=JOB})", () => {
	test("appends one IncomingMethod with canonical-spec JSON + contract hash", () => {
		const { domain, registry } = newDomain();
		domain.handle(
			"daily",
			{
				trigger: { cron: "0 9 * * *", tz: "Europe/Moscow" },
				catchup: "fire_once",
				overlap: "skip",
				deps: [{ rpc: "billing.GenerateReport" }],
				maxAttempts: 5,
				leaseTtlMs: 30_000,
				maxConcurrent: 1,
			},
			noop,
		);

		const methods = registry._handle.incomingMethods();
		expect(methods).toHaveLength(1);
		const m = methods[0];
		expect(m).toBeDefined();
		if (!m) return;
		// type=JOB=4 from registry.proto
		expect(m.type).toBe(4);
		expect(m.name).toBe("daily");
		// contract_hash is a 64-char hex SHA-256 of the canonical-spec JSON
		expect(m.contractHash).toMatch(/^[0-9a-f]{64}$/);
		// input_schema_json is the canonical spec — parse it back
		const json = new TextDecoder().decode(m.inputSchemaJson);
		const spec = JSON.parse(json) as {
			trigger: { cron: { expr: string; tz: string } };
			catchup: string;
			overlap: string;
			deps: Array<{ kind: string; target: string }>;
			maxAttempts: number;
			leaseTtlMs: number;
			maxConcurrent: number;
		};
		expect(spec.trigger.cron.expr).toBe("0 9 * * *");
		expect(spec.trigger.cron.tz).toBe("Europe/Moscow");
		expect(spec.catchup).toBe("fire_once");
		expect(spec.overlap).toBe("skip");
		expect(spec.deps[0]).toEqual({
			kind: "rpc",
			target: "billing.GenerateReport",
		});
		expect(spec.maxAttempts).toBe(5);
		expect(spec.leaseTtlMs).toBe(30_000);
		expect(spec.maxConcurrent).toBe(1);
	});

	test("delayed trigger serialises as runAtUnixMs", () => {
		const { domain, registry } = newDomain();
		const at = new Date("2026-06-01T12:00:00.000Z");
		domain.handle("once", { trigger: { delayed: { at } } }, noop);
		const m = registry._handle.incomingMethods()[0];
		if (!m) throw new Error("missing entry");
		const spec = JSON.parse(new TextDecoder().decode(m.inputSchemaJson));
		expect(spec.trigger.delayed.runAtUnixMs).toBe(at.getTime());
	});

	test("interval trigger serialises as everyMs", () => {
		const { domain, registry } = newDomain();
		domain.handle("poll", { trigger: { interval: 5000 } }, noop);
		const m = registry._handle.incomingMethods()[0];
		if (!m) throw new Error("missing entry");
		const spec = JSON.parse(new TextDecoder().decode(m.inputSchemaJson));
		expect(spec.trigger.interval.everyMs).toBe(5000);
	});

	test("deps array preserves order and shape", () => {
		const { domain, registry } = newDomain();
		domain.handle(
			"d",
			{
				trigger: { interval: 1000 },
				deps: [{ rpc: "svc.M" }, { event: "topic" }, { workflow: "wf-name" }],
			},
			noop,
		);
		const m = registry._handle.incomingMethods()[0];
		if (!m) throw new Error("missing entry");
		const spec = JSON.parse(new TextDecoder().decode(m.inputSchemaJson)) as {
			deps: Array<{ kind: string; target: string }>;
		};
		expect(spec.deps).toEqual([
			{ kind: "rpc", target: "svc.M" },
			{ kind: "event", target: "topic" },
			{ kind: "workflow", target: "wf-name" },
		]);
	});
});

describe("sb.job.handle — lookup() for subscriber dispatch", () => {
	test("lookup returns opts + fn for registered job", () => {
		const { domain } = newDomain();
		const handler = async () => {};
		domain.handle(
			"x",
			{ trigger: { interval: 1000 }, maxConcurrent: 7 },
			handler,
		);
		const entry = domain.lookup("x");
		expect(entry).toBeDefined();
		expect(entry?.fn).toBe(handler);
		expect(entry?.opts.maxConcurrent).toBe(7);
	});

	test("lookup returns undefined for unknown job", () => {
		const { domain } = newDomain();
		expect(domain.lookup("nope")).toBeUndefined();
	});
});
