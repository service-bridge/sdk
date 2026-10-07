import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { Registry } from "../registry/registry";
import { cronError } from "./cron";
import { JobDomain } from "./domain";
import type { JobOpts, Trigger } from "./types";

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
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { cron: "not a cron" } },
				noop,
			),
		).toThrow(/cron/i);
	});

	test("accepts valid 5-field cron", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { cron: "0 9 * * *" } },
				noop,
			),
		).not.toThrow();
	});

	test("rejects six-field cron client-side", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { cron: "* * * * * *" } },
				noop,
			),
		).toThrow(/5.field/i);
	});
});

describe("sb.job.handle — duplicate guard", () => {
	test("rejects duplicate name", () => {
		const { domain } = newDomain();
		domain.handle(
			"dup",
			{ version: "test-v1", trigger: { interval: 1000 } },
			noop,
		);
		expect(() =>
			domain.handle(
				"dup",
				{ version: "test-v1", trigger: { interval: 1000 } },
				noop,
			),
		).toThrow(/duplicate/i);
	});
});

describe("sb.job.handle — trigger oneof validation", () => {
	test("rejects both cron and delayed", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{
					version: "test-v1",
					trigger: {
						cron: "0 * * * *",
						delayed: { at: new Date() },
					} as Trigger,
				},
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
					version: "test-v1",
					trigger: {
						cron: "0 * * * *",
						delayed: { at: new Date() },
						interval: 1000,
					} as Trigger,
				},
				noop,
			),
		).toThrow(/exactly one/i);
	});

	test("rejects zero triggers", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle("j", { version: "test-v1", trigger: {} as Trigger }, noop),
		).toThrow(/exactly one/i);
	});
});

describe("sb.job.handle — delayed validation", () => {
	test("rejects invalid date string", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { delayed: { at: "not a date" } } },
				noop,
			),
		).toThrow(/delayed/i);
	});

	test("accepts Date", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { delayed: { at: new Date() } } },
				noop,
			),
		).not.toThrow();
	});

	test("accepts number", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { delayed: { at: Date.now() } } },
				noop,
			),
		).not.toThrow();
	});
});

describe("sb.job.handle — interval validation", () => {
	test("rejects 0", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { interval: 0 } },
				noop,
			),
		).toThrow(/interval/i);
	});

	test("rejects negative", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { interval: -100 } },
				noop,
			),
		).toThrow(/interval/i);
	});

	test("accepts positive", () => {
		const { domain } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ version: "test-v1", trigger: { interval: 100 } },
				noop,
			),
		).not.toThrow();
	});
});

describe("sb.job.handle — registry side effects (IncomingMethod{type=JOB})", () => {
	test("appends one IncomingMethod with canonical-spec JSON + contract hash", () => {
		const { domain, registry } = newDomain();
		domain.handle(
			"daily",
			{
				version: "test-v1",
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
			version: "test-v1";
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

	test("retry block uses the snake_case keys the runtime decodes", () => {
		const { domain, registry } = newDomain();
		domain.handle(
			"flaky",
			{
				version: "test-v1",
				trigger: { interval: 60_000 },
				retry: {
					initialMs: 2_000,
					maxMs: 120_000,
					multiplier: 3,
					jitter: 0.4,
				},
			},
			noop,
		);

		const m = registry._handle.incomingMethods()[0];
		expect(m).toBeDefined();
		if (!m) return;
		const spec = JSON.parse(
			new TextDecoder().decode(m.inputSchemaJson),
		) as Record<string, unknown>;

		// The retry block is snake_case while the document around it is camelCase:
		// it decodes into jobs.RetryPolicy, whose tags come from database columns.
		// Sending initialMs/maxMs leaves both zero, and a zero initial reads as
		// "no policy given" — the runtime then substitutes its own default and the
		// declared policy is discarded without a word.
		expect(spec.retry).toEqual({
			initial_ms: 2_000,
			max_ms: 120_000,
			multiplier: 3,
			jitter: 0.4,
		});
	});

	test("delayed trigger serialises as runAtUnixMs", () => {
		const { domain, registry } = newDomain();
		const at = new Date("2026-06-01T12:00:00.000Z");
		domain.handle(
			"once",
			{ version: "test-v1", trigger: { delayed: { at } } },
			noop,
		);
		const m = registry._handle.incomingMethods()[0];
		if (!m) throw new Error("missing entry");
		const spec = JSON.parse(new TextDecoder().decode(m.inputSchemaJson));
		expect(spec.trigger.delayed.runAtUnixMs).toBe(at.getTime());
	});

	test("interval trigger serialises as everyMs", () => {
		const { domain, registry } = newDomain();
		domain.handle(
			"poll",
			{ version: "test-v1", trigger: { interval: 5000 } },
			noop,
		);
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
				version: "test-v1",
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
			{ version: "test-v1", trigger: { interval: 1000 }, maxConcurrent: 7 },
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

describe("immutable job executable versions", () => {
	test("requires a version and retains exact old executable fingerprints", () => {
		const { domain, registry } = newDomain();
		expect(() =>
			domain.handle(
				"j",
				{ trigger: { interval: 1000 } } as unknown as JobOpts,
				noop,
			),
		).toThrow(/version/);
		const old = async () => {};
		domain.handle("j", { version: "v1", trigger: { interval: 1000 } }, old);
		const fingerprint = registry._handle.incomingMethods()[0]!.contractHash;
		domain.handle("j", { version: "v2", trigger: { interval: 1000 } }, noop);
		expect(domain.lookup("j", fingerprint)?.fn).toBe(old);
		expect(domain.lookup("j", "unknown")).toBeUndefined();
		expect(registry._handle.incomingMethods()).toHaveLength(2);
	});
});

test("job version canonical bytes match cross-SDK golden fingerprint", () => {
	const { domain, registry } = newDomain();
	domain.handle("golden", { version: "v1", trigger: { interval: 1000 } }, noop);
	const entry = registry._handle.incomingMethods()[0]!;
	expect(Buffer.from(entry.inputSchemaJson).toString()).toBe(
		'{"version":"v1","trigger":{"interval":{"everyMs":1000}}}',
	);
	expect(entry.contractHash).toBe(
		"1be70dfb3805071572dd48e43245ec178865dc98d254ac6840ff39a8f496b8c2",
	);
});

test("job concurrency rejects unbounded and invalid limits before registration", () => {
	const { domain, registry } = newDomain();
	for (const maxConcurrent of [-1, 0.5, 1025, Infinity, NaN]) {
		expect(() =>
			domain.handle(
				"bad",
				{ version: "v1", trigger: { interval: 1000 }, maxConcurrent },
				noop,
			),
		).toThrow(/maxConcurrent/);
	}
	expect(registry._handle.incomingMethods()).toHaveLength(0);
	domain.handle(
		"default",
		{ version: "v1", trigger: { interval: 1000 }, maxConcurrent: 0 },
		noop,
	);
});

// sdk/job-canonical-vectors.json is shared with the Go SDK (go/job): both must
// produce the same canonical bytes and contract hash for every input.
describe("canonical job spec — cross-SDK vectors", () => {
	const { vectors } = JSON.parse(
		readFileSync(
			new URL("../../../job-canonical-vectors.json", import.meta.url),
			"utf8",
		),
	) as {
		vectors: {
			name: string;
			input: JobOpts;
			canonical: string;
			sha256: string;
		}[];
	};
	for (const v of vectors) {
		test(v.name, () => {
			const { domain, registry } = newDomain();
			domain.handle("vector", v.input, noop);
			const entry = registry._handle.incomingMethods()[0]!;
			expect(Buffer.from(entry.inputSchemaJson).toString()).toBe(v.canonical);
			expect(entry.contractHash).toBe(v.sha256);
		});
	}
});

// sdk/cron-vectors.json is shared with the Go SDK, which validates with the
// runtime's own parser: both must accept and reject the same expressions.
describe("cron grammar — cross-SDK vectors", () => {
	const { vectors } = JSON.parse(
		readFileSync(
			new URL("../../../cron-vectors.json", import.meta.url),
			"utf8",
		),
	) as { vectors: { expr: string; valid: boolean }[] };
	for (const v of vectors) {
		test(`${JSON.stringify(v.expr)} is ${v.valid ? "valid" : "invalid"}`, () => {
			expect(cronError(v.expr) === null).toBe(v.valid);
		});
	}
});

test("an unknown cron time zone is rejected at declaration", () => {
	const { domain } = newDomain();
	expect(() =>
		domain.handle(
			"tz",
			{ version: "v1", trigger: { cron: "0 3 * * *", tz: "Mars/Olympus" } },
			noop,
		),
	).toThrow(/time zone/);
});
