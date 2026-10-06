import { describe, expect, it } from "bun:test";
import {
	AccessDeniedError,
	type ServiceBridgeError,
	StateError,
	TimeoutError,
	ValidationError,
} from "../errors";
import type { Logger } from "../logger";
import { silentLogger } from "../logger";
import type {
	EventEnvelope,
	EventsClient,
	PublishRequest,
	PublishResponse,
} from "../pb/servicebridge/v1/events";
import { PublishStatus } from "../pb/servicebridge/v1/events";
import type { SchemaPair } from "../serde/serializer";
import { InvalidEventNameError } from "./errors";
import { Publisher, type PublisherDeps } from "./publisher";

type Answer = (req: PublishRequest) => PublishResponse | Error | "hang";

const pair = {
	input: { encode: (v: unknown) => Buffer.from(JSON.stringify(v)) },
} as unknown as SchemaPair;

function client(answer: Answer, requests: PublishRequest[]): EventsClient {
	return {
		publish: (
			req: PublishRequest,
			_md: unknown,
			_opts: unknown,
			cb: (err: Error | null, res?: PublishResponse) => void,
		) => {
			requests.push(req);
			const out = answer(req);
			if (out === "hang") return;
			setTimeout(() => (out instanceof Error ? cb(out) : cb(null, out)), 0);
		},
	} as unknown as EventsClient;
}

const accept: Answer = (req) => ({
	results: req.events.map((e) => ({
		eventId: e.id,
		status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
		message: "",
	})),
});

function statusFor(
	status: PublishStatus,
	message = "",
	eventId?: string,
): Answer {
	return (req) => ({
		results: req.events.map((e) => ({
			eventId: eventId ?? e.id,
			status,
			message,
		})),
	});
}

function make(
	answer: Answer | null,
	over: Partial<PublisherDeps> = {},
): {
	pub: Publisher;
	requests: PublishRequest[];
	violations: unknown[];
	warns: string[];
} {
	const requests: PublishRequest[] = [];
	const violations: unknown[] = [];
	const warns: string[] = [];
	const logger: Logger = { ...silentLogger, warn: (m) => warns.push(m) };
	const c = answer ? client(answer, requests) : null;
	const pub = new Publisher({
		client: () => c,
		schemaIndex: {
			get: (n) =>
				n.startsWith("order.") ? { contractHash: "h1", pair } : undefined,
		},
		logger,
		timeoutMs: 2_000,
		maxPending: 100,
		xSbTraceFn: () => "trace-header",
		onPolicyViolation: (v) => violations.push(v),
		...over,
	});
	return { pub, requests, violations, warns };
}

const envelopes = (requests: PublishRequest[]): EventEnvelope[] =>
	requests.flatMap((r) => r.events);

describe("Publisher acknowledgement", () => {
	it("resolves after ACCEPTED with the event id and a complete envelope", async () => {
		const { pub, requests } = make(accept);
		const { eventId } = await pub.publish(
			"order.created",
			{ id: 1 },
			{
				partitionKey: "o-1",
				headers: { a: "b" },
				idempotencyKey: "k",
				occurredAtMs: 5,
			},
		);
		const env = envelopes(requests)[0] as EventEnvelope;
		expect(eventId).toBe(env.id);
		expect(env.id).toMatch(/^[0-9a-f-]{36}$/);
		expect(env).toMatchObject({
			name: "order.created",
			contractHash: "h1",
			partitionKey: "o-1",
			idempotencyKey: "k",
			headers: { a: "b" },
			occurredAtUnixMs: 5,
			xSbTrace: "trace-header",
		});
		expect(Buffer.from(env.payloadJson).toString()).toBe('{"id":1}');
	});

	it("REJECTED_DUPLICATE is a success carrying the original event id", async () => {
		const { pub } = make(
			statusFor(
				PublishStatus.PUBLISH_STATUS_REJECTED_DUPLICATE,
				"",
				"original-id",
			),
		);
		await expect(pub.publish("order.created", {})).resolves.toEqual({
			eventId: "original-id",
		});
	});

	it("REJECTED_CONFLICT is a CONFLICT error", async () => {
		const { pub } = make(
			statusFor(
				PublishStatus.PUBLISH_STATUS_REJECTED_CONFLICT,
				"different payload",
			),
		);
		const err = await pub.publish("order.created", {}).catch((e) => e);
		expect((err as ServiceBridgeError).code).toBe("CONFLICT");
		expect((err as Error).message).toContain("different payload");
	});

	it("REJECTED_INVALID_NAME is an InvalidEventNameError", async () => {
		const { pub } = make(
			statusFor(PublishStatus.PUBLISH_STATUS_REJECTED_INVALID_NAME),
		);
		await expect(pub.publish("order.created", {})).rejects.toBeInstanceOf(
			InvalidEventNameError,
		);
	});

	it("REJECTED_FORBIDDEN is an AccessDeniedError and a policy violation", async () => {
		const { pub, violations } = make(
			statusFor(PublishStatus.PUBLISH_STATUS_REJECTED_FORBIDDEN, "no rule"),
		);
		await expect(pub.publish("order.created", {})).rejects.toBeInstanceOf(
			AccessDeniedError,
		);
		expect(violations).toEqual([
			{
				declaration: "event.publish",
				value: "order.created",
				denySide: "self_egress",
				reason: "no rule",
			},
		]);
	});

	it("retries the SAME envelope after a transport error and after UNSPECIFIED", async () => {
		let n = 0;
		const { pub, requests } = make((req) => {
			n++;
			if (n === 1) return new Error("unavailable");
			if (n === 2)
				return statusFor(
					PublishStatus.PUBLISH_STATUS_UNSPECIFIED,
					"rate limit",
				)(req);
			return accept(req);
		});
		const { eventId } = await pub.publish("order.created", {});
		const ids = envelopes(requests).map((e) => e.id);
		expect(ids).toEqual([eventId, eventId, eventId]);
	});

	it("waits for a channel and sends on kick()", async () => {
		let ready = false;
		const requests: PublishRequest[] = [];
		const c = client(accept, requests);
		const { pub } = make(null, { client: () => (ready ? c : null) });
		const pending = pub.publish("order.created", {});
		await new Promise((r) => setTimeout(r, 20));
		expect(requests).toHaveLength(0);
		ready = true;
		pub.kick();
		await pending;
		expect(requests).toHaveLength(1);
	});
});

describe("Publisher bounds", () => {
	it("an event the runtime never received times out as 'not sent'", async () => {
		const { pub } = make(null, { timeoutMs: 30 });
		const err = await pub.publish("order.created", {}).catch((e) => e);
		expect(err).toBeInstanceOf(TimeoutError);
		expect((err as Error).message).toContain("not sent");
	});

	it("an event sent but never acknowledged times out as 'outcome unknown'", async () => {
		const { pub } = make(() => new Error("reset"), { timeoutMs: 60 });
		const err = await pub.publish("order.created", {}).catch((e) => e);
		expect(err).toBeInstanceOf(TimeoutError);
		expect((err as Error).message).toContain("outcome unknown");
	});

	it("a full queue fails publish with QUEUE_FULL", async () => {
		const { pub } = make(() => "hang", { maxPending: 2 });
		void pub.publish("order.created", {}).catch(() => {});
		void pub.publish("order.created", {}).catch(() => {});
		const err = await pub.publish("order.created", {}).catch((e) => e);
		expect((err as ServiceBridgeError).code).toBe("QUEUE_FULL");
		expect((err as ServiceBridgeError).retryable).toBe(true);
	});

	it("rejects an invalid name and an undeclared event locally", async () => {
		const { pub, requests } = make(accept);
		await expect(pub.publish("Bad Name", {})).rejects.toBeInstanceOf(
			InvalidEventNameError,
		);
		await expect(pub.publish("invoice.created", {})).rejects.toBeInstanceOf(
			StateError,
		);
		expect(requests).toHaveLength(0);
	});
});

describe("Publisher ordering", () => {
	it("puts at most one event per partition key in a request, keeping key order", async () => {
		const { pub, requests } = make(accept);
		const all = Promise.all([
			pub.publish("order.created", { n: 1 }, { partitionKey: "a" }),
			pub.publish("order.created", { n: 2 }, { partitionKey: "a" }),
			pub.publish("order.created", { n: 3 }, { partitionKey: "b" }),
			pub.publish("order.created", { n: 4 }),
			pub.publish("order.created", { n: 5 }),
		]);
		await all;
		const batches = requests.map((r) =>
			r.events.map((e) => JSON.parse(Buffer.from(e.payloadJson).toString()).n),
		);
		expect(batches[0]).toEqual([1, 3, 4, 5]);
		expect(batches[1]).toEqual([2]);
	});

	it("a transient failure of one key never lets a later event of that key overtake it", async () => {
		let first = true;
		const { pub, requests } = make((req) => {
			if (first) {
				first = false;
				return {
					results: req.events.map((e) => ({
						eventId: e.id,
						status: PublishStatus.PUBLISH_STATUS_UNSPECIFIED,
						message: "",
					})),
				};
			}
			return accept(req);
		});
		await Promise.all([
			pub.publish("order.created", { n: 1 }, { partitionKey: "a" }),
			pub.publish("order.created", { n: 2 }, { partitionKey: "a" }),
		]);
		const order = requests.flatMap((r) =>
			r.events.map((e) => JSON.parse(Buffer.from(e.payloadJson).toString()).n),
		);
		expect(order).toEqual([1, 1, 2]);
	});
});

describe("Publisher fire-and-forget and close", () => {
	it("fireAndForget resolves at once and only logs a terminal failure", async () => {
		const { pub, warns } = make(
			statusFor(PublishStatus.PUBLISH_STATUS_REJECTED_CONFLICT),
		);
		const { eventId } = await pub.publish(
			"order.created",
			{},
			{ fireAndForget: true },
		);
		expect(eventId).toMatch(/^[0-9a-f-]{36}$/);
		await new Promise((r) => setTimeout(r, 20));
		expect(warns.some((w) => w.includes("fire-and-forget publish lost"))).toBe(
			true,
		);
	});

	it("close() sends what it can, then rejects the rest with CONNECTION", async () => {
		const { pub } = make(() => "hang");
		const pending = pub.publish("order.created", {}).catch((e) => e);
		await pub.close(30);
		const err = await pending;
		expect((err as ServiceBridgeError).code).toBe("CONNECTION");
		await expect(pub.publish("order.created", {})).rejects.toBeInstanceOf(
			StateError,
		);
	});

	it("close() resolves early once the queue drained", async () => {
		const { pub } = make(accept);
		const sent = pub.publish("order.created", {});
		const started = Date.now();
		await pub.close(5_000);
		await sent;
		expect(Date.now() - started).toBeLessThan(1_000);
	});
});

describe("Publisher schema validation", () => {
	it("a payload the schema rejects is a ValidationError rejection, nothing is sent", async () => {
		const { pub, requests } = make(accept);
		const strict = new Publisher({
			client: () => null,
			schemaIndex: {
				get: () => ({
					contractHash: "h",
					pair: {
						input: {
							encode: () => {
								throw new Error("orderId: string expected");
							},
						},
					} as unknown as SchemaPair,
				}),
			},
			logger: silentLogger,
			timeoutMs: 1000,
			maxPending: 10,
			xSbTraceFn: () => "",
			onPolicyViolation: () => {},
		});
		const pending = strict.publish("order.created", { orderId: 1 });
		await expect(pending).rejects.toBeInstanceOf(ValidationError);
		void pub;
		expect(requests).toHaveLength(0);
	});
});

describe("Publisher sequencing", () => {
	it("a publish issued right after the previous one resolved is sent too", async () => {
		const { pub, requests } = make(accept);
		for (let i = 0; i < 5; i++) await pub.publish("order.created", { n: i });
		expect(requests).toHaveLength(5);
	});
});
