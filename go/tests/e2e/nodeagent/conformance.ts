// conformance.ts — the Node SDK side of the conformance scenarios
// (sdk/conformance). The Go runner reads the YAML, decides which SDK plays
// each role and drives this agent over stdio, one JSON object per line. The
// Go side of a role is go/tests/e2e/conformance_agent_test.go; both implement
// the same behaviours and report outcomes in the same shape, so a scenario's
// expectations hold for every pairing or name the SDK that breaks them.
//
// The SDK is imported by absolute path from SB_NODE_SDK_SRC (see agent.ts).

interface ConfConfig {
	url: string;
	key: string;
	protoFile: string;
	rpc: { method: string; behaviour: Behaviour }[];
	subscriptions: { pattern: string; filter?: Record<string, unknown> }[];
	deps: { service: string; methods: string[] }[];
	events: string[];
}

interface Behaviour {
	echo?: boolean;
	info?: boolean;
	sleepMs?: number;
	fail?: { code: string; message: string };
	throw?: string;
}

interface Command {
	id: number;
	cmd: "call" | "publish" | "awaitMethod" | "stop";
	service?: string;
	method?: string;
	payload?: Record<string, unknown>;
	timeoutMs?: number;
	transport?: "auto" | "direct" | "proxy";
	idempotencyKey?: string;
	name?: string;
	partitionKey?: string;
}

const parsed: ConfConfig = JSON.parse(process.env.SB_AGENT_CONFIG ?? "");
const config: ConfConfig = {
	...parsed,
	rpc: parsed.rpc ?? [],
	subscriptions: parsed.subscriptions ?? [],
	deps: parsed.deps ?? [],
	events: parsed.events ?? [],
};
const sdkSrc = process.env.SB_NODE_SDK_SRC ?? "";
if (!sdkSrc) throw new Error("conformance agent: SB_NODE_SDK_SRC is not set");

const { ServiceBridge } = await import(`${sdkSrc}/connection/service-bridge.ts`);
const { HandlerError, ServiceBridgeError } = await import(`${sdkSrc}/errors.ts`);

const RPC_SCHEMA = { protoFile: config.protoFile, input: "Echo", output: "EchoReply" };
const EVENT_SCHEMA = { protoFile: config.protoFile, input: "OrderEvent", output: "Nothing" };

function emit(msg: Record<string, unknown>): void {
	process.stdout.write(`${JSON.stringify(msg)}\n`);
}

function num(value: unknown): number {
	if (typeof value === "number") return value;
	const long = value as { toNumber?: () => number } | null;
	if (long && typeof long.toNumber === "function") return long.toNumber();
	return Number(value ?? 0);
}

interface HandlerCtx {
	deadline: number | null;
	idempotencyKey: string;
	caller: { serviceId: string } | null;
}

async function serve(
	method: string,
	b: Behaviour,
	req: { text?: string; n?: unknown },
	ctx: HandlerCtx,
): Promise<Record<string, unknown>> {
	emit({ type: "served", method });
	if (b.fail) throw new HandlerError(b.fail.code, b.fail.message);
	if (b.throw !== undefined) throw new Error(b.throw);
	if (b.info) {
		return {
			text: ctx.caller?.serviceId ?? "",
			n: ctx.deadline === null ? 0 : ctx.deadline - Date.now(),
			handledBy: ctx.idempotencyKey,
		};
	}
	if (b.sleepMs) await new Promise((r) => setTimeout(r, b.sleepMs));
	return { text: req.text ?? "", n: num(req.n), handledBy: "node" };
}

// outcome is the shape both agents report a call or publish in.
function outcome(err: unknown): Record<string, unknown> {
	if (err instanceof HandlerError) {
		return {
			error: {
				code: err.code,
				handlerCode: err.handlerCode,
				handlerMessage: err.message,
				retryable: err.retryable,
			},
		};
	}
	if (err instanceof ServiceBridgeError) {
		return { error: { code: err.code, retryable: err.retryable, message: err.message } };
	}
	return { error: { code: "NOT_AN_SDK_ERROR", message: String(err) } };
}

const sb = new ServiceBridge(config.url, config.key, {
	reconnectAttempts: 3,
	advertise: { host: "127.0.0.1", port: 0 },
});

for (const h of config.rpc) {
	sb.rpc.handle(
		h.method,
		(req: { text?: string; n?: unknown }, ctx: HandlerCtx) => serve(h.method, h.behaviour, req, ctx),
		{ schema: RPC_SCHEMA },
	);
}
for (const s of config.subscriptions) {
	sb.event.handle(
		s.pattern,
		(payload: { orderId?: string; amount?: number; currency?: string }, ctx: { eventName: string }) => {
			emit({
				type: "delivery",
				pattern: s.pattern,
				name: ctx.eventName,
				payload: {
					orderId: payload.orderId ?? "",
					amount: payload.amount ?? 0,
					currency: payload.currency ?? "",
				},
			});
		},
		{ schema: EVENT_SCHEMA, ...(s.filter ? { filter: s.filter } : {}) },
	);
}
for (const dep of config.deps) sb.service(dep.service, { rpc: dep.methods });
for (const name of config.events) sb.event.define(name, EVENT_SCHEMA);

try {
	await sb.start();
} catch (err) {
	emit({ type: "fatal", error: err instanceof Error ? err.message : String(err) });
	process.exit(1);
}
for (const dep of config.deps) {
	for (const method of dep.methods) await sb.useSchema(dep.service, method, RPC_SCHEMA);
}

const identity = sb.identity();
emit({
	type: "ready",
	serviceName: identity.serviceName,
	serviceId: identity.serviceId,
	instanceId: identity.instanceId,
});

async function awaitMethod(service: string, method: string): Promise<void> {
	const deadline = Date.now() + 15_000;
	while (Date.now() < deadline) {
		const entry = sb.serviceMap().get(service);
		if (entry?.methods.some((m: { name: string }) => m.name === method)) return;
		await new Promise((r) => setTimeout(r, 50));
	}
	throw new Error(`${service}/${method} never reached the service map`);
}

async function run(cmd: Command): Promise<unknown> {
	switch (cmd.cmd) {
		case "awaitMethod":
			await awaitMethod(cmd.service as string, cmd.method as string);
			return {};
		case "call":
			try {
				const reply = await sb.rpc.call(cmd.service, cmd.method, cmd.payload ?? {}, {
					...(cmd.timeoutMs ? { timeout: `${cmd.timeoutMs}ms` } : {}),
					...(cmd.transport ? { transport: cmd.transport } : {}),
					...(cmd.idempotencyKey ? { idempotencyKey: cmd.idempotencyKey } : {}),
				});
				return { reply: { text: reply.text ?? "", n: num(reply.n), handledBy: reply.handledBy ?? "" } };
			} catch (err) {
				return outcome(err);
			}
		case "publish":
			try {
				const { eventId } = await sb.event.publish(
					cmd.name,
					cmd.payload ?? {},
					cmd.partitionKey ? { partitionKey: cmd.partitionKey } : undefined,
				);
				return { eventId };
			} catch (err) {
				return outcome(err);
			}
		case "stop":
			await sb.stop();
			return {};
	}
}

for await (const line of console) {
	const text = line.trim();
	if (!text) continue;
	const cmd = JSON.parse(text) as Command;
	try {
		emit({ type: "result", id: cmd.id, ok: true, value: await run(cmd) });
	} catch (err) {
		emit({ type: "result", id: cmd.id, ok: false, error: err instanceof Error ? err.message : String(err) });
	}
	if (cmd.cmd === "stop") break;
}
