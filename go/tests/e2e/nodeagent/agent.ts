// agent.ts — the Node SDK side of the cross-language e2e tests.
//
// The Go suite spawns this with `bun run` and drives it over stdio: one JSON
// object per line in each direction. It exists because a contract that both
// SDKs merely agree with in their own tests is not a contract — only a live
// Node instance registered against the same runtime can prove that a Go caller
// reaches a Node handler, that the trace stays one tree across the boundary and
// that both sides derive the same contract hash from the same .proto.
//
// The SDK is imported by absolute path from SB_NODE_SDK_SRC. This file lives
// outside the Node SDK package, so a bare specifier here would resolve against
// a node_modules directory that does not exist above it; the SDK's own bare
// imports resolve fine because they are made from files inside that package.

interface AgentConfig {
	url: string;
	key: string;
	dataDir: string;
	protoFile: string;
	// rpcMethod is the handler this agent serves. Empty registers none.
	rpcMethod: string;
	// subOpSubject is opened as a USER operation inside the rpc handler, so the
	// Go side can find the callee's own operation and check whose child it is.
	subOpSubject: string;
	// callSubOpSubject is opened around an outgoing call, giving the Go handler
	// a named parent to be nested under.
	callSubOpSubject: string;
	deps: { service: string; methods: string[] }[];
	// subscribeEvents are consumed; publishEvents are declared for publishing.
	subscribeEvents: string[];
	publishEvents: string[];
}

const parsed: AgentConfig = JSON.parse(process.env.SB_AGENT_CONFIG ?? "");
// Go encodes an empty slice as null, so every list is defaulted here rather
// than at each use.
const config: AgentConfig = {
	...parsed,
	deps: parsed.deps ?? [],
	subscribeEvents: parsed.subscribeEvents ?? [],
	publishEvents: parsed.publishEvents ?? [],
};
const sdkSrc = process.env.SB_NODE_SDK_SRC ?? "";
if (!sdkSrc) throw new Error("agent: SB_NODE_SDK_SRC is not set");

const { ServiceBridge } = await import(`${sdkSrc}/connection/service-bridge.ts`);
const { Channel, Status, UserSubOp } = await import(
	`${sdkSrc}/telemetry/ops.ts`
);
const { runWithTrace } = await import(`${sdkSrc}/telemetry/context.ts`);
const { buildSchemaPair } = await import(`${sdkSrc}/serde/serializer.ts`);
const { computeContractHash } = await import(
	`${sdkSrc}/serde/contract-hash.ts`
);

const RPC_SCHEMA = {
	protoFile: config.protoFile,
	input: "Echo",
	output: "EchoReply",
};
// An event's contract hash pairs the payload with an empty reply, mirroring
// what the Go SDK derives from google.protobuf.Empty.
const EVENT_SCHEMA = {
	protoFile: config.protoFile,
	input: "OrderEvent",
	output: "Nothing",
};

function emit(msg: Record<string, unknown>): void {
	process.stdout.write(`${JSON.stringify(msg)}\n`);
}

// protobufjs decodes an int64 into a Long object, so a field the Go SDK hands
// its handlers as a plain integer arrives here as {low, high, unsigned}.
// Echoing it back unconverted encodes correctly but is useless to arithmetic,
// so the agent normalises at the boundary the way application code has to.
function toNum(value: unknown): number {
	if (typeof value === "number") return value;
	if (typeof value === "string") return Number(value);
	if (typeof value === "bigint") return Number(value);
	const long = value as { toNumber?: () => number } | null;
	if (long && typeof long.toNumber === "function") return long.toNumber();
	return 0;
}

const sb = new ServiceBridge(config.url, config.key, {
	reconnectIntervalMs: 500,
	reconnectAttempts: 3,
	certRefreshLeadMs: 60 * 60 * 1000,
	advertise: { host: "127.0.0.1", port: 0 },
	dataDir: config.dataDir,
});

if (config.rpcMethod) {
	sb.rpc.handle(
		config.rpcMethod,
		(req: { text?: string; n?: number }) => {
			// Opened inside the handler so it inherits the caller's trace from the
			// ALS context the SDK's call server established from X-SB-Trace. Its row
			// is the only proof the two languages ended up on one trace: the callee
			// emits no operation of its own for an RPC (ADR-0001).
			const op = sb.telemetry.startOp({
				channel: Channel.USER,
				kind: UserSubOp,
				subject: config.subOpSubject,
			});
			op.end(Status.SUCCESS);
			const n = toNum(req.n);
			emit({ type: "rpc", method: config.rpcMethod, req: { text: req.text ?? "", n } });
			return { text: req.text ?? "", n, handledBy: "node" };
		},
		{ schema: RPC_SCHEMA },
	);
}

for (const dep of config.deps) {
	sb.service(dep.service, { rpc: dep.methods });
}

for (const name of config.subscribeEvents) {
	sb.event.define(name, EVENT_SCHEMA);
	sb.event.handle(name, (payload: unknown) => {
		emit({ type: "event", name, payload });
	});
}

for (const name of config.publishEvents) {
	if (config.subscribeEvents.includes(name)) continue;
	sb.event.define(name, EVENT_SCHEMA);
}

await new Promise<void>((resolve, reject) => {
	const timer = setTimeout(() => reject(new Error("agent: connect timed out")), 20_000);
	sb.on("connected", () => {
		clearTimeout(timer);
		resolve();
	});
	sb.on("disconnected", (evt: { error?: Error; reason?: string }) => {
		clearTimeout(timer);
		reject(evt.error ?? new Error(evt.reason ?? "disconnected"));
	});
	sb.start().catch((err: unknown) => {
		clearTimeout(timer);
		reject(err instanceof Error ? err : new Error(String(err)));
	});
});

// Schemas for outgoing calls are registered after connect so a failure to load
// one is reported on a live client rather than swallowed by the start path.
for (const dep of config.deps) {
	for (const method of dep.methods) {
		await sb.useSchema(dep.service, method, RPC_SCHEMA);
	}
}

const identity = sb.identity();
emit({
	type: "ready",
	serviceName: identity.serviceName,
	serviceId: identity.serviceId,
	instanceId: identity.instanceId,
	// Both hashes travel back so the Go side can compare them against what it
	// derives from the same .proto. A mismatch is the failure that would
	// otherwise show up only as a call that finds no candidate.
	rpcContractHash: computeContractHash(await buildSchemaPair(RPC_SCHEMA)),
	eventContractHash: computeContractHash(await buildSchemaPair(EVENT_SCHEMA)),
});

// awaitMethod blocks until the runtime's snapshot reaches this instance. A
// registration made by the other side is not instantly visible here, and a call
// issued before it lands has no descriptor to route with — which reads like a
// contract failure but is only impatience.
async function awaitMethod(service: string, method: string): Promise<void> {
	const deadline = Date.now() + 15_000;
	while (Date.now() < deadline) {
		const entry = sb.serviceMap().get(service);
		if (entry?.methods.some((m: { name: string }) => m.name === method)) return;
		await new Promise((resolve) => setTimeout(resolve, 50));
	}
	throw new Error(`agent: ${service}/${method} never reached the service map`);
}

interface Command {
	id: number;
	cmd: "call" | "publish" | "awaitMethod" | "stop";
	service?: string;
	method?: string;
	name?: string;
	payload?: Record<string, unknown>;
	partitionKey?: string;
}

async function run(cmd: Command): Promise<unknown> {
	switch (cmd.cmd) {
		case "awaitMethod":
			await awaitMethod(cmd.service as string, cmd.method as string);
			return {};
		case "call": {
			// Wrapped in a user operation so the Go callee's own operation has a
			// named parent on this side of the boundary.
			const op = sb.telemetry.startOp({
				channel: Channel.USER,
				kind: UserSubOp,
				subject: config.callSubOpSubject,
			});
			try {
				// The Node SDK propagates trace context through AsyncLocalStorage,
				// and startOp does not enter a scope of its own. Without this
				// wrapper the call below mints a fresh root trace and the request
				// becomes two unrelated trees — the exact failure these tests exist
				// to catch, so the agent has to model correct application usage.
				const res = await runWithTrace(
					{ traceId: op.traceId, parentOpId: op.opId },
					() =>
						sb.rpc.call(cmd.service, cmd.method, cmd.payload ?? {}, {
							transport: "proxy",
						}),
				);
				op.end(Status.SUCCESS);
				return res;
			} catch (err) {
				op.end(Status.ERROR, err instanceof Error ? err.message : String(err));
				throw err;
			}
		}
		case "publish":
			return await sb.event.publish(
				cmd.name,
				cmd.payload ?? {},
				cmd.partitionKey ? { partitionKey: cmd.partitionKey } : undefined,
			);
		case "stop":
			await sb.stop();
			return {};
	}
}

for await (const line of console) {
	const text = line.trim();
	if (!text) continue;
	let cmd: Command;
	try {
		cmd = JSON.parse(text) as Command;
	} catch (err) {
		emit({ type: "fatal", error: `unparsable command ${text}: ${String(err)}` });
		continue;
	}
	try {
		const value = await run(cmd);
		emit({ type: "result", id: cmd.id, ok: true, value });
	} catch (err) {
		emit({
			type: "result",
			id: cmd.id,
			ok: false,
			error: err instanceof Error ? err.message : String(err),
		});
	}
	if (cmd.cmd === "stop") break;
}
