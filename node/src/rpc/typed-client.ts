import protobuf from "protobufjs";
import type { CallOpts } from "./client";

// TypedClient is the dynamic proxy returned by `sb.client(svc, protoFile)`.
// Methods listed in the .proto's `service` block become callable directly:
//
//   const payment = await sb.client("payment-svc", "./payment.proto");
//   const res = await payment.Charge({ userId: "u-1", amount: 100 });
//   for await (const tok of payment.Generate({ prompt: "..." })) { ... }
//
// Unary methods return Promise<Res>; streaming methods (responseStream=true)
// return AsyncIterable<Chunk>. CallOpts can be passed as a second argument to
// any method.
//
// @public — см. ./README.md
export type TypedClient = Record<
	string,
	((req: unknown, opts?: CallOpts) => Promise<unknown>) &
		((req: unknown, opts?: CallOpts) => AsyncIterable<unknown>)
>;

// MethodSpec describes one method extracted from a .proto service block.
// Used internally by ServiceBridge.client() to wire up the typed proxy.
export interface MethodSpec {
	name: string;
	requestType: string;
	responseType: string;
	responseStream: boolean;
}

// extractServiceMethods loads a .proto file and returns the list of methods
// defined in ANY service block. Throws if the file has no service.
//
// @internal — см. ./README.md
export async function extractServiceMethods(
	protoFile: string,
): Promise<MethodSpec[]> {
	let root: protobuf.Root;
	try {
		root = await protobuf.load(protoFile);
	} catch (err) {
		throw new Error(
			`rpc: client(${protoFile}): load proto: ${(err as Error).message}`,
		);
	}

	const methods: MethodSpec[] = [];
	visit(root, (obj) => {
		if (!(obj instanceof protobuf.Service)) return;
		for (const m of Object.values(obj.methods)) {
			methods.push({
				name: m.name,
				requestType: m.requestType,
				responseType: m.responseType,
				responseStream: m.responseStream === true,
			});
		}
	});

	if (methods.length === 0) {
		throw new Error(
			`rpc: client(${protoFile}): no service block found — typed client requires gRPC-native service definition`,
		);
	}
	return methods;
}

function visit(
	obj: protobuf.NamespaceBase,
	fn: (item: protobuf.ReflectionObject) => void,
): void {
	fn(obj);
	const nested = obj.nested;
	if (!nested) return;
	for (const child of Object.values(nested)) {
		if (child instanceof protobuf.Namespace) {
			visit(child, fn);
		} else {
			fn(child);
		}
	}
}
