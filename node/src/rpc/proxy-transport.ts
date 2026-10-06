import type { ChannelCredentials, ClientUnaryCall } from "@grpc/grpc-js";
import { CLIENT_CHANNEL_OPTIONS } from "../connection/tls-material";
import { type InvokeChunk, InvokeClient } from "../pb/servicebridge/v1/invoke";
import type { WireCall } from "./direct-transport";
import {
	asBuffer,
	CallFailure,
	currentTraceHeader,
	grpcFailure,
	handlerFailure,
	traceMetadata,
} from "./wire";

// ProxyTransport routes outbound RPC calls through the runtime's Invoke service
// over the SDK's own mTLS channel to the runtime. The caller-side contract hash
// is forwarded so the runtime resolver filters target instances by it
// (ADR 0001).
//
// @internal — см. ./README.md
export class ProxyTransport {
	private readonly client: InvokeClient;

	constructor(runtimeAddr: string, creds: ChannelCredentials) {
		this.client = new InvokeClient(runtimeAddr, creds, CLIENT_CHANNEL_OPTIONS);
	}

	close(): void {
		this.client.close();
	}

	async *callStream(
		targetServiceId: string,
		contractHash: Buffer,
		call: WireCall,
	): AsyncIterable<Uint8Array> {
		call.signal?.throwIfAborted();
		await this.ready(call);
		const traceHeader = currentTraceHeader();
		const stream = this.client.stream(
			{
				targetServiceId,
				method: call.method,
				payload: asBuffer(call.payload),
				requestId: call.requestId,
				idempotencyKey: call.idempotencyKey,
				contractHash,
				xSbTrace: traceHeader,
				excludeInstanceIds: call.excludeInstanceIds ?? [],
			},
			traceMetadata(traceHeader),
			{ deadline: call.deadline },
		);
		const abort = () => stream.cancel();
		call.signal?.addEventListener("abort", abort, { once: true });
		try {
			for await (const chunk of stream as AsyncIterable<InvokeChunk>) {
				if (chunk.errorCode)
					throw handlerFailure(chunk.errorCode, chunk.errorMessage);
				yield chunk.payload;
			}
		} catch (err) {
			if (err instanceof CallFailure) throw err;
			throw grpcFailure(`rpc ${call.method} via runtime`, err);
		} finally {
			call.signal?.removeEventListener("abort", abort);
			stream.cancel?.();
		}
	}

	async callUnary(
		targetServiceId: string,
		contractHash: Buffer,
		call: WireCall,
	): Promise<Uint8Array> {
		call.signal?.throwIfAborted();
		await this.ready(call);
		const traceHeader = currentTraceHeader();
		return new Promise((resolve, reject) => {
			let pending: ClientUnaryCall | undefined;
			const abort = () => pending?.cancel();
			call.signal?.addEventListener("abort", abort, { once: true });
			pending = this.client.unary(
				{
					targetServiceId,
					method: call.method,
					payload: asBuffer(call.payload),
					requestId: call.requestId,
					idempotencyKey: call.idempotencyKey,
					contractHash,
					xSbTrace: traceHeader,
					excludeInstanceIds: call.excludeInstanceIds ?? [],
				},
				traceMetadata(traceHeader),
				{ deadline: call.deadline },
				(err, resp) => {
					call.signal?.removeEventListener("abort", abort);
					if (err) {
						reject(grpcFailure(`rpc ${call.method} via runtime`, err));
						return;
					}
					if (resp.errorCode) {
						reject(handlerFailure(resp.errorCode, resp.errorMessage));
						return;
					}
					resolve(resp.payload);
				},
			);
		});
	}

	// ready proves the channel to the runtime is up before the request is
	// written; a failure here is pre-dispatch.
	private ready(call: WireCall): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			const abort = () =>
				reject(
					grpcFailure(`rpc ${call.method} via runtime`, {
						code: 1,
						details: "cancelled",
					}),
				);
			call.signal?.addEventListener("abort", abort, { once: true });
			this.client.waitForReady(call.deadline, (err) => {
				call.signal?.removeEventListener("abort", abort);
				if (err)
					reject(
						grpcFailure(`rpc ${call.method} via runtime: connect`, err, true),
					);
				else resolve();
			});
		});
	}
}
