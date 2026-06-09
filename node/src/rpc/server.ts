import * as grpc from "@grpc/grpc-js";
import { derToPem } from "../connection/pem";
import {
	type CallRequest,
	type CallResponse,
	CallService,
	type StreamChunk,
} from "../pb/servicebridge/v1/call";
import type { PolicyEvaluation } from "../pb/servicebridge/v1/registry";
import { runWithTrace } from "../telemetry/context";
import { mintRootContext, type TraceContext } from "../telemetry/trace-context";
import { parseXSbTrace } from "../telemetry/wire-trace";
import { evaluatePeerAcceptance, getPeerCertFromCall } from "./acceptance";
import type { DispatchPort } from "./dispatch-port";

// Credentials for the inbound call server: leaf cert + key (DER), CA chain (DER).
// Reuses the same certs the SDK obtained from runtime Bootstrap.
export interface CallServerCredentials {
	caChainDer: Buffer;
	leafCertDer: Buffer;
	privateKeyDer: Buffer;
}

// AdvertiseConfig is the operator-supplied advertise address. host is mandatory
// (no auto-detect — k8s/docker need explicit POD_IP via downward API), port=0
// asks the OS to pick a free port.
export interface AdvertiseConfig {
	host: string;
	port: number;
}

/**
 * Parse the inbound trace context from a CallRequest. Falls back to a fresh
 * root context when xSbTrace is missing or malformed — that keeps the local
 * trace consistent even when the caller side has not yet been wired for
 * propagation.
 * @internal
 */
function inboundTraceContext(req: CallRequest): TraceContext {
	const parsed = parseXSbTrace(req.xSbTrace);
	return parsed ?? mintRootContext();
}
// CallServer hosts the inbound Call.Unary gRPC service on the SDK instance.
// All incoming requests are dispatched to the DispatchPort which owns handler
// registration and schema (de)serialization. Errors thrown by handlers are
// converted to CallResponse.error_code/error_message — gRPC status stays OK.
//
// Acceptance check (Layer 2, ADR-0004): for each incoming direct call,
// the server extracts the caller's SPIFFE service UUID from the peer TLS cert
// and checks it against the callee's rpc.handle acceptance rules. If the
// private grpc-js accessor path is broken the server fails closed — all direct
// calls are rejected with PermissionDenied until the accessor is restored.
//
// Tracing (ADR-0001): the server emits NO op. It runs the handler inside
// the inbound call's trace context (parent = caller CALL.op_id) so the handler's
// nested ops (rpc.call / event.publish) parent to the single caller-owned
// RPC.CALL row. Callee errors flow back in the CallResponse and the caller
// records them on the CALL row.
//
// @internal — см. ./README.md
export class CallServer {
	private server: grpc.Server | null = null;
	private advertised: string | null = null;
	// Set to true when the grpc-js peer-cert accessor path is confirmed working.
	private peerCertAccessorOk = false;

	constructor(
		private readonly dispatch: DispatchPort,
		private readonly creds: CallServerCredentials,
		// Returns the current PolicyEvaluation for this service. May return null
		// when policy is not yet loaded (default-allow in that case).
		private readonly getPolicy: () => PolicyEvaluation | null = () => null,
	) {}

	async start(cfg: AdvertiseConfig): Promise<string> {
		if (this.server) {
			throw new Error("rpc: call server already started");
		}
		if (!cfg.host) {
			throw new Error("rpc: advertise.host is required");
		}

		// Smoke-test the peer-cert accessor path. We use the public
		// getAuthContext() API on grpc-js ServerCall — verify the fallback
		// shape walker still works on a dummy with a getAuthContext function.
		const testCall = {
			getAuthContext: () => ({ sslPeerCertificate: { subjectaltname: "" } }),
		};
		const testCert = getPeerCertFromCall(testCall);
		this.peerCertAccessorOk = testCert !== null;

		const server = new grpc.Server();
		server.addService(CallService, {
			unary: (
				call: grpc.ServerUnaryCall<CallRequest, CallResponse>,
				callback: grpc.sendUnaryData<CallResponse>,
			) => {
				this.handleUnary(call, callback);
			},
			stream: (call: grpc.ServerWritableStream<CallRequest, StreamChunk>) => {
				void this.handleStream(call);
			},
		});

		// grpc-js ServerCredentials.createSsl requires PEM-encoded buffers.
		const caChainPem = derToPem(this.creds.caChainDer, "CERTIFICATE");
		const certPem = derToPem(this.creds.leafCertDer, "CERTIFICATE");
		const keyPem = derToPem(this.creds.privateKeyDer, "PRIVATE KEY");

		const bound = await new Promise<number>((resolve, reject) => {
			const sc = grpc.ServerCredentials.createSsl(
				caChainPem,
				[{ private_key: keyPem, cert_chain: certPem }],
				true, // checkClientCertificate
			);
			// Bind on the advertise host directly so SO_REUSEADDR / IPv4 vs IPv6
			// matches the address that will be published in the registry. grpc-js
			// requires a non-empty hostname (it does not accept 0.0.0.0:0 with the
			// current TLS credentials path).
			server.bindAsync(`${cfg.host}:${cfg.port}`, sc, (err, port) => {
				if (err) reject(err);
				else resolve(port);
			});
		});

		this.server = server;
		this.advertised = `${cfg.host}:${bound}`;
		return this.advertised;
	}

	endpoint(): string {
		if (!this.advertised) throw new Error("rpc: call server not started");
		return this.advertised;
	}

	async stop(): Promise<void> {
		if (!this.server) return;
		const server = this.server;
		this.server = null;
		this.advertised = null;
		await new Promise<void>((resolve) => {
			server.tryShutdown((err) => {
				if (err) server.forceShutdown();
				resolve();
			});
		});
	}

	private async handleUnary(
		call: grpc.ServerUnaryCall<CallRequest, CallResponse>,
		callback: grpc.sendUnaryData<CallResponse>,
	): Promise<void> {
		const denial = this.checkPeerAcceptance(call, call.request.method);
		if (denial) {
			callback({
				code: grpc.status.PERMISSION_DENIED,
				message: denial,
			});
			return;
		}
		const req = call.request;
		const traceCtx = inboundTraceContext(req);
		// The handler runs in the call's trace context (parent = caller CALL.op_id)
		// so its nested ops parent to CALL. No RPC.HANDLE op is emitted — the single
		// RPC.CALL row owned by the caller SDK is the whole call (ADR-0001).
		await runWithTrace(traceCtx, async () => {
			try {
				const result = await this.dispatch.dispatchUnary(
					req.method,
					req.payload,
				);
				callback(null, {
					payload: Buffer.from(result.payload),
					errorCode: result.errorCode ?? "",
					errorMessage: result.errorMessage ?? "",
				});
			} catch (err) {
				const msg = (err as Error).message;
				callback(null, {
					payload: Buffer.alloc(0),
					errorCode: "INTERNAL",
					errorMessage: msg,
				});
			}
		});
	}

	// checkPeerAcceptance returns a denial reason string when the caller is not
	// permitted, or null when the call should proceed.
	private checkPeerAcceptance(call: object, methodName: string): string | null {
		return evaluatePeerAcceptance(
			this.peerCertAccessorOk,
			this.getPolicy(),
			call,
			methodName,
		);
	}

	private async handleStream(
		call: grpc.ServerWritableStream<CallRequest, StreamChunk>,
	): Promise<void> {
		const denial = this.checkPeerAcceptance(call, call.request.method);
		if (denial) {
			call.write({
				payload: Buffer.alloc(0),
				errorCode: "PERMISSION_DENIED",
				errorMessage: denial,
			});
			call.end();
			return;
		}
		const req = call.request;
		const traceCtx = inboundTraceContext(req);
		// Handler runs in the call's trace context; no RPC.HANDLE op (ADR-0001).
		await runWithTrace(traceCtx, async () => {
			try {
				for await (const item of this.dispatch.dispatchStream(
					req.method,
					req.payload,
				)) {
					if (item.errorCode) {
						call.write({
							payload: Buffer.alloc(0),
							errorCode: item.errorCode,
							errorMessage: item.errorMessage ?? "",
						});
						call.end();
						return;
					}
					call.write({
						payload: Buffer.from(item.payload ?? new Uint8Array()),
						errorCode: "",
						errorMessage: "",
					});
				}
				call.end();
			} catch (err) {
				const msg = (err as Error).message;
				call.write({
					payload: Buffer.alloc(0),
					errorCode: "INTERNAL",
					errorMessage: msg,
				});
				call.end();
			}
		});
	}
}
