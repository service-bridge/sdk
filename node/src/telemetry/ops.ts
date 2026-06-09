// ops.ts — OpHandle for SDK-side operation lifecycle (START + END frames).
// START frame = OpReport without finished_at_ms.
// END frame = OpReport with finished_at_ms set.
// @public — см. ./README.md

import { uuidv7 } from "uuidv7";
import {
	Channel,
	type OpReport,
	type PayloadAttachment,
	Status,
} from "../pb/servicebridge/v1/telemetry";
import { currentTraceContext } from "./context";
import {
	type CapturedAttachment,
	type CaptureMode,
	capPayload,
	DEFAULT_PAYLOAD_MAX_BYTES,
	resolveCaptureMode,
} from "./payload-capture";
import type { TelemetryRing } from "./ring";
import { ZERO_OP_ID } from "./trace-context";

export { Channel, Status };

// Runtime queues `meta`/`attrs` JSON via `jsonOrNull(buf)` — an empty buffer
// becomes SQL NULL, which violates the NOT NULL constraint on operations.meta
// and operations.attrs. Default to '{}' so the START frame always carries a
// valid JSON object.
const EMPTY_JSON_OBJECT = Buffer.from("{}");

// OpKind numeric values per-channel (mirrors Go enums.go).
export const RpcCall = 1;
export const EventPublish = 1;
export const EventDeliver = 2;
export const WorkflowRun = 1;
export const WorkflowSleep = 2;
export const WorkflowWaitEvent = 3;
export const WorkflowWaitSignal = 4;
export const JobExec = 1;
export const HttpHandle = 1;
export const UserSubOp = 1;

export interface StartOpParams {
	/** Required when called directly without ALS context. Otherwise inherited. */
	traceId?: string;
	/** Auto-minted UUIDv7 when omitted. */
	opId?: string;
	/** Auto-resolved from currentTraceContext().parentOpId when omitted. */
	parentOpId?: string;
	channel: Channel;
	kind: number;
	subject: string;
	peerServiceId?: string;
	businessKey?: string;
	attempt?: number;
	startedAtMs?: number;
	metaJson?: Buffer;
	attrsJson?: Buffer;
	/**
	 * Per-handler capture override. May only narrow the runtime-pushed effective
	 * mode (privacy ordering none < errors < all), never widen it.
	 */
	captureMode?: CaptureMode;
	/**
	 * Runtime-pushed effective capture mode (from the registry stream). The
	 * single authority for what may be captured. Defaults to "none" until the
	 * first registry snapshot arrives (fail-safe).
	 */
	effectiveCaptureMode?: CaptureMode;
	/**
	 * Per-direction payload byte cap for captured payloads. Defaults to
	 * DEFAULT_PAYLOAD_MAX_BYTES; threaded from ServiceBridgeOptions.payloadMaxBytes.
	 */
	payloadMaxBytes?: number;
}

interface ResolvedParams {
	traceId: string;
	opId: string;
	parentOpId: string;
	channel: Channel;
	kind: number;
	subject: string;
	peerServiceId: string;
	businessKey: string;
	attempt: number;
	metaJson: Buffer;
	attrsJson: Buffer;
	captureMode: CaptureMode;
	payloadMaxBytes: number;
}

// OpHandle controls the END frame for an in-flight operation.
// @public — см. ./README.md
export class OpHandle {
	private readonly ring: TelemetryRing;
	private readonly params: ResolvedParams;
	private readonly startedAtMs: number;
	private ended = false;
	// Buffered attachments for "errors" mode — emitted on ERROR end, dropped on
	// OK. Keyed by direction (1=IN, 2=OUT) so a re-capture of the same direction
	// (e.g. OUT on retry) overwrites last-wins instead of buffering a duplicate.
	private readonly bufferedPayloads = new Map<number, CapturedAttachment>();

	private constructor(
		ring: TelemetryRing,
		params: ResolvedParams,
		startedAtMs: number,
	) {
		this.ring = ring;
		this.params = params;
		this.startedAtMs = startedAtMs;
	}

	/**
	 * Start an operation, enqueue the START frame, return a handle for END.
	 * traceId / parentOpId default to the active TraceContext from ALS
	 * (set via runWithTrace). opId is auto-minted as a fresh UUIDv7.
	 */
	static start(ring: TelemetryRing, params: StartOpParams): OpHandle {
		const ctx = currentTraceContext();
		const traceId = params.traceId ?? ctx?.traceId ?? uuidv7();
		const parentOpId = params.parentOpId ?? ctx?.parentOpId ?? ZERO_OP_ID;
		const opId = params.opId ?? uuidv7();
		const startedAtMs = params.startedAtMs ?? Date.now();

		const resolved: ResolvedParams = {
			traceId,
			opId,
			parentOpId,
			channel: params.channel,
			kind: params.kind,
			subject: params.subject,
			peerServiceId: params.peerServiceId ?? "",
			businessKey: params.businessKey ?? "",
			attempt: params.attempt ?? 0,
			metaJson: params.metaJson ?? EMPTY_JSON_OBJECT,
			attrsJson: params.attrsJson ?? EMPTY_JSON_OBJECT,
			captureMode: resolveCaptureMode(
				params.effectiveCaptureMode ?? "none",
				params.captureMode,
			),
			payloadMaxBytes: params.payloadMaxBytes ?? DEFAULT_PAYLOAD_MAX_BYTES,
		};

		const handle = new OpHandle(ring, resolved, startedAtMs);
		handle.enqueueStartFrame();
		return handle;
	}

	/** Op identifier for this in-flight op (UUIDv7). */
	get opId(): string {
		return this.params.opId;
	}

	/**
	 * Record the retry attempt count on this in-flight op. The END frame carries
	 * the final value so a single RPC.CALL row reflects how many tries it took
	 * (ADR-0037..0042). No new row is minted per attempt.
	 */
	setAttempt(attempt: number): void {
		this.params.attempt = attempt;
	}

	/** Trace identifier this op belongs to. */
	get traceId(): string {
		return this.params.traceId;
	}

	/**
	 * Capture the inbound (request/input) payload for this op. Direction = IN.
	 * "all" emits immediately; "errors" buffers until end; "none" is a no-op.
	 */
	captureIn(bytes: Uint8Array, contractHash: string): void {
		this.capture(1, bytes, contractHash);
	}

	/**
	 * Capture the outbound (response/output) payload for this op. Direction = OUT.
	 */
	captureOut(bytes: Uint8Array, contractHash: string): void {
		this.capture(2, bytes, contractHash);
	}

	private capture(
		direction: number,
		bytes: Uint8Array,
		contractHash: string,
	): void {
		if (this.params.captureMode === "none") return;
		const { bytes: capped, originalSize } = capPayload(
			bytes,
			this.params.payloadMaxBytes,
		);
		const att: CapturedAttachment = {
			direction,
			bytes: capped,
			originalSize,
			contractHash,
		};
		if (this.params.captureMode === "all") {
			this.emitPayload(att);
		} else {
			// "errors": hold until end() learns the status. Last-wins per direction.
			this.bufferedPayloads.set(direction, att);
		}
	}

	private emitPayload(att: CapturedAttachment): void {
		const msg: PayloadAttachment = {
			traceId: this.params.traceId,
			opId: this.params.opId,
			direction: att.direction,
			bytes: Buffer.from(att.bytes),
			originalSize: att.originalSize,
			contractHash: att.contractHash,
		};
		this.ring.push("payloads", msg);
	}

	/**
	 * End the operation, enqueue the END frame.
	 * Idempotent — calling end() twice is a no-op.
	 */
	end(status: Status, statusMessage?: string): void {
		if (this.ended) return;
		this.ended = true;
		this.flushBufferedPayloads(status);
		this.enqueueEndFrame(status, statusMessage ?? "");
	}

	// flushBufferedPayloads emits "errors"-mode buffered attachments when the op
	// failed, and clears the buffer either way. Statuses other than OK/RUNNING
	// (ERROR, TIMEOUT, ABANDONED) count as failures worth capturing.
	private flushBufferedPayloads(status: Status): void {
		const failed = status !== Status.SUCCESS && status !== Status.PENDING;
		if (failed) {
			for (const att of this.bufferedPayloads.values()) this.emitPayload(att);
		}
		this.bufferedPayloads.clear();
	}

	private enqueueStartFrame(): void {
		const report: OpReport = {
			traceId: this.params.traceId,
			opId: this.params.opId,
			parentOpId: this.params.parentOpId,
			channel: this.params.channel,
			kind: this.params.kind,
			subject: this.params.subject,
			peerServiceId: this.params.peerServiceId,
			businessKey: this.params.businessKey,
			attempt: this.params.attempt,
			startedAtMs: this.startedAtMs,
			finishedAtMs: undefined,
			status: Status.PENDING,
			statusMessage: "",
			metaJson: this.params.metaJson,
			attrsJson: this.params.attrsJson,
		};
		this.ring.push("ops", report);
	}

	private enqueueEndFrame(status: Status, statusMessage: string): void {
		// END delta: only fields that change from START. Runtime upserts finished op.
		const report: OpReport = {
			traceId: this.params.traceId,
			opId: this.params.opId,
			parentOpId: "",
			channel: this.params.channel,
			kind: this.params.kind,
			subject: "",
			peerServiceId: "",
			businessKey: "",
			attempt: this.params.attempt,
			startedAtMs: 0,
			finishedAtMs: Date.now(),
			status,
			statusMessage,
			metaJson: Buffer.alloc(0),
			attrsJson: Buffer.alloc(0),
		};
		this.ring.push("ops", report);
	}
}
