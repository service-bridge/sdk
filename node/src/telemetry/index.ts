// index.ts — public API for the telemetry module.
// @public — см. ./README.md

export {
	als,
	currentTraceContext,
	runWithTrace,
} from "./context";
export type { LogFields } from "./logs";
export { LogLevel, makeLogger } from "./logs";
export type { Labels } from "./metrics";
export { MetricKind, makeCounter, makeGauge, makeHistogram } from "./metrics";
export type { StartOpParams } from "./ops";
export {
	Channel,
	EventDeliver,
	EventPublish,
	HttpHandle,
	JobExec,
	OpHandle,
	RpcCall,
	Status,
	UserSubOp,
	WorkflowRun,
	WorkflowSleep,
	WorkflowWaitEvent,
	WorkflowWaitSignal,
} from "./ops";
export type { CapturedAttachment, CaptureMode } from "./payload-capture";
export { capPayload, resolveCaptureMode } from "./payload-capture";
export { cpuPercent, ProcessSampler } from "./process-sampler";
export type {
	RingBudgets,
	RingItem,
	RingKind,
	RingMessage,
} from "./ring";
export { TelemetryRing } from "./ring";
export type { TraceContext } from "./trace-context";
export { mintRootContext, ZERO_OP_ID } from "./trace-context";
export type {
	ClientTelemetryStream,
	DropObserver,
	TelemetryClientLike,
	TelemetryTransportOptions,
} from "./transport";
export { adaptTelemetryClient, TelemetryTransport } from "./transport";
export { formatXSbTrace, parseXSbTrace } from "./wire-trace";
