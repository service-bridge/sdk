export {
	type AdvertiseConfig,
	type CallOpts,
	type ConnectedEvent,
	type DisconnectedEvent,
	type Identity,
	type MethodDescriptor,
	type MethodType,
	type PolicyViolationEvent,
	type ReconnectingEvent,
	type RpcHandlerOpts,
	type SchemaSpec,
	ServiceBridge,
	type ServiceBridgeOptions,
	type ServiceDeps,
	type ServiceInstanceInfo,
	type ServiceMapEntry,
	type TelemetryAPI,
	type WorkflowHandlerOpts,
} from "./src/connection/service-bridge";
export { ConnectionError } from "./src/connection/service-bridge-error";
export { ConfigurationError, ServiceBridgeError } from "./src/errors";
export type { EventDomain } from "./src/events/domain";
export { InvalidEventNameError, OutboxFullError } from "./src/events/errors";
export type { PublishOpts } from "./src/events/publisher";
export type {
	CatchupPolicy,
	CronTrigger,
	DeclaredDep,
	DelayedTrigger,
	IntervalTrigger,
	JobHandler,
	JobHandlerCtx,
	JobOpts,
	OverlapPolicy,
	RetryPolicy,
	Trigger,
} from "./src/job/index";
export { JobDomain } from "./src/job/index";
export type { RetryOpts } from "./src/rpc/client";
export type { RpcDomain } from "./src/rpc/domain";
export { RpcAccessDeniedError } from "./src/rpc/errors";
export { NoLiveInstanceError } from "./src/rpc/lb";
export type { TypedClient } from "./src/rpc/typed-client";
// Everything needed to call sb.telemetry.startOp(): the params type names
// Channel, and the kind is a per-channel numeric constant. Wrap the work in
// the returned handle's run(fn) to make it the parent of everything inside.
export {
	Channel,
	EventDeliver,
	EventPublish,
	HttpHandle,
	JobExec,
	type OpHandle,
	type StartOpParams,
	Status,
	UserSubOp,
	WorkflowRun,
} from "./src/telemetry/index";
export type { WorkflowDomain } from "./src/workflow/domain";
export {
	WorkflowAccessDeniedError,
	WorkflowNotFoundError,
	WorkflowTerminalError,
} from "./src/workflow/errors";
export { JsonPathError } from "./src/workflow/jsonpath";
export { WorkflowValidationError } from "./src/workflow/validate";
