// Thin gRPC adapter from RuntimeOps (runner-facing) to WorkflowsClient
// (wire-facing). Used by ServiceBridge to feed the WorkflowSubscriber.
//
// All JSON encoding lives here so the runner stays transport-agnostic.
//
// @internal — см. ./README.md

import type { WorkflowsClient } from "../pb/servicebridge/v1/workflows";
import type { RuntimeOps } from "./runner";

export function makeRuntimeOps(
	rpc: WorkflowsClient,
	getInstanceId: () => string,
): RuntimeOps {
	return {
		beginStep(args) {
			return new Promise((resolve, reject) => {
				rpc.beginStep(
					{
						runId: args.runId,
						stepId: args.stepId,
						parentStepId: args.parentStepId,
						kind: args.kind,
						inputSnapshot: Buffer.from(
							JSON.stringify(args.inputSnapshot ?? null),
							"utf8",
						),
						leaseEpoch: args.leaseEpoch,
						instanceId: getInstanceId(),
					},
					(err, resp) => {
						if (err) return reject(err);
						if (resp.alreadyDone && resp.cachedOutput.length > 0) {
							try {
								const cached = JSON.parse(
									Buffer.from(resp.cachedOutput).toString("utf8"),
								);
								return resolve({ alreadyDone: true, cachedOutput: cached });
							} catch (parseErr) {
								return reject(parseErr);
							}
						}
						resolve({ alreadyDone: resp.alreadyDone });
					},
				);
			});
		},
		completeStep(args) {
			return new Promise((resolve, reject) => {
				rpc.completeStep(
					{
						runId: args.runId,
						stepId: args.stepId,
						output: Buffer.from(JSON.stringify(args.output ?? null), "utf8"),
						leaseEpoch: args.leaseEpoch,
					},
					(err) => {
						if (err) return reject(err);
						resolve();
					},
				);
			});
		},
		failStep(args) {
			return new Promise((resolve, reject) => {
				rpc.failStep(
					{
						runId: args.runId,
						stepId: args.stepId,
						errorCode: args.errorCode,
						errorMessage: args.errorMessage,
						leaseEpoch: args.leaseEpoch,
						retriable: args.retriable,
					},
					(err, resp) => {
						if (err) return reject(err);
						resolve({ nextAction: resp.nextAction });
					},
				);
			});
		},
		park(args) {
			return new Promise((resolve, reject) => {
				rpc.park(
					{
						runId: args.runId,
						stepId: args.stepId,
						leaseEpoch: args.leaseEpoch,
						sleep: args.sleep,
						eventWait: args.eventWait,
						signalWait: args.signalWait,
						nestedRun: args.nestedRun,
					},
					(err) => {
						if (err) return reject(err);
						resolve();
					},
				);
			});
		},
		completeRun(args) {
			return new Promise((resolve, reject) => {
				rpc.completeRun(
					{
						runId: args.runId,
						finalState: Buffer.from(
							JSON.stringify(args.finalState ?? {}),
							"utf8",
						),
						leaseEpoch: args.leaseEpoch,
						terminalStatus: args.terminalStatus ?? "",
					},
					(err) => {
						if (err) return reject(err);
						resolve();
					},
				);
			});
		},
	};
}
