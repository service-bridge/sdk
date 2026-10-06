import { describe, expect, it } from "bun:test";
import { ConnectionError } from "./connection/service-bridge-error";
import { ServiceBridgeError } from "./errors";
import { InvalidEventNameError, OutboxFullError } from "./events/errors";
import { RpcAccessDeniedError } from "./rpc/errors";
import { NoLiveInstanceError } from "./rpc/lb";
import {
	WorkflowAccessDeniedError,
	WorkflowNotFoundError,
	WorkflowRunFailedError,
	WorkflowTerminalError,
} from "./workflow/errors";

describe("error hierarchy", () => {
	const errors: [string, Error][] = [
		["ConnectionError", new ConnectionError("provision", new Error("boom"))],
		["InvalidEventNameError", new InvalidEventNameError("Bad Name")],
		["OutboxFullError", new OutboxFullError(100)],
		["RpcAccessDeniedError", new RpcAccessDeniedError("svc", "M", "denied")],
		["NoLiveInstanceError", new NoLiveInstanceError("no live instance")],
		["WorkflowAccessDeniedError", new WorkflowAccessDeniedError("wf", "no")],
		["WorkflowNotFoundError", new WorkflowNotFoundError("wf")],
		["WorkflowTerminalError", new WorkflowTerminalError("run-1", "success")],
		[
			"WorkflowRunFailedError",
			new WorkflowRunFailedError("run-1", "failed", "X", "boom"),
		],
	];

	// The point of the base class: catching SDK failures must not require
	// enumerating every concrete class, and an error added later must not
	// escape existing catch blocks.
	for (const [name, err] of errors) {
		it(`${name} is caught by the one SDK predicate`, () => {
			expect(err).toBeInstanceOf(ServiceBridgeError);
			expect(err).toBeInstanceOf(Error);
		});
	}

	it("every error reports its own name, not the base one", () => {
		for (const [name, err] of errors) {
			expect(err.name).toBe(name);
		}
	});

	it("a plain Error is not mistaken for an SDK failure", () => {
		expect(new Error("unrelated")).not.toBeInstanceOf(ServiceBridgeError);
	});
});
