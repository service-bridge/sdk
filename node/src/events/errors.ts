import { ServiceBridgeError } from "../errors";

// @public — см. ./README.md
export class InvalidEventNameError extends ServiceBridgeError {
	constructor(eventName: string) {
		super(
			"INVALID_EVENT_NAME",
			`events: invalid event name "${eventName}" — must match ^[a-z0-9_-]+(\\.[a-z0-9_-]+)*$`,
		);
		this.name = "InvalidEventNameError";
	}
}
