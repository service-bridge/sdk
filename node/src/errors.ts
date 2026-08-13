// Base class every error this SDK throws inherits from.
//
// Without a shared ancestor a caller has to enumerate all seven concrete
// classes to tell an SDK failure from an application one, and any error added
// later silently escapes every existing catch.
//
// @public — см. ../README.md
export class ServiceBridgeError extends Error {
	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "ServiceBridgeError";
	}
}
