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

// Thrown when an option is invalid — a bound that is not a positive integer,
// a contradictory combination, anything a retry cannot fix.
//
// It has to be its own type because the connect path classifies failures by
// gRPC status, and a plain Error carries none: it reads as code -1, which the
// reconnect logic treats as transient. A typo in a bound would then loop
// forever, reporting a provisioning failure that never happened.
//
// @public — см. ../README.md
export class ConfigurationError extends ServiceBridgeError {
	constructor(message: string) {
		super(message);
		this.name = "ConfigurationError";
	}
}
