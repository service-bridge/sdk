// The SDK's only sink for its own diagnostics. Pass `logger` in
// ServiceBridgeOptions to route them into the application's logger; the
// default writes warnings and errors to the console and drops debug/info, so a
// library never floods the host's stdout.
//
// @public — см. ../README.md

export type LogAttrs = Record<string, unknown>;

/** Structured logger the SDK writes its own diagnostics to. */
export interface Logger {
	debug(message: string, attrs?: LogAttrs): void;
	info(message: string, attrs?: LogAttrs): void;
	warn(message: string, attrs?: LogAttrs): void;
	error(message: string, attrs?: LogAttrs): void;
}

const noop = () => {};

/** Default logger: warn/error to the console, debug/info dropped. */
export const consoleLogger: Logger = {
	debug: noop,
	info: noop,
	warn: (message, attrs) =>
		attrs
			? console.warn(`[servicebridge] ${message}`, attrs)
			: console.warn(`[servicebridge] ${message}`),
	error: (message, attrs) =>
		attrs
			? console.error(`[servicebridge] ${message}`, attrs)
			: console.error(`[servicebridge] ${message}`),
};

/** A logger that discards everything — for tests and embedded use. */
export const silentLogger: Logger = {
	debug: noop,
	info: noop,
	warn: noop,
	error: noop,
};
