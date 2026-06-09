// @public — см. ./README.md
export class InvalidEventNameError extends Error {
	override readonly name = "InvalidEventNameError";

	constructor(eventName: string) {
		super(
			`events: invalid event name "${eventName}" — must match ^[a-z0-9_-]+(\\.[a-z0-9_-]+)*$`,
		);
	}
}

// @public — см. ./README.md
export class OutboxFullError extends Error {
	override readonly name = "OutboxFullError";

	constructor(cap: number) {
		super(
			`events: outbox cap reached (${cap} rows) — cannot enqueue new event`,
		);
	}
}
