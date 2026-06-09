// event-name.ts — event name validation. Routing (AMQP wildcards) lives on
// the server side only — ADR-0011.
// @public — см. ./README.md

const EVENT_NAME_RE = /^[a-z0-9_-]+(\.[a-z0-9_-]+)*$/;

/**
 * validEventName reports whether name conforms to the required pattern:
 * lowercase alphanumeric segments separated by dots; no leading/trailing/consecutive dots.
 */
export function validEventName(name: string): boolean {
	return EVENT_NAME_RE.test(name);
}
