/**
 * Resolve the host that an SDK instance advertises as its public HTTP server
 * (ADR 0001). Mirrors `resolveAdvertise` semantics in src/connection, but for
 * the HTTP plane:
 *
 *   1. explicit `host` argument          → used as-is
 *   2. fallback "127.0.0.1"              → одноразовый console.warn
 *
 * @internal — потребляется только интеграциями.
 */

let _warned = false;

/** Сбрасывает warn-флаг (для тестов). */
export function _resetHostWarn(): void {
	_warned = false;
}

export function resolveHttpAdvertiseHost(explicit?: string): string {
	if (explicit && explicit.length > 0) return explicit;
	if (!_warned) {
		_warned = true;
		console.warn(
			"[ServiceBridge] http advertise host not configured — falling back to 127.0.0.1. " +
				"Pass { host } to the HTTP plugin for cross-host reachability.",
		);
	}
	return "127.0.0.1";
}
