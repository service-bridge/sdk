// HTTP edge protection shared by the Express, Fastify and Hono adapters.
// It runs before HTTP.HANDLE creation so rejected probes never enter telemetry.
// @internal — см. ../README.md

export interface HttpRateLimitOptions {
	/** Requests accepted from one client during a window. */
	limit?: number;
	/** Fixed-window duration in milliseconds. */
	windowMs?: number;
	/** Hard cap for tracked clients; oldest entries are evicted above the cap. */
	maxClients?: number;
	/** Trusted proxies at the right side of X-Forwarded-For. Zero ignores it. */
	trustProxyHops?: number;
}

export interface HttpSecurityOptions {
	/** Disable all edge checks. */
	enabled?: boolean;
	/** Reject common secret, VCS, PHP and CMS discovery probes with 404. */
	blockCommonScanners?: boolean;
	/** Per-client limiter. `false` disables it. */
	rateLimit?: false | HttpRateLimitOptions;
}

export interface HttpSecurityRequest {
	method: string;
	pathname: string;
	remoteAddress?: string | null;
	forwardedFor?: string | null;
}

export type HttpSecurityDecision =
	| { allowed: true }
	| { allowed: false; status: 404 | 429; retryAfterSeconds?: number };

interface Counter {
	count: number;
	windowStartedAt: number;
}

const DEFAULT_LIMIT = 300;
const DEFAULT_WINDOW_MS = 60_000;
const DEFAULT_MAX_CLIENTS = 10_000;

const SECRET_FILE_RE =
	/(?:^|\/)(?:\.env(?:[._~-].*)?|\.git(?:\/|$)|\.svn(?:\/|$)|\.hg(?:\/|$)|id_rsa(?:\.|$)|credentials(?:\.json)?$|service-account\.json$|gcp-(?:key|credentials)\.json$|firebase-adminsdk\.json$|application_default_credentials\.json$)/i;
const PHP_PROBE_RE =
	/(?:^|\/)(?:phpinfo|info|debug|test|i|p|pi|phpversion|server-(?:info|status))(?:\.php)?(?:[.~_-].*)?$/i;
const CMS_PROBE_RE =
	/(?:^|\/)(?:wp-admin|wp-content|wp-includes|wp-json|wordpress|xmlrpc\.php|vendor\/phpunit|administrator)(?:\/|$)/i;

function positiveInteger(value: number | undefined, fallback: number): number {
	return Number.isSafeInteger(value) && (value ?? 0) > 0
		? (value as number)
		: fallback;
}

function normalizedPath(pathname: string): string {
	let path = pathname || "/";
	try {
		path = decodeURIComponent(path);
	} catch {
		// Malformed encoding is still checked in its raw form.
	}
	return path.replace(/\/{2,}/g, "/").toLowerCase();
}

export function isCommonScannerPath(pathname: string): boolean {
	const path = normalizedPath(pathname);
	return (
		SECRET_FILE_RE.test(path) ||
		PHP_PROBE_RE.test(path) ||
		CMS_PROBE_RE.test(path) ||
		path.includes("rest_route=/wp/") ||
		path.includes("rest_route=/batch/") ||
		path.includes("rest_route=%2fwp%2f") ||
		path.includes("rest_route=%2fbatch%2f")
	);
}

function cleanAddress(value: string): string {
	const trimmed = value.trim();
	if (trimmed.startsWith("[")) {
		const end = trimmed.indexOf("]");
		if (end > 0) return trimmed.slice(1, end);
	}
	if (trimmed.startsWith("::ffff:")) return trimmed.slice(7);
	const ipv4WithPort = trimmed.match(/^(\d{1,3}(?:\.\d{1,3}){3}):\d+$/);
	return ipv4WithPort?.[1] ?? trimmed;
}

export function clientAddress(
	remoteAddress: string | null | undefined,
	forwardedFor: string | null | undefined,
	trustProxyHops: number,
): string {
	if (trustProxyHops > 0 && forwardedFor) {
		const chain = forwardedFor.split(",").map(cleanAddress).filter(Boolean);
		const index = chain.length - trustProxyHops;
		if (index >= 0 && chain[index]) return chain[index];
	}
	return cleanAddress(remoteAddress || "unknown");
}

/** Stateful per-adapter guard. One instance is created for each attached app. */
export class HttpRequestGuard {
	private readonly enabled: boolean;
	private readonly blockCommonScanners: boolean;
	private readonly limit: number | null;
	private readonly windowMs: number;
	private readonly maxClients: number;
	private readonly trustProxyHops: number;
	private readonly clients = new Map<string, Counter>();

	constructor(options: HttpSecurityOptions = {}) {
		this.enabled = options.enabled !== false;
		this.blockCommonScanners = options.blockCommonScanners !== false;
		this.limit =
			options.rateLimit === false
				? null
				: positiveInteger(options.rateLimit?.limit, DEFAULT_LIMIT);
		this.windowMs = positiveInteger(
			options.rateLimit === false ? undefined : options.rateLimit?.windowMs,
			DEFAULT_WINDOW_MS,
		);
		this.maxClients = positiveInteger(
			options.rateLimit === false ? undefined : options.rateLimit?.maxClients,
			DEFAULT_MAX_CLIENTS,
		);
		this.trustProxyHops =
			options.rateLimit === false
				? 0
				: Math.max(0, Math.trunc(options.rateLimit?.trustProxyHops ?? 0));
	}

	check(req: HttpSecurityRequest, now = Date.now()): HttpSecurityDecision {
		if (!this.enabled) return { allowed: true };
		if (this.blockCommonScanners && isCommonScannerPath(req.pathname)) {
			return { allowed: false, status: 404 };
		}
		if (this.limit === null) return { allowed: true };

		const key = clientAddress(
			req.remoteAddress,
			req.forwardedFor,
			this.trustProxyHops,
		);
		let counter = this.clients.get(key);
		if (!counter || now - counter.windowStartedAt >= this.windowMs) {
			counter = { count: 0, windowStartedAt: now };
			this.clients.delete(key);
			this.clients.set(key, counter);
		}
		counter.count++;
		if (this.clients.size > this.maxClients) {
			const oldest = this.clients.keys().next().value;
			if (oldest !== undefined) this.clients.delete(oldest);
		}
		if (counter.count <= this.limit) return { allowed: true };

		return {
			allowed: false,
			status: 429,
			retryAfterSeconds: Math.max(
				1,
				Math.ceil((this.windowMs - (now - counter.windowStartedAt)) / 1000),
			),
		};
	}
}
