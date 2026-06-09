/**
 * Route — canonical HTTP route entry collected by an integration (Express /
 * Fastify / Hono) and published into the SB registry as METHOD_TYPE_HTTP.
 *
 * @internal — потребляется только интеграциями `servicebridge/{express,
 * fastify,hono}` через `ServiceBridge.routes`. Прикладной код использует
 * subpath-API, см. ../../userDocs/integrations.md.
 */
export interface Route {
	/** Uppercase HTTP method: "GET", "POST", "PUT", "PATCH", "DELETE", etc. */
	method: string;
	/** Raw framework pattern. SDK не нормализует — косметика на UI Service Map. */
	pattern: string;
	/** Source framework, for diagnostics and (later) Service Map UI badges. */
	source: "express" | "fastify" | "hono";
}

/**
 * Хук, передаваемый коллектору при конструировании. Реализуется `ServiceBridge`:
 * `setEndpoint` синхронно обновляет `Registry._httpEndpoint`; `triggerRestart`
 * рестартует Registry-watch стрим, если SDK уже запущен (no-op до `start()`).
 *
 * @internal
 */
export interface RouteSink {
	setEndpoint(endpoint: string): void;
	triggerRestart(): void;
}

/**
 * RouteCollector аккумулирует роуты по ключу `${method} ${pattern}` (дедуп) и
 * управляет HTTP-endpoint жизненным циклом инстанса.
 *
 * Один публичный метод `publishHttp({host, port})`:
 *   1. Записывает endpoint в Registry (`sink.setEndpoint`).
 *   2. Триггерит `sink.triggerRestart()` — рестарт Registry-watch стрима.
 *      Если SDK ещё не стартовал, `triggerRestart` — no-op; endpoint просто
 *      оседает и попадёт в первый `RegisterRequest`.
 *
 * @internal
 */
export class RouteCollector {
	private readonly _routes = new Map<string, Route>();
	private readonly _sink: RouteSink;

	constructor(sink: RouteSink) {
		this._sink = sink;
	}

	/** Добавляет роут. Дедуп по `${method} ${pattern}`. */
	add(route: Route): void {
		const key = `${route.method} ${route.pattern}`;
		this._routes.set(key, route);
	}

	/** Сколько роутов уже собрано (для диагностики тестов). */
	size(): number {
		return this._routes.size;
	}

	/**
	 * Записывает endpoint в Registry и триггерит `triggerRestart` callback.
	 * Безопасен до `sb.start()` — `triggerRestart` тогда no-op.
	 */
	publishHttp(endpoint: { host: string; port: number }): void {
		this._sink.setEndpoint(`${endpoint.host}:${endpoint.port}`);
		this._sink.triggerRestart();
	}

	/** Snapshot собранных роутов в порядке добавления (insertion order Map). */
	snapshot(): readonly Route[] {
		return Array.from(this._routes.values());
	}
}
