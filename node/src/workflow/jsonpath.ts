import { ServiceBridgeError } from "../errors";
// JSONPath-lite evaluator for workflow state expressions.
//
// Supported syntax (workflows.md §JSONPath-lite):
//   - $.foo.bar          — path lookup
//   - $.list[N]          — array index (non-negative integer)
//   - $.list[*]          — entire array (passes through)
//   - $.list[*].field    — map field over every array element
//
// Anything outside this grammar — invalid expression, throws.
// Strings not starting with "$." are literals (returned as-is).
// { literal: "$.weird" } escapes a literal that would otherwise parse.
//
// @internal — см. ./README.md

// JsonExpression is the declarative expression type accepted in `input`,
// `idempotencyKey`, `forEach.from`, `when`, `compensate.input` etc.
//   - string starting with "$." — path
//   - { literal: string } — escape for literals that look like paths
//   - any other JSON value (literal object/array/number/boolean/null) — passes through,
//     but object children are recursively evaluated as JsonExpressions
export type JsonExpression =
	| string
	| number
	| boolean
	| null
	| { literal: string }
	| JsonExpression[]
	| { [key: string]: JsonExpression };

// Predicate — `when` clauses.
export type Predicate =
	| string // truthy JsonExpression
	| { not: Predicate }
	| { equals: [JsonExpression, JsonExpression] }
	| { in: [JsonExpression, JsonExpression] } // [value, array]
	| { and: Predicate[] }
	| { or: Predicate[] };

export type State = Record<string, unknown>;

// JsonPathError is thrown for malformed path expressions. Missing paths are
// NOT errors — they resolve to `undefined`.
export class JsonPathError extends ServiceBridgeError {
	constructor(
		message: string,
		public readonly expr: string,
	) {
		super(`workflow/jsonpath: ${message}: "${expr}"`);
		this.name = "JsonPathError";
	}
}

// PATH_TOKEN_RE matches one path segment after the leading "$":
//   .name        — dotted identifier (alnum + underscore)
//   [123]        — numeric index
//   [*]          — wildcard
// Anything else fails parsing.
const PATH_TOKEN_RE = /^\.([a-zA-Z_][a-zA-Z0-9_]*)|^\[(\*|\d+)\]/;

function isLiteralEscape(v: unknown): v is { literal: string } {
	return (
		typeof v === "object" &&
		v !== null &&
		!Array.isArray(v) &&
		Object.keys(v).length === 1 &&
		typeof (v as { literal?: unknown }).literal === "string"
	);
}

// evalPath resolves a `$.…` path against state. Returns `undefined` on miss.
// Throws JsonPathError on syntax errors.
export function evalPath(expr: string, state: State): unknown {
	if (!expr.startsWith("$")) {
		throw new JsonPathError("path must start with $", expr);
	}
	let rest = expr.slice(1);
	// Iterator over the current value(s). Starts as a single scalar (state).
	// Wildcards convert it into an array; subsequent `.field` maps over it.
	let cursor: unknown = state;
	let wildcardActive = false;

	while (rest.length > 0) {
		const m = PATH_TOKEN_RE.exec(rest);
		if (!m) {
			throw new JsonPathError("unparsable token", expr);
		}
		rest = rest.slice(m[0].length);
		const field = m[1];
		const bracket = m[2];

		if (field !== undefined) {
			if (wildcardActive && Array.isArray(cursor)) {
				cursor = cursor.map((item) =>
					item !== null && typeof item === "object"
						? (item as Record<string, unknown>)[field]
						: undefined,
				);
			} else if (cursor !== null && typeof cursor === "object") {
				cursor = (cursor as Record<string, unknown>)[field];
			} else {
				cursor = undefined;
			}
			continue;
		}

		// bracket access
		if (bracket === "*") {
			if (cursor === undefined || cursor === null) {
				cursor = [];
				wildcardActive = true;
				continue;
			}
			if (!Array.isArray(cursor)) {
				throw new JsonPathError("[*] on non-array", expr);
			}
			wildcardActive = true;
			continue;
		}
		// numeric index
		const idx = Number.parseInt(bracket!, 10);
		if (Array.isArray(cursor)) {
			cursor = cursor[idx];
		} else {
			cursor = undefined;
		}
		// numeric index collapses wildcard if it was active
		wildcardActive = false;
	}

	return cursor;
}

// evalLiteralOrPath evaluates a JsonExpression node against state.
//   - "$.foo"          → path lookup
//   - "literal"        → "literal"
//   - { literal: "x" } → "x"
//   - array            → array of evaluated elements
//   - object           → object with evaluated values
//   - number/bool/null → passed through
export function evalLiteralOrPath(expr: JsonExpression, state: State): unknown {
	if (expr === null) return null;
	if (typeof expr === "string") {
		return expr.startsWith("$.") || expr === "$" ? evalPath(expr, state) : expr;
	}
	if (typeof expr === "number" || typeof expr === "boolean") return expr;
	if (Array.isArray(expr)) {
		return expr.map((e) => evalLiteralOrPath(e, state));
	}
	if (isLiteralEscape(expr)) return expr.literal;
	const out: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(expr)) {
		out[k] = evalLiteralOrPath(v as JsonExpression, state);
	}
	return out;
}

function deepEqual(a: unknown, b: unknown): boolean {
	if (a === b) return true;
	if (a === null || b === null) return false;
	if (typeof a !== typeof b) return false;
	if (typeof a !== "object") return false;
	if (Array.isArray(a) !== Array.isArray(b)) return false;
	if (Array.isArray(a)) {
		const ba = b as unknown[];
		if (a.length !== ba.length) return false;
		return a.every((v, i) => deepEqual(v, ba[i]));
	}
	const ao = a as Record<string, unknown>;
	const bo = b as Record<string, unknown>;
	const ak = Object.keys(ao);
	const bk = Object.keys(bo);
	if (ak.length !== bk.length) return false;
	return ak.every((k) => deepEqual(ao[k], bo[k]));
}

// evalPredicate evaluates a `when` predicate to boolean.
export function evalPredicate(pred: Predicate, state: State): boolean {
	if (typeof pred === "string") {
		return Boolean(evalLiteralOrPath(pred, state));
	}
	if ("not" in pred) return !evalPredicate(pred.not, state);
	if ("equals" in pred) {
		const [l, r] = pred.equals;
		return deepEqual(evalLiteralOrPath(l, state), evalLiteralOrPath(r, state));
	}
	if ("in" in pred) {
		const [v, arr] = pred.in;
		const value = evalLiteralOrPath(v, state);
		const list = evalLiteralOrPath(arr, state);
		if (!Array.isArray(list)) return false;
		return list.some((x) => deepEqual(x, value));
	}
	if ("and" in pred) return pred.and.every((p) => evalPredicate(p, state));
	if ("or" in pred) return pred.or.some((p) => evalPredicate(p, state));
	throw new Error(
		`workflow/jsonpath: unknown predicate shape: ${JSON.stringify(pred)}`,
	);
}
