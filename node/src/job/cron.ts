// Cron grammar of the runtime: the five-field standard parser of
// robfig/cron/v3 (minute hour day-of-month month day-of-week), no seconds, no
// @descriptors. The Go SDK uses that parser directly; this is the same grammar,
// so an expression one SDK accepts the other accepts too, and both reject what
// the runtime would refuse at registration. Shared cases: sdk/cron-vectors.json.
//
// @internal — см. ./README.md

interface Bounds {
	min: number;
	max: number;
	names?: Record<string, number>;
}

const MONTHS: Record<string, number> = {
	jan: 1,
	feb: 2,
	mar: 3,
	apr: 4,
	may: 5,
	jun: 6,
	jul: 7,
	aug: 8,
	sep: 9,
	oct: 10,
	nov: 11,
	dec: 12,
};
const DAYS: Record<string, number> = {
	sun: 0,
	mon: 1,
	tue: 2,
	wed: 3,
	thu: 4,
	fri: 5,
	sat: 6,
};

const FIELDS: Bounds[] = [
	{ min: 0, max: 59 },
	{ min: 0, max: 23 },
	{ min: 1, max: 31 },
	{ min: 1, max: 12, names: MONTHS },
	{ min: 0, max: 6, names: DAYS },
];

/** Returns why `expr` is not a valid cron expression, or null when it is. */
export function cronError(expr: string): string | null {
	const fields = expr.split(/\s+/).filter((f) => f !== "");
	if (fields.length !== FIELDS.length)
		return `expected exactly 5 fields, found ${fields.length}`;
	for (let i = 0; i < fields.length; i++) {
		for (const part of (fields[i] as string).split(",")) {
			const err = rangeError(part, FIELDS[i] as Bounds);
			if (err) return `${err}: ${fields[i]}`;
		}
	}
	return null;
}

// rangeError follows robfig's getRange: `*` or `?`, a number or name, `a-b`,
// each optionally followed by `/step`; `a/step` runs from a to the maximum.
function rangeError(expr: string, b: Bounds): string | null {
	const rangeAndStep = expr.split("/");
	const lowAndHigh = (rangeAndStep[0] as string).split("-");
	let start: number;
	let end: number;
	if (lowAndHigh[0] === "*" || lowAndHigh[0] === "?") {
		start = b.min;
		end = b.max;
	} else {
		const low = intOrName(lowAndHigh[0] as string, b.names);
		if (typeof low === "string") return low;
		start = low;
		if (lowAndHigh.length === 1) end = start;
		else if (lowAndHigh.length === 2) {
			const high = intOrName(lowAndHigh[1] as string, b.names);
			if (typeof high === "string") return high;
			end = high;
		} else return `too many hyphens: ${expr}`;
	}
	let step = 1;
	if (rangeAndStep.length === 2) {
		const parsed = nonNegativeInt(rangeAndStep[1] as string);
		if (typeof parsed === "string") return parsed;
		step = parsed;
		if (lowAndHigh.length === 1) end = b.max;
	} else if (rangeAndStep.length > 2) return `too many slashes: ${expr}`;
	if (start < b.min)
		return `beginning of range (${start}) below minimum (${b.min})`;
	if (end > b.max) return `end of range (${end}) above maximum (${b.max})`;
	if (start > end)
		return `beginning of range (${start}) beyond end of range (${end})`;
	if (step === 0) return "step of range should be a positive number";
	return null;
}

function intOrName(s: string, names?: Record<string, number>): number | string {
	const named = names?.[s.toLowerCase()];
	if (named !== undefined) return named;
	return nonNegativeInt(s);
}

// strconv.Atoi: optional sign, decimal digits only.
function nonNegativeInt(s: string): number | string {
	if (!/^[+-]?\d+$/.test(s)) return `failed to parse int from ${s}`;
	const n = Number(s);
	if (!Number.isSafeInteger(n)) return `failed to parse int from ${s}`;
	if (n < 0) return `negative number (${n}) not allowed: ${s}`;
	return n;
}

/** Returns why `tz` is not an IANA time zone, or null when it is. */
export function timeZoneError(tz: string): string | null {
	try {
		new Intl.DateTimeFormat("en-US", { timeZone: tz });
		return null;
	} catch {
		return `unknown time zone ${tz}`;
	}
}
