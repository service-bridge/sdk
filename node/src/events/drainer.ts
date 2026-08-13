import type { Identity } from "../connection/service-bridge";
import type { EventsClient } from "../pb/servicebridge/v1/events";
import { PublishStatus } from "../pb/servicebridge/v1/events";
import type { Storage } from "../sqlite/storage";
import type { Logger } from "./publisher";

// MAX_BACKOFF_MS is the delay every attempt past the ladder reuses: transient
// failures retry forever, the ladder only caps the rate.
// @internal
const MAX_BACKOFF_MS = 600_000;
// BACKOFF_MS is the retry delay ladder for transient failures (±25% jitter
// applied).
// @internal
const BACKOFF_MS: readonly number[] = [
	1_000,
	5_000,
	30_000,
	120_000,
	MAX_BACKOFF_MS,
];

// SELECT_DUE_SQL reads the next batch of deliverable rows. Column order matches
// event_outbox_pending_order_idx, so SQLite walks the index in ORDER BY order
// and LIMIT stops the scan — no temp b-tree sort of the whole backlog.
// @internal — см. ./README.md
export const SELECT_DUE_SQL = `SELECT * FROM event_outbox
  WHERE status='pending' AND next_attempt_at_ms <= ?
  ORDER BY enqueued_at_ms ASC, id ASC
  LIMIT ?`;

// SELECT_NEXT_ATTEMPT_SQL finds when the earliest deferred row becomes due, so
// the idle wait lasts exactly that long instead of polling.
// @internal
const SELECT_NEXT_ATTEMPT_SQL = `SELECT MIN(next_attempt_at_ms) AS next_at_ms
  FROM event_outbox WHERE status='pending'`;

// RETRY_SQL re-arms a row for a later attempt.
// @internal
const RETRY_SQL = `UPDATE event_outbox
  SET status='pending', attempts=?, last_error=?, next_attempt_at_ms=?
  WHERE id=?`;

// FAIL_SQL parks a row that no retry can fix.
// @internal
const FAIL_SQL = `UPDATE event_outbox
  SET status='failed', attempts=?, last_error=?
  WHERE id=?`;

// OutboxRow is a row selected from event_outbox.
// @internal
interface OutboxRow {
	id: string;
	name: string;
	payload: Buffer;
	payload_json: Buffer;
	contract_hash: string;
	partition_key: string;
	idempotency_key: string;
	fire_and_forget: number;
	headers: string;
	occurred_at_ms: number;
	enqueued_at_ms: number;
	status: string;
	attempts: number;
	last_error: string;
	next_attempt_at_ms: number;
	x_sb_trace: string;
}

// @public — см. ./README.md
export interface DrainerDeps {
	storage: Storage;
	rpcClient: EventsClient;
	identity: () => Identity | null;
	batchSize: number;
	logger: Logger;
	clockFn?: () => number;
	sleepFn?: (ms: number, signal: AbortSignal) => Promise<void>;
	// Invoked when the runtime rejects a publish for policy reasons
	// (PUBLISH_STATUS_REJECTED_FORBIDDEN). Lets the owner surface a
	// `policy_violation` event for an async (outbox) denial that can't be
	// thrown back to the publish() caller.
	onPolicyViolation?: (v: {
		declaration: string;
		value: string;
		denySide: string;
		reason: string;
	}) => void;
}

// Drainer reads pending rows from the SQLite outbox, publishes them to the
// runtime via Events.Publish, and updates their status based on the response.
// Edge-triggered: kick() schedules an immediate wakeup from the wait state.
// @public — см. ./README.md
export class Drainer {
	private readonly deps: DrainerDeps;
	private readonly clockFn: () => number;
	private readonly sleepFn: (ms: number, signal: AbortSignal) => Promise<void>;

	private running = false;
	private pendingKick = false;
	private wakeResolve: (() => void) | null = null;
	private loopDone: Promise<void> = Promise.resolve();

	constructor(deps: DrainerDeps) {
		this.deps = deps;
		this.clockFn = deps.clockFn ?? (() => Date.now());
		this.sleepFn = deps.sleepFn ?? defaultSleep;
	}

	// start begins the background drain loop.
	start(): void {
		if (this.running) return;
		this.running = true;
		this.loopDone = this.loop();
	}

	// kick wakes up the drainer immediately if it is waiting.
	// Edge-triggered: if already running an iteration, marks pendingKick
	// so the next wait is also skipped.
	kick(): void {
		this.pendingKick = true;
		if (this.wakeResolve) {
			const resolve = this.wakeResolve;
			this.wakeResolve = null;
			resolve();
		}
	}

	// stop signals the loop to exit and waits for the current iteration to finish.
	async stop(): Promise<void> {
		this.running = false;
		this.kick(); // wake up from any wait
		await this.loopDone;
	}

	private async loop(): Promise<void> {
		while (this.running) {
			const now = this.clockFn();
			const { storage, rpcClient, identity, batchSize } = this.deps;

			// Select pending rows whose next_attempt_at_ms is due.
			const rows = storage
				.prepare(SELECT_DUE_SQL)
				.all(now, batchSize) as OutboxRow[];

			if (rows.length === 0) {
				if (!this.running) break;
				await this.waitForWork();
				continue;
			}

			// Claim: mark rows as inflight so a crash-restart resets them.
			const ids = rows.map((r) => r.id);
			const placeholders = ids.map(() => "?").join(",");
			storage
				.prepare(
					`UPDATE event_outbox SET status='inflight' WHERE id IN (${placeholders})`,
				)
				.run(...ids);

			// Build and send PublishRequest.
			const ident = identity();
			const events = rows.map((r) => ({
				id: r.id,
				name: r.name,
				payload: r.payload,
				payloadJson: r.payload_json ?? Buffer.alloc(0),
				contractHash: r.contract_hash,
				partitionKey: r.partition_key,
				idempotencyKey: r.idempotency_key,
				fireAndForget: false,
				headers: (() => {
					try {
						return JSON.parse(r.headers) as Record<string, string>;
					} catch {
						return {};
					}
				})(),
				occurredAtUnixMs: r.occurred_at_ms,
				xSbTrace: r.x_sb_trace ?? "",
			}));

			let results: {
				eventId: string;
				status: PublishStatus;
				message: string;
			}[] = [];
			let netError: Error | null = null;

			try {
				const response = await new Promise<{
					results: {
						eventId: string;
						status: PublishStatus;
						message: string;
					}[];
				}>((resolve, reject) => {
					rpcClient.publish(
						{
							publisherServiceId: ident?.serviceId ?? "",
							publisherInstanceId: ident?.instanceId ?? "",
							events,
						},
						(err, res) => {
							if (err) reject(err);
							else
								resolve(
									res ?? {
										results: [],
									},
								);
						},
					);
				});
				results = response.results;
			} catch (err) {
				netError = err instanceof Error ? err : new Error(String(err));
			}

			if (netError) {
				// The runtime is unreachable. Transport failure says nothing about
				// the event itself, so it never burns a terminal budget: rows are
				// re-armed with the (saturating) backoff ladder and retried for as
				// long as the outbox lives. That is the whole point of the outbox.
				const message = netError.message;
				storage.transaction(() => {
					for (const row of rows) {
						this.rearm(row, message);
					}
				});
				this.deps.logger.error(
					`drainer: publish of ${rows.length} event(s) failed, will retry — ${message}`,
				);
				// Continue loop — will respect backoff via next_attempt_at_ms filter.
				continue;
			}

			// Process per-event results. One transaction for the whole batch: a
			// per-row autocommit would append a WAL frame and a commit record per
			// event.
			const resultMap = new Map(results.map((r) => [r.eventId, r]));
			const doneIds: string[] = [];
			const violations: {
				declaration: string;
				value: string;
				denySide: string;
				reason: string;
			}[] = [];
			let transientCount = 0;

			storage.transaction(() => {
				for (const row of rows) {
					const entry = resultMap.get(row.id);
					const status =
						entry?.status ?? PublishStatus.PUBLISH_STATUS_UNSPECIFIED;

					switch (status) {
						case PublishStatus.PUBLISH_STATUS_ACCEPTED:
						case PublishStatus.PUBLISH_STATUS_REJECTED_DUPLICATE:
							// Success or idempotent duplicate — delete from outbox.
							doneIds.push(row.id);
							break;

						case PublishStatus.PUBLISH_STATUS_REJECTED_INVALID_NAME:
							// Terminal: the name will not become valid on a retry.
							storage
								.prepare(FAIL_SQL)
								.run(row.attempts + 1, `rejected:${status}`, row.id);
							break;

						case PublishStatus.PUBLISH_STATUS_REJECTED_FORBIDDEN: {
							// Policy denial (event.publish gate). Terminal — retrying
							// won't help; mark failed and surface a policy_violation so
							// the async denial is observable (publish() is fire-and-forget
							// into the outbox, so it can't be thrown back to the caller).
							const reason = entry?.message || "event.publish denied by policy";
							storage
								.prepare(FAIL_SQL)
								.run(row.attempts + 1, `forbidden:${reason}`, row.id);
							violations.push({
								declaration: "event.publish",
								value: row.name,
								denySide: "self_egress",
								reason,
							});
							break;
						}

						default:
							// Unspecified means the runtime could not say anything about
							// this event — transient, same infinite-retry treatment as a
							// transport failure.
							transientCount++;
							this.rearm(row, "unspecified");
							break;
					}
				}

				if (doneIds.length > 0) {
					const donePlaceholders = doneIds.map(() => "?").join(",");
					storage
						.prepare(
							`DELETE FROM event_outbox WHERE id IN (${donePlaceholders})`,
						)
						.run(...doneIds);
					storage.adjustOutboxRowCount(-doneIds.length);
				}
			});

			// Observers run after commit: onPolicyViolation publishes an event of
			// its own, which would re-enter storage.transaction().
			if (transientCount > 0) {
				this.deps.logger.warn(
					`drainer: UNSPECIFIED status for ${transientCount} event(s) — treating as transient`,
				);
			}
			for (const violation of violations) {
				this.deps.logger.warn(
					`drainer: event ${violation.value} rejected — ${violation.reason}`,
				);
				this.deps.onPolicyViolation?.(violation);
			}

			// If kick was requested during this iteration, loop immediately.
			if (this.pendingKick) {
				this.pendingKick = false;
			}
		}
	}

	// rearm schedules the row for another attempt on the backoff ladder. The
	// last rung repeats forever — the runtime being down must not consume a
	// delivery budget.
	private rearm(row: OutboxRow, lastError: string): void {
		const attempts = row.attempts + 1;
		const base = BACKOFF_MS[attempts - 1] ?? MAX_BACKOFF_MS;
		const jitter = Math.floor(base * 0.25 * (Math.random() * 2 - 1));
		this.deps.storage
			.prepare(RETRY_SQL)
			.run(attempts, lastError, this.clockFn() + base + jitter, row.id);
	}

	// waitForWork blocks until kick() fires or the earliest deferred row is due.
	// With nothing pending it waits for kick() alone — an idle SDK schedules no
	// timers at all.
	private async waitForWork(): Promise<void> {
		if (this.pendingKick) {
			this.pendingKick = false;
			return;
		}

		const next = this.deps.storage.prepare(SELECT_NEXT_ATTEMPT_SQL).get() as {
			next_at_ms: number | null;
		} | null;
		const nextAtMs = next?.next_at_ms ?? null;
		if (nextAtMs !== null && nextAtMs <= this.clockFn()) return;

		const controller = new AbortController();
		await new Promise<void>((resolve) => {
			this.wakeResolve = resolve;
			if (nextAtMs !== null) {
				void this.sleepFn(nextAtMs - this.clockFn(), controller.signal).then(
					resolve,
				);
			}
		});
		// Cancels the pending timer when kick() won the race.
		controller.abort();
		this.wakeResolve = null;
		this.pendingKick = false;
	}
}

// defaultSleep resolves after ms, or immediately when the signal aborts,
// clearing the timer either way.
// @internal
function defaultSleep(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise<void>((resolve) => {
		const timer = setTimeout(resolve, ms);
		signal.addEventListener(
			"abort",
			() => {
				clearTimeout(timer);
				resolve();
			},
			{ once: true },
		);
	});
}
