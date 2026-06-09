import type { Identity } from "../connection/service-bridge";
import type { EventsClient } from "../pb/servicebridge/v1/events";
import { PublishStatus } from "../pb/servicebridge/v1/events";
import type { Storage } from "../sqlite/storage";
import type { Logger } from "./publisher";

// BACKOFF_MS is the retry delay ladder after network failures (±25% jitter applied).
// @internal
const BACKOFF_MS = [1_000, 5_000, 30_000, 120_000, 600_000] as const;
// MAX_ATTEMPTS before an outbox row is permanently failed.
// @internal
const MAX_ATTEMPTS = 5;

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
	sleepFn?: (ms: number) => Promise<void>;
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
	private readonly sleepFn: (ms: number) => Promise<void>;

	private running = false;
	private pendingKick = false;
	private wakeResolve: (() => void) | null = null;
	private loopDone: Promise<void> = Promise.resolve();

	constructor(deps: DrainerDeps) {
		this.deps = deps;
		this.clockFn = deps.clockFn ?? (() => Date.now());
		this.sleepFn =
			deps.sleepFn ?? ((ms) => new Promise<void>((r) => setTimeout(r, ms)));
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
				.prepare(
					`SELECT * FROM event_outbox
          WHERE status='pending' AND next_attempt_at_ms <= ?
          ORDER BY enqueued_at_ms ASC, id ASC
          LIMIT ?`,
				)
				.all(now, batchSize) as OutboxRow[];

			if (rows.length === 0) {
				if (!this.running) break;
				// Edge-triggered wait: consume pendingKick or wait for kick()/1s timer.
				if (this.pendingKick) {
					this.pendingKick = false;
					continue;
				}
				await Promise.race([
					new Promise<void>((resolve) => {
						this.wakeResolve = resolve;
					}),
					this.sleepFn(1_000),
				]);
				this.wakeResolve = null;
				this.pendingKick = false;
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
				// Network error: retry all inflight rows with backoff.
				const nowRetry = this.clockFn();
				for (const row of rows) {
					const newAttempts = row.attempts + 1;
					if (newAttempts >= MAX_ATTEMPTS) {
						storage
							.prepare(
								`UPDATE event_outbox
                SET status='failed', attempts=?, last_error='max_attempts'
                WHERE id=?`,
							)
							.run(newAttempts, row.id);
					} else {
						const baseDelay =
							BACKOFF_MS[Math.min(newAttempts - 1, 4)] ?? 600_000;
						const jitter = Math.floor(
							baseDelay * 0.25 * (Math.random() * 2 - 1),
						);
						const nextAt = nowRetry + baseDelay + jitter;
						storage
							.prepare(
								`UPDATE event_outbox
                SET status='pending', attempts=?, last_error=?, next_attempt_at_ms=?
                WHERE id=?`,
							)
							.run(newAttempts, netError.message, nextAt, row.id);
					}
				}
				// Continue loop — will respect backoff via next_attempt_at_ms filter.
				continue;
			}

			// Process per-event results.
			const resultMap = new Map(results.map((r) => [r.eventId, r]));
			for (const row of rows) {
				const entry = resultMap.get(row.id);
				const status =
					entry?.status ?? PublishStatus.PUBLISH_STATUS_UNSPECIFIED;

				switch (status) {
					case PublishStatus.PUBLISH_STATUS_ACCEPTED:
					case PublishStatus.PUBLISH_STATUS_REJECTED_DUPLICATE:
						// Success or idempotent duplicate — delete from outbox.
						storage.prepare("DELETE FROM event_outbox WHERE id=?").run(row.id);
						break;

					case PublishStatus.PUBLISH_STATUS_REJECTED_INVALID_NAME:
						// Terminal failures — mark as failed.
						storage
							.prepare(
								`UPDATE event_outbox
                SET status='failed', attempts=?, last_error=?
                WHERE id=?`,
							)
							.run(row.attempts + 1, `rejected:${status}`, row.id);
						break;

					case PublishStatus.PUBLISH_STATUS_REJECTED_FORBIDDEN: {
						// Policy denial (event.publish gate). Terminal — retrying
						// won't help; mark failed and surface a policy_violation so
						// the async denial is observable (publish() is fire-and-forget
						// into the outbox, so it can't be thrown back to the caller).
						const reason = entry?.message || "event.publish denied by policy";
						storage
							.prepare(
								`UPDATE event_outbox
                SET status='failed', attempts=?, last_error=?
                WHERE id=?`,
							)
							.run(row.attempts + 1, `forbidden:${reason}`, row.id);
						this.deps.logger.warn(
							`drainer: event ${row.name} (${row.id}) rejected — ${reason}`,
						);
						this.deps.onPolicyViolation?.({
							declaration: "event.publish",
							value: row.name,
							denySide: "self_egress",
							reason,
						});
						break;
					}

					default:
						// Transient unspecified — treat as retryable.
						this.deps.logger.warn(
							`drainer: UNSPECIFIED status for event ${row.id} — treating as transient`,
						);
						{
							const newAttempts = row.attempts + 1;
							if (newAttempts >= MAX_ATTEMPTS) {
								storage
									.prepare(
										`UPDATE event_outbox
                    SET status='failed', attempts=?, last_error='max_attempts'
                    WHERE id=?`,
									)
									.run(newAttempts, row.id);
							} else {
								const baseDelay =
									BACKOFF_MS[Math.min(newAttempts - 1, 4)] ?? 600_000;
								const jitter = Math.floor(
									baseDelay * 0.25 * (Math.random() * 2 - 1),
								);
								const nextAt = this.clockFn() + baseDelay + jitter;
								storage
									.prepare(
										`UPDATE event_outbox
                    SET status='pending', attempts=?, last_error='unspecified', next_attempt_at_ms=?
                    WHERE id=?`,
									)
									.run(newAttempts, nextAt, row.id);
							}
						}
						break;
				}
			}

			// If kick was requested during this iteration, loop immediately.
			if (this.pendingKick) {
				this.pendingKick = false;
			}
		}
	}
}
