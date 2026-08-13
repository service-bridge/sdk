# Jobs

← [Workflows](./workflows.md) · Дальше: [Тестирование](./testing.md) →

Runtime-triggered scheduled tasks: cron, delayed, interval. Handler выполняется на стороне SDK-сервиса, триггер живёт в runtime.

> **Не путать с Workflows.** Jobs — это **одношаговый трекер** (один handler, одна попытка плюс retry). Workflows — это **многошаговый durable процесс** с явным `start()` от вызывающей стороны и compensation flow. Внутри handler'а job можно сделать `sb.rpc.call(...)`, `sb.event.publish(...)` или `sb.workflow.start(...)`.

## Содержание

- [Концепция](#концепция)
- [Регистрация](#регистрация)
- [Триггеры](#триггеры)
- [Handler context](#handler-context)
- [Идемпотентность handler-а](#идемпотентность-handler-а)
- [At-least-once delivery, retry, DLQ](#at-least-once-delivery-retry-dlq)
- [Catchup policy (пропущенные тики после простоя)](#catchup-policy)
- [Overlap policy (что если предыдущий run ещё бежит)](#overlap-policy)
- [Declared deps + access policy](#declared-deps--access-policy)
- [Multi-replica семантика](#multi-replica-семантика)
- [DST (переход на летнее/зимнее время)](#dst)
- [Persistence и выживание рестарта runtime](#persistence-и-выживание-рестарта-runtime)
- [Лимиты и quotas](#лимиты-и-quotas)
- [Observability](#observability)
- [Out-of-scope v1](#out-of-scope-v1)
- [Шпаргалка](#шпаргалка)

## Концепция

- **Runtime триггерит** — это единственный source of truth по расписанию. Cron-выражения, delayed run_at и interval хранятся в Postgres и фигурируют в Service Map.
- **SDK выполняет** — handler работает на стороне твоего сервиса. Можно запускать сколько угодно реплик одного сервиса; runtime гарантирует, что каждый тик доставляется **ровно одной живой реплике** (lease + heartbeat).
- **Persistent state** — schedules переживают рестарт runtime. Delayed-job, заведённый сегодня на завтра, сработает даже если runtime между временами перезагрузится.

## Регистрация

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(url, serviceKey);

sb.job.handle(
  "daily-report",
  {
    trigger: { cron: "0 9 * * *", tz: "Europe/Moscow" },
    catchup: "fire_once",
    overlap: "skip",
    deps: [{ rpc: "billing.GenerateReport" }],
    maxAttempts: 5
  },
  async (ctx) => {
    await sb.rpc.call("billing", "GenerateReport", { date: ctx.localScheduledAt });
  },
)

await sb.start();
```

`sb.job.handle(...)` **должен** вызываться ДО `sb.start()`. Дубликат имени job выбрасывает исключение сразу при регистрации.

## Триггеры

| Тип | Когда срабатывает | Пример |
|---|---|---|
| `cron` | 5-полевой cron, **минимум 1 минута** между фаерами. Опциональная IANA-tz (default `UTC`). | `{ cron: "*/15 * * * *" }` — каждые 15 минут |
| `delayed` | Один раз в конкретный момент (UTC ms / Date / ISO string). | `{ delayed: { at: Date.now() + 60_000 } }` |
| `interval` | Каждые N ms (минимум 100ms, конфигурируемо). | `{ interval: 5_000 }` — каждые 5 секунд |

6-полевой cron (с секундами) **не поддерживается** — для суб-минутных интервалов используется `interval`.

## Handler context

Handler принимает один аргумент `ctx: JobHandlerCtx`:

```ts
async (ctx) => {
  ctx.jobName;          // string — имя job
  ctx.executionId;      // string — id этого execution
  ctx.scheduledAt;      // Date — UTC момент фаера (из scheduled_at_unix_ms)
  ctx.localScheduledAt; // Date — то же в tz cron-а (или UTC для interval/delayed)
  ctx.attempt;          // number — 1, 2, 3, ... счётчик попыток
  ctx.idempotencyKey;   // string — stable per (def, scheduled_at)
  ctx.signal;           // AbortSignal — aborts при потере lease / реконнекте
}
```

Trace-контекст на `ctx` **не приходит** — он распространяется автоматически. Любой `sb.rpc.call` / `sb.event.publish` / `sb.workflow.start`, сделанный внутри handler-а, наследует входящий trace (runtime прокидывает его через `X-SB-Trace`, SDK разворачивает в AsyncLocalStorage). Тебе ничего передавать руками не нужно.

## Идемпотентность handler-а

> ⚠️ **Handler ОБЯЗАН быть идемпотентен по `ctx.idempotencyKey`, НЕ по `ctx.attempt`.**

ServiceBridge доставляет каждый тик **at-least-once**. Один и тот же `ctx.idempotencyKey` может прийти несколько раз — после lease expiry, после краша SDK-реплики, после network partition. `ctx.attempt` диагностический, не для дедупликации.

Безопасный паттерн:

```ts
async (ctx) => {
  // Effects-таблица с UNIQUE на idempotency_key.
  const result = await db.query(`
    INSERT INTO daily_reports (idempotency_key, report_date, payload)
    VALUES ($1, $2, $3)
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING id
  `, [ctx.idempotencyKey, ctx.localScheduledAt, payload]);

  if (result.rows.length === 0) {
    // Дубль — уже обработали этот тик в предыдущей попытке. Ок, выходим.
    return;
  }
  // ... continue with side effects
};
```

Опасный паттерн:

```ts
async (ctx) => {
  if (ctx.attempt === 1) {
    await chargeCustomer(ctx.localScheduledAt); // ❌ дубль на retry!
  }
};
```

## At-least-once delivery, retry, DLQ

Жизненный цикл execution:

1. Scheduler вычисляет `next_fire_at`, в нужный момент INSERT-ит execution `status='pending'`.
2. Dispatcher claim-ит через `FOR UPDATE SKIP LOCKED`, ставит `lease_expires_at = now + leaseTtlMs`, push-ит в живую SDK-реплику.
3. SDK исполняет handler, после успеха шлёт `JobResult{success}` → `status='success'`.
4. На исключение в handler-е:
   - Если retryable (default `true`) и `attempt < maxAttempts` → пауза по retry_policy, `status='pending'`, increment `attempt`.
   - Иначе → `status='dead_letter'`, запись в `jobs_dlq`.
5. Если SDK падает или теряет связь до ACK — lease истекает (`leaseTtlMs`) или heartbeat прерывается (15s) → execution возвращается в `pending`, dispatch на другую реплику.

Retry-policy задаётся per-job (default: exponential 1s → 600s, multiplier=2, jitter=25%).

Чтобы пометить ошибку как **не**-retryable (сразу в DLQ):

```ts
async (ctx) => {
  if (badInput) {
    const err = new Error("bad input");
    (err as any).retryable = false;
    throw err;
  }
};
```

## Catchup policy

Когда runtime простоял дольше cron/interval — что делать с пропущенными тиками?

| Policy | Поведение | Когда выбирать |
|---|---|---|
| `skip` (**default**) | Пропущенные тики **игнорируются**. Возобновляется со следующего будущего тика. | Большинство кейсов — daily-report-у не нужно прислать 47 счетов после 2-дневного даунтайма. |
| `fire_once` | Ровно один "догоняющий" вызов сразу после восстановления. | Когда важно отметить «пропустили окно, давай хотя бы один прогон». |
| `fire_all` | Все пропущенные тики (до cap'а). | Backfill data-pipeline где **каждый** тик имеет значение. |

`fire_all` ограничен per-job cap'ом (default 10 000) и глобальным budget'ом runtime (default 50 000 events на recovery) — без этого 6-часовой outage с секундной точностью тиков взорвал бы Postgres. Превышение → метрики `jobs_catchup_truncated_total` / `jobs_catchup_starvation_total`.

## Overlap policy

Что если **следующий** тик наступает, а **предыдущий** ещё бежит?

| Policy | Поведение |
|---|---|
| `skip` (**default**) | Новый тик не запускается, пропускается. |
| `buffer_one` | Один тик может стоять в очереди следом за running. Остальные пропускаются. |
| `allow` | Запускается параллельно. Ограничено `maxConcurrent` (если не задан — берётся runtime-настройка `jobs.max_concurrent_per_job`, default 1). |

```ts
sb.job.handle(
  "heavy-etl",
  {
    trigger: { interval: 60_000 },
    overlap: "allow",
    maxConcurrent: 5, // до 5 параллельных запусков
  },
  async (ctx) => { /* ... */ },
)
```

## Declared deps + access policy

Если handler делает downstream-вызовы, объяви их в `deps` — runtime проверяет ACL на регистрации:

```ts
sb.job.handle(
  "send-reminders",
  {
    trigger: { cron: "0 8 * * *" },
    deps: [
    { rpc: "notifications.SendEmail" },
    { event: "reminders.sent" },
    { workflow: "send-reminder-flow" },
    ]
  },
  async (ctx) => { /* ... */ },
)
```

Сервис должен иметь:
- `job.handle` capability — без неё runtime отвергает регистрацию jobs.
- egress-правило на каждый объявленный dep (`rpc.call` на `notifications.SendEmail`, `event.publish` на `reminders.sent` и т.д.).

`job.handle` проверяется на регистрации: без неё `sb.start()` падает. Egress по `deps` проверяется в момент **выполнения** job-а — это та же egress-модель, что у шага workflow.

Jobs — **self-only**: их триггерит только сам runtime по расписанию, нет inbound-вызова от других сервисов. Поэтому у job нет ingress-политики — наружу торчит только то, что handler сам вызывает через объявленные deps. Подробнее в [access-policy.md](./access-policy.md).

## Multi-replica семантика

Развернул 3 реплики одного сервиса — runtime гарантирует:

- Job-definition одна на сервис (upsert по `(service_id, name)`).
- Каждый тик = одна execution row = один handler-вызов одновременно. Round-robin по живым репликам.
- Реплика крашится мид-handler → heartbeat (5s tick, 15s timeout) детектит → execution возвращается в pending → другая реплика подхватит с `ctx.attempt += 1`, тот же `ctx.idempotencyKey`.

Это автоматически — никакого distributed lock на стороне SDK не нужно.

## DST

Cron с tz `America/New_York` в ночь fall-back: локальное `02:30` бывает два раза подряд (EDT → EST). Runtime **fire once по local time**: ровно один execution на «локальный момент», второй проход подавляется UNIQUE-индексом `(def_id, local_scheduled_at)`.

```ts
sb.job.handle(
  "ny-daily",
  {
    trigger: { cron: "0 2 * * *", tz: "America/New_York" }
  },
  async (ctx) => { /* ровно один вызов в день, в т.ч. в дни DST */ },
)
```

Spring-forward (02:30 не существует): пропускается — handler в этот день не вызывается. Это поведение `robfig/cron/v3` + `time.LoadLocation` стандартное.

## Persistence и выживание рестарта runtime

| Таблица | Что хранит |
|---|---|
| `job_definitions` | имя, trigger, политики, deps, retry-policy |
| `job_schedules` | next_fire_at, last_fire_at — per-definition state |
| `job_executions` | каждая попытка: status, attempt, lease, trace_id, idempotency_key |
| `jobs_dlq` | execution_id + last_error для job'ов, исчерпавших retries |

Idempotency key каждой execution = `def_id ':' scheduled_at_unix_micros`. UNIQUE на этом ключе **математически** исключает двойной enqueue одного тика — защита от clock-skew и enqueue-гонок при рестарте runtime.

При рестарте runtime:
1. `ReclaimAllInFlight` — все `in_flight` сразу возвращаются в `pending` (instance_id-ы больше не валидны).
2. `LoadSchedules` — восстановление in-memory heap из DB.
3. `CatchupPolicy` применяется к пропущенным тикам.
4. Принимаем gRPC-соединения от SDK.

## Лимиты и quotas

Лимиты — это runtime-настройки в таблице `runtime_settings` (секция `jobs`), редактируются в UI-консоли на `/settings`. Это **не** env-переменные.

| Параметр | Setting key | Default |
|---|---|---|
| Max jobs per service | `jobs.max_per_service` | 100 |
| Min cron interval | — (forced 5-field parser-ом) | 1 минута |
| Min interval (ms) | `jobs.min_interval_ms` | 100 |
| Max concurrent per job (overlap=allow) | `jobs.max_concurrent_per_job` | 1 |
| Register rate per service | `jobs.register_rate_per_min` | 10/min |
| Catchup global budget | `jobs.catchup_budget` | 50 000 |
| Catchup per-job cap | `jobs.catchup_per_job` | 10 000 |
| Heartbeat interval | `jobs.heartbeat_interval_ms` | 5000 (5s) |
| Heartbeat timeout | `jobs.heartbeat_timeout_ms` | 15000 (15s) |

Превышение лимитов на регистрацию → gRPC-ошибка от `sb.start()`.

## Observability

Runtime ведёт по каждому job in-memory счётчики (ключ `service:name:...`):

- enqueued — по `service:name:trigger_kind`
- completed — по `service:name:status`, где `status` ∈ `success` | `dead_letter`
- in-flight — gauge по `service:name`
- lease-expirations — по `service:name`
- catchup-truncated / catchup-starvation — по `service:name`
- stale-ack-dropped — по `service:name` (epoch-fenced поздно пришедший ACK)
- dispatch-latency — aggregate `count` / `sum_ms` / `max_ms` по `service:name`

Главный сигнал для трейсинга: trace-context рождается на enqueue в runtime, передаётся в SDK через `X-SB-Trace`, разворачивается в AsyncLocalStorage вокруг handler-а и автоматически наследуется всеми downstream `sb.rpc` / `sb.event` / `sb.workflow` вызовами. На `ctx` он не выставляется.

В Service Map jobs появляется как producer-узел: `[scheduler] → jobs.<service>.<name> → handler`.

## Out-of-scope v1

Намеренно **не** входит в v1 (можно добавить позже без breaking change):

- Manual trigger через API (`TriggerJob` RPC)
- Pause / resume schedule
- Dynamic single-job schedule update (нужно re-register всего набора)
- `unique` business-key (дедуп на уровне бизнес-ключа поверх idempotency)
- `buffer_all` overlap (буфер из >1 pending)
- `runOnStart` опция (отдельная семантика от catchup)
- Fan-out на ВСЕ реплики (для broadcast — используй Event Publish)

## Шпаргалка

```ts
// Cron daily.
sb.job.handle(
  "daily-report",
  {
    trigger: { cron: "0 9 * * *", tz: "Europe/Moscow" },
    catchup: "fire_once",
    deps: [{ rpc: "billing.GenerateReport" }]
  },
  async (ctx) => {
    await sb.rpc.call("billing", "GenerateReport", { date: ctx.localScheduledAt });
  },
)

// Delayed one-shot.
sb.job.handle(
  "send-reminder",
  {
    trigger: { delayed: { at: Date.now() + 24 * 3600_000 } }
  },
  async (ctx) => { /* ... */ },
)

// Interval polling.
sb.job.handle(
  "poll-status",
  {
    trigger: { interval: 5_000 },
    overlap: "skip"
  },
  async (ctx) => {
    if (ctx.signal.aborted) return;
    await sb.rpc.call("status", "Check", {});
  },
)

await sb.start();
```

→ Дальше: [Тестирование](./testing.md)
