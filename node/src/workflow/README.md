# workflow

## Зона ответственности

Domain namespace для durable workflows. Покрывает обе стороны контракта:

- **Owner side** — `handle(name, def, opts?)` регистрирует workflow в Registry,
  canonicalize'ит граф (ADR-W-002), считает `contract_hash` (SHA-256 от
  canonical JSON) и пишет его как `contractHashOverride` записи Registry.
  `WorkflowSubscriber` + thin `run` выполняют присланные runtime'ом
  RunAssignment.
- **Caller side** — `start / signal / cancel / await / query / replay`
  — gRPC-операции к `Workflows.*`. Доступны после `ServiceBridge.start()`
  (до привязки канала бросают).

Не реализует ни ретраи, ни backoff, ни circuit breaker — runtime
единственный source of truth для recovery (ADR-W-018: thin runner).

Модуль не имеет своего `index.ts`. Наружу из пакета реэкспортируются только
`WorkflowDomain` (тип) и `WorkflowAccessDeniedError` (через корневой
`index.ts` SDK). Остальные экспорты модуля видны лишь его внутренним
потребителям (`connection/service-bridge`).

## Публичный контракт

Публичная поверхность пакета — методы `WorkflowDomain` и
`WorkflowAccessDeniedError`.

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `WorkflowDomain.handle(name, def, opts?)` | `void` | — | Регистрирует workflow. `validate(def, {workflowName: name})` → `canonicalize` → `fingerprint` → запись в Registry как `METHOD_TYPE_WORKFLOW` с canonical-JSON графа в `inputSchemaJson` и fingerprint'ом в `contractHashOverride`. `opts?: WorkflowHandlerOpts` (`{ input? }`) — input-schema; при наличии перебивает `def.input`. |
| `WorkflowDomain.start(name, input, opts?)` | `Promise<{runId}>` | — | Запускает новый run. `input` JSON-кодируется (`null`, если undefined). Trace context из ALS форматируется в `x_sb_trace`; пустая строка → runtime минтит свежий root. PERMISSION_DENIED → `WorkflowAccessDeniedError` (+ `policy_violation`), NOT_FOUND → `WorkflowNotFoundError`. |
| `WorkflowStartOpts.idempotencyKey` | `string` | `""` | Дедупликация запуска на стороне runtime. |
| `WorkflowStartOpts.timeoutSec` | `number` | `0` | Wall-clock timeout run'а в секундах; `0` = без override. |
| `WorkflowStartOpts.parentRunId` | `string` | `""` | Помечает запуск как sub-workflow родителя (динамическая cycle-detection, ADR-W-019). Проставляется раннером для `type:"workflow"` step'ов; вызывающий обычно оставляет undefined. |
| `WorkflowDomain.signal(runId, signalName, payload)` | `Promise<void>` | — | Внешний signal к parked `wait_signal` step. `payload` JSON-кодируется. FAILED_PRECONDITION → `WorkflowTerminalError`. |
| `WorkflowDomain.cancel(runId)` | `Promise<void>` | — | Cooperative cancel — runtime переводит run в `compensating`, dispatcher отрабатывает compensation steps в reverse order. FAILED_PRECONDITION → `WorkflowTerminalError`. |
| `WorkflowDomain.await(runId)` | `Promise<Record<string, unknown>>` | — | Server-streaming RPC `Workflows.Await`. Держит последний `RunStatusUpdate`; на `end` резолвится распарсенным final state **только** при терминальном статусе `success`. Любой другой терминальный статус (`failed` / `cancelled` / `failed_compensated`) → reject с сообщением `terminal status "<status>"`. Пустой стрим → reject. |
| `WorkflowDomain.query(runId)` | `Promise<{status, state, steps[]}>` | — | Point-in-time snapshot: `status` (строка run-статуса), `state` (распарсенный map), `steps[]` (`stepId`, `status`, `output`, `lastError`, `compensatedBy?`). `compensatedBy` — step_id compensation step'а, заполнен только для шагов со статусом `compensated`. |
| `WorkflowDomain.replay(runId, opts?)` | `Promise<{runId}>` | — | Forks new run из `runId`. `opts.fromStepId` — точка fork'а (`""` = с начала). Reuse frozen plan source run'а. |
| `WorkflowAccessDeniedError` | класс | — | gate #5 bilateral check на `Workflows.Start` (PERMISSION_DENIED). Поля: `workflowName`, `reason`. Эмитит `policy_violation` (`declaration:"workflow.run"`, `denySide:"self_egress"`). |

## Приватный контракт

Экспорты модуля, не выходящие за пределы пакета (потребитель —
`connection/service-bridge`), и test-only элементы.

| Имя | Тип | Что делает |
|-----|-----|------------|
| `WorkflowDomain._attachRpc(rpc)` | метод (`@internal`) | Вызывается `ServiceBridge.start()` с готовым `WorkflowsClient`. До вызова caller-side операции бросают `Error` («RPC channel not yet attached»). |
| `WorkflowNotFoundError` | класс | NOT_FOUND от `Workflows.Start`. Поле `workflowName`. |
| `WorkflowTerminalError` | класс | FAILED_PRECONDITION — операция (signal/cancel) над терминальным run. Поля: `runId`, `status`. |
| `WorkflowDef` | type | `{ input?: SchemaShape, steps: Step[], retry?: Partial<RetryOpts>, maxParallelism?: number, timeoutSec?: number }`. |
| `Step` | union type | `CallStep` \| `PublishStep` \| `SleepStep` \| `WaitEventStep` \| `WaitSignalStep` \| `WorkflowStep` \| `ParallelStep` \| `SequenceStep` \| `LocalStep`. `type`: `call`, `publish`, `sleep`, `wait_event`, `wait_signal`, `workflow`, `parallel`, `sequence`, `local`. `forEach` — поле `parallel`/`sequence` (`ForEachSpec`), а не отдельный тип step'а. |
| `JsonExpression` / `Predicate` | type | Declarative expr: `$.input.foo` / `$.list[*].field` / `{literal}` / literal JSON. `Predicate` — `when`-клаузы (`not`/`equals`/`in`/`and`/`or`). |
| `canonicalize(value)` | функция | Лексикографическая сортировка ключей, undefined/функции опускаются; детерминированный JSON для `contract_hash`. |
| `fingerprint(value)` | функция | SHA-256-hex от canonical JSON. |
| `validate(def, opts)` | функция | Graph-control: id-формат `^[a-z0-9_]+$` + уникальность, `waitFor` acyclic + ссылки на известные id, depth ≤ 16 / steps ≤ 512, `compensate` только на `call`/`publish`, `forEach.from`/`forEach.as` валидны, workflow self-ref check (`opts.workflowName`), синтаксис всех `$.…`. Не валидирует `opts` step'а — это compile-time. Бросает `WorkflowValidationError`. |
| `evalPath` / `evalLiteralOrPath` / `evalPredicate` | функции | Runtime-вычисление JSONPath-lite и предикатов против state. Missing path → `undefined` (не ошибка); синтаксис-ошибка → `JsonPathError`. |
| `run(steps, ctx, deps)` | функция | Thin runner. forward: dependency-scheduling по `waitFor` с `ctx.maxParallelism` (0 = unlimited); per-step `beginStep` → eval → `dispatch` (один `sb.<domain>.<op>` или `park`) → `completeStep`/`failStep`. `ctx.compensating` → reverse traversal через `buildCompensateOpts`. Park-step бросает `RunnerParkedError`. Никаких внутренних retry/backoff/CB. |
| `RunContext` | `{runId, leaseEpoch, state, compensating, maxParallelism}` | Контекст одного прогона раннера. |
| `SbDomains` / `RuntimeOps` | interface | Поверхности, от которых зависит раннер: `{rpc.call, event.publish, workflow.start/await}` и runtime-checkpoint (`beginStep`/`completeStep`/`failStep`/`park`/`completeRun`). |
| `RunnerDeps.wrapStep` | `<T>(info: StepSpanInfo, fn) => Promise<T>` (optional) | Hook вокруг КАЖДОЙ исполняемой единицы: каждый step, каждая fanout-группа (`role:"group"`), каждая ветка fanout (`role:"branch"`, только при `forEach`), каждая компенсация (`role:"compensation"`). Subscriber открывает один `USER.SUBOP` op (parent = текущий trace context) и выполняет `fn` в `childContext(parent, spanOpId)` — это превращает плоский trace в дерево `run → step → op`. Meta: `step_id`, `step_name`; для компенсации `is_compensation`+`compensates_for_step_id=<forward step.id>`. Без hook'а единица выполняется без span/scope. |
| `StepSpanInfo` | `{runId, stepId, stepName, role, isCompensation?, compensatesForStepId?}` | Идентичность step span, передаваемая в `wrapStep`. `role`: `step`\|`group`\|`branch`\|`compensation`. |
| `RunnerParkedError` | класс | Сигнал раннера, что step запарковался (sleep/wait_event/wait_signal) — runtime возобновит run. |
| `WorkflowSubscriber` | класс (`@internal`) | Консьюмер `Workflows.Subscribe` + lease-heartbeat'ы. Жизненный цикл стрима держит `registry/StreamSupervisor` (лестница `[1s,5s,15s,30s,60s]`, ±20% jitter). Подменяет wire-`frozenPlan` локально зарегистрированным графом (восстановить `local.fn`). Парсит `RunAssignment.xSbTrace` через `parseXSbTrace` и оборачивает выполнение в `runWithTrace(parsed, fn)` (ALS seed run-root scope). По завершении вычисляет `terminalStatus`: forward → `success`; `compensating` + `cancelReason === "step_failure"` → `failed_compensated`; иначе → `cancelled`; и шлёт `CompleteRun`. WORKFLOW.RUN op эмитит runtime сам (ADR 0007 §5/ADR 0003 §1). |
| `WorkflowSubscriber.start()` | `() => void` | Открывает стрим через supervisor и запускает единственный sweep-таймер (10 с, `unref`), который сверяет identity и рассылает heartbeat по всем живым lease. |
| `WorkflowSubscriber.close()` | `() => void` | Гасит sweep-таймер и pending-таймер реконнекта, `cancel()` стрима, сбрасывает все lease. |
| `SubscriberDeps.identity` | `() => SubscriberIdentity \| null` | Резолвится на КАЖДЫЙ `Subscribe` и КАЖДЫЙ heartbeat. `null` → стрим не открывается, повтор по лестнице. |
| `SubscriberDeps.reconnectOpts` | `ReconnectDelayOptions?` | Тестовый hook: пиннит лестницу/jitter, чтобы reconnect наблюдался за миллисекунды. |
| `SubscriberIdentity` | `{serviceId, instanceId}` (`@internal`) | Идентичность сессии, к которой привязаны стрим и lease-heartbeat'ы. |
| `makeRuntimeOps(rpc, getInstanceId)` | функция (`@internal`) | Адаптер `WorkflowsClient` → `RuntimeOps`. Весь JSON-encoding bridge без бизнес-логики. |

## Архитектурные решения и почему

- **Thin runner (ADR-W-018).** `run` — только композиция `eval →
  sb.<domain>.<op> → Complete/FailStep`. Никаких retry / backoff /
  maxAttempts / circuit-breaker в теле раннера — runtime владеет recovery.
  Единственные вхождения этих слов в `runner.ts` — комментарии, явно
  фиксирующие это инвариантное отсутствие.
- **Canonical graph + contract_hash (ADR-W-002).** Один и тот же граф у
  разных инстансов даёт идентичный fingerprint. Runtime отвергает
  регистрацию с тем же `name` и другим `contract_hash` — это либо
  переименование, либо реальный contract drift. Wire-`frozenPlan` нужен
  runtime для canonicalization; SDK исполняет собственную копию графа, чтобы
  сохранить `local.fn`-замыкания (их съедает `JSON.stringify`).
- **Caller-side ops через единый `WorkflowsClient`.** Тот же gRPC-клиент
  использует subscriber. Один канал — одно управление creds на cert
  rotation.
- **`_attachRpc` lazy wiring.** Domain создаётся в конструкторе SB до
  открытия mTLS-канала; `start()` подцепляет реальный `WorkflowsClient`.
  Тесты домена работают без gRPC через мок.
- **`await` резолвится только на `success`.** Стрим `Workflows.Await` шлёт
  и промежуточные апдейты (пустой state), и финальный (со state). SDK
  держит последний и на `end` различает: `success` → resolve(final state);
  `failed`/`cancelled`/`failed_compensated` → reject. Run-статусы — единый
  словарь (`pending`/`running`/`waiting`/`success`/`failed`/`cancelling`/
  `cancelled`/`compensating`/`failed_compensated`); нет `completed`/
  `succeeded`/`compensated` на уровне run (`compensated` — статус step'а).
- **TemplatableOpts (ADR-W-018).** Опции step'ов (`CallStep.opts`,
  `PublishStep.opts`, `WorkflowStep.opts`) композируются над
  `rpc.CallOpts` / `events.PublishOpts` / `workflow.WorkflowStartOpts`.
  Добавление нового поля в нижележащий тип без обновления workflow types
  ломает compile (см. `types.test-d.ts`).
- **`timeoutSec` user-facing в секундах.** Wire-поле `timeout_sec` (uint32,
  ADR-0006 исключение для user-API) — отличается от wall-clock unix-ms
  внутренних полей.
- **Trace propagation (ADR 0006 §3, ADR 0003 §1).** `start()` пушит `X-SB-Trace` из ALS
  в `StartRunRequest.x_sb_trace`. Subscriber парсит `RunAssignment.xSbTrace`
  через `parseXSbTrace` и оборачивает выполнение в `runWithTrace`. Cross-step
  `sb.rpc.call` / `sb.event.publish` / nested `sb.workflow.start` через ALS
  получают канонический X-SB-Trace в свои wire-headers — runtime эмиттит
  RPC.CALL / EVENT.PUBLISH / WORKFLOW.RUN op'ы с правильным `parent_op_id`.
  WORKFLOW.RUN op (root) пишет runtime сам — SDK не дублирует.
- **Автомат реконнекта — `registry/StreamSupervisor`**, общий с events- и
  job-подписчиками. Свой while-цикл по промисам сбрасывал счётчик попыток на
  `resolve()` из `stream.on("end")`, то есть на ЧИСТОМ закрытии стрима — рантайм,
  штатно закрывающий стримы, получал реконнект раз в секунду вечно. Счётчик
  сбрасывает только пришедший `RunAssignment`.
- **`identity` — замыкание, а не значение.** `Control.RefreshCert` выдаёт новый
  `instance_id` на каждую ротацию сертификата. Зафиксированный при
  конструировании id уходил бы в heartbeat'ы инстанса, который рантайм уже
  снёс: lease истекает, `ReclaimExpiredLeases` переназначает прогон, шаги
  выполняются второй раз параллельно с первым раннером. Sweep сверяет
  `instanceId` с тем, с которым открыт стрим, и при расхождении делает
  `supervisor.restart()`.
- **Один sweep-таймер на все прогоны.** `setInterval` на каждый активный прогон
  давал 500 таймеров на 500 прогонов, делающих одно и то же с одним периодом.
  Sweep проходит по map lease'ов; таймер `unref`-нут — heartbeat не должен быть
  причиной, по которой процесс не завершается.
- **`stopHeartbeat(runId, leaseEpoch)` сверяет эпоху.** Переназначение того же
  `runId`, пока предыдущий exec ещё разматывается, поднимает `leaseEpoch`. Без
  сверки `finally` старого exec гасил heartbeat НОВОГО назначения: lease
  истекает → переназначение → живая блокировка.
- **Отказ heartbeat не снимает lease.** И ошибка в callback, и синхронный throw
  логируются, lease остаётся: транзиентный отказ канала не должен молча
  оставлять прогон на реклейм по таймауту без единой строки диагностики.

## Зависимости

Зависит от:
- `registry/registry` — `Registry`, `WorkflowHandlerOpts`,
  `Registry._handle.workflow(...)` (запись graph+hash в Registry).
- `registry/stream-supervisor` — `StreamSupervisor` (жизненный цикл
  Subscribe-стрима); `utils/reconnect-ladder` — тип `ReconnectDelayOptions`.
- `pb/servicebridge/v1/workflows` — gRPC stubs `WorkflowsClient` + все
  request/response типы.
- `rpc/client` — `CallOpts`, `RetryOpts` (база для templatable step opts).
- `events/publisher` — `PublishOpts`, `Logger` interface для subscriber.
- `telemetry/context`, `telemetry/wire-trace` — propagation trace context
  (ALS, `runWithTrace`, format/parse X-SB-Trace).

Используется:
- `connection/service-bridge` — конструирует `WorkflowDomain`, в `start()`
  привязывает `WorkflowsClient` через `_attachRpc`, строит `RuntimeOps` через
  `makeRuntimeOps`, передаёт `wrapStep`-hook и `identity: () => currentIdentity`,
  запускает `WorkflowSubscriber` после первого Welcome (если есть хотя бы один
  workflow handler).
</content>
