# workflow

## Зона ответственности

Клиентская часть durable workflow: объявление определений (`sb.workflow.handle`), управление прогонами (`start / signal / cancel / await / query / replay / retryCompensation`) и исполнение задач, которые runtime выдаёт инстансу-владельцу (локальная функция, RPC-вызов, публикация события, компенсация). DAG, готовность шагов, выражения, ретраи, таймеры, ожидания, вложенные прогоны и порядок компенсаций интерпретирует runtime (ADR 0003 runtime) — модуль их не реализует.

## Публичный контракт

Наружу (корневой `index.ts`) выходят тип `WorkflowDomain`, типы DSL и ошибки.

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `WorkflowDomain.handle(name, def)` | `void` | — | Объявляет workflow сервиса. Определение кодируется в proto `WorkflowDefinition` и уходит в регистрации (`IncomingMethod.workflow`); runtime проверяет его и считает fingerprint. Повторное объявление имени бросает. Объявлять до `start()`. |
| `WorkflowDomain.start(service, name, input, opts?)` | `Promise<{runId}>` | — | Запуск прогона workflow `name` сервиса `service`. Контекст трассы из ALS уходит в `x_sb_trace`. |
| `WorkflowStartOpts.idempotencyKey` | `string` | `""` | Повторный старт с тем же ключом возвращает существующий прогон. |
| `WorkflowStartOpts.timeoutMs` | `number` | `0` — таймаут определения | Таймаут прогона; по истечении — `timed_out`. |
| `WorkflowDomain.signal(runId, name, payload, opts?)` | `Promise<{duplicate}>` | — | Ставит сигнал в FIFO-очередь прогона. |
| `WorkflowSignalOpts.signalId` | `string` | `""` | Ключ идемпотентности сигнала: повтор с тем же id не ставится в очередь, `duplicate: true`. |
| `WorkflowDomain.cancel(runId)` | `Promise<void>` | — | Останавливает прогон; компенсации выполняются до `cancelled`. |
| `WorkflowDomain.await(runId)` | `Promise<Record<string, unknown>>` | — | Выход прогона (карта состояния) при `success`; иначе `WorkflowRunFailedError`. |
| `WorkflowDomain.query(runId)` | `Promise<RunSnapshot>` | — | Статус, `stopReason`, `waitingReason`, шаги, непрочитанные сигналы. |
| `WorkflowDomain.replay(runId, opts?)` | `Promise<{runId}>` | `fromStepId=""` — с начала | Новый прогон по замороженному определению и входу источника; с `fromStepId` (шаг верхнего уровня) независимые от него успешные шаги копируются. |
| `WorkflowDomain.retryCompensation(runId)` | `Promise<void>` | — | Повторяет упавшие компенсации прогона `failed_compensated`. |
| `WorkflowDef` | interface | — | `version?`, `input?` (JSON Schema), `steps`, `retry?`, `maxParallelism?`, `timeoutMs?`. |
| `Step` | union | — | `call`, `publish`, `local`, `sleep`, `wait_event`, `wait_signal`, `workflow`, `parallel`, `sequence`; общие поля `id`, `waitFor`, `when`, `timeoutMs`, `retry`. |
| `JsonExpression` / `Predicate` | type | — | Путь `$.a.b`, `{literal}`, объект/массив выражений; предикаты `not / equals / in / and / or` и путь-truthy. |
| `RetryPolicy` (`WorkflowRetryPolicy` в корне) | interface | `maxAttempts=1`, `baseDelayMs=200`, `factor=2`, `maxDelayMs=5000`, `jitter=0` | Политика повторов задачи, применяет runtime. |
| `LocalContext` | interface | — | `signal` (отмена при потере lease, дедлайне шага, остановке клиента), `runId`, `stepId`, `attempt`. |
| `RunSnapshot` / `StepSnapshot` / `RunStatus` / `StepStatus` | types | — | Статусы прогона `active|compensating|success|failed|cancelled|timed_out|failed_compensated`, шагов `pending|leased|parked|success|failed|compensated`. |
| `WorkflowAccessDeniedError` | класс | — | PermissionDenied: политика Start или вызывающий не владелец, не запустивший и без явного правила. Эмитит `policy_violation`. |
| `WorkflowNotFoundError` | класс | — | Нет такого workflow или прогона. |
| `WorkflowTerminalError` | класс | — | Операция над прогоном в неподходящем состоянии. |
| `WorkflowRunFailedError` | класс | — | `await` завершённого не `success`: `status`, `errorCode`, `errorMessage`. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `WorkflowDomain._attachRpc(rpc)` | метод | — | `ServiceBridge.start()` подключает `WorkflowsClient`; до этого caller-операции бросают. @internal |
| `WorkflowDomain._definition(name)` / `_size()` | методы | — | Локальные функции и версия объявленного workflow; число объявленных. @internal |
| `encodeDefinition(name, def)` / `encodeExpr(v)` | функции | — | DSL → `WorkflowDefinition`; строка с `$` — путь, `{literal}` — литерал, объект/массив — по членам. @internal |
| `WorkflowExecutor` | класс | — | Стрим `Workflows.Subscribe` (через `registry/StreamSupervisor`), исполнение задачи, heartbeat по токенам с интервалом из задачи, отмена исполнения по потерянному lease и дедлайну шага, отчёт `CompleteTask`/`FailTask` с повтором при `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`RESOURCE_EXHAUSTED`. @internal |
| `ExecutorDeps.wrapSpan` | `(info, fn) => Promise` | — | USER.SUBOP вокруг локального шага и компенсации; call/publish без обёртки. @internal |
| `SpanInfo` | interface | — | `runId`, `stepId`, `workflow`, `isCompensation`, `compensatesStepId`. @internal |

## Архитектурные решения и почему

- **SDK — исполнитель задач.** Раньше каждый SDK интерпретировал DAG, держал lease на весь прогон и сам парковался; это давало двойное исполнение веток, потерю событий и livelock после падения. Теперь у SDK нет интерпретатора, JSONPath, валидации и канонизации: определение кодируется и уходит в runtime, задачи приходят готовыми (вход, цель, опции разрешены).
- **Lease на попытку шага.** Heartbeat отправляет токены пачкой; runtime возвращает потерянные, и их исполнение отменяется через `AbortSignal` — результат такой попытки не отправляется. Разрыв стрима не прерывает исполняемые задачи: их lease продлевается unary-heartbeat'ом.
- **Без дубля спанов.** RPC.CALL и EVENT.PUBLISH шага сами висят под корнем прогона (`x_sb_trace` задачи); USER.SUBOP открывается только для локальных шагов и компенсаций (метаданные `is_compensation`, `compensates_for_step_id`).
- **`version` вместо fingerprint в SDK.** Локальные функции не путешествуют; задача несёт `version`, и неизвестная версия отвечает `UNSUPPORTED_VERSION` без повторов.

## Зависимости

- Опирается на: `pb/servicebridge/v1/workflows` (стабы), `registry/registry` (`Handle.workflow`), `registry/stream-supervisor`, `telemetry/context`, `telemetry/wire-trace`, `errors` (`ServiceBridgeError`).
- Используется: `connection/service-bridge` (создаёт `WorkflowDomain`, подключает `WorkflowsClient`, запускает `WorkflowExecutor` после первого Welcome при объявленных workflow, передаёт `wrapSpan`).
