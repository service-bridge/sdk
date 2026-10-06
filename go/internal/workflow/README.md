# internal/workflow

## Зона ответственности

Клиентская часть durable workflow для Go SDK: кодирование объявленного графа в proto `WorkflowDefinition`, caller-операции (`Start`, `Signal`, `Cancel`, `Await`, `Query`, `Replay`, `RetryCompensation`) и исполнитель задач, которые runtime выдаёт инстансу-владельцу (локальная функция, вызов, публикация, компенсация). DAG, выражения, ретраи, таймеры, ожидания и компенсации интерпретирует runtime (ADR 0003 runtime); здесь их нет.

## Публичный контракт

Пакет внутренний; наружу его выставляет `servicebridge.WorkflowDomain` (`generic.go`).

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Encode(name, def)` | `(Encoded, error)` | — | `wf.Definition` → `WorkflowDefinition` + карта локальных функций по id шага. Path — путь, Name и строка — литерал, `map[string]any`/`[]any` — по членам, прочее — JSON-литерал. Ошибка — только то, что нельзя передать (nil-шаг, Local без Fn, неJSON-значение, неизвестный вид компенсации). |
| `NewCaller(CallerConfig)` | `(*Caller, error)` | — | Caller-сторона над `ClientSource`. |
| `Caller.Start/Signal/Cancel/RetryCompensation/Await/Query/Replay` | методы | — | RPC `Workflows`. `Signal` возвращает `duplicate`. `Await` — выход при `success`, иначе `*RunFailedError`. |
| `StartArgs` | struct | — | `Service`, `Workflow`, `Input`, `IdempotencyKey`, `TimeoutMs`. |
| `SignalArgs` | struct | — | `RunID`, `Signal`, `Payload`, `SignalID`. |
| `RunSnapshot`, `StepSnapshot`, `PendingSignal` | struct | — | Ответ `Query`: статус, `StopReason`, `WaitingReason`, шаги, очередь сигналов. |
| `NewExecutor(ExecutorConfig)` | `(*Executor, error)` | — | Исполнитель задач: `Start(ctx)`, `Stop()`, `InFlight()`. |
| `ExecutorConfig` | struct | `ErrorCode` nil → `"ERROR"` | `Clients`, `Identity`, `Definitions` (локальные функции по `(workflow, version, stepID)`), `Effects` (`Call`, `Publish`), `WrapSpan`, `ErrorCode`, `Backoff`, `OnError`, `Logger`. |
| `CallSpec`, `PublishSpec`, `Span` | struct | — | Разрешённая задача вызова/публикации; описание USER.SUBOP для локального шага и компенсации. |
| `StaticCallTargets(steps)` | `[]CallTarget` | — | Литеральные цели call-шагов и call-компенсаций — проверка объявленных зависимостей при `Start` клиента. |
| `ErrAccessDenied`, `ErrWorkflowNotFound`, `ErrRunTerminal`, `ErrRunFailed`, `ErrInvalidConfig`, `ErrNoIdentity` | `error` | — | Сентинелы; `AccessDeniedError`, `NotFoundError`, `TerminalError`, `RunFailedError` разворачиваются в них. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `beatTick` | const | 250 мс | Шаг проверки, каким задачам пора heartbeat; интервал каждой задачи задаёт runtime (`heartbeat_interval_ms`). @internal |
| `reportBackoff` | var | 200 мс, 1 с, 3 с | Повторы `CompleteTask`/`FailTask` при `Unavailable`/`DeadlineExceeded`/`ResourceExhausted`. @internal |
| `errUnsupportedVersion` | error | — | Задача для версии без локального кода: `FailTask` с `UNSUPPORTED_VERSION`, `non_retriable`. @internal |
| `flattenSteps`, `compensationSuffix` | — | — | Обход графа для `StaticCallTargets`. @internal |

## Архитектурные решения и почему

- **Исполнитель вместо раннера.** Прежний раннер повторял DAG-логику Node SDK и расходился с ним (MaxParallelism игнорировался, backoff жил в SDK). Теперь runtime решает всё, SDK исполняет одну задачу за раз на горутину и сообщает результат по токену.
- **Lease на попытку.** Heartbeat — unary, пачкой по токенам; потерянные токены отменяют контекст исполнения (`context.CancelCause`), результат такой попытки не отправляется. Разрыв стрима задачи не отменяет. Дедлайн шага — `context.WithDeadline`.
- **USER.SUBOP только для локальных шагов и компенсаций**, call/publish трассируются собственными операциями под корнем прогона.

## Зависимости

- Опирается на: `internal/pb/servicebridge/v1`, `internal/stream` (`Supervisor`, `Backoff`), `internal/telemetry` (контекст трассы), `workflow` (DSL).
- Используется: `servicebridge` (`generic.go`: `WorkflowDomain`, адаптер `executor` для `Effects`/`Definitions`; `servicebridge.go`: проводка `Caller`/`Executor`), `errors.go` (классификация сентинелов).
