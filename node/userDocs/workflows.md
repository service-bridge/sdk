# Workflows

← [Events](./events.md) · Дальше: [Jobs](./jobs.md) · [Integrations](./integrations.md) →

Durable workflows: DAG шагов, который интерпретирует runtime. Runtime хранит состояние каждого шага, сам решает, какие шаги готовы, держит таймеры, ожидания событий и сигналов, вложенные прогоны, повторы и компенсации. SDK объявляет определение и исполняет задачи, которые runtime ему выдаёт: локальные функции, RPC-вызовы, публикации и компенсации.

> **Не путать с Jobs.** Workflows — долгоживущие многошаговые процессы, которые запускает вызывающая сторона (`sb.workflow.start(...)`). Jobs — одношаговые задачи по расписанию. См. [jobs.md](./jobs.md).

## Содержание

- [Модель](#модель)
- [Объявление workflow](#объявление-workflow)
- [Типы шагов](#типы-шагов)
- [Выражения (JsonExpression)](#выражения-jsonexpression)
- [Группы: parallel / sequence / forEach](#группы-parallel--sequence--foreach)
- [Повторы, таймауты, параллельность](#повторы-таймауты-параллельность)
- [Компенсации](#компенсации)
- [Запуск и управление прогоном](#запуск-и-управление-прогоном)
- [Права](#права)
- [Поведение при сбоях](#поведение-при-сбоях)
- [Шпаргалка](#шпаргалка)

## Модель

- Определение кодируется в proto и уходит в runtime при регистрации сервиса. Runtime проверяет его (id шагов, ссылки `waitFor`, отсутствие циклов, глубина ≤ 10, ≤ 500 шагов, синтаксис путей и фильтров) и считает fingerprint. Невалидное определение — ошибка регистрации.
- Прогон копирует замороженный план (frozen plan): новое объявление того же имени не меняет уже идущие прогоны.
- Готовый шаг-задача (`local`, `call`, `publish`, компенсация) выдаётся одному живому инстансу сервиса-владельца с **lease на эту попытку**. Инстанс продлевает lease heartbeat'ом (интервал задаёт runtime) и сообщает результат. Если инстанс пропал — по истечении lease шаг выдаётся другому (at-least-once эффекта шага).
- `sleep`, `wait_event`, `wait_signal`, вложенный `workflow` и группы держит runtime: падение любого инстанса во время ожидания ничего не теряет.
- `version` определения нужен для `local`-шагов: функция не уходит в runtime, задача несёт версию, и инстанс без этой версии отвечает `UNSUPPORTED_VERSION`. Изменили функцию — поменяйте `version`.

## Объявление workflow

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(url, key);
sb.service("email", { rpc: ["Send"] });
sb.service("accounts", { rpc: ["Create", "Delete"] });

sb.workflow.handle("onboard-user", {
  version: "1",
  input: { type: "object", required: ["userId"] },
  steps: [
    { type: "call", id: "send_welcome", service: "email", method: "Send",
      input: { template: "welcome", userId: "$.input.userId" } },
    { type: "call", id: "provision", service: "accounts", method: "Create",
      input: { userId: "$.input.userId" }, waitFor: ["send_welcome"],
      compensate: { method: "Delete", input: { id: "$.provision.id" } } },
    { type: "local", id: "summary", waitFor: ["provision"],
      fn: (state) => ({ account: (state.provision as { id: string }).id }) },
  ],
});

await sb.start();
```

Вызывать `handle` нужно **до `sb.start()`**. Имя workflow принадлежит сервису: если его уже объявляет другой сервис с живым инстансом, регистрация отклоняется.

| Поле `WorkflowDef` | Тип | По умолчанию | Что делает |
|------|-----|--------------|------------|
| `steps` | `Step[]` | — | Шаги. |
| `version` | `string` | `""` | Версия кода `local`-шагов; часть fingerprint. |
| `input` | JSON Schema | нет | Схема входа; `start` с неподходящим входом отклоняется. |
| `retry` | `RetryPolicy` | 1 попытка | Политика повторов задач без своей. |
| `maxParallelism` | `number` | `0` (без лимита) | Сколько задач одного прогона исполняется одновременно. |
| `timeoutMs` | `number` | `0` | Таймаут прогона; по истечении — статус `timed_out`. |

## Типы шагов

| `type` | Что делает | Обязательные поля |
|--------|-----------|-------------------|
| `call` | RPC через `sb.rpc.call` на инстансе-владельце. | `service`, `method` |
| `publish` | Событие через `sb.event.publish`; выход — `{ eventId }`. | `event` |
| `local` | Ваша функция `fn(state, ctx)`; выход — её результат. | `fn` |
| `sleep` | Durable-таймер в runtime. | `durationMs` |
| `wait_event` | Ждёт событие `event`, подходящее под `filter`; выход — payload. | `event` |
| `wait_signal` | Ждёт сигнал `signal`; выход — payload сигнала. | `signal` |
| `workflow` | Runtime запускает вложенный прогон и ждёт его; выход — выход ребёнка. | `workflow` (+ `service`, по умолчанию свой) |
| `parallel` / `sequence` | Группа шагов (`steps`), опционально `forEach`. | `steps` |

Общие поля: `id` (`^[a-z0-9_]+$`, уникален в графе, не `input`), `waitFor` (id соседей в той же группе), `when` (предикат; ложь — шаг пропущен, выход `null`), `timeoutMs` (дедлайн шага; истёк — шаг и прогон падают), `retry` (для задач).

`local`: `fn(state, ctx)`, где `state` — `{ input, <id шага>: выход, ... }`, `ctx` — `{ signal, runId, stepId, attempt }`. `ctx.signal` срабатывает при потере lease, дедлайне шага и остановке клиента — прерывайте работу по нему.

Опции `call` (`opts`): `timeoutMs`, `transport` (`auto`/`direct`/`proxy`), `idempotencyKey`, `requestId`, `retry` (повторы самого RPC внутри клиента). Опции `publish`: `idempotencyKey`, `partitionKey`, `headers`.

## Выражения (JsonExpression)

Поля входа, целей, опций и фильтров принимают выражения, которые runtime вычисляет при активации шага:

- строка с `$` — путь: `$.input.userId`, `$.provision.id`, `$.items[0]`, `$.items[*].id` (поле каждого элемента);
- `{ literal: value }` — значение как есть (для строк, начинающихся с `$`);
- объект или массив — вычисляется по членам;
- остальное — литерал.

Отсутствующий путь — «нет значения» (в объекте поле опускается, в массиве `null`). Внутри итерации `forEach` доступны имя `as` и короткие id соседей по итерации.

Предикат `when`: путь (истинно, если значение есть и не `false`/`0`/`""`/`null`), `{ not }`, `{ equals: [a, b] }`, `{ in: [value, list] }`, `{ and: [...] }`, `{ or: [...] }`.

`wait_event.filter`: ключ — путь в payload события, значение — ожидаемое значение (выражение, вычисляемое при парковке). Совпасть должны все пары.

## Группы: parallel / sequence / forEach

```ts
{ type: "parallel", id: "ship_all",
  forEach: { from: "$.input.items", as: "item" },
  steps: [
    { type: "call", id: "ship", service: "warehouse", method: "Ship", input: { sku: "$.item.sku" } },
    { type: "local", id: "log", waitFor: ["ship"], fn: (s) => s.ship },
  ] }
```

Для каждого элемента создаются шаги `ship:0`, `log:0`, `ship:1`, ... Выход группы — карта id дочерних шагов → выход (`{ "ship:0": ..., "log:0": ... }`). `sequence` выполняет шаги по порядку (и итерации — одну за другой). Развёртка ограничена 10 000 шагов на прогон.

## Повторы, таймауты, параллельность

- `retry` шага (иначе `retry` определения): `maxAttempts`, `baseDelayMs`, `factor`, `maxDelayMs`, `jitter`. Runtime ставит следующую попытку через `min(maxDelayMs, baseDelayMs·factor^(n-1))·(1±jitter)`. Потеря lease попыткой не считается.
- `timeoutMs` шага — дедлайн: задача получает его и отменяется, runtime помечает шаг `failed` с `TIMEOUT`.
- `maxParallelism` ограничивает одновременно исполняемые задачи прогона — это делает runtime.

## Компенсации

`compensate` на `call`/`publish` — обратное действие при остановке прогона (падение шага, `cancel`, таймаут):

```ts
compensate: { method: "Refund", input: { chargeId: "$.charge.id" }, retry: { maxAttempts: 3 } }
```

Без `type` компенсация зеркалит шаг (`call`/`publish`), пустые `service`/`method`/`event` берутся из шага. Runtime отменяет исполняемые задачи и ожидания, отменяет незавершённых детей, затем выполняет компенсации успешных шагов в обратном порядке завершения, каждую — со своей политикой повторов. Компенсированный шаг получает статус `compensated`.

Итог: `failed` (упал шаг), `cancelled`, `timed_out` — если все компенсации прошли; `failed_compensated` — если какая-то исчерпала попытки (остальные всё равно выполняются). Такой прогон можно повторить: `sb.workflow.retryCompensation(runId)` (или из консоли).

## Запуск и управление прогоном

Доступно после `await sb.start()`.

```ts
const { runId } = await sb.workflow.start("billing", "charge-order", { orderId: "o-1" }, {
  idempotencyKey: "charge:o-1",
  timeoutMs: 300_000,
});

const { duplicate } = await sb.workflow.signal(runId, "approved", { by: "admin" }, { signalId: "approve-o-1" });
await sb.workflow.cancel(runId);
const output = await sb.workflow.await(runId);            // success → карта выходов
const snap = await sb.workflow.query(runId);              // статус, waitingReason, шаги, очередь сигналов
const { runId: again } = await sb.workflow.replay(runId, { fromStepId: "provision" });
await sb.workflow.retryCompensation(runId);
```

- Статусы прогона: `active`, `compensating`, `success`, `failed`, `cancelled`, `timed_out`, `failed_compensated`. Статусы шагов: `pending`, `leased`, `parked`, `success`, `failed`, `compensated`.
- `waitingReason` объясняет паузу: `no_instance` (нет живого инстанса с этой версией), `retry`, `sleep`, `signal`, `event`, `child`.
- Сигналы — очередь: два одинаковых сигнала доставляются оба по порядку; `signalId` делает повторную отправку безопасной. Не больше 1000 непрочитанных сигналов на прогон.
- `await` отклоняется `WorkflowRunFailedError` (`status`, `errorCode`, `errorMessage`) для любого исхода, кроме `success`.

| Ошибка | Когда |
|--------|-------|
| `WorkflowAccessDeniedError` | Отказ политики или чужой прогон. |
| `WorkflowNotFoundError` | Нет такого workflow у сервиса или нет прогона. |
| `WorkflowTerminalError` | `signal`/`cancel` завершённого прогона, `retryCompensation` не `failed_compensated`. |
| `WorkflowRunFailedError` | `await` завершился не `success`. |

## Права

- `start` проходит двустороннюю проверку политики: egress `workflow.run` вызывающего и acceptance `workflow.handle` владельца.
- `signal`, `query`, `await`, `cancel`, `replay`, `retryCompensation` разрешены владельцу workflow, сервису, запустившему прогон, и сервису с **явным** правилом egress `workflow.run` на этот workflow (отсутствие правил не считается). Остальные получают `WorkflowAccessDeniedError`.
- При регистрации runtime проверяет цели шагов по политике и присылает предупреждения (`workflow.step.call` и т. п.).

## Поведение при сбоях

| Сценарий | Что происходит |
|----------|----------------|
| Задача бросила ошибку | `FailTask` с кодом ошибки; runtime повторяет по политике или останавливает прогон. |
| Инстанс упал во время задачи | Lease истекает, шаг выдаётся другому инстансу. |
| Инстанс упал во время ожидания | Ничего: ожидание держит runtime, продолжение придёт любому живому инстансу. |
| Lease потерян во время исполнения | Heartbeat возвращает токен как потерянный, `ctx.signal` срабатывает, результат не отправляется. |
| Нет инстанса нужной версии | Прогон ждёт с `waitingReason: "no_instance"`. |

## Шпаргалка

```ts
sb.workflow.handle("onboard", {
  version: "1",
  steps: [
    { type: "call", id: "provision", service: "accounts", method: "Create",
      input: { userId: "$.input.userId" },
      compensate: { method: "Delete", input: { id: "$.provision.id" } } },
    { type: "wait_signal", id: "approve", signal: "approved", waitFor: ["provision"], timeoutMs: 86_400_000 },
    { type: "publish", id: "notify", event: "user.onboarded", waitFor: ["approve"],
      input: { userId: "$.input.userId" } },
  ],
});
await sb.start();
const { runId } = await sb.workflow.start(sb.identity()!.serviceName, "onboard", { userId: "u-1" });
await sb.workflow.signal(runId, "approved", {});
const output = await sb.workflow.await(runId);
```

→ Дальше: [Jobs](./jobs.md) · [Integrations](./integrations.md)
