# Workflows

← [Events](./events.md) · Дальше: [Jobs](./jobs.md) · [Integrations](./integrations.md) →

Durable workflows: DAG из шагов с persistent state. Runtime владеет recovery, retry-логикой и компенсацией — SDK-runner тонкий и не делает собственных ретраев.

> **Не путать с Jobs.** Workflows — это **долгоживущие multi-step бизнес-процессы**, которые запускает вызывающая сторона через `sb.workflow.start(...)`. Jobs — это runtime-triggered одношаговые задачи по расписанию (cron / delayed). Документация по Jobs — [jobs.md](./jobs.md).

## Содержание

- [Концепция](#концепция)
- [Регистрация workflow](#регистрация-workflow)
- [Типы шагов](#типы-шагов)
- [Как ссылаться на данные (JsonExpression)](#как-ссылаться-на-данные-jsonexpression)
- [Группы: parallel / sequence / forEach](#группы-parallel--sequence--foreach)
- [Compensation (rollback)](#compensation-rollback)
- [Запуск, signal, cancel, await, query, replay](#caller-side-операции)
- [Access policy](#access-policy)
- [Поведение при сбоях](#поведение-при-сбоях)
- [Шпаргалка](#шпаргалка)

## Концепция

Workflow — это **frozen plan**: декларативный DAG шагов. Runner на стороне SDK тонкий: для каждого шага он делает `beginStep → eval JsonExpression → один вызов (`sb.rpc.call` / `sb.event.publish` / `sb.workflow.start` / park) → completeStep`. Никаких ретраев, backoff, circuit breaker на SDK-стороне — recovery (lease, heartbeat, dispatcher, compensation) полностью у runtime.

При `handle` SDK canonicalize'ит граф и считает SHA-256 (`contract_hash`). Runtime отвергает повторную регистрацию того же имени с другим хешем. Уже запущенные run'ы исполняются по своему сохранённому plan'у, поэтому новая версия определения их не ломает.

## Регистрация workflow

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(url, key);

sb.workflow.handle("onboard-user", {
  input: { userId: { type: "string" } },
  steps: [
    { type: "call", id: "send_welcome", service: "email", method: "Send",
      input: { template: "welcome", userId: "$.input.userId" } },
    { type: "call", id: "provision", service: "accounts", method: "Create",
      input: { userId: "$.input.userId" }, waitFor: ["send_welcome"] },
    { type: "publish", id: "notify", event: "user.onboarded",
      input: { userId: "$.input.userId", accountId: "$.provision.id" },
      waitFor: ["provision"] },
  ],
});

await sb.start();
```

`id` шага должен матчить `^[a-z0-9_]+$`. Вызывать `handle` нужно **до `sb.start()`**.

Поля `WorkflowDef`:

| Поле | Тип | По умолчанию | Что делает |
|------|-----|--------------|------------|
| `steps` | `Step[]` | — (обязательно) | Шаги DAG. |
| `input` | JSON Schema (объект) | нет | Схема входа run'а. Свободный JSON-Schema-объект; без `fieldNumber` (это не RPC/event-контракт). |
| `retry` | `Partial<RetryOpts>` | runtime default | Workflow-level retry policy. |
| `maxParallelism` | `number` | `0` (unlimited) | Лимит одновременно исполняемых шагов в run'е. |
| `timeoutSec` | `number` | `0` (без override) | Wall-clock timeout всего run'а, в **секундах**. |

## Типы шагов

| `type` | Семантика | Обязательные поля |
|--------|-----------|-------------------|
| `call` | RPC-вызов к другому сервису (`sb.rpc.call`). | `id`, `service`, `method`, `input` |
| `publish` | Публикация события (`sb.event.publish`). | `id`, `event`, `input` |
| `workflow` | Запускает nested workflow и ждёт его завершения. | `id`, `workflow`, `input` |
| `sleep` | Durable-таймер: park run'а на `durationSec`, runtime будит. | `id`, `durationSec` |
| `wait_event` | Park до входящего события по имени + опциональному `filter`. | `id`, `event` |
| `wait_signal` | Park до внешнего `sb.workflow.signal(...)`. | `id`, `signal` |
| `parallel` | Группа: все вложенные шаги стартуют сразу, шаг готов после всех. | `id`, `steps` |
| `sequence` | Группа: вложенные шаги по порядку. | `id`, `steps` |
| `local` | Локальная JS-функция без I/O. Её результат идёт в state. | `id`, `fn` |

Полей `payload`/`name`/`branches`/`do` нет — это распространённая ошибка. Везде, где шаг передаёт данные, поле называется `input`; имя nested-workflow — `workflow`; вложенные шаги группы — `steps`.

Любой шаг может иметь:

- `waitFor: string[]` — id шагов, после завершения которых этот шаг готов к запуску.
- `when: Predicate` — условное выполнение. `Predicate` — это truthy-`JsonExpression` (строка) либо `{ not }` / `{ equals: [a, b] }` / `{ in: [value, array] }` / `{ and: [...] }` / `{ or: [...] }`. Если false → шаг пропускается, в state кладётся `null`.
- `timeoutSec: number` — workflow-control timeout шага, в **секундах**. Для `wait_event` / `wait_signal` — таймаут ожидания.
- `retry: Partial<RetryOpts>` — step-level retry policy.
- `opts` — типизированная композиция над опциями нижележащего домена (`CallOpts` / `PublishOpts` / `WorkflowStartOpts`). Например `idempotencyKey`, `transport`, `timeout`.
- `compensate` — compensation handler (только на `call` / `publish`).

`fn` у `local` получает текущий state-объект: `fn: (state) => Promise<unknown>`.

## Как ссылаться на данные (JsonExpression)

Любое поле, помеченное как `JsonExpression`, принимает:

1. **Literal** — обычное значение (`"hello"`, `42`, `{ ... }`). Объекты обходятся рекурсивно, так что вложенные `$.…` тоже резолвятся.
2. **Path** — строка вида `$.<сегменты>`:
   - `$.input.userId` — поле входа run'а.
   - `$.<stepId>` — **выход шага** целиком (результат лежит в state прямо под id шага, без `.result`).
   - `$.provision.id` — поле выхода шага `provision`.
   - `$.items[0].name` — индекс.
   - `$.items[*].id` — wildcard (для filter / forEach): мапит поле по всем элементам массива.
3. **Literal-escape** — `{ literal: "$.looks-like-path" }`, чтобы передать строку, которая иначе распарсилась бы как path.

Missing path резолвится в `undefined` (это не ошибка), а синтаксически неверный path бросает на этапе валидации. Резолв происходит на стороне runner'а перед `beginStep` — runtime получает уже materialized input snapshot.

## Группы: parallel / sequence / forEach

`forEach` — это **поле** шага `parallel` или `sequence`, а не отдельный тип шага. Без `forEach` группа выполняет свои `steps` один раз; с `forEach` — по разу на каждый элемент массива.

### `parallel`

```ts
{
  type: "parallel",
  id: "fan_out",
  steps: [
    { type: "call", id: "a", service: "svc-a", method: "Do", input: "$.input" },
    { type: "call", id: "b", service: "svc-b", method: "Do", input: "$.input" },
  ],
}
```

Все `steps` стартуют одновременно (с учётом `maxParallelism`). Группа готова после завершения всех.

### `sequence`

```ts
{
  type: "sequence",
  id: "pipeline",
  steps: [
    { type: "call", id: "step1", service: "svc", method: "Step1", input: "$.input" },
    { type: "call", id: "step2", service: "svc", method: "Step2", input: "$.step1" },
  ],
}
```

### `forEach`

```ts
{
  type: "parallel",
  id: "process_items",
  forEach: { from: "$.input.items", as: "item" },
  steps: [
    { type: "call", id: "process", service: "worker", method: "Handle",
      input: { item: "$.item" } },
  ],
}
```

Для каждого элемента `from` создаётся под-state, в который привязывается значение под именем `forEach.as` (здесь — `$.item`). На `parallel` итерации идут параллельно (в пределах `maxParallelism`), на `sequence` — по порядку. `as` должен матчить `^[a-z0-9_]+$`.

## Compensation (rollback)

Шаг `call` или `publish` может декларировать `compensate` — обратное действие. Поле данных называется `input` (как и у самого шага):

```ts
{
  type: "call",
  id: "charge",
  service: "payments",
  method: "Charge",
  input: { amount: "$.input.amount" },
  compensate: {
    service: "payments",
    method: "Refund",
    input: { chargeId: "$.charge.transactionId" },
  },
}
```

`compensate` (`CompensateSpec`): `input` обязателен; опционально `type` (`"call"` | `"publish"`, по умолчанию совпадает с типом шага), `service`/`method` (для call), `event` (для publish), `retry`, `idempotencyKey`. Если `service`/`method`/`event` не заданы — берутся из самого шага.

При `cancel` или провале runtime переводит run в `compensating` и шлёт SDK assignment с флагом компенсации. Runner идёт **в обратном порядке** по завершённым шагам и для каждого, у кого есть `compensate` и непустой output, вызывает обратную операцию через `sb.rpc.call` / `sb.event.publish`. Терминальный статус run'а после компенсации: `cancelled` (отмена пользователем) или `failed_compensated` (компенсация после падения шага).

## Caller-side операции

Caller-side операции доступны только **после `await sb.start()`** (до привязки gRPC-канала они бросают):

```ts
const { runId } = await sb.workflow.start("onboard-user", { userId: "u-1" }, {
  idempotencyKey: "onboard:u-1",   // повторный start с тем же ключом вернёт существующий runId
  timeoutSec: 300,                 // секунды
});

await sb.workflow.signal(runId, "user-approved", { approvedBy: "admin" });
await sb.workflow.cancel(runId);

// await: server-stream до терминального статуса. Резолвится final state'ом
// ТОЛЬКО при статусе "success"; на failed/cancelled/failed_compensated — reject.
const finalState = await sb.workflow.await(runId);

// query: point-in-time снимок. status — строка, state — map, steps — массив.
const { status, state, steps } = await sb.workflow.query(runId);

// replay: новый run, форкнутый от runId с шага fromStepId ("" = с начала).
const { runId: replayRunId } = await sb.workflow.replay(runId, { fromStepId: "provision" });
```

Терминальный статус успеха — строка **`success`** (не `completed`/`succeeded`). Полный словарь run-статусов: `pending` / `running` / `waiting` / `success` / `failed` / `cancelling` / `cancelled` / `compensating` / `failed_compensated`. `compensated` — это статус **шага** (в `query().steps[].status`), а не run'а; у такого шага заполнено поле `compensatedBy`.

`query().steps[]` содержит: `stepId`, `status`, `output`, `lastError`, опциональный `compensatedBy`.

Ошибки caller-side операций:

| Класс | Когда |
|-------|-------|
| `WorkflowAccessDeniedError` | gate #5 denial: peer не может стартовать/трогать чужой workflow (PERMISSION_DENIED). Поля `workflowName`, `reason`. |
| `WorkflowNotFoundError` | `start` по незарегистрированному имени workflow (NOT_FOUND). |
| `WorkflowTerminalError` | операция (`signal` / `cancel`) над уже терминальным run'ом, например `cancel` на `success` (FAILED_PRECONDITION). Поля `runId`, `status`. |

## Access policy

Workflows интегрированы с access policy (ADR-0004):

- **Register-time walk.** При регистрации runtime проходит все `call` / `publish` / `workflow` шаги frozen plan'а и проверяет egress-policy каждого. Warnings приходят в первой `RegistrySnapshot` с `declaration = "workflow.<name>.step.<id>"`.
- **Bilateral check на caller-side.** Когда peer ≠ owner, `start` / `signal` / `cancel` / `replay` проходят двусторонний check (egress вызывающего + acceptance владельца). По умолчанию — allow; denial → `WorkflowAccessDeniedError` и `policy_violation` с `declaration: "workflow.run"`.

## Поведение при сбоях

| Сценарий | Что происходит |
|----------|----------------|
| Throw из `call` / `publish` шага | Runner шлёт `failStep` (без retriable-флага со своей стороны). Решение retry / fail / compensate принимает **runtime** по своей policy. |
| Падение SDK-инстанса между шагами | Heartbeat истекает → runtime инкрементит `lease_epoch` и переотправляет run другому инстансу того же сервиса. Уже выполненные шаги отдают cached output через идемпотентность `beginStep` (он возвращает `alreadyDone`). |
| Исчерпан retry-policy | Run переходит в `failed`; если есть `compensate`-шаги — runtime запускает компенсацию (итог `failed_compensated`). |
| `cancel` во время выполнения | Run переходит в `compensating`, выполняется обратный проход по completed-шагам, итог — `cancelled`. |

## Шпаргалка

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(url, key);

// Owner side: объявить workflow (до sb.start()).
sb.workflow.handle("onboard", {
  steps: [
    { type: "call", id: "welcome", service: "email", method: "Send",
      input: { userId: "$.input.userId" } },
    { type: "call", id: "provision", service: "accounts", method: "Create",
      input: { userId: "$.input.userId" }, waitFor: ["welcome"],
      compensate: { service: "accounts", method: "Delete",
                    input: { id: "$.provision.id" } } },
    { type: "publish", id: "notify", event: "user.onboarded",
      input: { userId: "$.input.userId" }, waitFor: ["provision"] },
  ],
});

await sb.start();

// Caller side: запустить и дождаться терминального статуса (resolve на "success").
const { runId } = await sb.workflow.start("onboard", { userId: "u-1" });
const state = await sb.workflow.await(runId);
```

→ Дальше: [Jobs](./jobs.md) · [Integrations](./integrations.md)
