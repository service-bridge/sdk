# Тестирование

← [Jobs](./jobs.md) · Дальше: [Integrations](./integrations.md) →

Юнит-тестирование зарегистрированных RPC- и event-хендлеров без живого рантайма: `service-bridge/testing`. Читается линейно.

## Содержание

- [Краткая модель](#краткая-модель)
- [1. RPC-хендлер: регистрация и вызов](#1-rpc-хендлер-регистрация-и-вызов)
- [2. Исходящие RPC-вызовы: моки и записи](#2-исходящие-rpc-вызовы-моки-и-записи)
- [3. Event-хендлер: доставка и ack/nack](#3-event-хендлер-доставка-и-acknack)
- [4. Исходящая публикация событий](#4-исходящая-публикация-событий)
- [5. Паттерн: тестируемая фабрика хендлера](#5-паттерн-тестируемая-фабрика-хендлера)
- [6. Чего харнесс не делает](#6-чего-харнесс-не-делает)
- [7. Шпаргалка](#7-шпаргалка)

---

## Краткая модель

`service-bridge/testing` даёт `createTestHarness()` — in-memory двойник `sb.rpc` + `sb.event`:

- `harness.rpc` (`TestRpcDomain`) — `handle()`/`invoke()` для входящих RPC, `call()`/`mockResponse()` для исходящих.
- `harness.event` (`TestEventDomain`) — `handle()`/`deliver()` для входящих событий, `publish()` для исходящих.

Никакой сети, SQLite или живого рантайма. Хендлер вызывается напрямую с типизированным объектом — Protobuf encode/decode остаётся за рамками теста (это забота serde/CallServer, не бизнес-логики хендлера).

```sh
bun add service-bridge
```

```ts
import { createTestHarness } from "service-bridge/testing";
```

---

## 1. RPC-хендлер: регистрация и вызов

```ts
harness.rpc.handle<Req, Res>(name: string, fn: (req: Req) => Promise<Res> | Res): void
harness.rpc.invoke<Req, Res>(name: string, req: Req): Promise<Res>
```

`fn` — тот же `RpcHandlerFn`, что принимает `sb.rpc.handle(name, fn, opts)` в продакшене (`opts.schema` здесь не нужен — харнесс не кодирует payload). `invoke()` зовёт `fn(req)` напрямую и возвращает результат; если хендлер бросает — ошибка пробрасывается как есть.

```ts
const harness = createTestHarness();

harness.rpc.handle("Charge", async (req: { userId: string; amount: number }) => {
  if (req.amount <= 0) throw new Error("amount must be positive");
  return { transactionId: `tx-${req.userId}`, ok: true };
});

const res = await harness.rpc.invoke("Charge", { userId: "u-1", amount: 42 });
// { transactionId: "tx-u-1", ok: true }

await expect(harness.rpc.invoke("Charge", { userId: "u-2", amount: -1 }))
  .rejects.toThrow("amount must be positive");
```

`invoke()` бросает `no RPC handler registered for "..."`, если под этим именем ничего не зарегистрировано.

---

## 2. Исходящие RPC-вызовы: моки и записи

Хендлер, который сам зовёт `rpc.call(...)`, тестируется через тот же харнесс:

```ts
harness.rpc.mockResponse(serviceName: string, methodName: string, responder: Res | ((payload, opts?) => Res | Promise<Res>)): void
harness.rpc.calls(): readonly { serviceName; methodName; payload; opts? }[]
```

```ts
harness.rpc.mockResponse("fraud-svc", "Check", { blocked: false });
// или ответ, вычисленный из payload:
harness.rpc.mockResponse("fraud-svc", "Check", (req: { userId: string }) => ({
  blocked: req.userId === "banned-user",
}));

// ... хендлер внутри зовёт rpc.call("fraud-svc", "Check", { userId })

expect(harness.rpc.calls()).toEqual([
  { serviceName: "fraud-svc", methodName: "Check", payload: { userId: "u-1" } },
]);
```

Без `mockResponse(...)` для пары `(serviceName, methodName)` вызов `call()` бросает — забытый мок падает тестом сразу, а не превращается в `undefined` где-то дальше по цепочке.

---

## 3. Event-хендлер: доставка и ack/nack

```ts
harness.event.handle(pattern: string, fn: (payload: unknown) => Promise<void> | void): void
harness.event.deliver(name: string, payload: unknown): Promise<{ outcome: "ack" } | { outcome: "nack"; reason: string }>
```

`deliver()` воспроизводит контракт `Subscriber.handleDelivery` из продакшена: нет хендлера под точным именем → `ack` (routing — на сервере); хендлер бросает → `nack` с `String(error)`; все хендлеры отработали успешно → `ack`. Несколько хендлеров на одно имя вызываются по порядку регистрации, первый throw останавливает доставку.

```ts
harness.event.handle("payment.charged", async (payload) => {
  const { transactionId } = payload as { transactionId: string };
  await sendReceipt(transactionId); // должен быть идемпотентен — delivery at-least-once
});

const result = await harness.event.deliver("payment.charged", { transactionId: "tx-1" });
// { outcome: "ack" }
```

Attempt/retry-ветка проверяется повтором `deliver()` с тем же payload: реальный `EventHandlerFn` не получает номер попытки (его нет в контракте `sb.event.handle`), поэтому тест моделирует «вторую попытку» вторым вызовом `deliver()` и проверяет `ack`/`nack` каждого:

```ts
let dbDown = true;
harness.event.handle("payment.charged", async () => {
  if (dbDown) throw new Error("db unavailable");
});

const first = await harness.event.deliver("payment.charged", {});
// { outcome: "nack", reason: "Error: db unavailable" }

dbDown = false;
const retried = await harness.event.deliver("payment.charged", {});
// { outcome: "ack" }
```

---

## 4. Исходящая публикация событий

```ts
harness.event.publish<T>(name: string, payload: T, opts?: PublishOpts): Promise<{ eventId: string }>
harness.event.published(): readonly { name; payload; opts? }[]
```

```ts
// внутри хендлера: await event.publish("payment.charged", { transactionId, amount });

expect(harness.event.published()).toEqual([
  { name: "payment.charged", payload: { transactionId: "tx-u-1", amount: 42 } },
]);
```

`publish()` только записывает вызов и возвращает свежий `eventId` — имя события не валидируется, payload не кодируется. Это точка наблюдения «что хендлер опубликовал», не замена реального `Publisher` (outbox, schema, gRPC).

---

## 5. Паттерн: тестируемая фабрика хендлера

Хендлеры, которым нужен исходящий канал, пишутся как фабрика от узкой зависимости — не как замыкание над глобальным `sb`:

```ts
import type { EventDomain } from "service-bridge";
import type { RpcDomain } from "service-bridge";

function makeChargeHandler(deps: {
  rpc: Pick<RpcDomain, "call">;
  event: Pick<EventDomain, "publish">;
}) {
  return async (req: { userId: string; amount: number }) => {
    const fraud = await deps.rpc.call<{ userId: string }, { blocked: boolean }>(
      "fraud-svc", "Check", { userId: req.userId },
    );
    if (fraud.blocked) throw new Error(`user ${req.userId} blocked`);

    const transactionId = `tx-${req.userId}`;
    await deps.event.publish("payment.charged", { transactionId, amount: req.amount });
    return { transactionId, ok: true };
  };
}

// продакшен:
const sb = new ServiceBridge(URL, KEY);
sb.rpc.handle("Charge", makeChargeHandler(sb), { schema: { protoFile: "./payment.proto", input: "ChargeRequest", output: "ChargeReply" } });

// тест:
const harness = createTestHarness();
harness.rpc.mockResponse("fraud-svc", "Check", { blocked: false });
harness.rpc.handle("Charge", makeChargeHandler(harness));
const res = await harness.rpc.invoke("Charge", { userId: "u-1", amount: 42 });
```

`Pick<RpcDomain, "call">` и `Pick<EventDomain, "publish">` — структурные типы: `harness.rpc`/`harness.event` подходят под них без каста, потому что у `TestRpcDomain.call`/`TestEventDomain.publish` та же сигнатура, что у продакшен-методов.

---

## 6. Чего харнесс не делает

| Не делает | Почему |
|---|---|
| Protobuf encode/decode payload-а | Хендлер получает и возвращает типизированные объекты напрямую — так же, как их видит его собственная бизнес-логика после декодирования на реальном пути. |
| Wire-маппинг ошибок (`errorCode`/`errorMessage`) | `invoke()` пробрасывает ошибку хендлера как есть, чтобы `rejects.toThrow(...)` проверял бизнес-сообщение. |
| Streaming RPC (`handleStream`) | Не входит в текущий охват; `handle`/`invoke` — только unary. |
| Workflow-шаги | Раннер требует чекпоинтинга состояния в рантайме (persist/resume/replay) — без рантайма шаг нельзя ни закоммитить, ни реплеить честно. |
| Валидация имени события, идемпотентность, партиционирование | `TestEventDomain` — recorder исходящих публикаций, не замена `Publisher`. |
| Живой gRPC, SQLite outbox | Харнесс работает целиком в памяти процесса теста. |

---

## 7. Шпаргалка

```ts
import { createTestHarness } from "service-bridge/testing";

const harness = createTestHarness();

// RPC
harness.rpc.handle("Charge", async (req) => ({ ok: true }));
const res = await harness.rpc.invoke("Charge", { amount: 1 });

harness.rpc.mockResponse("other-svc", "Method", { ok: true });
// ... вызов хендлера, который сам зовёт rpc.call(...)
harness.rpc.calls(); // readonly RpcCallRecord[]

// Events
harness.event.handle("payment.charged", async (payload) => { /* ... */ });
await harness.event.deliver("payment.charged", { transactionId: "tx-1" });
// { outcome: "ack" } | { outcome: "nack"; reason: string }

// ... вызов хендлера, который сам зовёт event.publish(...)
harness.event.published(); // readonly PublishedEventRecord[]

harness.reset(); // очищает rpc + event
```

→ Дальше: [Integrations](./integrations.md)
