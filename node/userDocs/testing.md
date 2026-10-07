# Тестирование

← [Jobs](./jobs.md) · Дальше: [Integrations](./integrations.md) →

Юнит-тестирование RPC- и event-хендлеров без живого рантайма и без сети: `service-bridge/testing`. Читается линейно.

## Содержание

- [Краткая модель](#краткая-модель)
- [1. Настройка харнесса](#1-настройка-харнесса)
- [2. Входящий RPC: invoke и invokeStream](#2-входящий-rpc-invoke-и-invokestream)
- [3. Исходящие RPC-вызовы: respond и calls](#3-исходящие-rpc-вызовы-respond-и-calls)
- [4. Входящее событие: deliver](#4-входящее-событие-deliver)
- [5. Исходящая публикация: published](#5-исходящая-публикация-published)
- [6. Чего харнесс не делает](#6-чего-харнесс-не-делает)
- [7. Шпаргалка](#7-шпаргалка)

---

## Краткая модель

`createTestHarness()` создаёт настоящий `ServiceBridge` и запускает его на in-memory runtime. Подменена только сеть, остальное — продакшен-путь:

- хендлеры и зависимости регистрируются обычным API: `sb.rpc.handle`, `sb.rpc.handleStream`, `sb.event.handle`, `sb.event.define`, `sb.client`, `sb.useSchema`;
- запросы и ответы кодируются схемами, как на проводе — ошибка схемы падает в тесте, а не в e2e;
- ошибки хендлера проходят тот же маппинг, что видит вызывающий (`HandlerError` с `handlerCode`);
- публикации идут через настоящий Publisher, доставки — через настоящий Subscriber.

Тот же код регистрации работает и в продакшене, и в тесте: в тест передаётся `h.sb`.

```ts
import { createTestHarness } from "service-bridge/testing";
```

Сценарии совпадают с Go-харнессом `sbtest`.

---

## 1. Настройка харнесса

```ts
createTestHarness(opts?: { callDefaults?: CallOpts; publishTimeoutMs?: number }): TestHarness
```

| Член | Что делает |
|---|---|
| `h.sb` | Бридж под тестом. Хендлеры и зависимости регистрируются на нём до `h.start()`. |
| `h.start()` | Грузит схемы и запускает бридж на in-memory runtime. Сеть не открывается. |
| `h.invoke` / `h.invokeStream` | Входящий вызов хендлера (§2). |
| `h.respond` / `h.respondStream` | Ответ на исходящий вызов кода под тестом (§3). |
| `h.calls()` | Исходящие вызовы по порядку (§3). |
| `h.deliver` | Доставка события подписке (§4). |
| `h.published()` | Опубликованные события (§5). |
| `h.reset()` | Забывает записанные вызовы и публикации; регистрации и ответчики остаются. |
| `h.stop()` | Останавливает бридж. |

Пример ниже — из `src/testing/example.test.ts`: хендлер `Charge` зовёт `fraud-svc`, публикует `payment.charged` и возвращает ответ.

```ts
import { join } from "node:path";
import { HandlerError } from "service-bridge";
import { createTestHarness } from "service-bridge/testing";

const SHOP = join(import.meta.dir, "testdata", "shop.proto");

async function setup() {
  const h = createTestHarness();
  const { sb } = h;
  // Та же регистрация, что в продакшене.
  await sb.client("fraud-svc", SHOP, { methods: ["Check"] });
  sb.event.define("payment.charged", { protoFile: SHOP, input: "PaymentCharged", output: "PaymentCharged" });
  sb.rpc.handle(
    "Charge",
    async (req: { userId: string; amount: number }) => {
      const verdict = await sb.rpc.call<{ userId: string }, { blocked: boolean }>(
        "fraud-svc", "Check", { userId: req.userId },
      );
      if (verdict.blocked) throw new HandlerError("BLOCKED", `user ${req.userId} is blocked`);
      const transactionId = `tx-${req.userId}`;
      await sb.event.publish("payment.charged", { transactionId, amount: req.amount });
      return { transactionId, ok: true };
    },
    { schema: { protoFile: SHOP, method: "Charge" } },
  );
  await h.start();
  return h;
}
```

---

## 2. Входящий RPC: invoke и invokeStream

```ts
h.invoke<Req, Res>(method: string, req: Req, opts?: InvokeOpts): Promise<Res>
h.invokeStream<Req, Chunk>(method: string, req: Req, opts?: InvokeOpts): Promise<Chunk[]>

interface InvokeOpts {
  caller?: { serviceId: string; instanceId: string };
  requestId?: string;        // по умолчанию UUID
  idempotencyKey?: string;
  signal?: AbortSignal;
  deadline?: number;         // абсолютный дедлайн, unix-ms
}
```

`req` кодируется схемой хендлера, проходит реальный dispatch, ответ декодируется. `InvokeOpts` попадают в `ctx` хендлера. `invokeStream` собирает все чанки в массив.

Ошибки приходят в той форме, которую увидел бы вызывающий:

- `HandlerError` с `handlerCode` из `HandlerError` хендлера, иначе `"INTERNAL"`;
- отказ до хендлера — `ServiceBridgeError` с кодом статуса: неизвестный метод — `NOT_FOUND`, недекодируемый запрос или не тот вид метода — `VALIDATION`.

```ts
const h = await setup();
h.respond("fraud-svc", "Check", () => ({ blocked: true }));

const err = await h.invoke("Charge", { userId: "u-2", amount: 1 }).catch((e) => e);

expect(err).toBeInstanceOf(HandlerError);
expect((err as HandlerError).handlerCode).toBe("BLOCKED");
await h.stop();
```

---

## 3. Исходящие RPC-вызовы: respond и calls

```ts
h.respond<Req, Res>(service: string, method: string, fn: (req: Req, call: CallRecord) => Res | Promise<Res>): void
h.respondStream<Req, Chunk>(service: string, method: string, fn: (req: Req, call: CallRecord) => AsyncIterable<Chunk> | Iterable<Chunk>): void
h.calls(): readonly CallRecord[]

interface CallRecord {
  service: string;
  method: string;
  payload: unknown;   // запрос после кодирования и декодирования схемой
  opts: CallOpts;
}
```

`respond` отвечает на `sb.rpc.call` и typed-клиент, `respondStream` — на `sb.stream`. Запрос и ответ проходят схему вызывающего в обе стороны, поэтому нужна объявленная схема (`sb.client` или `sb.useSchema`) — без неё `ConfigurationError`, как в продакшене. Ответчик, бросивший `HandlerError`, даёт вызывающему тот же бизнес-код; любая другая ошибка — `"INTERNAL"`.

Вызов без ответчика записывается в `calls()` и падает `NO_LIVE_INSTANCE`: забытый `respond` — ошибка теста, а не тихий `undefined`.

```ts
const h = await setup();
h.respond("fraud-svc", "Check", () => ({ blocked: false }));

const res = await h.invoke("Charge", { userId: "u-1", amount: 42 });

expect(res).toEqual({ transactionId: "tx-u-1", ok: true });
expect(h.calls().map((c) => [c.service, c.method, c.payload])).toEqual([
  ["fraud-svc", "Check", { userId: "u-1" }],
]);
await h.stop();
```

---

## 4. Входящее событие: deliver

```ts
h.deliver(name: string, payload: unknown, opts?: DeliverOpts): Promise<DeliveryResult>

interface DeliverOpts {
  matchedPatterns?: string[];        // вместо вычисленных правилами runtime
  attempt?: number;                  // ctx.attempt, по умолчанию 1
  partitionKey?: string;
  headers?: Record<string, string>;
}

interface DeliveryResult {
  acked: boolean;
  reason: string;                    // текст Nack
  matchedPatterns: string[];
}
```

Доставка идёт через настоящий Subscriber. Совпавшие шаблоны вычисляются правилами runtime (`*` — один сегмент, `#` — ноль и более) по подпискам сервиса; фильтры подписок не вычисляются. Payload кодируется схемой первой совпавшей подписки; без схемы передайте `Uint8Array`. Нужна хотя бы одна подписка, иначе `ConfigurationError`.

Результат — то, что подписчик ответил бы runtime: `acked: true`, если все хендлеры совпавших шаблонов успешны; первый throw — `acked: false` с текстом ошибки в `reason`; ни одного своего шаблона — `acked: false`, «no handler for matched patterns».

```ts
const h = createTestHarness();
let dbDown = true;
h.sb.event.handle("payment.*", async () => {
  if (dbDown) throw new Error("db unavailable");
}, { schema: { protoFile: SHOP, input: "PaymentCharged", output: "PaymentCharged" } });
await h.start();

const first = await h.deliver("payment.charged", { transactionId: "tx-1", amount: 1 });
// { acked: false, reason: "db unavailable", matchedPatterns: ["payment.*"] }

dbDown = false;
const retried = await h.deliver("payment.charged", { transactionId: "tx-1", amount: 1 }, { attempt: 2 });
// { acked: true, reason: "", matchedPatterns: ["payment.*"] }
await h.stop();
```

`matchPattern(pattern, name)` из того же модуля применяет те же правила маршрутизации — для проверки шаблонов без доставки.

---

## 5. Исходящая публикация: published

```ts
h.published(): readonly PublishedRecord[]

interface PublishedRecord {
  id: string;
  name: string;
  payload: unknown;          // декодирован схемой из sb.event.define
  payloadJson: unknown;      // JSON-вид, по которому runtime считает фильтры
  partitionKey: string;
  idempotencyKey: string;
  headers: Record<string, string>;
  occurredAtMs: number;
}
```

Публикация проходит настоящий Publisher: проверку имени, наличие `define`, кодирование схемой. In-memory runtime подтверждает каждое событие (`ACCEPTED`), поэтому `publish` резолвится, как после ACK.

```ts
expect(h.published().map((p) => [p.name, p.payload])).toEqual([
  ["payment.charged", { transactionId: "tx-u-1", amount: 42 }],
]);
```

---

## 6. Чего харнесс не делает

| Не делает | Почему |
|---|---|
| Политика доступа, фильтры подписок | Это поведение runtime; проверяется e2e против настоящего runtime. |
| Ретраи, лизы и DLQ доставки | Тоже runtime: `deliver` возвращает Ack/Nack, повтор моделируется повторным `deliver`. |
| Jobs и workflow | Расписание, лизы и чекпоинты шагов живут в runtime. |
| Сеть, mTLS, reconnect | Харнесс работает целиком в памяти процесса теста. |

---

## 7. Шпаргалка

```ts
import { createTestHarness } from "service-bridge/testing";

const h = createTestHarness();
// регистрация на h.sb — как в продакшене
await h.start();

// входящий RPC
await h.invoke("Charge", { userId: "u-1", amount: 1 });
await h.invokeStream("Countdown", { n: 3 });

// исходящий RPC
h.respond("fraud-svc", "Check", () => ({ blocked: false }));
h.calls();          // readonly CallRecord[]

// события
await h.deliver("payment.charged", { transactionId: "tx-1", amount: 1 });   // { acked, reason, matchedPatterns }
h.published();      // readonly PublishedRecord[]

h.reset();          // очистить calls и published
await h.stop();
```

→ Дальше: [Integrations](./integrations.md)
