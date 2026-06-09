# Quickstart

← [Introduction](./introduction.md) · Дальше: [RPC](./rpc.md) →

5-минутный путь от нуля до работающего RPC-вызова между двумя сервисами.

## Предусловия

- Запущенный ServiceBridge runtime (см. `runtime/README.md`).
- PostgreSQL 18+.
- Bun 1.x или Node 18+.

## 1. Установка

```sh
bun add service-bridge
```

## 2. Получение bootstrap-ключей

Каждому сервису (минимум двум — callee и caller) нужен bootstrap-ключ. Откройте дашборд на `http://localhost:14444`, зайдите в **Services → Create service**, задайте имя — дашборд вернёт строку `sb.Cgj...` со встроенным CA. Скопируйте её и сохраните как переменную окружения.

Из исходников рантайма тот же ключ выдаёт `sbkey-gen` (CA берётся из Postgres, файлы сертификатов не нужны):

```sh
cd runtime
go run ./cmd/sbkey-gen \
  -name payment-svc \
  -dsn  "postgresql://user:pass@localhost:5433/servicebridge?sslmode=disable"
```

Подробнее в [Operations §6](./operations.md#6-security-bootstrap-key-mtls-ротация).

```sh
# .env
SERVICEBRIDGE_URL=localhost:14445
PAYMENT_KEY=sb.Cgj...XYZ
CHECKOUT_KEY=sb.Cgj...UVW
```

## 3. Общий контракт

Один `.proto`-файл с `service` блоком — он становится источником и сигнатуры, и схемы payload'а.

```proto
// payment.proto
syntax = "proto3";

service PaymentService {
  rpc Charge(ChargeRequest) returns (ChargeResponse);
}

message ChargeRequest {
  string user_id = 1;
  double amount  = 2;
}

message ChargeResponse {
  string transaction_id = 1;
  bool   ok             = 2;
}
```

## 4. Callee — принимает вызовы

```ts
// payment-svc.ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(
  process.env.SERVICEBRIDGE_URL!,
  process.env.PAYMENT_KEY!,
);

sb.rpc.handle<
  { userId: string; amount: number },
  { transactionId: string; ok: boolean }
>(
  "Charge",
  async (req) => ({
    transactionId: `tx-${req.userId}`,
    ok: req.amount > 0,
  }),
  { schema: { protoFile: "./payment.proto" } }, // input/output авто из service block
);

await sb.start();
console.log("payment online:", sb.identity()?.serviceName);
```

## 5. Caller — typed client

```ts
// checkout-svc.ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(
  process.env.SERVICEBRIDGE_URL!,
  process.env.CHECKOUT_KEY!,
);

// Одна строка: декларация зависимости + загрузка схемы + типизированный proxy
const payment = await sb.client("payment-svc", "./payment.proto");

await sb.start();

const result = await payment.Charge({ userId: "u-42", amount: 100 });
console.log(result); // { transactionId: "tx-u-42", ok: true }

await sb.stop();
```

## 6. Запуск

```sh
# в первом терминале
bun run payment-svc.ts

# во втором
bun run checkout-svc.ts
```

## Что произошло под капотом

1. `sb.start()` сделал bootstrap: parsed key → request leaf cert from runtime → установил mTLS-канал.
2. Callee отправил `RegisterRequest` с одной `incoming.rpc = "Charge"` + её `contract_hash`.
3. Caller через `sb.client()` зарегистрировал `outgoing.deps = [payment-svc.Charge]`, runtime через `RegisterAndWatch` прислал ему snapshot всех инстансов `payment-svc`.
4. `payment.Charge(...)` сериализовал payload в Protobuf, по умолчанию `transport: "auto"` выбрал direct (callee имеет `call_endpoint` от default-advertise), отправил mTLS-вызов на CallServer callee.
5. Callee декодировал payload, вызвал handler, закодировал ответ, вернул.

## Дальше

- Полный RPC-гайд (stream, schemas, transport, resilience, idempotency, version routing, ошибки): [RPC](./rpc.md)
- Pub/sub: [Events](./events.md)
- Durable workflows: [Workflows](./workflows.md)
- Cron / delayed / interval jobs: [Jobs](./jobs.md)
- Express / Fastify / Hono поверх SB: [Integrations](./integrations.md)
- Lifecycle, mTLS, env, troubleshooting: [Operations](./operations.md)

→ Дальше: [RPC](./rpc.md)
