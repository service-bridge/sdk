# Access Policy

[← к индексу](./index.md)

ServiceBridge поддерживает гранулярную политику доступа: оператор может ограничить, что сервис **может регистрировать** (capabilities), что он **может отправлять** (egress), и **кто может его адресовать** (acceptance). По умолчанию всё разрешено — ограничения опциональны.

## Концепты

Семь capability-флагов: четыре acceptance (`*.handle`) и три egress.

| Handler-side / acceptance (что регистрирую, кто меня зовёт) | Egress / action (что я отправляю) |
|---|---|
| `rpc.handle` | `rpc.call` |
| `event.handle` (включая wildcard) | `event.publish` |
| `workflow.handle` | `workflow.run` |
| `job.handle` | — (jobs self-only: нет egress и нет внешнего caller'а) |

- **Capability** — boolean-флаг на сервисе. Грубый kill-switch: выключенный флаг денит независимо от правил.
- **Egress rule** — что сервис может отправлять. Хранится в `service_policy_rules` с `direction='E'`.
- **Acceptance rule** — кто/что может адресовать этот сервис. `direction='A'`.

При вызове RPC runtime делает **bilateral check**: caller'у должно быть разрешено отправить (`rpc.call`), callee — принять (`rpc.handle`). Оба должны разрешать. Так же устроен `workflow.run`/`workflow.handle`. Для `event.publish` проверка только egress-сторона (у событий нет per-service маршрутизации).

## Default-allow

Сразу после регистрации через UI консоль сервис может всё. В БД нет ни одного `service_policy_rules` для него; runtime трактует «нет правил для (service, action)» как разрешение.

Оператор добавляет правила, чтобы ограничить.

## SDK warnings при старте

Runtime в первом снапшоте после `RegisterAndWatch` посылает SDK `PolicyEvaluation` со списком нарушений в декларациях. SDK:

- логирует `console.warn` на каждое нарушение,
- эмитит `policy_violation` event:

```ts
sb.on('policy_violation', ({ declaration, value, denySide, reason }) => {
  // declaration: 'rpc.call' | 'rpc.handle' | 'event.publish' | 'event.handle'
  //              | 'workflow.run' | 'workflow.handle'
  // value: 'payments/charge' | 'orders.*' | ...
  // denySide: 'capability' | 'self_egress' | 'self_acceptance' | 'peer_acceptance'
  // reason: человекочитаемое объяснение от runtime
});
```

Для строгого режима (prod):

```ts
const sb = new ServiceBridge(url, key, {
  failOnPolicyViolation: true,
});
// При любом warning в первом снапшоте SDK не бросает исключение из start(),
// а эмитит `disconnected` и сам останавливается (вызывает stop()).
// reason начинается с "policy:", в error лежит ConnectionError.
sb.on('disconnected', ({ reason, error }) => {
  if (reason.startsWith('policy:')) {
    console.error('policy violations on start:', error);
    process.exit(1);
  }
});
await sb.start();
```

Также доступен геттер — последний снапшот политики, который runtime прислал
для твоего сервиса (`null`, пока не пришёл первый кадр реестра):

```ts
const evaluation = sb.policyEvaluation();
// { capabilities: string[],   // ['rpc.handle', 'event.handle', ...]
//   egress: PolicyRule[],     // мои egress-правила
//   acceptance: PolicyRule[], // мои acceptance-правила
//   warnings: PolicyViolation[] }
```

## Service Map с policy

`sb.serviceMap()` возвращает `ReadonlyMap<string, ServiceMapEntry>` (ключ — имя сервиса):

```ts
interface ServiceMapEntry {
  methods: MethodDescriptor[];                       // rpc.handle / workflow / job / http / published events
  instances: ServiceInstanceInfo[];                  // живые инстансы + endpoint'ы + health
  eventSubscriptions: EventSubscriptionDescriptor[]; // мои event.handle паттерны (с wildcards)
  outgoingCalls: OutgoingCallDescriptor[];           // мои rpc.call / workflow.run / http зависимости
}
```

Виден только caller'у — его собственный сервис и сервисы из его outgoing-deps (через `sb.service(...)`).

## Создание сервиса

Сервисы регистрируются через UI консоль рантайма (Services → Create service). Создайте сервис, получите ключ через дашборд, обновите env. По умолчанию новый сервис не ограничен — всё разрешено. Ограничения добавляются правилами политики (ниже).

## CLI: редактирование политики

Все команды требуют `--dsn` (Postgres DSN рантайма).

```sh
# Посмотреть текущую политику
sb-policy show --dsn=... --service=analytics

# Отключить capability (только *.handle: rpc.handle / event.handle / workflow.handle / job.handle)
sb-policy capability set --dsn=... --service=payments --cap=event.handle --value=false

# Добавить egress правило
sb-policy action add --dsn=... --service=analytics --kind=rpc.call --target=payments/charge

# Удалить egress
sb-policy action remove --dsn=... --service=analytics --kind=rpc.call --target=payments/charge

# Добавить acceptance (кому можно меня вызывать)
sb-policy acceptance add --dsn=... --service=payments --kind=rpc.handle --caller=analytics --method=charge

# Удалить
sb-policy acceptance remove --dsn=... --service=payments --kind=rpc.handle --caller=analytics --method=charge
```

Изменения вступают в силу <1s — Postgres NOTIFY автоматически обновляет in-memory snapshot в runtime.

## Wildcards

В `event.publish` и `event.handle` поддерживаются AMQP wildcards:
- `*` — ровно один сегмент (между точками)
- `#` — ноль или более сегментов

При публикации event'а runtime матчит его имя против patterns каждого подписчика через `TopicMatch`.

При **регистрации подписки** на pattern P, runtime проверяет, что у сервиса есть acceptance rule R, который **накрывает** P через `PatternContains` (P ⊆ R). Например:

- Acceptance rule `orders.#` разрешает подписки `orders.*`, `orders.created`, `orders.payment.received`.
- Acceptance rule `orders.*` (один сегмент) разрешает `orders.created`, но не `orders.#` или `orders.payment.received`.

## Что происходит при нарушении

| Где | Что видит код |
|---|---|
| **Регистрация handler'а с отключённой capability** | Handler **не регистрируется** (runtime тихо пропускает его), но `start()` не падает — сервис остаётся жив. Пропуск приходит как warning (`policy_violation`). |
| **Объявление outgoing dep / subscription не покрытое правилами** | Регистрация проходит. SDK получает warning (`policy_violation`). Реальная попытка вызова денится в рантайме. |
| **`sb.rpc.call(...)`** | Промис реджектится `RpcAccessDeniedError` (маппинг с gRPC `PERMISSION_DENIED`). |
| **`sb.workflow.start(...)`** | Промис реджектится `WorkflowAccessDeniedError`. |
| **`sb.event.publish(...)`** | Publish — fire-and-forget в локальный outbox, поэтому отказ **не** бросается в caller. Envelope получает `PUBLISH_STATUS_REJECTED_FORBIDDEN`, строка в outbox помечается `failed` (терминально, без ретраев), а SDK эмитит `policy_violation` с `denySide: 'self_egress'`. |

## Глобальный граф для UI

UI-дашборду доступен runtime endpoint `UI.GetServiceGraph` (отдельный gRPC сервис). Возвращает полный граф всех активных сервисов с handlers, instances, outgoing decls, event subs, policy rules. **Не** экспортируется через обычный SDK — UI-консумер отдельный.

## Ссылки

- ADR-0004 (`runtime/docs/adr/0004-access-security-tls.md`) — детальное обоснование
- `runtime/internal/access/README.md` — internals реализации
- `runtime/cmd/sb-policy/README.md` — полная спецификация CLI
