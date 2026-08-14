# ServiceBridge Go SDK — документация

Go SDK для [ServiceBridge runtime](https://github.com/servicebridge2/runtime).

```sh
go get github.com/service-bridge/sdk/go
```

Модуль называется `github.com/service-bridge/sdk/go`, пакет внутри — `servicebridge`. Во всех примерах он импортируется под псевдонимом `sb`.

## Документация — по доменам

Каждый файл — самодостаточный гайд по одной фиче: от объявления до пограничных случаев в проде. Читается линейно.

| Документ | О чём |
|----------|-------|
| [Introduction](./introduction.md) | Что такое ServiceBridge, что делает SDK и чего он не делает |
| [Quickstart](./quickstart.md) | Два процесса, один вызов, пять минут |
| [**RPC**](./rpc.md) | `sb.Handle` / `sb.HandleStream`, `sb.NewMethod`, `sb.Call`, `sb.Stream`, транспорт, ретраи, размыкатель, идемпотентность, маршрутизация по контракту |
| [Events](./events.md) | `sb.DefineEvent`, `sb.PublishEvent`, `sb.SubscribeEvent`, локальный outbox, шаблоны подписок, гарантии доставки |
| [Workflows](./workflows.md) | `c.Workflow` — durable DAG: шаги, предикаты, компенсации, сигналы, replay |
| [Jobs](./jobs.md) | `c.Job` — cron / interval / one-shot, лизы, ретраи, идемпотентность |
| [Integrations](./integrations.md) | `sbhttp` и `sbgin`: свой HTTP-сервер в Service Map и в трейсе |
| [Тестирование](./testing.md) | `sbtest` — юнит-тест обработчиков без сети и без рантайма |
| [Access Policy](./access-policy.md) | Что видит SDK при ограничениях политики доступа |
| [Operations](./operations.md) | Конструктор и опции, lifecycle, identity, телеметрия, ротация сертификатов, troubleshooting |
| [API reference](./api-reference.md) | Компактный справочник публичных сигнатур |
| [References](./references.md) | Внутренние README модулей, ADR рантайма, требования |

## Где что искать

**«Как объявить обработчик X?»** → файл соответствующего домена: [RPC](./rpc.md#2-обработчики), [Events](./events.md#3-подписка), [Workflows](./workflows.md#2-объявление-графа), [Jobs](./jobs.md#2-объявление-задачи), [Integrations](./integrations.md).

**«Как вызвать чужой RPC?»** → [RPC §3](./rpc.md#3-исходящие-вызовы).

**«Почему мой обработчик не зарегистрировался?»** → [Operations §8](./operations.md#8-troubleshooting). Почти всегда — объявление после `Start` (`CodeState`) или `WithCallerOnly` (`CodeConfig`).

**«Какие ошибки бывают и как их ловить?»** → [RPC §9](./rpc.md#9-ошибки), полный список кодов — [API reference](./api-reference.md#ошибки).

**«Откуда берётся схема сообщения?»** → [RPC §1](./rpc.md#1-схема-берётся-из-сгенерированных-типов). Отдельного шага регистрации схемы нет.

**«Какие значения по умолчанию у опций?»** → [Operations §1](./operations.md#1-конструктор-и-опции).

**«Как юнит-тестировать обработчик без живого рантайма?»** → [Тестирование](./testing.md). Обязательно прочитать [§6 — чего двойник не делает](./testing.md#6-чего-двойник-не-воспроизводит).

**«В каких единицах время?»** → миллисекунды везде, кроме пяти полей workflow. [Operations §9](./operations.md#9-единицы-времени).
