# Jobs

[← к индексу](./index.md)

Работа по расписанию: cron, фиксированный интервал или один запуск в заданный момент. Расписание, лиз и ретраи держит рантайм.

## Содержание

1. [Концепция](#1-концепция)
2. [Объявление задачи](#2-объявление-задачи)
3. [Триггеры](#3-триггеры)
4. [Опции](#4-опции)
5. [Контекст исполнения](#5-контекст-исполнения)
6. [Идемпотентность](#6-идемпотентность)
7. [Ошибки и ретраи](#7-ошибки-и-ретраи)
8. [Пропущенные тики и наложение](#8-пропущенные-тики-и-наложение)
9. [Объявленные зависимости](#9-объявленные-зависимости)
10. [Несколько инстансов](#10-несколько-инстансов)
11. [Ошибки объявления](#11-ошибки-объявления)

## 1. Концепция

Задача — это именованная работа, которую рантайм запускает по расписанию на одном из инстансов сервиса, объявившего её.

- Входа и выхода у задачи нет. Единственный исход — ошибка или её отсутствие.
- Расписание живёт в рантайме, а не в вашем процессе: перезапуск сервиса не сбивает и не задваивает тики.
- Исполнение выдаётся под лизом. Если инстанс замолчал, рантайм переназначает исполнение другому.
- Доставка исполнения — at-least-once. Обработчик обязан быть идемпотентным.

## 2. Объявление задачи

```go
func declareRollup(c *sb.Client) error {
	nightly, err := job.Cron("0 3 * * *", "UTC")
	if err != nil {
		return err
	}
	return c.Job.Handle("nightly-rollup",
		job.NewSpec(nightly,
			job.WithVersion("v1"),
			job.WithOverlap(job.OverlapSkip),
			job.WithCatchup(job.CatchupFireOnce),
			job.WithMaxAttempts(5),
			job.WithDeps(job.RPC("billing-svc.Rollup")),
		),
		func(ctx context.Context, exec job.Execution) error {
			return rollup(ctx, exec.IdempotencyKey)
		})
}
```

Импорт: `import "github.com/service-bridge/sdk/go/job"`.

`c.Job.Handle` — метод, а не свободная функция: параметр типа тут не нужен, у задачи нет ни входа, ни выхода.

Объявлять нужно **до** `Start`. Позже — `CodeState`.

**`job.WithVersion` обязателен.** Это неизменяемая версия исполняемого кода обработчика: она входит в отпечаток задачи, и изменение логики обработчика требует новой версии. Новые срабатывания запускает только последняя объявленная версия; исполнения, созданные раньше, уходят той версии, под которую были назначены. Спецификация без версии отвергается при объявлении с `CodeValidation` (`job.ErrVersion`).

Спецификация валидируется здесь же, при объявлении. Это важнее, чем кажется: cron-выражение с опечаткой зарегистрировалось бы как обычная строка, и задача просто никогда не сработала бы — без единой ошибки где-либо.

## 3. Триггеры

Значение типа `job.Trigger` можно получить только одним из трёх конструкторов. Поэтому задача по построению несёт ровно один триггер: структура с тремя необязательными полями допускала бы ноль триггеров и три сразу.

```go
func triggers() error {
	nightly, err := job.Cron("0 3 * * *", "Europe/Amsterdam") // пять полей, секунд нет
	if err != nil {
		return err
	}
	beat, err := job.Interval(30 * time.Second)
	if err != nil {
		return err
	}
	once, err := job.At(time.Now().Add(2 * time.Hour))
	if err != nil {
		return err
	}
	_, _, _ = nightly, beat, once
	return nil
}
```

| Конструктор | Что делает |
|---|---|
| `job.Cron(expr, tz)` | Пять полей: минута, час, день месяца, месяц, день недели. **Секунды полем не являются.** `tz` — имя зоны IANA; пустая строка оставляет выбор рантайму. |
| `job.Interval(d)` | Каждые `d`. На проводе — целые миллисекунды, поэтому интервал меньше миллисекунды отвергается. Минимальный интервал держит рантайм. |
| `job.At(t)` | Один раз, в момент `t`. Нулевое время отвергается. |

Cron-выражение разбирается тем же парсером и с той же конфигурацией полей, с которой регистрируется рантайм, поэтому расхождения между «прошло валидацию в SDK» и «принято рантаймом» не бывает. `@daily` и прочие дескрипторы не поддерживаются.

## 4. Опции

```go
func declareTuned(c *sb.Client) error {
	beat, err := job.Interval(1 * time.Minute)
	if err != nil {
		return err
	}
	return c.Job.Handle("heartbeat",
		job.NewSpec(beat,
			job.WithVersion("v1"),
			job.WithMaxAttempts(3),
			job.WithLeaseTTL(2*time.Minute),
			job.WithMaxConcurrent(1),
			job.WithRetry(job.RetryPolicy{
				InitialMs:  1000,
				MaxMs:      60000,
				Multiplier: 2,
				Jitter:     0.2,
			}),
		),
		func(ctx context.Context, exec job.Execution) error {
			return rebuildSearchIndex(ctx)
		})
}
```

| Опция | По умолчанию | Что делает |
|---|---|---|
| `job.WithVersion(v)` | нет — обязательна | Неизменяемая версия исполняемого кода обработчика. |
| `job.WithCatchup(p)` | решает рантайм | Что делать с тиками, пропущенными пока рантайм лежал. |
| `job.WithOverlap(p)` | решает рантайм | Что делать, если задача сработала, а предыдущий запуск ещё идёт. |
| `job.WithDeps(deps...)` | нет | Исходящие вызовы, которые задача делает. Повторное применение добавляет. |
| `job.WithMaxAttempts(n)` | решает рантайм | Сколько раз пробовать одно срабатывание. |
| `job.WithLeaseTTL(d)` | решает рантайм | Сколько рантайм ждёт молчащий инстанс, прежде чем переназначить исполнение. |
| `job.WithMaxConcurrent(n)` | решает рантайм; в SDK — 32 обработчика | Потолок одновременных исполнений этой задачи — и на диспетчере рантайма, и на обработчиках SDK. Не больше 1024. При `OverlapSkip` в SDK всегда работает один обработчик. |
| `job.WithRetry(p)` | решает рантайм | Своя экспоненциальная задержка вместо серверной. `RetryPolicy{InitialMs, MaxMs, Multiplier, Jitter}`. |

**Значений по умолчанию SDK не хранит.** Неприменённая опция просто не попадает в спецификацию, и её проставляет рантайм из своих настроек. Копия дефолтов в SDK стала бы вторым источником правды и разошлась бы с первой же правкой настроек рантайма — поэтому в таблице выше стоит «решает рантайм», а не число.

В `job.Spec` лимиты — указатели (`MaxAttempts *int`, `LeaseTTLMs *int64`, `MaxConcurrent *int`, `Retry *RetryPolicy`): `nil` значит «не задано», и поле не уходит рантайму. Заданный лимит уходит, даже если он равен `0`. Стройте спецификацию через `job.NewSpec` и опции — они сами заполняют указатели.

## 5. Контекст исполнения

```go
func declareWithContext(c *sb.Client) error {
	daily, err := job.Cron("0 4 * * *", "UTC")
	if err != nil {
		return err
	}
	return c.Job.Handle("expire-carts", job.NewSpec(daily, job.WithVersion("v1")),
		func(ctx context.Context, exec job.Execution) error {
			log.Printf("job %s exec %s attempt %d scheduled at %d",
				exec.Name, exec.ID, exec.Attempt, exec.ScheduledAtUnixMs)
			return expireCarts(ctx, exec.IdempotencyKey)
		})
}
```

| Поле `job.Execution` | Что это |
|---|---|
| `Name` | Имя задачи. |
| `ID` | Идентификатор этого исполнения. |
| `ScheduledAtUnixMs` | Момент, на который был запланирован тик, unix-мс. |
| `LocalScheduledAtUnixMs` | Тот же момент в локальной зоне задачи, unix-мс. |
| `Attempt` | Номер попытки. Меняется на каждом ретрае. |
| `IdempotencyKey` | Ключ, одинаковый для всех попыток одного планового срабатывания. |

`ctx` отменяется при остановке клиента и при потере потока исполнений. `Stop` ждёт работающие обработчики до своего дедлайна.

## 6. Идемпотентность

**Идемпотентность строится по `IdempotencyKey`, а не по `Attempt`.**

Ключ один и тот же на всех попытках одного планового срабатывания; номер попытки меняется на каждом ретрае. Дедупликация по `Attempt` считает каждый ретрай новой работой — то есть не защищает ни от чего.

```go
func declareIdempotent(c *sb.Client) error {
	hourly, err := job.Cron("0 * * * *", "UTC")
	if err != nil {
		return err
	}
	return c.Job.Handle("settle-payouts", job.NewSpec(hourly, job.WithVersion("v1")),
		func(ctx context.Context, exec job.Execution) error {
			fresh, err := insertIfAbsent(ctx, "payout:"+exec.IdempotencyKey)
			if err != nil {
				return err
			}
			if !fresh {
				return nil // это исполнение уже прошло
			}
			_, err = settle(ctx, exec.IdempotencyKey)
			return err
		})
}
```

## 7. Ошибки и ретраи

Ошибка из обработчика означает «попробуй ещё раз»: рантайм повторит исполнение по своей политике задержек, пока не кончится бюджет попыток.

Когда повторять бессмысленно — заворачивайте ошибку в `sb.NonRetryable`:

```go
func declarePermanent(c *sb.Client) error {
	daily, err := job.Cron("30 2 * * *", "UTC")
	if err != nil {
		return err
	}
	return c.Job.Handle("import-feed", job.NewSpec(daily, job.WithVersion("v1")),
		func(ctx context.Context, exec job.Execution) error {
			if err := rollup(ctx, exec.IdempotencyKey); err != nil {
				if errors.Is(err, errMalformedFeed) {
					return sb.NonRetryable(err)
				}
				return err
			}
			return nil
		})
}
```

Ошибка, обёрнутая в `sb.NonRetryable`, уезжает рантайму помеченной как неретраебельная: оставшиеся попытки на неё не тратятся, и исполнение сразу уходит в DLQ. Текст исходной ошибки сохраняется. `sb.NonRetryable(nil)` возвращает `nil`.

Пока поток исполнений открыт, клиент шлёт рантайму сигнал жизни, и он продлевает лизы всех исполнений в работе. Первый сигнал уходит сразу, дальше период задаёт рантайм (до первого ответа — 5 секунд, не чаще раза в 100 мс). Три подряд неудачных сигнала — и поток переоткрывается: молчащий инстанс рантайм иначе сочтёт мёртвым и переназначит его исполнения, а в логах не останется объяснения, почему одна и та же задача выполняется дважды.

## 8. Пропущенные тики и наложение

```go
func policies() (job.CatchupPolicy, job.OverlapPolicy) {
	return job.CatchupFireOnce, job.OverlapSkip
}
```

| `CatchupPolicy` | Что делает |
|---|---|
| `job.CatchupSkip` | Забывает пропущенные тики и планирует следующий. |
| `job.CatchupFireOnce` | Срабатывает один раз за весь пропуск. |
| `job.CatchupFireAll` | Срабатывает на каждый пропущенный тик, в пределах бюджета рантайма. |

| `OverlapPolicy` | Что делает |
|---|---|
| `job.OverlapSkip` | Новое срабатывание отбрасывается. |
| `job.OverlapAllow` | Выполняется, в пределах `WithMaxConcurrent`. |
| `job.OverlapBufferOne` | Ровно одно срабатывание встаёт в очередь за текущим. |

Ежедневная выгрузка, которую бессмысленно догонять по три раза, — это `CatchupFireOnce` плюс `OverlapSkip`.

## 9. Объявленные зависимости

```go
func declareDeps(c *sb.Client) error {
	daily, err := job.Cron("0 5 * * *", "UTC")
	if err != nil {
		return err
	}
	return c.Job.Handle("nightly-billing",
		job.NewSpec(daily,
			job.WithVersion("v1"),
			job.WithDeps(
				job.RPC("billing-svc.Rollup"),
				job.Event("billing.rolled_up"),
				job.Workflow("invoice"),
			),
		),
		func(ctx context.Context, exec job.Execution) error {
			return rollup(ctx, exec.IdempotencyKey)
		})
}
```

Объявленные зависимости рисуются на карте сервисов и проверяются политикой доступа. Три вида: `job.RPC("service.Method")`, `job.Event("name")`, `job.Workflow("name")`.

## 10. Несколько инстансов

Задачу можно объявить на всех инстансах сервиса — это нормальный режим. Рантайм выдаёт исполнение **одному** из них под лизом; остальные ничего не получают.

Если держатель лиза замолчал дольше `WithLeaseTTL`, исполнение переназначается. Именно поэтому доставка at-least-once, а обработчик обязан быть идемпотентным: в редком случае переназначения работа может выполниться дважды.

Результат, отправленный держателем устаревшего лиза, отбрасывается — это и есть фенсинг.

## 11. Ошибки объявления

Все они проверяются при объявлении, до того как задача уедет в рантайм, и сравниваются через `errors.Is`:

| Сентинел | Причина |
|---|---|
| `job.ErrVersion` | Не задана версия (`job.WithVersion`). |
| `job.ErrNoTrigger` | Спецификация без триггера. |
| `job.ErrCronFieldCount` | В cron-выражении не пять полей. |
| `job.ErrCronExpr` | Cron-выражение не разбирается. |
| `job.ErrCronTZ` | Зона не является известным именем IANA. |
| `job.ErrInterval` | Интервал меньше миллисекунды. |
| `job.ErrRunAt` | Нулевое время одноразового запуска. |
| `job.ErrCatchupPolicy` · `job.ErrOverlapPolicy` | Неизвестное значение политики. |
| `job.ErrDepKind` · `job.ErrDepTarget` | Зависимость не того вида или без цели. |
| `job.ErrRetryInitial` | В политике повторов нет положительной начальной задержки. |
| `job.ErrNegativeLimit` | Отрицательный лимит или `MaxConcurrent` больше 1024. |
| `job.ErrEmptyName` · `job.ErrNoHandler` | Пустое имя задачи или отсутствующий обработчик. |
| `job.ErrDuplicateName` | Имя задачи уже объявлено. |

Все они приезжают из `c.Job.Handle` завёрнутыми в `*sb.Error` с кодом `CodeValidation`, поэтому ловить можно и по коду, и по конкретному сентинелу.

---

Дальше: [Workflows](./workflows.md) · [Operations](./operations.md)
