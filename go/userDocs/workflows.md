# Workflows

[← к индексу](./index.md)

Durable DAG. Граф объявляется один раз; интерпретирует его рантайм: он хранит состояние каждого шага, решает, какие шаги готовы, держит таймеры, ожидания, вложенные прогоны, повторы и компенсации. Клиент исполняет только задачи, которые рантайм ему выдаёт: локальные функции, вызовы, публикации и компенсации.

## Содержание

1. [Концепция](#1-концепция)
2. [Объявление графа](#2-объявление-графа)
3. [Виды шагов](#3-виды-шагов)
4. [Пути и литералы](#4-пути-и-литералы)
5. [Условия](#5-условия)
6. [Группы и forEach](#6-группы-и-foreach)
7. [Компенсации](#7-компенсации)
8. [Локальный шаг](#8-локальный-шаг)
9. [Управление прогоном](#9-управление-прогоном)
10. [Ошибки объявления](#10-ошибки-объявления)

## 1. Концепция

- **Граф объявляется, а не программируется.** Вы описываете шаги и их зависимости; порядок исполнения рантайм выводит из `WaitFor`.
- **Определение уходит в рантайм при регистрации.** Рантайм проверяет его и считает отпечаток (fingerprint); прогон копирует замороженный план, поэтому новое объявление того же имени не меняет идущие прогоны.
- **Состояние прогона — это JSON.** Вход прогона лежит под ключом `input`, выход каждого шага — под идентификатором шага.
- **Lease на попытку шага.** Готовая задача выдаётся одному живому инстансу сервиса-владельца; клиент продлевает lease heartbeat'ом с интервалом, который задаёт рантайм. Пропал инстанс — по истечении lease шаг получает другой (эффект шага — at-least-once).
- **Ожидания держит рантайм.** `Sleep`, `WaitEvent`, `WaitSignal`, `SubWorkflow` и группы — строки шагов в рантайме; падение инстанса во время ожидания ничего не теряет.
- **Шаг `call` идёт в обычный типизированный хендлер** — по паре типов, объявленной через `sb.NewMethod` (см. [§3.1](#31-шаг-call-требует-объявленной-зависимости)).
- **`Local` опознаётся по `ID` и `Definition.Version`.** Функция не уходит в рантайм; изменили её — поменяйте `Version`.

## 2. Объявление графа

```go
func declareCheckout(c *sb.Client) error {
	return c.Workflow.Handle("checkout", wf.Definition{
		Input: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"orderId": map[string]any{"type": "string"},
			},
			"required": []any{"orderId"},
		},
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{
					ID: "reserve",
					Compensate: &wf.Compensation{
						Service: wf.Name("inventory-svc"),
						Method:  wf.Name("Release"),
						Input:   wf.Path("$.reserve"),
					},
				},
				Service: wf.Name("inventory-svc"),
				Method:  wf.Name("Reserve"),
				Input:   wf.Path("$.input"),
			},
			wf.Call{
				Control: wf.Control{ID: "charge", WaitFor: []string{"reserve"}, Timeout: 30 * time.Second},
				Service: wf.Name("payment-svc"),
				Method:  wf.Name("Charge"),
				Input:   wf.Path("$.input"),
			},
			wf.Publish{
				Control: wf.Control{
					ID:      "announce",
					WaitFor: []string{"charge"},
					When:    wf.Truthy(wf.Path("$.charge.ok")),
				},
				Event: wf.Name("order.placed"),
				Input: wf.Path("$.input"),
			},
		},
	})
}
```

Импорт: `import wf "github.com/service-bridge/sdk/go/workflow"`.

Объявлять нужно **до** `Start`. Позже — `CodeState`.

`Definition`:

| Поле | По умолчанию | Что делает |
|---|---|---|
| `Version` | `""` | Версия кода `Local`-шагов; часть отпечатка. |
| `Input` | нет | JSON Schema входа прогона; `Start` с неподходящим входом отклоняется. |
| `Steps` | — | Шаги верхнего уровня. |
| `Retry` | 1 попытка | Политика повторов задач (`Call`, `Publish`, `Local`) без своей. |
| `MaxParallelism` | 0 — без лимита | Сколько задач одного прогона исполняется одновременно; соблюдает рантайм. |
| `Timeout` | 0 — нет | Ограничение на весь прогон; по истечении — статус `timed_out`. |

`Control`, общий для всех видов шагов:

| Поле | Что делает |
|---|---|
| `ID` | Имя шага внутри workflow. Только `^[a-z0-9_]+$`, уникально по всему графу, включая вложенность. |
| `WaitFor` | Идентификаторы шагов, которых этот дожидается. Порядок внутри списка значения не имеет. |
| `When` | Условие. Ложный предикат пропускает шаг, и всё, что он произвёл бы, разрешается в ничто. |
| `Compensate` | Обратное действие. Допустимо только на `Call` и `Publish`. |
| `Timeout` | Дедлайн шага. По истечении задача отменяется, шаг падает с `TIMEOUT`, прогон останавливается. Это не таймаут самого вызова — тот в `CallOpts.Timeout`. |
| `Retry` | Политика повторов задачи вместо той, что задана на уровне графа. Backoff `min(MaxDelay, BaseDelay·Factor^(n-1))·(1±Jitter)` применяет рантайм. |

Шаги верхнего уровня стартуют параллельно; `WaitFor` объявляет зависимости, из которых складываются уровни исполнения.

## 3. Виды шагов

Набор закрыт: маркер-метод интерфейса `wf.Step` неэкспортируемый, поэтому объявить десятый вид шага, который рантайм не знает, невозможно.

| Вид | Что делает | Своё |
|---|---|---|
| `wf.Call` | Вызывает метод другого сервиса. | `Service`, `Method` (`Target`), `Input`, `Opts *CallOpts` |
| `wf.Publish` | Публикует durable-событие. | `Event` (`Target`), `Input`, `Opts *PublishOpts` |
| `wf.Sleep` | Паркует шаг на durable-таймере в рантайме. | `Duration time.Duration` |
| `wf.WaitEvent` | Ждёт подходящее событие; выход — payload. | `Event string`, `Filter map[string]any` |
| `wf.WaitSignal` | Ждёт внешний сигнал по имени. | `Signal string` |
| `wf.SubWorkflow` | Рантайм запускает вложенный прогон и ждёт его; выход — выход ребёнка. | `Service` (`Target`, nil — свой сервис), `Workflow` (`Target`), `Input`, `IdempotencyKey`, `Timeout` |
| `wf.Parallel` | Группа: вложенные шаги стартуют разом. | `Steps []Step`, `ForEach *ForEach` |
| `wf.Sequence` | Группа: вложенные шаги идут по очереди. | `Steps []Step`, `ForEach *ForEach` |
| `wf.Local` | Выполняет Go-функцию в объявившем процессе. | `Fn LocalFunc` |

Таймер `Sleep` держит рантайм, а не SDK, поэтому прогон переживает рестарт всех инстансов.

### 3.1. Шаг `call` требует объявленной зависимости

Вызываемый — обычный хендлер, объявленный через `sb.Handle[Req, Resp]`. Чтобы шаг до него дошёл, тот же сервис и метод должны быть объявлены как зависимость с указанием типов:

```go
func declareDeps(c *sb.Client) error {
	inventory := sb.NewClient(c, "inventory-svc")
	if _, err := sb.NewMethod[*pb.ReserveRequest, *pb.ReserveReply](inventory, "Reserve"); err != nil {
		return err
	}
	_, err := sb.NewMethod[*pb.ReleaseRequest, *pb.ReleaseReply](inventory, "Release")
	return err
}
```

Зачем это нужно: маршрутизация по версии контракта сверяет хеш пары «запрос — ответ» точным равенством, а шаг сам по себе несёт только имя метода и JSON-дерево. Пара типов даёт и хеш, и кодирование.

Что откуда берётся:

| Что | Откуда |
|---|---|
| Байты запроса | JSON-дерево `Input` читается в `Req` |
| Contract hash | Пара `(Req, Resp)` из `NewMethod` |
| Выход шага в состоянии прогона | `Resp` в виде своего JSON-зеркала |

**Форма JSON-зеркала.** Это вывод `protojson`, а не Go-структуры: 64-битные целые — строки (`"9007199254740993"`), перечисления — имена значений (`"STATUS_ACTIVE"`), `bytes` — base64. В эту же форму пишется `Input`, и в ней же выход шага ложится в состояние, поэтому значение, вышедшее из одного шага, входит в следующий без потерь. Число вместо строки для 64-битного поля тоже принимается — но выше 2^53 оно уже потеряло точность в самом JSON.

**Незнакомое поле в `Input` — ошибка шага**, а не молчаливо пропущенное значение. Опечатка в имени поля видна сразу.

**Незаявленная зависимость.** Если `Service` и `Method` записаны литералами (`wf.Name`), несовпадение ловится на `Start` — до первого прогона, с кодом `CodeConfig`:

```
Client.Start: CONFIG: workflow "checkout" step "reserve": inventory-svc/Reserve is not a
declared dependency: bind it with servicebridge.NewMethod[Req, Resp](
servicebridge.NewClient(c, "inventory-svc"), "Reserve") before Start
```

Если же имя вычисляется из состояния (`wf.Path`), до запуска шага его не существует — такой шаг падает с тем же сообщением уже в прогоне.

`c.Service(name, sb.ServiceDeps{...})` объявляет только ребро графа сервисов, без типов, и для шага `call` его недостаточно.

```go
func waitSteps() []wf.Step {
	return []wf.Step{
		wf.Sleep{
			Control:  wf.Control{ID: "cooldown"},
			Duration: 5 * time.Minute,
		},
		wf.WaitEvent{
			Control: wf.Control{ID: "await_payment", WaitFor: []string{"cooldown"}},
			Event:   "payment.settled",
			// Ключ — путь в payload события, значение — литерал или путь в состоянии.
			Filter: map[string]any{"$.orderId": wf.Path("$.input.orderId")},
		},
		wf.WaitSignal{
			Control: wf.Control{ID: "await_approval", WaitFor: []string{"await_payment"}},
			Signal:  "approval",
		},
	}
}
```

## 4. Пути и литералы

Два строковых типа разводят выражение и данные:

- `wf.Path("$.reserve.id")` читается из состояния прогона в момент исполнения шага.
- `wf.Name("payment-svc")` — литерал, записанный при объявлении.

Литерал, который выглядит как путь, экранировать не нужно: тип сам говорит, что есть что.

Грамматика пути: `$`, дальше любое число сегментов `.field`, `[N]` и `[*]`. `[*]` с последующим `.field` собирает это поле из каждого элемента в массив.

```go
func paths() []wf.Path {
	return []wf.Path{
		"$.input",                 // вход прогона
		"$.charge.transactionId",  // поле из выхода шага charge
		"$.reserve.items[0].sku",  // элемент массива
		"$.reserve.items[*].sku",  // это поле из каждого элемента, массивом
	}
}
```

Пути вычисляет рантайм в момент активации шага. Путь, который никуда не ведёт, — «нет значения» (в объекте поле опускается, в массиве — `null`): шаг, пропущенный по условию, оставляет выход `null`, и каждый потребитель обязан это пережить. Синтаксически неверный путь рантайм отвергает при регистрации. Внутри итерации `ForEach` доступны имя `As` и короткие идентификаторы соседей по итерации.

`Path` можно класть внутрь дерева значений — они разрешаются на любой глубине:

```go
func callWithTree() wf.Step {
	return wf.Call{
		Control: wf.Control{ID: "notify"},
		Service: wf.Name("mail-svc"),
		Method:  wf.Name("Send"),
		Input: map[string]any{
			"to":       wf.Path("$.input.email"),
			"template": "order_placed",
			"vars": map[string]any{
				"order": wf.Path("$.input.orderId"),
				"total": wf.Path("$.charge.amount"),
			},
		},
	}
}
```

`Target` — тоже закрытый союз: имя сервиса, метода, события или workflow может быть только `wf.Name` или `wf.Path`, и третий вариант ловит компилятор, а не валидация.

## 5. Условия

```go
func predicates() []wf.Predicate {
	return []wf.Predicate{
		wf.Truthy(wf.Path("$.charge.ok")),
		wf.Not(wf.Truthy(wf.Path("$.input.dryRun"))),
		wf.Equals(wf.Path("$.input.currency"), "EUR"),
		wf.In(wf.Path("$.input.tier"), []any{"gold", "platinum"}),
		wf.And(
			wf.Truthy(wf.Path("$.charge.ok")),
			wf.Equals(wf.Path("$.input.currency"), "EUR"),
		),
		wf.Or(
			wf.Truthy(wf.Path("$.input.express")),
			wf.Equals(wf.Path("$.input.tier"), "platinum"),
		),
	}
}
```

| Конструктор | Держится, когда |
|---|---|
| `wf.Truthy(p)` | Значение по пути присутствует и не равно `false`, нулю или пустой строке. |
| `wf.Not(p)` | Вложенный предикат не держится. |
| `wf.Equals(l, r)` | Обе стороны разрешаются в одно JSON-значение. Любая сторона — путь, литерал или дерево из того и другого. |
| `wf.In(v, list)` | `v` — элемент `list`. |
| `wf.And(preds...)` | Держатся все. |
| `wf.Or(preds...)` | Держится хотя бы один. |

Набор закрыт; собрать предикат можно только этими конструкторами, поэтому его форма всегда корректна.

## 6. Группы и forEach

```go
func groups() wf.Step {
	return wf.Parallel{
		Control: wf.Control{ID: "notify_all"},
		ForEach: &wf.ForEach{From: wf.Path("$.input.recipients"), As: "recipient"},
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "send"},
				Service: wf.Name("mail-svc"),
				Method:  wf.Name("Send"),
				Input:   wf.Path("$.recipient"),
			},
		},
	}
}
```

- `wf.Parallel` запускает вложенные шаги разом и завершается, когда завершатся все.
- `wf.Sequence` выполняет их по очереди.
- `ForEach` — свойство группы, а не отдельный вид шага: `From` — путь к списку (список известен только во время прогона; известный при объявлении пишется шагами), `As` — имя элемента в состоянии, в том же алфавите, что идентификатор шага.

Шаги итерации получают идентификаторы с суффиксом (`send:0`, `send:1`), выход группы — карта этих идентификаторов в их выходы. Пустой список завершает группу сразу с пустой картой. Вложенность ограничена десятью уровнями, шагов в графе — не больше 500, развёртка `ForEach` — 10 000 шагов на прогон.

## 7. Компенсации

```go
func compensated() wf.Step {
	return wf.Call{
		Control: wf.Control{
			ID: "reserve",
			Compensate: &wf.Compensation{
				Kind:     wf.CompensateCall,
				Service:  wf.Name("inventory-svc"),
				Method:   wf.Name("Release"),
				Input:    wf.Path("$.reserve"),
				Retry:    &wf.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second},
				CallOpts: &wf.CallOpts{IdempotencyKey: wf.Path("$.input.orderId")},
			},
		},
		Service: wf.Name("inventory-svc"),
		Method:  wf.Name("Reserve"),
		Input:   wf.Path("$.input"),
	}
}
```

- Компенсация допустима только на `Call` и `Publish` — больше ни у чего нет эффекта, который нужно откатывать. На остальных видах это ошибка объявления.
- `Kind` пустой означает «как у шага»: у `Call` — вызов, у `Publish` — публикация. Явно задаётся через `wf.CompensateCall` / `wf.CompensatePublish`.
- `Input` компенсации обычно ссылается на **выход** компенсируемого шага: чтобы отменить бронь, нужен её идентификатор.
- `Retry` — повторы самой компенсации (иначе `Retry` шага, иначе графа).

Когда шаг провалился окончательно, прогон отменён или истёк его таймаут, рантайм отменяет исполняемые задачи и ожидания, отменяет незавершённые вложенные прогоны и выполняет компенсации успешных шагов в обратном порядке завершения. Компенсированный шаг получает статус `compensated`. Итог: `failed` / `cancelled` / `timed_out`, если все компенсации прошли; `failed_compensated`, если какая-то исчерпала попытки (остальные всё равно выполняются). Такой прогон повторяется через `c.Workflow.RetryCompensation(ctx, runID)` или из консоли.

## 8. Локальный шаг

```go
func localStep() wf.Step {
	return wf.Local{
		Control: wf.Control{ID: "score", WaitFor: []string{"charge"}},
		Fn: func(ctx context.Context, state map[string]any) (any, error) {
			input, _ := state["input"].(map[string]any)
			orderID, _ := input["orderId"].(string)
			return map[string]any{"risk": len(orderID) % 7}, nil
		},
	}
}
```

Замыкание не уходит в рантайм. Задача несёт `Version` определения и `ID` шага, и клиент подставляет локально объявленную функцию; если такой версии в процессе нет — шаг падает с `UNSUPPORTED_VERSION` без повторов. Поэтому меняйте `Definition.Version` вместе с кодом `Local`.

`state` — снимок состояния, который рантайм собрал при активации шага. Возвращённое значение станет выходом шага. `ctx` отменяется при потере lease (результат такой попытки не отправляется), дедлайне шага и остановке клиента; `wf.TaskOf(ctx)` возвращает `RunID`, `StepID` и `Attempt`.

## 9. Управление прогоном

```go
func drive(ctx context.Context, c *sb.Client) error {
	runID, err := c.Workflow.Start(ctx, "orders-svc", "checkout",
		map[string]any{"orderId": "o-1"},
		sb.WithRunIdempotencyKey("checkout-o-1"),
		sb.WithRunTimeout(10*time.Minute),
	)
	if err != nil {
		return err
	}

	snap, err := c.Workflow.Query(ctx, runID)
	if err != nil {
		return err
	}
	log.Println("status:", snap.Status, "waiting:", snap.WaitingReason)

	if _, err := c.Workflow.Signal(ctx, runID, "approval", map[string]any{"ok": true}, sb.WithSignalID("approve-o-1")); err != nil {
		return err
	}

	out, err := c.Workflow.Await(ctx, runID)
	var failed *sb.RunFailedError
	if errors.As(err, &failed) {
		log.Println("run ended", failed.Status, failed.ErrorCode)
		return nil
	}
	log.Println("output:", out)
	return err
}
```

| Операция | Что делает |
|---|---|
| `Start(ctx, service, name, input, opts...)` | Запускает прогон workflow `name` сервиса `service`. |
| `Query(ctx, runID)` | `RunSnapshot`: `Status`, `StopReason`, `WaitingReason`, `Output`, `ErrorCode/ErrorMessage`, шаги (`StepSnapshot`: `Status`, `Attempt`, `Output`, `ErrorCode`, `ErrorMessage`, `WaitingReason`, `WaitKey`, `ChildRunID`, `CompensatesStepID`) и непрочитанные сигналы. |
| `Signal(ctx, runID, signal, payload, opts...)` | Ставит сигнал в FIFO-очередь прогона; возвращает `duplicate` для повторного `WithSignalID`. Не больше 1000 непрочитанных сигналов. |
| `Cancel(ctx, runID)` | Останавливает прогон; уже сделанное компенсируется, итог — `cancelled`. |
| `Await(ctx, runID)` | Ждёт завершения: выход прогона (карта состояния) при `success`, иначе ошибка с `*sb.RunFailedError` внутри. |
| `Replay(ctx, runID, fromStepID)` | Новый прогон по замороженному плану и входу; с `fromStepID` (шаг верхнего уровня) независимые от него успешные шаги копируются. |
| `RetryCompensation(ctx, runID)` | Повторяет упавшие компенсации прогона `failed_compensated`. |

Статусы прогона: `active`, `compensating`, `success`, `failed`, `cancelled`, `timed_out`, `failed_compensated`. Статусы шагов: `pending`, `leased`, `parked`, `success`, `failed`, `compensated`. `WaitingReason`: `no_instance` (нет живого инстанса этой версии), `retry`, `sleep`, `signal`, `event`, `child`.

Права: `Start` — двусторонняя политика (`workflow.run` вызывающего и `workflow.handle` владельца). `Signal`, `Query`, `Await`, `Cancel`, `Replay`, `RetryCompensation` — владелец, сервис, запустивший прогон, или сервис с **явным** egress-правилом `workflow.run` на этот workflow; остальным — `CodeAccessDenied`.

| Код | Когда |
|---|---|
| `CodeNotFound` | Нет такого workflow у сервиса или нет прогона. |
| `CodeAccessDenied` | Отказ политики или чужой прогон. |
| `CodeTerminal` | Сигнал или отмена завершённого прогона; `RetryCompensation` не для `failed_compensated`; `Await` увидел неуспешный исход. |

## 10. Ошибки объявления

`c.Workflow.Handle` только кодирует граф (ошибка — то, что нельзя передать: шаг `nil`, `Local` без функции, значение, не сериализуемое в JSON). Проверяет граф рантайм при регистрации, и `c.Start` возвращает его отказ (`InvalidArgument`, текст называет шаг и правило). Имя, которое уже объявляет другой сервис с живым инстансом, отклоняется (`AlreadyExists`).

Что проверяет рантайм:

- `ID` шага подходит под `^[a-z0-9_]+$`, не `input` и уникален по всему графу.
- `WaitFor` ссылается на соседей в той же группе и не образует цикла.
- Компенсация стоит только на `Call` или `Publish`, цель компенсации определена.
- Каждый путь — в значениях, фильтре, опциях, предикате, `ForEach.From` — синтаксически разбирается; ключи `Filter` — пути payload.
- `SubWorkflow` без `Service` не запускает свой же workflow (цикл через цепочку предков рантайм ловит при запуске).
- `Sleep.Duration` положительный, `MaxParallelism` ≤ 1024, политика повторов корректна.
- Глубина вложенности не больше 10, шагов не больше 500.

Отдельно клиент на `Start` проверяет, что у каждого `call` с литеральной целью объявлена зависимость (`CodeConfig`).

---

Дальше: [Jobs](./jobs.md) · [Тестирование](./testing.md)
