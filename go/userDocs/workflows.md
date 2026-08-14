# Workflows

[← к индексу](./index.md)

Durable DAG. Граф объявляется один раз; исполняет его рантайм, состояние между шагами он же и хранит, поэтому прогон переживает рестарт и компенсируется при сбое или отмене.

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

- **Граф объявляется, а не программируется.** Вы описываете шаги и их зависимости; порядок исполнения выводится из `WaitFor`.
- **Состояние прогона — это JSON.** Вход прогона лежит под ключом `input`, выход каждого шага — под идентификатором этого шага. Шаги пишутся обычными Go-значениями: карты, срезы, строки, числа.
- **Шаг `call` идёт в обычный типизированный хендлер.** JSON-дерево шага перекладывается в protobuf-сообщение вызываемого и обратно — по паре типов, которую вы объявили через `sb.NewMethod` (см. [§3.1](#31-шаг-call-требует-объявленной-зависимости)).
- **Шаги переживают рестарт.** Каждый выполненный шаг чекпойнтится в рантайме; после падения инстанса прогон продолжается с того места, где остановился, а уже выполненный шаг не выполняется заново.
- **Сбой компенсируется в обратном порядке.** Шаги, у которых объявлена компенсация, откатываются от последнего к первому.
- **Граф исполняется из объявившего его процесса.** Рантайм назначает шаг, а тело шага берётся из локально объявленного графа — иначе `Local` с Go-замыканием был бы невозможен.

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
				Control: wf.Control{ID: "charge", WaitFor: []string{"reserve"}, TimeoutSec: 30},
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
| `Input` | нет | JSON Schema входа прогона. Едет внутри замороженного графа; по ней рантайм проверяет `Start`. |
| `Steps` | — | Шаги верхнего уровня. Хотя бы один обязателен. |
| `Retry` | решает рантайм | Политика повторов для каждого шага-операции, у которого нет своей. |
| `MaxParallelism` | решает рантайм | Потолок одновременно исполняемых шагов одного прогона. |
| `TimeoutSec` | решает рантайм | Ограничение на весь прогон, **в секундах**. |

Нулевое поле в замороженный граф не попадает — умолчание остаётся за рантаймом.

`Control`, общий для всех видов шагов:

| Поле | Что делает |
|---|---|
| `ID` | Имя шага внутри workflow. Только `^[a-z0-9_]+$`, уникально по всему графу, включая вложенность. |
| `WaitFor` | Идентификаторы шагов, которых этот дожидается. Порядок внутри списка значения не имеет. |
| `When` | Условие. Ложный предикат пропускает шаг, и всё, что он произвёл бы, разрешается в ничто. |
| `Compensate` | Обратное действие. Допустимо только на `Call` и `Publish`. |
| `TimeoutSec` | Сколько шаг может оставаться незавершённым, **в секундах**. По истечении прогон начинает компенсироваться. Это не таймаут самого вызова — тот живёт в `CallOpts.Timeout`. |
| `Retry` | Политика повторов вместо той, что задана на уровне графа. |

Шаги верхнего уровня стартуют параллельно; `WaitFor` объявляет зависимости, из которых складываются уровни исполнения.

## 3. Виды шагов

Набор закрыт: маркер-метод интерфейса `wf.Step` неэкспортируемый, поэтому объявить десятый вид шага, который рантайм не знает, невозможно.

| Вид | Что делает | Своё |
|---|---|---|
| `wf.Call` | Вызывает метод другого сервиса. | `Service`, `Method` (`Target`), `Input`, `Opts *CallOpts` |
| `wf.Publish` | Публикует durable-событие. | `Event` (`Target`), `Input`, `Opts *PublishOpts` |
| `wf.Sleep` | Паркует прогон на durable-таймере в рантайме. | `DurationSec int64` |
| `wf.WaitEvent` | Ждёт подходящее событие. | `Event` (`Target`), `Filter map[string]any` |
| `wf.WaitSignal` | Ждёт внешний сигнал по имени. | `Signal string` |
| `wf.SubWorkflow` | Запускает другой workflow и ждёт его завершения. | `Workflow` (`Target`), `Input`, `Opts *StartOpts` |
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
			Control:     wf.Control{ID: "cooldown"},
			DurationSec: 300,
		},
		wf.WaitEvent{
			Control: wf.Control{ID: "await_payment", WaitFor: []string{"cooldown"}},
			Event:   wf.Name("payment.settled"),
			Filter:  map[string]any{"orderId": wf.Path("$.input.orderId")},
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

Путь, который никуда не ведёт, разрешается в `nil`, а не в ошибку: шаг, пропущенный по условию, не оставляет после себя ничего, и каждый потребитель обязан это пережить. Ошибка — только у синтаксически неверного выражения и у `[*]` над тем, что не массив.

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

Группа не может быть пустой. Вложенность ограничена десятью уровнями, всего шагов в графе — не больше 500; обе границы проверяются при объявлении.

## 7. Компенсации

```go
func compensated() wf.Step {
	return wf.Call{
		Control: wf.Control{
			ID: "reserve",
			Compensate: &wf.Compensation{
				Kind:           wf.CompensateCall,
				Service:        wf.Name("inventory-svc"),
				Method:         wf.Name("Release"),
				Input:          wf.Path("$.reserve"),
				IdempotencyKey: wf.Path("$.input.orderId"),
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
- `IdempotencyKey` — строка или путь; компенсация выполняется не больше одного раза на ключ.

Компенсации запускаются в обратном порядке, когда шаг провалился окончательно или прогон отменён.

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

Замыкание не переживает сериализацию, поэтому в замороженный граф и в его отпечаток оно не попадает. Шаг опознаётся по `ID`: рантайм присылает назначение, а тело подставляет локально объявленный граф. Отсюда следствие — два графа, различающиеся только замыканием, дают один отпечаток: они описывают одну и ту же работу.

`state` — снимок состояния прогона на момент исполнения шага. Возвращённое значение станет выходом шага и ляжет в состояние под его `ID`. `ctx` отменяется при отмене прогона и при остановке клиента.

## 9. Управление прогоном

```go
func drive(ctx context.Context, c *sb.Client) error {
	runID, err := c.Workflow.Start(ctx, "checkout",
		map[string]any{"orderId": "o-1"},
		sb.WithRunIdempotencyKey("checkout-o-1"),
		sb.WithRunTimeoutSec(600),
	)
	if err != nil {
		return err
	}

	snap, err := c.Workflow.Query(ctx, runID)
	if err != nil {
		return err
	}
	log.Println("status:", snap.Status, "steps:", len(snap.Steps))

	if err := c.Workflow.Signal(ctx, runID, "approval", map[string]any{"ok": true}); err != nil {
		return err
	}

	state, err := c.Workflow.Await(ctx, runID)
	if err != nil {
		return err
	}
	log.Println("final state:", state)
	return nil
}
```

| Операция | Что делает |
|---|---|
| `Start(ctx, name, input, opts...)` | Запускает прогон, возвращает его идентификатор. |
| `Query(ctx, runID)` | Читает прогон, не дожидаясь: `RunSnapshot{RunID, Status, State, Steps}`, где каждый `StepSnapshot` несёт `StepID`, `Status`, `Output`, `LastError`, `CompensatedBy`. |
| `Signal(ctx, runID, signal, payload)` | Доставляет сигнал прогону, припаркованному на `WaitSignal`. |
| `Cancel(ctx, runID)` | Останавливает прогон; уже сделанное компенсируется. |
| `Await(ctx, runID)` | Блокируется до завершения и возвращает финальное состояние. |
| `Replay(ctx, runID, fromStepID)` | Форкает завершённый прогон в **новый**, переисполняя с указанного шага. Пустой `fromStepID` переигрывает весь прогон. Возвращается идентификатор нового прогона. |

Опции запуска:

| Опция | Что делает |
|---|---|
| `sb.WithRunIdempotencyKey(k)` | Повторный `Start` вернёт существующий прогон вместо второго. |
| `sb.WithRunTimeoutSec(sec)` | Ограничивает весь прогон. Секунды — это единица контракта workflow. |

**`Await` возвращает состояние только у успешного прогона.** Любой другой терминальный исход — отменённый прогон, прогон, откатившийся компенсациями, — приезжает ошибкой с кодом `CodeTerminal`: у компенсированного прогона нет результата, который можно отдать. Своего таймаута у `Await` нет — его ограничивает только ваш `ctx`.

Коды на этом пути:

| Код | Когда |
|---|---|
| `CodeNotFound` | Имени workflow в mesh нет. |
| `CodeAccessDenied` | Политика доступа отказала. |
| `CodeTerminal` | Прогон уже завершён: сигнал или отмена пришли поздно; либо `Await` увидел неуспешный терминальный статус. |
| `CodeValidation` | Объявление графа отвергнуто. |

## 10. Ошибки объявления

Граф проверяется целиком при `c.Workflow.Handle`, до того как что-либо уедет в рантайм: замораживание — единственный путь на провод, и невалидный граф зарегистрировать нельзя. Ошибка приезжает как `*sb.Error` с кодом `CodeValidation`; внутри лежит структура, называющая шаг и поле.

Что проверяется:

- Имя workflow непустое, шагов хотя бы один.
- `ID` шага подходит под `^[a-z0-9_]+$` и уникален по всему графу, включая вложенные группы.
- `WaitFor` ссылается на существующие шаги и не образует цикла.
- Компенсация стоит только на `Call` или `Publish`.
- `Target` непустой; каждый `Path` — в любом месте дерева значений, в фильтре, в опциях, в предикате — синтаксически разбирается.
- `SubWorkflow` не запускает workflow, которому сам принадлежит.
- `DurationSec` у `Sleep` не отрицательный, у `Local` есть функция, группа непустая, алиас `ForEach` подходит под алфавит идентификатора.
- Глубина вложенности не больше 10, шагов не больше 500.

---

Дальше: [Jobs](./jobs.md) · [Тестирование](./testing.md)
