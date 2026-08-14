# Тестирование

[← к индексу](./index.md)

`sbtest` — in-memory двойник, который прогоняет ваши обработчики без сети, без рантайма и без локального хранилища.

Прежде чем читать про то, что двойник умеет, прочитайте [§6](#6-чего-двойник-не-воспроизводит) — про то, чего он не умеет. Это важнее.

## Содержание

1. [Краткая модель](#1-краткая-модель)
2. [Входящий RPC](#2-входящий-rpc)
3. [Исходящие вызовы](#3-исходящие-вызовы)
4. [События](#4-события)
5. [Тестируемая фабрика обработчика](#5-тестируемая-фабрика-обработчика)
6. [Чего двойник не воспроизводит](#6-чего-двойник-не-воспроизводит)
7. [Шпаргалка](#7-шпаргалка)

## 1. Краткая модель

```go
func harness() *sbtest.Harness {
	h := sbtest.New()
	_ = h.RPC   // входящие обработчики и исходящие вызовы
	_ = h.Event // объявления, подписки и публикации
	return h
}
```

Импорт: `import "github.com/service-bridge/sdk/go/sbtest"`.

Один харнесс на тест: регистрации и записи живут в экземпляре, поэтому параллельные тесты не видят друг друга. `h.Reset()` очищает оба двойника.

Обработчики получают уже декодированные значения, а их ошибки долетают **как есть**, без транспортной классификации — тест проверяет ту бизнес-ошибку, которую вы написали.

## 2. Входящий RPC

```go
func TestChargeAccepts(t *testing.T) {
	h := sbtest.New()
	if err := sbtest.Handle(h.RPC, "Charge", chargeHandler); err != nil {
		t.Fatal(err)
	}

	res, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "Charge",
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() {
		t.Fatal("expected the charge to be accepted")
	}
}
```

- `sbtest.Handle` регистрирует обработчик под именем метода. Занятое имя **отвергается**, а не перетирается: рантайм тоже отвергает повторное объявление, а тест, тихо потерявший первую регистрацию, проходит по неверной причине.
- `sbtest.Invoke` вызывает зарегистрированный обработчик. Имя, под которым ничего не зарегистрировано, — `sbtest.ErrNoHandler`.

Проверка ошибки обработчика:

```go
func TestChargeRejectsZero(t *testing.T) {
	h := sbtest.New()
	if err := sbtest.Handle(h.RPC, "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			if req.GetAmount() <= 0 {
				return nil, errZeroAmount
			}
			return &paymentpb.ChargeReply{Ok: true}, nil
		}); err != nil {
		t.Fatal(err)
	}

	_, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "Charge", &paymentpb.ChargeRequest{Amount: 0})
	if !errors.Is(err, errZeroAmount) {
		t.Fatalf("want errZeroAmount, got %v", err)
	}
}
```

## 3. Исходящие вызовы

```go
func TestPlaceOrderCharges(t *testing.T) {
	h := sbtest.New()

	if err := sbtest.RespondWith(h.RPC, "payment-svc", "Charge",
		&paymentpb.ChargeReply{Ok: true, TransactionId: "tx-1"}); err != nil {
		t.Fatal(err)
	}

	res, err := sbtest.Call[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "payment-svc", "Charge",
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetTransactionId() != "tx-1" {
		t.Fatalf("unexpected transaction: %q", res.GetTransactionId())
	}

	calls := h.RPC.Calls()
	if len(calls) != 1 || calls[0].Service != "payment-svc" || calls[0].Method != "Charge" {
		t.Fatalf("unexpected calls: %+v", calls)
	}
}
```

Ответ, зависящий от запроса, задаётся вычислением:

```go
func arrangeDynamic(h *sbtest.Harness) error {
	return sbtest.Respond(h.RPC, "payment-svc", "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			return &paymentpb.ChargeReply{
				Ok:            req.GetAmount() > 0,
				TransactionId: "tx-" + req.GetUserId(),
			}, nil
		})
}
```

- `Respond` при повторном вызове **заменяет** предыдущий ответ, в отличие от `Handle`: задавать разный ответ на разный случай — это то, чем занимается тест, а два обработчика под одним именем — ошибка объявления.
- Вызов без заданного ответа возвращает `sbtest.ErrNoResponse`, а не нулевую структуру. Забытый `Respond` — ошибка в тесте, и молчаливый ноль прячет её до утверждения где-то ниже, которое уже не назовёт причину.
- `h.RPC.Calls()` возвращает перехваченные вызовы в порядке, в котором они произошли; `Input` — то значение, которое передал вызывающий, а не его декодированная копия.

## 4. События

```go
func TestOrderPlacedSendsReceipt(t *testing.T) {
	h := sbtest.New()

	if err := sbtest.Define[*orderpb.OrderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatal(err)
	}

	var seen string
	if err := sbtest.Subscribe(h.Event, "order.placed",
		func(ctx context.Context, e *orderpb.OrderPlaced) error {
			seen = e.GetOrderId()
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	delivery, err := sbtest.Publish(context.Background(), h.Event, "order.placed",
		&orderpb.OrderPlaced{OrderId: "o-1", Total: 4200})
	if err != nil {
		t.Fatal(err)
	}
	if !delivery.Acked {
		t.Fatalf("delivery was nacked: %v", delivery.Err)
	}
	if seen != "o-1" {
		t.Fatalf("handler saw %q", seen)
	}
}
```

- `Define` обязателен перед `Publish`: рантайм отклоняет публикацию события, которое не регистрировали, и тест, пропустивший объявление, проходил бы против формы, которую прод не принимает.
- `Delivery{Name, Acked, Err}` — что стало с доставкой. `Acked` равно `false`, когда обработчик вернул ошибку: рантайм такую доставку отклоняет и повторяет позже.
- Первый упавший обработчик решает судьбу всей доставки — остальные не выполняются, ровно как в проде.
- Доставка, которую никто не обработал, подтверждается: маршрутизация принадлежит рантайму.
- `h.Event.Published()` и `h.Event.Deliveries()` возвращают записи, старые первыми.

## 5. Тестируемая фабрика обработчика

Обработчик стоит писать как обычную функцию, а регистрацию держать отдельно. Тогда прод регистрирует её в клиенте, а тест — в двойнике, и тестируется ровно тот код, который поедет в прод.

```go
// orders/charge.go
func NewChargeHandler(deps Deps) func(context.Context, *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
	return func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
		if err := deps.Ledger.Debit(ctx, req.GetUserId(), req.GetAmount()); err != nil {
			return nil, err
		}
		return &paymentpb.ChargeReply{Ok: true, TransactionId: "tx-" + req.GetUserId()}, nil
	}
}
```

```go
func wireProduction(c *sb.Client, deps Deps) error {
	return sb.Handle(c, "Charge", NewChargeHandler(deps))
}
```

```go
func wireTest(h *sbtest.Harness, deps Deps) error {
	return sbtest.Handle(h.RPC, "Charge", NewChargeHandler(deps))
}
```

## 6. Чего двойник не воспроизводит

Список короткий и он важнее всего остального в этом документе. `sbtest` **не** воспроизводит:

- маршрутизацию рантайма, включая сопоставление шаблонов событий;
- политику доступа;
- лизы, эпохи и фенсинг;
- ретраи, задержки и размыкатели;
- стриминг;
- workflow;
- идемпотентность и дедупликацию;
- порядок по ключу партиции.

**Зелёный тест на двойнике не означает работающий прод.** Двойник, притворяющийся полноценным рантаймом, вреднее его отсутствия: тест зеленеет там, где прод падает, и уверенность оказывается ложной. Всё перечисленное проверяется только end-to-end против живого рантайма.

Практический вывод: `sbtest` — для доменной логики внутри обработчика. Для маршрутизации, доставки, политик и оркестрации нужен запущенный рантайм.

## 7. Шпаргалка

| Что | Как |
|---|---|
| Создать харнесс | `h := sbtest.New()` |
| Очистить | `h.Reset()` |
| Только один двойник | `sbtest.NewRPC()` · `sbtest.NewEvent()` |
| Зарегистрировать обработчик | `sbtest.Handle(h.RPC, "Method", fn)` |
| Вызвать обработчик | `sbtest.Invoke[Req, Res](ctx, h.RPC, "Method", req)` |
| Задать ответ на исходящий вызов | `sbtest.Respond(h.RPC, "svc", "Method", fn)` |
| Задать готовый ответ | `sbtest.RespondWith(h.RPC, "svc", "Method", res)` |
| Сделать исходящий вызов | `sbtest.Call[Req, Res](ctx, h.RPC, "svc", "Method", req)` |
| Прочитать перехваченные вызовы | `h.RPC.Calls()` |
| Объявить событие | `sbtest.Define[T](h.Event, "name")` |
| Подписаться | `sbtest.Subscribe(h.Event, "name", fn)` |
| Опубликовать и доставить | `sbtest.Publish(ctx, h.Event, "name", payload)` |
| Прочитать публикации и доставки | `h.Event.Published()` · `h.Event.Deliveries()` |

Сентинелы отказа — `sbtest.ErrNoHandler`, `ErrNoResponse`, `ErrTypeMismatch`, `ErrDuplicate`, `ErrInvalidArg`; сравниваются через `errors.Is`.

Подробности контракта — в [`sbtest/README.md`](../sbtest/README.md).

---

Дальше: [Operations](./operations.md) · [Access Policy](./access-policy.md)
