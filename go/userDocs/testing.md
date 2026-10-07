# Тестирование

[← к индексу](./index.md)

`sbtest` прогоняет ваши обработчики внутри настоящего `*sb.Client`, у которого сетевые края заменены памятью: без рантайма, без слушателя, без TLS.

Прежде чем читать про то, что харнесс умеет, прочитайте [§7](#7-чего-харнесс-не-воспроизводит) — про то, чего он не умеет.

## Содержание

1. [Краткая модель](#1-краткая-модель)
2. [Входящий RPC](#2-входящий-rpc)
3. [Исходящие вызовы](#3-исходящие-вызовы)
4. [Публикации](#4-публикации)
5. [Доставка событий](#5-доставка-событий)
6. [Тестируемая фабрика обработчика](#6-тестируемая-фабрика-обработчика)
7. [Чего харнесс не воспроизводит](#7-чего-харнесс-не-воспроизводит)
8. [Шпаргалка](#8-шпаргалка)

## 1. Краткая модель

```go
func TestSomething(t *testing.T) {
	h := sbtest.New(t) // настоящий клиент с in-memory транспортом
	// объявления — на h.Client, обычным API SDK
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// дальше: sbtest.Invoke, h.Deliver, h.Calls(), h.Published()
}
```

Импорт: `import "github.com/service-bridge/sdk/go/sbtest"`.

- `sbtest.New(t, opts...)` строит харнесс вокруг свежего клиента. `opts` — обычные опции `sb.New`. Клиент останавливается сам, когда тест заканчивается.
- `h.Client` — настоящий `*sb.Client`. Обработчики, подписки, зависимости и события объявляются на нём ровно так же, как в проде: `sb.Handle`, `sb.HandleStream`, `sb.SubscribeEvent`, `sb.NewMethod`, `sb.DefineEvent`.
- `h.Start(ctx)` запечатывает объявления и делает клиент готовым — как `c.Start` против рантайма. Объявление после `Start` — `CodeState`, как в проде.

Запрос и ответ проходят через ту же protobuf-кодировку, ту же обёртку обработчика, тот же маппинг ошибок, ту же очередь публикаций и ту же маршрутизацию событий, что и в проде. Подменено только то, что ответили бы рантайм и пиры.

Один харнесс на тест: ответы и записи живут в экземпляре, поэтому параллельные тесты не видят друг друга. `h.Reset()` забывает все заданные ответы и записи.

## 2. Входящий RPC

```go
func TestChargeAccepts(t *testing.T) {
	h := sbtest.New(t)
	if err := sb.Handle(h.Client, "Charge", chargeHandler); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	res, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h, "Charge",
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100},
		sbtest.WithCaller("orders-svc-id", "inst-1"),
		sbtest.WithIdempotencyKey("charge:o-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() {
		t.Fatal("expected the charge to be accepted")
	}
}
```

`sbtest.Invoke` вызывает обработчик так, как вызвал бы пир: запрос кодируется, диспетчер клиента его декодирует, запускает обработчик и кодирует ответ, а тот декодируется в `Resp`. Несовпадение типов падает так же, как по сети.

Что обработчик видит в `sb.CallInfoFromContext`:

| `InvokeOption` | Поле `CallInfo` | По умолчанию |
|---|---|---|
| `sbtest.WithCaller(serviceID, instanceID)` | `CallerServiceID`, `CallerInstanceID` | пусто |
| `sbtest.WithRequestID(id)` | `RequestID` | новый UUID |
| `sbtest.WithIdempotencyKey(key)` | `IdempotencyKey` | пусто |

`Deadline` — дедлайн `ctx`, переданного в `Invoke`; отмена этого `ctx` отменяет контекст обработчика.

### Ошибки в форме вызывающего

```go
func TestReserveOutOfStock(t *testing.T) {
	h := sbtest.New(t)
	if err := sb.Handle(h.Client, "Reserve",
		func(ctx context.Context, req *inventorypb.ReserveRequest) (*inventorypb.ReserveReply, error) {
			return nil, &sb.HandlerError{Code: "OUT_OF_STOCK", Message: "sku " + req.GetSku()}
		}); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := sbtest.Invoke[*inventorypb.ReserveRequest, *inventorypb.ReserveReply](
		context.Background(), h, "Reserve", &inventorypb.ReserveRequest{Sku: "42"})
	var he *sb.HandlerError
	if !errors.Is(err, sb.ErrHandler) || !errors.As(err, &he) || he.Code != "OUT_OF_STOCK" {
		t.Fatalf("want OUT_OF_STOCK, got %v", err)
	}
}
```

| Что сделал обработчик | Что вернёт `Invoke` |
|---|---|
| Вернул `*sb.HandlerError` | `*sb.Error` с `CodeHandler`; `errors.As` достаёт тот же `*sb.HandlerError` |
| Вернул другую ошибку или запаниковал | `CodeHandler`, `HandlerError.Code == "INTERNAL"` |
| Вернул ошибку SDK из вложенного вызова как есть | `CodeHandler`, `HandlerError.Code == "INTERNAL"`, а не бизнес-код нижестоящего сервиса |
| Метод не зарегистрирован | `CodeNotFound` |
| Запрос не декодируется | `CodeValidation` |

### Стриминг

`sbtest.InvokeStream[Req, Chunk](ctx, h, method, req, opts...)` вызывает обработчик `sb.HandleStream` и собирает его чанки в `[]Chunk`. Ошибки — по тем же правилам; чанки, отправленные до сбоя, возвращаются вместе с ошибкой.

## 3. Исходящие вызовы

```go
func TestCheckoutCharges(t *testing.T) {
	h := sbtest.New(t)
	payment := sb.NewClient(h.Client, "payment-svc")
	charge, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")
	if err != nil {
		t.Fatal(err)
	}
	if err := sb.Handle(h.Client, "Checkout", newCheckoutHandler(charge)); err != nil {
		t.Fatal(err)
	}
	if err := sbtest.Respond(h, "payment-svc", "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			return &paymentpb.ChargeReply{Ok: req.GetAmount() > 0, TransactionId: "tx-1"}, nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := sbtest.Invoke[*orderpb.CheckoutRequest, *orderpb.CheckoutReply](
		context.Background(), h, "Checkout", &orderpb.CheckoutRequest{OrderId: "o-1"}); err != nil {
		t.Fatal(err)
	}

	calls := h.Calls()
	if len(calls) != 1 || calls[0].Service != "payment-svc" || calls[0].Method != "Charge" {
		t.Fatalf("unexpected calls: %+v", calls)
	}
	req, err := sbtest.DecodeCall[*paymentpb.ChargeRequest](calls[0])
	if err != nil || req.GetAmount() <= 0 {
		t.Fatalf("charged %v, %v", req, err)
	}
}
```

- `sbtest.Respond(h, service, method, fn)` задаёт ответ на каждый исходящий вызов `service/method`: `sb.Call`, `Call` объявленного метода и шаг `wf.Call` в workflow. Запрос декодируется в `Req`, ответ кодируется из `Resp`, поэтому несовпадение типов падает так же, как по сети.
- `*sb.HandlerError` из `fn` — ответ с этим бизнес-кодом; любая другая ошибка — ответ `INTERNAL`.
- Повторный `Respond` на ту же пару заменяет предыдущий ответ.
- `sbtest.RespondStream(h, service, method, fn)` делает то же для исходящего `sb.Stream`; `fn` возвращает срез чанков.
- Вызов без заданного ответа падает с `sbtest.ErrNoResponse`, а не возвращает нулевую структуру. Забытый `Respond` — ошибка в тесте, и молчаливый ноль спрятал бы её до утверждения, которое уже не назовёт причину.
- `h.Calls()` возвращает исходящие вызовы в порядке, в котором они произошли, включая вызовы без ответа. `CallRecord` несёт `Service`, `Method`, `Payload`, `IdempotencyKey`, `BusinessKey`, `Transport`; `sbtest.DecodeCall[T](rec)` декодирует запрос обратно.

## 4. Публикации

```go
func TestPlaceOrderPublishes(t *testing.T) {
	h := sbtest.New(t)
	if err := sb.Handle(h.Client, "Place",
		func(ctx context.Context, req *orderpb.PlaceRequest) (*orderpb.PlaceReply, error) {
			id, err := sb.PublishEvent(ctx, h.Client, "order.placed",
				&orderpb.OrderPlaced{OrderId: req.GetOrderId()},
				sb.WithPartitionKey(req.GetOrderId()))
			if err != nil {
				return nil, err
			}
			return &orderpb.PlaceReply{EventId: id}, nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := sbtest.Invoke[*orderpb.PlaceRequest, *orderpb.PlaceReply](
		context.Background(), h, "Place", &orderpb.PlaceRequest{OrderId: "o-1"}); err != nil {
		t.Fatal(err)
	}

	published := h.Published()
	if len(published) != 1 || published[0].Name != "order.placed" || published[0].PartitionKey != "o-1" {
		t.Fatalf("published %+v", published)
	}
	e, err := sbtest.DecodePublished[*orderpb.OrderPlaced](published[0])
	if err != nil || e.GetOrderId() != "o-1" {
		t.Fatalf("decoded %v, %v", e, err)
	}
}
```

Каждая публикация проходит через настоящую очередь публикаций клиента; in-memory рантайм принимает каждый конверт. `h.Published()` возвращает принятые события по порядку: `ID`, `Name`, `Payload`, `PayloadJSON`, `PartitionKey`, `IdempotencyKey`, `Headers`, `OccurredAtMs`. Невалидное имя события падает с `CodeInvalidEventName`, как в проде.

## 5. Доставка событий

```go
func TestOrderPlacedSendsReceipt(t *testing.T) {
	h := sbtest.New(t)
	var seen string
	if err := sb.SubscribeEvent(h.Client, "order.*",
		func(ctx context.Context, e *orderpb.OrderPlaced) error {
			seen = e.GetOrderId()
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	res, err := h.Deliver(context.Background(), "order.placed", &orderpb.OrderPlaced{OrderId: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Acked {
		t.Fatalf("delivery was nacked: %s", res.Reason)
	}
	if seen != "o-1" {
		t.Fatalf("handler saw %q", seen)
	}
}
```

`h.Deliver(ctx, name, payload, opts...)` отдаёт событие подписчику клиента так, как это делает рантайм. Харнесс сам вычисляет шаблоны подписок, совпавшие с именем, по правилам рантайма (`*` — ровно один сегмент, `#` — ноль или больше), и подписчик запускает обработчики именно этих шаблонов. Payload проходит через настоящую кодировку, обработчик получает его декодированным в свой тип, `sb.DeliveryFromContext` работает.

`DeliveryResult`:

| Поле | Что это |
|---|---|
| `Acked` | `true`, если все обработчики совпавших шаблонов вернули `nil`. |
| `Reason` | Причина отклонения; пусто при подтверждении. Если ни один шаблон не совпал — `no handler for matched patterns`. |
| `MatchedPatterns` | Шаблоны, которые несла доставка. |

| `DeliverOption` | Что задаёт |
|---|---|
| `sbtest.WithMatchedPatterns(patterns...)` | Шаблоны доставки вместо вычисленных. Так воспроизводится решение фильтра рантайма: фильтры харнесс не вычисляет. |
| `sbtest.WithAttempt(n)` | Номер попытки (по умолчанию `1`). |
| `sbtest.WithDeliveryPartitionKey(k)` | Ключ партиции конверта. |
| `sbtest.WithDeliveryHeaders(h)` | Заголовки конверта. |

`sbtest.MatchPattern(pattern, name)` отвечает, направит ли рантайм событие `name` в подписку `pattern`, — удобно, чтобы проверить сам шаблон.

## 6. Тестируемая фабрика обработчика

Обработчик стоит писать как обычную функцию, а регистрацию держать отдельно. Тогда прод и тест регистрируют одну и ту же функцию, и тестируется ровно тот код, который поедет в прод.

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
func Wire(c *sb.Client, deps Deps) error {
	return sb.Handle(c, "Charge", NewChargeHandler(deps))
}
```

В проде `Wire` получает клиент из `sb.New`, в тесте — `h.Client`.

## 7. Чего харнесс не воспроизводит

`sbtest` подменяет рантайм и пиров, поэтому **не** воспроизводит то, что делает рантайм:

- политику доступа;
- фильтры подписок (их решение задаётся `WithMatchedPatterns`);
- лизы, ретраи доставки, DLQ;
- ретраи вызовов, балансировку и размыкатели;
- workflow и jobs.

**Зелёный тест на харнессе не означает работающий прод.** Всё перечисленное проверяется только end-to-end против живого рантайма. `sbtest` — для доменной логики внутри обработчика и для того, что обработчик вызывает и публикует.

## 8. Шпаргалка

| Что | Как |
|---|---|
| Создать харнесс | `h := sbtest.New(t)` |
| Объявить обработчик, подписку, зависимость | обычный API на `h.Client` |
| Запустить | `h.Start(ctx)` |
| Вызвать обработчик | `sbtest.Invoke[Req, Resp](ctx, h, "Method", req, opts...)` |
| Вызвать стриминговый обработчик | `sbtest.InvokeStream[Req, Chunk](ctx, h, "Method", req)` |
| Задать ответ на исходящий вызов | `sbtest.Respond(h, "svc", "Method", fn)` |
| Задать ответ на исходящий стрим | `sbtest.RespondStream(h, "svc", "Method", fn)` |
| Прочитать исходящие вызовы | `h.Calls()` · `sbtest.DecodeCall[T](rec)` |
| Прочитать публикации | `h.Published()` · `sbtest.DecodePublished[T](e)` |
| Доставить событие | `h.Deliver(ctx, "name", payload, opts...)` |
| Проверить шаблон | `sbtest.MatchPattern(pattern, name)` |
| Очистить ответы и записи | `h.Reset()` |

Сентинелы самого харнесса — `sbtest.ErrNoResponse` (ответ не задан) и `sbtest.ErrInvalidArg` (`nil`-харнесс, пустое имя, `nil`-функция); сравниваются через `errors.Is`.

Подробности контракта — в [`sbtest/README.md`](../sbtest/README.md).

---

Дальше: [Operations](./operations.md) · [Access Policy](./access-policy.md)
