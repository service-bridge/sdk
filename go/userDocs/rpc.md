# RPC

[← к индексу](./index.md)

Запрос/ответ между сервисами поверх mTLS: балансировка, ретраи и размыкатель — на стороне вызывающего, маршрутизация и политика — на стороне рантайма.

## Содержание

1. [Схема берётся из сгенерированных типов](#1-схема-берётся-из-сгенерированных-типов)
2. [Обработчики](#2-обработчики)
3. [Исходящие вызовы](#3-исходящие-вызовы)
4. [CallOption](#4-calloption)
5. [Транспорт: auto, direct и proxy](#5-транспорт-auto-direct-и-proxy)
6. [Стриминг](#6-стриминг)
7. [Устойчивость: балансировка, размыкатель, ретраи](#7-устойчивость-балансировка-размыкатель-ретраи)
8. [Ключ идемпотентности](#8-ключ-идемпотентности)
9. [Ошибки](#9-ошибки)

## Краткая модель

```
объявить (до Start)                     вызвать (после Start)
─────────────────────                   ──────────────────────
sb.Handle(c, name, fn)                  charge.Call(ctx, req)
sb.HandleStream(c, name, fn)            sb.Call[Req, Resp](ctx, c, svc, m, req)
sb.NewMethod[Req, Resp](sc, name)       sb.Stream[Req, Chunk](ctx, c, svc, m, req)
c.Service(name, deps)
```

Всё, чему нужен параметр типа, — **свободная функция с клиентом первым аргументом**: в Go нет генерик-методов. Всё, чему параметр типа не нужен, осталось методом домена (`c.Job`, `c.Workflow`, `c.Telemetry`).

## 1. Схема берётся из сгенерированных типов

Отдельного файла схемы, на который надо указать SDK, нет. Нет и шага «зарегистрировать схему». Типы запроса и ответа **и есть** контракт: SDK читает protobuf-дескриптор из сгенерированной структуры, выводит из него JSON Schema и хеш контракта и отправляет их в регистрации.

```proto
// payment.proto
syntax = "proto3";
package demo.payment;
option go_package = "example.com/orders/paymentpb";

message ChargeRequest { string user_id = 1; int64 amount = 2; string currency = 3; }
message ChargeReply   { bool ok = 1; string transaction_id = 2; }
```

```sh
protoc -I . --go_out=. --go_opt=module=example.com/orders payment.proto
```

Блок `service` не нужен, `protoc-gen-go-grpc` не нужен. Метод именуется строкой, под которой вы его регистрируете.

**Маршрутизация по версии контракта.** Хеш выводится из пары типов, поэтому два деплоя, собранные под разные формы сообщений, — это два разных контракта. Рантайм отдаёт вызывающему только те инстансы, которые публикуют ровно запрошенный хеш. Blue-green-выкатка сама маршрутизирует `v1→v1` и `v2→v2` вместо того, чтобы падать на декодировании.

Практическое следствие: если после выкатки вызовы начали возвращать `CodeNoLiveInstance`, а инстансы callee живы, — вы поменяли форму сообщения на одной стороне и не поменяли на другой.

## 2. Обработчики

### Unary

```go
func declareCharge(c *sb.Client) error {
	return sb.Handle(c, "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			if req.GetAmount() <= 0 {
				return nil, fmt.Errorf("amount must be positive, got %d", req.GetAmount())
			}
			return &paymentpb.ChargeReply{
				Ok:            true,
				TransactionId: "tx-" + req.GetUserId(),
			}, nil
		})
}
```

Оба параметра типа выводятся из переданной функции — писать их руками не нужно.

`ctx` обработчика несёт контекст трассировки вызывающего: всё, что вы вызовете с этим `ctx`, попадёт в то же дерево трейса. Передавайте его вниз.

### Контекст входящего вызова

```go
func declareShip(c *sb.Client) error {
	return sb.Handle(c, "Ship",
		func(ctx context.Context, req *shippb.ShipRequest) (*shippb.ShipReply, error) {
			info, _ := sb.CallInfoFromContext(ctx)
			log.Println("call", info.RequestID, "from", info.CallerServiceID, "key", info.IdempotencyKey)
			return &shippb.ShipReply{}, nil
		})
}
```

`sb.CallInfoFromContext(ctx)` возвращает `sb.CallInfo`:

| Поле | Что это |
|---|---|
| `RequestID` | Идентификатор логического вызова; одинаков во всех повторах вызывающего. |
| `IdempotencyKey` | Ключ, который передал вызывающий через `WithIdempotencyKey`; пусто, если не передал. |
| `CallerServiceID` | Вызывающий сервис: SPIFFE-идентичность прямого вызывающего или сервис, который назвал рантайм на прокси-пути. |
| `CallerInstanceID` | Вызывающий инстанс. Пусто на прокси-пути, где пир — рантайм. |
| `Deadline` | Дедлайн вызывающего; нулевое значение, если его нет. |

`ctx` обработчика отменяется, когда вызывающий отменил вызов или истёк его дедлайн.

### Ошибка из обработчика

Ошибка обработчика — это **ответ**, а не сбой транспорта: она едет вызывающему в теле ответа со статусом `OK`, размыкатель на неё не реагирует, SDK такой вызов не повторяет.

Чтобы ответить своим кодом, верните `*sb.HandlerError`:

```go
func declareReserve(c *sb.Client) error {
	return sb.Handle(c, "Reserve",
		func(ctx context.Context, req *inventorypb.ReserveRequest) (*inventorypb.ReserveReply, error) {
			if !inStock(req.GetSku()) {
				return nil, &sb.HandlerError{Code: "OUT_OF_STOCK", Message: "sku " + req.GetSku()}
			}
			return &inventorypb.ReserveReply{}, nil
		})
}
```

Вызывающий получает `*sb.Error` с `CodeHandler`, а `errors.As` достаёт из него тот же `*sb.HandlerError` с вашим `Code` и `Message` — см. [§9](#9-ошибки).

Любая другая ошибка и паника обработчика уходят вызывающему с кодом `INTERNAL`: он получает `CodeHandler`, а `*sb.HandlerError` внутри несёт `Code == "INTERNAL"` и текст ошибки. Паника перехватывается и логируется со стеком — процесс не падает. Ошибка SDK из вложенного вызова, которую обработчик вернул как есть, тоже уходит как `INTERNAL`, а не бизнес-кодом нижестоящего сервиса: чтобы передать код дальше, оберните его в свой `*sb.HandlerError`.

### Ограничения

- Регистрация возможна только **до** `Start`. Позже — `CodeState`.
- На клиенте с `WithCallerOnly()` регистрация обработчика отвергается с `CodeConfig`: у него нет входящего слушателя.
- Два обработчика на одно имя — ошибка (`CodeValidation`).
- Пустое имя или `nil`-функция — `CodeValidation`.

## 3. Исходящие вызовы

### Объявленный метод (рекомендуется)

```go
func declareDeps(c *sb.Client) (*sb.Method[*paymentpb.ChargeRequest, *paymentpb.ChargeReply], error) {
	payment := sb.NewClient(c, "payment-svc")
	return sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")
}
```

`sb.NewClient` сам по себе не объявляет ничего — он только именует сервис. Объявление делает `sb.NewMethod`: он записывает исходящую зависимость на `payment-svc.Charge` и связывает схему из параметров типа. Забыть «загрузить схему» здесь нельзя — такой шаг не существует.

Вызов:

```go
func charge(ctx context.Context, m *sb.Method[*paymentpb.ChargeRequest, *paymentpb.ChargeReply]) error {
	res, err := m.Call(ctx, &paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})
	if err != nil {
		return err
	}
	log.Println("ok:", res.GetOk())
	return nil
}
```

`*sb.Method` безопасен для конкурентного использования и рассчитан на то, чтобы жить рядом с зависимостью, которая его использует, — стройте его один раз при сборке графа зависимостей.

### Разовая форма

```go
func chargeOnce(ctx context.Context, c *sb.Client) error {
	res, err := sb.Call[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		ctx, c, "payment-svc", "Charge",
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100},
		sb.WithTimeout(5*time.Second),
		sb.WithBusinessKey("order-42"),
	)
	if err != nil {
		return err
	}
	log.Println("ok:", res.GetOk())
	return nil
}
```

Тип ответа из аргументов не выводится, поэтому оба параметра приходится писать. Если вызываете метод больше одного раза — объявите его через `sb.NewMethod`.

### Объявление зависимостей одним блоком

Когда нужен крупноблочный service map или когда метод вызывается только через нетипизированную форму:

```go
func declareServiceDeps(c *sb.Client) error {
	return c.Service("payment-svc", sb.ServiceDeps{
		RPC:       []string{"Charge", "Refund"},
		Workflows: []string{"checkout"},
		HTTP:      []string{"POST /orders"},
	})
}
```

Дубликаты схлопываются: одна и та же зависимость, объявленная и через `NewMethod`, и через `Service`, попадёт в кадр регистрации один раз.

События и задачи зависимостями не являются — событие доставляет рантайм, а задача срабатывает по своему расписанию. Попытка объявить их через `ServiceDeps` невозможна по типу.

## 4. CallOption

| Опция | По умолчанию | Что делает |
|---|---|---|
| `WithTimeout(d)` | `30s` (`sb.DefaultCallTimeout`) | Ограничивает один вызов; для стрима — весь стрим целиком. Более ранний дедлайн `ctx` по-прежнему действует. |
| `WithTransport(t)` | `TransportAuto` | Путь вызова. См. [§5](#5-транспорт-auto-direct-и-proxy). |
| `WithIdempotencyKey(k)` | нет | Ваш ключ дедупликации; callee читает его из `sb.CallInfoFromContext`. Набор повторов SDK он не расширяет. SDK ключ не выдумывает. |
| `WithBusinessKey(k)` | нет | Помечает вызов в трейсе доменным идентификатором — номером заказа, идентификатором клиента, — чтобы оператор нашёл его без внутреннего id операции. |

### Дефолты на весь клиент

```go
func withDefaults(key string) (*sb.Client, error) {
	return sb.New("localhost:14445", key,
		sb.WithCallDefaults(
			sb.WithTimeout(10*time.Second),
			sb.WithTransport(sb.TransportProxy),
		),
	)
}
```

`WithCallDefaults` принимает те же `CallOption`, что и сам вызов — словарь один, а не два. Дефолты действуют на всех путях вызова: `sb.Call`, `sb.Stream`, объявленные методы и шаги `wf.Call` в workflow. Опция, переданная в конкретный вызов, перекрывает дефолт.

## 5. Транспорт: auto, direct и proxy

| | `TransportAuto` (по умолчанию) | `TransportDirect` | `TransportProxy` |
|---|---|---|---|
| Путь | Напрямую к инстансу по mTLS; если прямой путь отказал **до отправки запроса**, следующая попытка идёт через рантайм | Только напрямую | Только через рантайм |
| Кто выбирает инстанс | балансировщик SDK, на прокси-попытке — рантайм | балансировщик SDK | рантайм |
| Балансировка и размыкатель | применяются к прямым попыткам | применяются | не применяются на стороне SDK |
| Требует адрес callee | для прямого пути | да — инстанс без `call_endpoint` не кандидат | нет |

Переход `auto` на прокси идёт без паузы: отказал путь, а не callee. Инстансы, до которых не удалось достучаться напрямую, SDK передаёт рантайму, и тот пробует их последними.

### advertise на стороне callee

Инстанс, который обслуживает RPC, должен объявить адрес, по которому его наберут:

```go
func serveOn(key string) (*sb.Client, error) {
	return sb.New("localhost:14445", key,
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051))
}
```

- По умолчанию — `127.0.0.1` и порт `0`. Порт `0` просит свободный порт у ОС и объявляет тот, который она выдала.
- Адрес объявляется **как есть**. В контейнере передавайте реальный адрес: угадывать его из окружения неверно чаще, чем верно.
- Инстанс, который только вызывает, объявляется через `sb.WithCallerOnly()` — входящий слушатель не поднимается, регистрация обработчика запрещена. `WithCallerOnly` и `WithAdvertise` противоречат друг другу и вместе дают `CodeConfig`.

### Приём вызова на стороне callee

Входящий сервер проверяет вызывающего по правилам приёма (acceptance) из политики доступа. Отказ приезжает вызывающему как `CodeAccessDenied`. Отсутствие правил — разрешение. Вызовы от отозванного сервиса или инстанса отвергаются сразу, тоже с `PERMISSION_DENIED`.

`WithInboundLimits(calls, queued)` (по умолчанию `256` / `256`) задаёт, сколько обработчиков работает одновременно и сколько вызовов ждёт свободного. За обеими границами вызывающий получает `RESOURCE_EXHAUSTED` с доказательством «не отправлено в обработчик» и повторяет на другом инстансе.

Отказы до обработчика, которые видит вызывающий:

| Ситуация | gRPC-статус | Повтор на другом инстансе |
|---|---|---|
| Инстанс ещё не получил первый снапшот реестра | `UNAVAILABLE "not ready"` | да |
| Инстанс останавливается (`Stop`) | `UNAVAILABLE "draining"` | да |
| Все слоты и вся очередь заняты | `RESOURCE_EXHAUSTED` | да |
| Правила приёма не пускают, вызывающий отозван | `PERMISSION_DENIED` | нет |
| Метод не зарегистрирован | `NOT_FOUND` | нет |
| Не тот вид вызова (unary против stream) | `FAILED_PRECONDITION` | нет |
| Запрос не декодируется | `INVALID_ARGUMENT` | нет |

Временные отказы несут трейлер `x-sb-not-dispatched: 1` — доказательство, что обработчик не запускался. Стрим отказывает тем же статусом, а не чанком с ошибкой.

## 6. Стриминг

Серверный стриминг — самостоятельная форма, а не надстройка. Обработчик отправляет через колбэк, вызывающий получает `iter.Seq2` и обходит его обычным `range`.

```go
func declareGenerate(c *sb.Client) error {
	return sb.HandleStream(c, "Generate",
		func(ctx context.Context, req *genpb.GenRequest, send func(*genpb.Token) error) error {
			for i, word := range strings.Fields(req.GetPrompt()) {
				if err := send(&genpb.Token{Text: word, Index: int32(i)}); err != nil {
					return err
				}
			}
			return nil
		})
}
```

```go
func consume(ctx context.Context, c *sb.Client) error {
	req := &genpb.GenRequest{Prompt: "write a haiku", MaxTokens: 64}
	for tok, err := range sb.Stream[*genpb.GenRequest, *genpb.Token](ctx, c, "gen-svc", "Generate", req) {
		if err != nil {
			return err
		}
		fmt.Print(tok.GetText(), " ")
	}
	return nil
}
```

Объявленный метод стримится так же — `m.Stream(ctx, req)`.

Что здесь важно:

- **`send` блокируется, пока вызывающий отстаёт** — это и есть backpressure — и возвращает ошибку, когда вызывающий ушёл. Обработчик, останавливающийся на первой ошибке `send`, останавливается тогда, когда нужно.
- **Выход из цикла рвёт стрим.** `break`, `return` или ошибка — Go по построению выполняет уборку итератора, поэтому брошенный стрим не может оставить работающим обработчик на той стороне.
- **Стримы не ретраятся.** Повтор доставил бы заново куски, которые вызывающий уже потребил.
- **`WithTimeout` ограничивает весь стрим**, по умолчанию 30 с. Для долгого стрима передайте больший таймаут.
- Ошибка приезжает вторым значением. После ошибки итерация заканчивается.

## 7. Устойчивость: балансировка, размыкатель, ретраи

Балансировка и размыкатель работают на прямом пути (`TransportAuto`, `TransportDirect`), потому что инстанс там выбирает SDK.

### Отбор кандидатов

Кандидатом становится инстанс, который: публикует запрошенный хеш контракта, объявил `call_endpoint`, не отозван, не отсечён локальным размыкателем и не помечен рантаймом как нездоровый. Если подсказка о здоровье исключила бы всех, она игнорируется. Из кандидатов выбирается менее загруженный из двух случайных (power of two choices по числу вызовов в полёте).

Три причины «некуда идти» различимы и лечатся по-разному, поэтому SDK считает их отдельно:

| Ситуация | Что это значит |
|---|---|
| Никто не публикует контракт | Другая форма сообщений, сервис не задеплоен, имя метода не то |
| Публикуют, но без адреса | Инстансы подняты как `WithCallerOnly` или без `WithAdvertise` |
| Все отсечены | Размыкатель разомкнут у всех инстансов |

Все три приезжают как `CodeNoLiveInstance`.

### Размыкатель

Считается по паре (сервис, инстанс): один больной под не должен сбрасывать трафик со своих здоровых соседей.

| Параметр | Значение |
|---|---|
| Окно | 10 с, 10 корзин |
| Порог выборки | 10 вызовов — ниже него доля отказов считается шумом и размыкатель закрыт |
| Доля отказов, размыкающая цепь | 0.5 |
| Время в разомкнутом состоянии | 30 с |
| Забвение неиспользуемой записи | 60 с |

Отказом считаются `CONNECTION`, `TIMEOUT`, `OVERLOADED` и внутренние статусы транспорта (`INTERNAL`, `UNKNOWN`, `DATA_LOSS`, `ABORTED`). Ответ обработчика, в том числе `*sb.HandlerError`, и любой другой код — успех: инстанс ответил.

### Ретраи

Бюджет попыток — `WithCallAttempts(n)`, по умолчанию `3`. Это **всего попыток**, считая первую: три означает один вызов и два повтора.

Задержки: 200 мс, множитель 2, потолок 5 с, джиттер ±30 %.

SDK повторяет вызов **только когда может доказать, что обработчик не запускался**:

| Доказательство | Пример |
|---|---|
| Не нашлось кандидата | `CodeNoLiveInstance` на этапе выбора |
| Канал к выбранному инстансу не стал готов до записи запроса | инстанс недостижим, TLS-рукопожатие не прошло |
| Отказ с трейлером `x-sb-not-dispatched: 1` | callee останавливается, ещё не готов, перегружен |

Всё остальное возвращается как есть: таймаут, оборванное посреди вызова соединение, `UNAVAILABLE` без трейлера, ответ обработчика. Эффект мог уже случиться, и молчаливый повтор `Charge` превратил бы одно списание в три. Ключ идемпотентности этого правила не меняет. Стримы не повторяются никогда.

**Практический вывод.** После `CodeTimeout` исход вызова неизвестен. Повторять ли — решение вызывающего, и безопасно оно только тогда, когда callee дедуплицирует по ключу (см. [§8](#8-ключ-идемпотентности)).

## 8. Ключ идемпотентности

```go
func chargeIdempotent(ctx context.Context, m *sb.Method[*paymentpb.ChargeRequest, *paymentpb.ChargeReply], orderID string) error {
	_, err := m.Call(ctx, &paymentpb.ChargeRequest{UserId: "u-1", Amount: 100},
		sb.WithIdempotencyKey("charge:"+orderID),
		sb.WithBusinessKey(orderID),
	)
	return err
}
```

Ключ доходит до callee в `CallInfo.IdempotencyKey`. Дедупликацию делает сам обработчик — например, уникальным индексом по ключу в своей базе. SDK ключ не хранит и по нему не дедуплицирует.

Ключ должен быть выведен из домена и стабилен между попытками одной и той же логической работы: `"charge:" + orderID`, а не случайный UUID на каждый вызов. SDK ключ не генерирует: сгенерированный за вас ключ менялся бы на каждой попытке и не защищал ни от чего.

Ключ вызова (`WithIdempotencyKey`) и ключ события (`WithEventIdempotencyKey`) названы по-разному, потому что едут в разные места.

## 9. Ошибки

`*sb.Error` — единственный тип ошибки, который возвращает SDK, поэтому один `errors.As` ловит любой его сбой и не устаревает при добавлении нового кода. Классификация живёт в поле `Code`; сентинелы сравниваются только по коду и игнорируют `Op`, `Msg` и обёрнутую причину.

```go
func classify(ctx context.Context, m *sb.Method[*paymentpb.ChargeRequest, *paymentpb.ChargeReply]) {
	_, err := m.Call(ctx, &paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})

	var sbErr *sb.Error
	if errors.As(err, &sbErr) {
		log.Printf("%s failed with %s: %s", sbErr.Op, sbErr.Code, sbErr.Msg)
	}

	switch {
	case errors.Is(err, sb.ErrAccessDenied):
		// политика доступа отказала в этом вызове
	case errors.Is(err, sb.ErrNoLiveInstance):
		// сейчас этот контракт никто не обслуживает
	case errors.Is(err, sb.ErrHandler):
		var he *sb.HandlerError
		if errors.As(err, &he) && he.Code == "OUT_OF_STOCK" {
			// собственный бизнес-ответ callee
		}
	case errors.Is(err, sb.ErrState):
		// вызов до Start или на остановленном клиенте
	}

	if sbErr != nil && sbErr.Retryable() {
		// временная ситуация, ничего не выполнено — можно повторить позже
	}
}
```

Коды, которые встречаются на пути вызова:

| Код | Сентинел | Когда |
|---|---|---|
| `CodeNoLiveInstance` | `ErrNoLiveInstance` | Идти некуда: контракт никто не публикует, никто не объявил адрес или у всех разомкнут размыкатель. Ретраебельный. |
| `CodeConnection` | `ErrConnection` | Callee или рантайм недостижимы (`UNAVAILABLE`), канал не стал готов. Ретраебельный. |
| `CodeOverloaded` | `ErrOverloaded` | Callee или рантайм сбрасывают нагрузку (`RESOURCE_EXHAUSTED`). Ретраебельный. |
| `CodeTimeout` | `ErrTimeout` | Дедлайн истёк, исход неизвестен. Не ретраебельный. |
| `CodeCancelled` | `ErrCancelled` | Вызывающий отменил `ctx`. |
| `CodeAccessDenied` | `ErrAccessDenied` | Политика доступа mesh отказала, вызывающий отозван. |
| `CodeNotFound` | `ErrNotFound` | Метод не зарегистрирован у callee. |
| `CodeHandler` | `ErrHandler` | Обработчик callee ответил ошибкой; `errors.As` достаёт `*sb.HandlerError` с его кодом — своим или `INTERNAL`, если обработчик вернул другую ошибку или запаниковал. |
| `CodeValidation` | `ErrValidation` | Запрос не кодируется, callee не разобрал запрос, объявление отвергнуто локально. |
| `CodeState` | `ErrState` | Не та фаза жизненного цикла: объявление после `Start`, вызов на остановленном клиенте. |
| `CodeConfig` | `ErrConfig` | Конфигурация, с которой SDK отказывается работать, — например, обработчик на `WithCallerOnly`. |
| `CodeInternal` | `ErrInternal` | gRPC-статус `INTERNAL`, `UNKNOWN`, `ABORTED`, `DATA_LOSS`; всё остальное. |

`(*sb.Error).Retryable()` истинен ровно для `CONNECTION`, `NO_LIVE_INSTANCE`, `OVERLOADED` и `QUEUE_FULL`: ситуация временная и ничего не выполнено. `TIMEOUT` не ретраебельный — исход неизвестен.

Полный список кодов — в [API reference](./api-reference.md#ошибки).

---

Дальше: [Events](./events.md) · [Operations](./operations.md)
