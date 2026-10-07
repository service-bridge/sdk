# Integrations

[← к индексу](./index.md)

Подключение собственного HTTP-сервера приложения к ServiceBridge: публикация роутов в Service Map и один спан на запрос.

## Содержание

1. [Модель](#1-модель)
2. [net/http](#2-nethttp)
3. [chi](#3-chi)
4. [gin](#4-gin)
5. [Endpoint, Route и опции](#5-endpoint-route-и-опции)
6. [Что видно в Service Map](#6-что-видно-в-service-map)
7. [Трассировка и захват тел](#7-трассировка-и-захват-тел)
8. [Другой фреймворк](#8-другой-фреймворк)
9. [Ошибки](#9-ошибки)

## 1. Модель

**ServiceBridge не проксирует ваш бизнес-HTTP.** Сервер поднимаете вы; рантайм в трафик не вмешивается. Интеграция делает ровно две вещи:

1. Публикует список роутов и адрес сервера в Service Map и в discovery.
2. Оборачивает каждый запрос в один спан `HTTP.HANDLE`, чтобы HTTP-запрос и вызванные из него RPC и события оказались в одном дереве трейса.

`net/http` и chi имеют одну и ту же форму middleware — `func(http.Handler) http.Handler`, — поэтому оба обслуживаются пакетом `sbhttp` из основного модуля. gin такой совместимости не даёт и живёт в отдельном модуле `sbgin`: в Go нет необязательных зависимостей, и gin внутри основного модуля попал бы в граф зависимостей каждого пользователя SDK.

## 2. net/http

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	sb "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/sbhttp"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051))
	if err != nil {
		log.Fatal(err)
	}

	integration, err := sbhttp.New(c)
	if err != nil {
		log.Fatal(err)
	}

	mux := sbhttp.NewMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"` + r.PathValue("id") + `"}`))
	})

	if err := integration.Publish(mux.Routes(), sbhttp.Endpoint{Host: os.Getenv("POD_IP"), Port: 3000}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	srv := &http.Server{Addr: ":3000", Handler: integration.Middleware(mux)}
	log.Fatal(srv.ListenAndServe())
}
```

`sbhttp.NewMux` — тонкая обёртка над `http.ServeMux`, которая запоминает шаблоны по мере регистрации. Нужна она потому, что у `http.ServeMux` нет способа перечислить свои роуты: момент регистрации — единственное место, где список вообще существует.

Короче то же самое:

```go
func publishMux(i *sbhttp.Integration, mux *sbhttp.Mux) error {
	return i.PublishMux(mux, sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000})
}
```

## 3. chi

У chi своя таблица роутов, поэтому обход идёт через `chi.Walk`, включая под-роутеры. Middleware при этом тот же самый — chi принимает форму `net/http` как есть.

```go
func wireChi(c *sb.Client) (http.Handler, error) {
	integration, err := sbhttp.New(c)
	if err != nil {
		return nil, err
	}

	r := chi.NewRouter()
	r.Use(integration.Middleware)
	r.Post("/orders", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	r.Route("/orders/{id}", func(sub chi.Router) {
		sub.Get("/", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
	})

	if err := integration.PublishChi(r, sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000}); err != nil {
		return nil, err
	}
	return r, nil
}
```

`PublishChi` вызывайте **после** регистрации роутов: он читает то, что уже есть в роутере.

## 4. gin

```sh
go get github.com/service-bridge/sdk/go/sbgin
```

```go
func wireGin(c *sb.Client) (*gin.Engine, error) {
	integration, err := sbhttp.New(c)
	if err != nil {
		return nil, err
	}

	engine := gin.New()
	engine.Use(sbgin.Middleware(integration)) // до роутов
	engine.POST("/orders", func(ctx *gin.Context) {
		ctx.JSON(http.StatusCreated, gin.H{"ok": true})
	})

	if err := sbgin.Publish(integration, engine, sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000}); err != nil {
		return nil, err
	}
	return engine, nil
}
```

`sbgin.Middleware` регистрируйте **до** роутов: gin выполняет обработчики в порядке регистрации, и зарегистрированный раньше завершает запрос первым.

Шаблон роута для спана `sbgin` берёт из `c.FullPath()`. Всё, кроме gin-специфики, — жизненный цикл операции, захват тел, отображение статуса, публикация адреса — живёт в `sbhttp` и переиспользуется, а не копируется. `sbgin` — отдельный Go-модуль `github.com/service-bridge/sdk/go/sbgin` со своим `go.mod`.

## 5. Endpoint, Route и опции

| Опция `sbhttp.New` | По умолчанию | Что делает |
|---|---|---|
| `sbhttp.WithLogger(log)` | `slog.Default()` | Логгер для предупреждения о loopback-адресе и для спана, который не удалось стартовать. |
| `sbhttp.WithTrustTraceHeader()` | выключено | Принимать входящий `X-SB-Trace` как родителя спана. |
| `sbhttp.WithRouteResolver(fn)` | нет | Шаблон роута для роутеров, которые middleware не читает сам. |

```go
func endpoint() sbhttp.Endpoint {
	return sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000}
}
```

| Поле | По умолчанию | Что делает |
|---|---|---|
| `Host` | `127.0.0.1` плюс одноразовое предупреждение в лог | Адрес, по которому до вашего HTTP-сервера дозваниваются. |
| `Port` | обязателен | Порт. Вне `1..65535` — `sbhttp.ErrPort`. |

Хост задаётся явно или это loopback. Автоопределение из окружения в контейнере почти всегда даёт неверный адрес, а тихая догадка хуже явного требования. Порт обязателен потому, что сервер, привязанный к `:0`, на момент объявления его ещё не знает.

`Route` — это `{Method, Pattern}`. Шаблон хранится ровно в том виде, в каком его держит фреймворк: его отрисовывает консоль, и нормализация здесь привела бы к тому, что объявленное имя перестало бы совпадать с тем, что фреймворк сообщает во время запроса.

Шаблон, зарегистрированный без метода (`mux.HandleFunc("/health", …)` матчится на любой глагол), получает метод `*`. Цена честности: карточка в Service Map у такого роута будет, а сшитой статистики по нему — нет. Выдумывать за вас `GET` хуже.

## 6. Что видно в Service Map

Каждый роут объявляется входящим методом типа HTTP с именем `"<МЕТОД> <шаблон>"` — без схем и без хеша контракта: рантайм не проксирует HTTP и декларацию со схемой ответа отвергает.

Публиковать можно и до `Start`, и после:

- **До `Start`** адрес и роуты оседают в наборе объявлений и уезжают в первой регистрации.
- **После `Start`** интеграция переоткрывает поток реестра, чтобы роуты доехали сейчас, а не при следующем переподключении.

Повторный вызов идемпотентен: уже объявленный роут второй раз не объявляется — два ряда с одним именем в одном кадре откатили бы регистрацию целиком.

Порядок роутов нормализуется сортировкой. `chi.Walk` обходит map методов и выдаёт их в произвольном порядке; без сортировки кадр регистрации отличался бы от старта к старту.

### Исходящая зависимость на чужой HTTP

Если ваш сервис ходит в HTTP другого сервиса, объявите это — тогда ребро появится на карте:

```go
func declareHTTPDep(c *sb.Client) error {
	return c.Service("orders-svc", sb.ServiceDeps{
		HTTP: []string{"POST /orders"},
	})
}
```

Проксирования это не включает — сам HTTP-запрос вы по-прежнему делаете своим клиентом.

## 7. Трассировка и захват тел

**Спан именуется шаблоном роута, а не путём**: `http.handle:<МЕТОД>/<шаблон>`, например `http.handle:GET//orders/{id}`. Путь сделал бы каждый id пользователя отдельным субъектом. Запрос, не совпавший ни с одним роутом, получает шаблон `*` (`sbhttp.UnmatchedRoute`). Спан несёт `meta {method, route, status}`.

`Middleware` находит шаблон **до** обработчика, в таком порядке:

1. функция из `sbhttp.WithRouteResolver`, если задана;
2. обёрнутый `*sbhttp.Mux` или `*http.ServeMux`;
3. контекст роутинга chi;
4. `r.Pattern`, когда middleware обёрнут вокруг одного роута внутри `ServeMux`.

Для роутера, который middleware прочитать не может, передайте резолвер:

```go
func newIntegration(c *sb.Client, router interface{ Match(*http.Request) string }) (*sbhttp.Integration, error) {
	return sbhttp.New(c, sbhttp.WithRouteResolver(func(r *http.Request) string {
		return router.Match(r) // шаблон с ведущим слешем или "" — ничего не совпало
	}))
}
```

`sbhttp.RouteOf(r)` читает шаблон, который `net/http` положил в `r.Pattern`, без метода; `(*sbhttp.Mux).Route(r)` возвращает шаблон, которым mux обслужил бы запрос, или `""`.

**Возвращённый запрос обязан заменить исходный.** `Middleware` делает это за вас. Если пишете свой адаптер через `Begin`, помните: без подмены `*http.Request` вызовы RPC и публикации событий изнутри обработчика не найдут родителя и начнут собственный корневой трейс — один запрос распадётся на два дерева.

Бизнес-ключ операции берётся из заголовка `Idempotency-Key`; если его нет — `"<МЕТОД> <шаблон>"`, без query-строки.

**Входящий `X-SB-Trace` по умолчанию игнорируется**: на публичной границе клиент не должен вклеивать свои запросы в чужие трейсы. За шлюзом, который сам ставит заголовок, включите приём:

```go
func trustedIntegration(c *sb.Client) (*sbhttp.Integration, error) {
	return sbhttp.New(c, sbhttp.WithTrustTraceHeader())
}
```

Битый заголовок даёт новый корневой трейс, а не ошибку.

Как исход запроса отображается в статус операции:

| Что произошло | Статус |
|---|---|
| Паника обработчика | `ERROR`, «handler panic». Паника летит дальше — `http.ErrAbortHandler` работает как обычно. |
| Соединение захвачено обработчиком (вебсокет) | `SUCCESS` |
| Клиент отвалился до возврата из обработчика | `TIMEOUT`, «client abort» |
| Код ответа ≥ 400 | `ERROR`, «HTTP `<код>`» |
| Иначе | `SUCCESS` |

**Режим захвата тел присылает рантайм**; по умолчанию у рантайма режим `errors` — тела сохраняются для упавших запросов. Пока захват выключен, обёртка ответа ничего не буферизует — запрос за телеметрию не платит. Когда включён, берётся не больше лимита байт на каждое направление (по умолчанию 65536), остальное течёт напрямую, поэтому большая загрузка в памяти не оседает.

Тело запроса SDK заранее не читает: захватывается только то, что прочитал сам обработчик, до лимита. Ошибки чтения и `Close` остаются операциями исходного тела.

**SDK тела не маскирует.** Они захватываются как есть, маскирует их рантайм при приёме. Защиты периметра (фильтра сканеров, лимита запросов) в интеграции тоже нет.

Спан, который не удалось стартовать, запроса не стоит: сбой логируется, запрос обслуживается без телеметрии.

Обёртка ответа отражает набор интерфейсов исходного writer'а — `http.Flusher`, `http.Hijacker`, `io.ReaderFrom`, — а не выдумывает свой. Обёртка, потерявшая `Flusher`, сломала бы SSE; приписавшая себе `Hijacker` — уронила бы апгрейд до вебсокета.

## 8. Другой фреймворк

Если фреймворк не `net/http`-совместимый, адаптер строится поверх двух точек `sbhttp`:

- `integration.Begin(r, route)` → `(*http.Request, *sbhttp.Operation, error)` — стартует операцию; `route` — шаблон роута, который совпал (`""` записывается как `*`). Возвращённый запрос надо положить туда, где фреймворк держит свой.
- `op.Finish(sbhttp.Outcome{...})` — закрывает операцию. Повторный вызов ничего не делает.
- `op.Capturing()` и `op.PayloadLimit()` — спрашивать **до** того, как трогать тело.
- `op.CaptureResponse(body)` — записать тело ответа.
- `integration.Logger()` — логгер интеграции; через него адаптер сообщает о спане, который не удалось стартовать.

Список роутов передаётся напрямую:

```go
func publishManual(i *sbhttp.Integration) error {
	routes := []sbhttp.Route{
		{Method: "POST", Pattern: "/orders"},
		{Method: "GET", Pattern: "/orders/{id}"},
	}
	return i.Publish(routes, sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000})
}
```

Вся поверхность `Operation` типизирована стандартной библиотекой, поэтому адаптер может жить в отдельном модуле — ровно так устроен `sbgin`.

## 9. Ошибки

Оборачиваются как `fmt.Errorf("sbhttp: <действие>: %w", …)` и проверяются через `errors.Is`:

| Сентинел | Причина |
|---|---|
| `sbhttp.ErrNoRuntime` | В `sbhttp.New` не передан клиент или у него нет рекордера и объявлений. |
| `sbhttp.ErrNoRouter` | В `PublishMux` / `PublishChi` / `sbgin.Publish` пришёл `nil`. |
| `sbhttp.ErrPort` | Порт вне `1..65535`. |

Подробнее — в [`sbhttp/README.md`](../sbhttp/README.md) и [`sbgin/README.md`](../sbgin/README.md).

---

Дальше: [Тестирование](./testing.md) · [Operations](./operations.md)
