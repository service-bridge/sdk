# HTTP integrations — Go SDK reference

ServiceBridge does **not** proxy business HTTP. You run your own server; the integration publishes its routes to the Service Map and wraps each request in one `HTTP.HANDLE` span so the request and the RPCs and events it triggers land in the same trace.

## Signatures

```go signature
func New(rt Runtime, opts ...Option) (*Integration, error)
func WithLogger(log *slog.Logger) Option

func (i *Integration) Middleware(next http.Handler) http.Handler
func (i *Integration) Begin(r *http.Request) (*http.Request, *Operation, error)
func (i *Integration) Publish(routes []Route, ep Endpoint) error
func (i *Integration) PublishMux(m *Mux, ep Endpoint) error
func (i *Integration) PublishChi(r chi.Routes, ep Endpoint) error
func (i *Integration) Logger() *slog.Logger

func NewMux() *Mux
func (m *Mux) Handle(pattern string, handler http.Handler)
func (m *Mux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request)
func (m *Mux) Routes() []Route
```

Package `sbhttp` covers `net/http` and chi (both use `func(http.Handler) http.Handler`). gin lives in the separate module `sbgin`:

```go signature
func Middleware(integration *sbhttp.Integration) gin.HandlerFunc
func Publish(integration *sbhttp.Integration, engine *gin.Engine, endpoint sbhttp.Endpoint) error
```

```sh
go get github.com/service-bridge/sdk/go/sbgin
```

## Endpoint

```go signature
type Endpoint struct {
	Host string
	Port int
}
```

`Host` empty falls back to `127.0.0.1` with a one-time warning — guessing an address from the environment is wrong in a container almost every time. `Port` is required (a server bound to `:0` does not know it at declaration time); outside `1..65535` → `sbhttp.ErrPort`.

## Complete program — net/http

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

	// sbhttp.NewMux remembers the patterns: net/http cannot enumerate a mux,
	// so registration time is the only place the route list exists.
	mux := sbhttp.NewMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		// r already carries the span context: calls made with r.Context()
		// join the same trace tree.
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"` + r.PathValue("id") + `"}`))
	})

	if err := integration.PublishMux(mux,
		sbhttp.Endpoint{Host: os.Getenv("POD_IP"), Port: 3000}); err != nil {
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

## Complete program — chi

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
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

	r := chi.NewRouter()
	r.Use(integration.Middleware) // chi takes the net/http shape as is
	r.Post("/orders", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	r.Route("/orders/{id}", func(sub chi.Router) {
		sub.Get("/", func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
	})

	// After the routes are registered — PublishChi walks what is there,
	// sub-routers included.
	if err := integration.PublishChi(r, sbhttp.Endpoint{Host: os.Getenv("POD_IP"), Port: 3000}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	log.Fatal(http.ListenAndServe(":3000", r))
}
```

## Complete program — gin

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	sb "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/sbgin"
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

	engine := gin.New()
	// BEFORE the routes: gin runs handlers in registration order.
	engine.Use(sbgin.Middleware(integration))
	engine.POST("/orders", func(ctx *gin.Context) {
		ctx.JSON(http.StatusCreated, gin.H{"ok": true})
	})

	if err := sbgin.Publish(integration, engine,
		sbhttp.Endpoint{Host: os.Getenv("POD_IP"), Port: 3000}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	log.Fatal(engine.Run(":3000"))
}
```

## Any other framework

Build an adapter on two entry points. `Begin` returns a **new** `*http.Request` that must replace the one the handler sees — without it, calls made inside the handler start their own trace root and one request becomes two trees.

```go
package adapter

import (
	"net/http"

	"github.com/service-bridge/sdk/go/sbhttp"
)

func Wrap(i *sbhttp.Integration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, op, err := i.Begin(r)
		if err != nil {
			i.Logger().Error("span not started", "error", err)
			next.ServeHTTP(w, r)
			return
		}
		defer func() {
			p := recover()
			op.Finish(sbhttp.Outcome{
				StatusCode: http.StatusOK,
				Aborted:    req.Context().Err() != nil,
				Panicked:   p != nil,
			})
			if p != nil {
				panic(p)
			}
		}()
		next.ServeHTTP(w, req)
	})
}
```

Ask `op.Capturing()` and `op.PayloadLimit()` **before** touching a body: with capture off the bytes are discarded and the read alone costs more than the span. `op.CaptureResponse(body)` records the response.

Route lists can be handed over directly:

```go
package routes

import "github.com/service-bridge/sdk/go/sbhttp"

func Publish(i *sbhttp.Integration) error {
	return i.Publish([]sbhttp.Route{
		{Method: "POST", Pattern: "/orders"},
		{Method: "GET", Pattern: "/orders/{id}"},
	}, sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000})
}
```

## Outcome mapping

| Condition | Operation status |
|---|---|
| `Panicked` | `ERROR`, "handler panic" (the panic is re-raised) |
| `Hijacked` | `SUCCESS` |
| `Aborted` | `TIMEOUT`, "client abort" |
| `StatusCode >= 400` | `ERROR`, "HTTP `<code>`" |
| otherwise | `SUCCESS` |

`StatusCode` zero counts as 200, which is what `net/http` sends for a handler that wrote nothing.

## Publishing rules

- A route is declared as an HTTP incoming method named `"<METHOD> <pattern>"`, with no schema and no contract hash.
- Publishing before `Start` rides the first registration; after `Start` it reopens the registry stream so the routes arrive now.
- Repeat calls are idempotent — a route already declared is not declared twice (two rows with one name roll the whole registration back).
- A pattern registered without a method gets method `*`. It gets a Service Map card but no stitched statistics.
- The business key comes from the `Idempotency-Key` header, falling back to `"<METHOD> <path>"`.

## Gotchas

- Register the gin middleware **before** the routes; call `PublishChi` / `sbgin.Publish` **after** them.
- Do not drop the request returned by `Begin`.
- `sbhttp.New(nil)` → `sbhttp.ErrNoRuntime`; a nil router → `sbhttp.ErrNoRouter`.
- Body capture is off unless the runtime enables it; a local setting can only narrow it, never widen it.
