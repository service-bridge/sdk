// Package sbgin connects a gin engine to ServiceBridge.
//
// It is a module of its own on purpose. Go has no optional dependency: a gin
// import inside the main SDK module would land in the dependency graph of every
// user, including everyone who never touches gin. Everything that is not
// gin-specific — the operation lifecycle, body capture, status mapping, route
// publication — lives in sbhttp and is used from here, not copied.
package sbgin

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/service-bridge/sdk/go/sbhttp"
)

// Middleware wraps every request in one HTTP.HANDLE span.
//
// Register it before the routes — gin runs middleware in registration order and
// a handler registered earlier finishes the request first.
func Middleware(integration *sbhttp.Integration) gin.HandlerFunc {
	return func(c *gin.Context) {
		req, op, err := integration.Begin(c.Request)
		if err != nil {
			integration.Logger().Error("sbgin: http span not started",
				"error", err, "method", c.Request.Method, "path", c.Request.URL.Path)
			c.Next()
			return
		}

		original := c.Writer
		writer := &responseWriter{ResponseWriter: original, capture: op.Capturing(), limit: op.PayloadLimit()}
		c.Request = req
		c.Writer = writer

		defer func() {
			p := recover()
			status, hijacked, body := writer.snapshot()
			op.CaptureResponse(body)
			op.Finish(sbhttp.Outcome{
				StatusCode: status,
				// gin runs inside the server's ServeHTTP, which cancels the
				// request context only after it returns; a cancellation visible
				// here means the client really went away.
				Aborted:  c.Request.Context().Err() != nil,
				Hijacked: hijacked,
				Panicked: p != nil,
			})
			c.Writer = original
			if p != nil {
				panic(p)
			}
		}()

		c.Next()
	}
}

// Publish declares every route the engine holds and publishes the endpoint of
// the application's own HTTP server. gin keeps its own route table, so
// enumeration goes through engine.Routes().
func Publish(integration *sbhttp.Integration, engine *gin.Engine, endpoint sbhttp.Endpoint) error {
	if integration == nil || engine == nil {
		return fmt.Errorf("sbgin: publish: %w", sbhttp.ErrNoRouter)
	}
	infos := engine.Routes()
	routes := make([]sbhttp.Route, 0, len(infos))
	for _, info := range infos {
		routes = append(routes, sbhttp.Route{Method: info.Method, Pattern: info.Path})
	}
	if err := integration.Publish(routes, endpoint); err != nil {
		return fmt.Errorf("sbgin: publish: %w", err)
	}
	return nil
}

// responseWriter observes the status and, while capture is on, the first limit
// bytes of the body. Embedding gin.ResponseWriter keeps every capability of the
// original — Flush, Hijack, Pusher, CloseNotify — instead of hiding it behind a
// wrapper that implements only what it needs.
type responseWriter struct {
	gin.ResponseWriter

	capture bool
	limit   int

	mu       sync.Mutex
	hijacked bool
	body     []byte
}

func (w *responseWriter) Write(b []byte) (int, error) {
	w.record(b)
	return w.ResponseWriter.Write(b)
}

func (w *responseWriter) WriteString(s string) (int, error) {
	if w.capture {
		w.record([]byte(s))
	}
	return w.ResponseWriter.WriteString(s)
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.mu.Lock()
	w.hijacked = true
	w.mu.Unlock()
	return w.ResponseWriter.Hijack()
}

func (w *responseWriter) record(b []byte) {
	if !w.capture || len(b) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	free := w.limit - len(w.body)
	if free <= 0 {
		return
	}
	if len(b) > free {
		b = b[:free]
	}
	w.body = append(w.body, b...)
}

// snapshot reports how the response ended. gin defaults the status to 200,
// which is what net/http sends for a handler that wrote nothing.
func (w *responseWriter) snapshot() (status int, hijacked bool, body []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Status(), w.hijacked, w.body
}

// compile-time proof that the wrapper is still a full gin.ResponseWriter and an
// http.Hijacker: gin's context assigns it back into c.Writer.
var (
	_ gin.ResponseWriter = (*responseWriter)(nil)
	_ http.Hijacker      = (*responseWriter)(nil)
)
