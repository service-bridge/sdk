package sbhttp

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"sync"
)

// Middleware wraps a handler in one HTTP.HANDLE span. Its signature is the
// net/http middleware shape, which chi takes as is:
//
//	mux.Handle("/", integ.Middleware(routes))
//	router.Use(integ.Middleware)
//
// A span that cannot be started never costs the request: the failure is
// logged and the handler runs untraced.
func (i *Integration) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, op, err := i.Begin(r)
		if err != nil {
			i.log.Error("sbhttp: http span not started", "error", err, "method", r.Method, "path", r.URL.Path)
			next.ServeHTTP(w, r)
			return
		}

		rec := newResponseRecorder(w, op.Capturing(), op.PayloadLimit())
		defer func() {
			p := recover()
			status, hijacked, body := rec.snapshot()
			op.CaptureResponse(body)
			op.Finish(Outcome{
				StatusCode: status,
				// The server cancels a request context when the outermost
				// ServeHTTP returns; this runs inside it, so a cancellation
				// here means the client really went away.
				Aborted:  req.Context().Err() != nil,
				Hijacked: hijacked,
				Panicked: p != nil,
			})
			if p != nil {
				panic(p)
			}
		}()

		next.ServeHTTP(wrapResponseWriter(w, rec), req)
	})
}

// responseRecorder observes the status code and, while capture is on, the
// first limit bytes of the response body.
//
// It is guarded by a mutex because the span is closed from the middleware
// goroutine while a handler may still be writing from one of its own.
type responseRecorder struct {
	w       http.ResponseWriter
	capture bool
	limit   int

	mu          sync.Mutex
	status      int
	wroteHeader bool
	hijacked    bool
	body        []byte
}

func newResponseRecorder(w http.ResponseWriter, capture bool, limit int) *responseRecorder {
	return &responseRecorder{w: w, capture: capture, limit: limit, status: http.StatusOK}
}

func (r *responseRecorder) Header() http.Header { return r.w.Header() }

func (r *responseRecorder) WriteHeader(code int) {
	r.mu.Lock()
	if !r.wroteHeader {
		r.wroteHeader = true
		r.status = code
	}
	r.mu.Unlock()
	r.w.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.markWritten()
	r.record(b)
	return r.w.Write(b)
}

// Unwrap lets http.ResponseController reach the real writer for the
// capabilities this wrapper does not re-expose (deadlines, EnableFullDuplex).
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.w }

func (r *responseRecorder) markWritten() {
	r.mu.Lock()
	r.wroteHeader = true
	r.mu.Unlock()
}

func (r *responseRecorder) record(b []byte) {
	if !r.capture || len(b) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	free := r.limit - len(r.body)
	if free <= 0 {
		return
	}
	if len(b) > free {
		b = b[:free]
	}
	r.body = append(r.body, b...)
}

// snapshot reports what the response ended up being.
func (r *responseRecorder) snapshot() (status int, hijacked bool, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status, r.hijacked, r.body
}

func (r *responseRecorder) flush() {
	r.markWritten()
	r.w.(http.Flusher).Flush()
}

func (r *responseRecorder) hijack() (net.Conn, *bufio.ReadWriter, error) {
	r.mu.Lock()
	r.hijacked = true
	r.mu.Unlock()
	return r.w.(http.Hijacker).Hijack()
}

// readFrom keeps sendfile alive: with capture off the bytes go straight to the
// underlying ReaderFrom. With capture on they have to pass through Write, which
// is what records them.
func (r *responseRecorder) readFrom(src io.Reader) (int64, error) {
	r.markWritten()
	if r.capture {
		return io.Copy(writeOnly{r}, src)
	}
	return r.w.(io.ReaderFrom).ReadFrom(src)
}

// writeOnly hides every method but Write, so io.Copy cannot rediscover
// readFrom and recurse into it.
type writeOnly struct{ r *responseRecorder }

func (w writeOnly) Write(b []byte) (int, error) { return w.r.Write(b) }

// The wrappers below re-expose the optional interfaces of the original writer,
// one type per combination. A single type implementing all three would claim
// capabilities the original does not have — streaming handlers, websocket
// upgrades and sendfile all decide by type assertion, and a wrapper that lies
// in either direction breaks them.

type rrF struct{ *responseRecorder }

func (r rrF) Flush() { r.flush() }

type rrH struct{ *responseRecorder }

func (r rrH) Hijack() (net.Conn, *bufio.ReadWriter, error) { return r.hijack() }

type rrR struct{ *responseRecorder }

func (r rrR) ReadFrom(src io.Reader) (int64, error) { return r.readFrom(src) }

type rrFH struct{ *responseRecorder }

func (r rrFH) Flush()                                       { r.flush() }
func (r rrFH) Hijack() (net.Conn, *bufio.ReadWriter, error) { return r.hijack() }

type rrFR struct{ *responseRecorder }

func (r rrFR) Flush()                                { r.flush() }
func (r rrFR) ReadFrom(src io.Reader) (int64, error) { return r.readFrom(src) }

type rrHR struct{ *responseRecorder }

func (r rrHR) Hijack() (net.Conn, *bufio.ReadWriter, error) { return r.hijack() }
func (r rrHR) ReadFrom(src io.Reader) (int64, error)        { return r.readFrom(src) }

type rrFHR struct{ *responseRecorder }

func (r rrFHR) Flush()                                       { r.flush() }
func (r rrFHR) Hijack() (net.Conn, *bufio.ReadWriter, error) { return r.hijack() }
func (r rrFHR) ReadFrom(src io.Reader) (int64, error)        { return r.readFrom(src) }

func wrapResponseWriter(w http.ResponseWriter, rec *responseRecorder) http.ResponseWriter {
	_, flusher := w.(http.Flusher)
	_, hijacker := w.(http.Hijacker)
	_, readerFrom := w.(io.ReaderFrom)
	switch {
	case flusher && hijacker && readerFrom:
		return rrFHR{rec}
	case flusher && hijacker:
		return rrFH{rec}
	case flusher && readerFrom:
		return rrFR{rec}
	case hijacker && readerFrom:
		return rrHR{rec}
	case flusher:
		return rrF{rec}
	case hijacker:
		return rrH{rec}
	case readerFrom:
		return rrR{rec}
	default:
		return rec
	}
}
