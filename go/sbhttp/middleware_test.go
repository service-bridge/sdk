package sbhttp_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"github.com/service-bridge/sdk/go/sbhttp"
)

func serve(integ *sbhttp.Integration, r *http.Request, handler http.HandlerFunc) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	integ.Middleware(handler).ServeHTTP(w, r)
	return w
}

func ok(http.ResponseWriter, *http.Request) {}

func TestRequestEmitsExactlyOneHandleOperation(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	serve(integ, httptest.NewRequest(http.MethodGet, "/users/42", nil), ok)

	start, end := startEnd(t, rt)
	if start.GetChannel() != pb.Channel_HTTP {
		t.Errorf("channel: got %v, want HTTP", start.GetChannel())
	}
	if start.GetKind() != uint32(telemetry.OpKindHTTPHandle) {
		t.Errorf("kind: got %d, want %d (handle)", start.GetKind(), telemetry.OpKindHTTPHandle)
	}
	if start.GetStatus() != pb.Status_PENDING {
		t.Errorf("START status: got %v, want PENDING", start.GetStatus())
	}
	// The runtime turns the first slash back into a space to match the declared
	// route name, so the path keeps its own leading slash.
	if want := "http.handle:GET//users/42"; start.GetSubject() != want {
		t.Errorf("subject: got %q, want %q", start.GetSubject(), want)
	}
	if end.GetStatus() != pb.Status_SUCCESS {
		t.Errorf("END status: got %v, want SUCCESS", end.GetStatus())
	}
}

func TestStatusCodeMapsOntoOperationStatus(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		status  pb.Status
		message string
	}{
		{"no write is 200", ok, pb.Status_SUCCESS, ""},
		{"below 400 succeeds", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }, pb.Status_SUCCESS, ""},
		{"redirect succeeds", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusFound) }, pb.Status_SUCCESS, ""},
		{"400 fails", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) }, pb.Status_ERROR, "HTTP 400"},
		{"500 fails", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }, pb.Status_ERROR, "HTTP 500"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			integ, rt := newIntegration(t, telemetry.ModeNone)
			serve(integ, httptest.NewRequest(http.MethodGet, "/x", nil), tc.handler)
			_, end := startEnd(t, rt)
			if end.GetStatus() != tc.status {
				t.Errorf("status: got %v, want %v", end.GetStatus(), tc.status)
			}
			if end.GetStatusMessage() != tc.message {
				t.Errorf("status message: got %q, want %q", end.GetStatusMessage(), tc.message)
			}
		})
	}
}

func TestClientAbortIsTimeout(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	entered := make(chan struct{})
	finished := make(chan struct{})
	traced := integ.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		traced.ServeHTTP(w, r)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/slow", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	go func() {
		<-entered
		cancel()
	}()
	if _, err := srv.Client().Do(req); err == nil { //nolint:bodyclose // the request never completes
		t.Fatal("cancelled request must fail on the client")
	}
	<-finished

	_, end := startEnd(t, rt)
	if end.GetStatus() != pb.Status_TIMEOUT {
		t.Errorf("status: got %v, want TIMEOUT", end.GetStatus())
	}
	if end.GetStatusMessage() != "client abort" {
		t.Errorf("status message: got %q, want %q", end.GetStatusMessage(), "client abort")
	}
}

func TestBusinessKeyPrefersIdempotencyHeader(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	r := httptest.NewRequest(http.MethodPost, "/orders", nil)
	r.Header.Set("Idempotency-Key", "order-7")
	serve(integ, r, ok)

	start, _ := startEnd(t, rt)
	if start.GetBusinessKey() != "order-7" {
		t.Errorf("business key: got %q, want %q", start.GetBusinessKey(), "order-7")
	}
}

func TestBusinessKeyFallsBackToMethodAndPath(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	serve(integ, httptest.NewRequest(http.MethodPost, "/orders", nil), ok)

	start, _ := startEnd(t, rt)
	if want := "POST /orders"; start.GetBusinessKey() != want {
		t.Errorf("business key: got %q, want %q", start.GetBusinessKey(), want)
	}
}

func TestIncomingTraceHeaderIsAdopted(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	incoming := telemetry.TraceContext{TraceID: uuid.New(), ParentOpID: uuid.New()}
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set(telemetry.HeaderName, telemetry.FormatHeader(incoming))
	serve(integ, r, ok)

	start, _ := startEnd(t, rt)
	if start.GetTraceId() != incoming.TraceID.String() {
		t.Errorf("trace id: got %q, want %q", start.GetTraceId(), incoming.TraceID)
	}
	if start.GetParentOpId() != incoming.ParentOpID.String() {
		t.Errorf("parent op id: got %q, want %q", start.GetParentOpId(), incoming.ParentOpID)
	}
}

func TestMalformedTraceHeaderStartsNewRoot(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set(telemetry.HeaderName, "not-a-trace")
	w := serve(integ, r, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	if w.Code != http.StatusOK {
		t.Errorf("a broken trace header must not fail the request: got %d", w.Code)
	}
	start, _ := startEnd(t, rt)
	if start.GetParentOpId() != "" {
		t.Errorf("parent op id: got %q, want empty (root)", start.GetParentOpId())
	}
	if _, err := uuid.Parse(start.GetTraceId()); err != nil {
		t.Errorf("trace id %q is not a fresh uuid: %v", start.GetTraceId(), err)
	}
}

func TestHandlerRunsUnderTheOperationContext(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	serve(integ, httptest.NewRequest(http.MethodGet, "/x", nil), func(_ http.ResponseWriter, r *http.Request) {
		_, nested, err := rt.rec.Start(r.Context(), telemetry.OpSpec{
			Channel: pb.Channel_USER,
			Kind:    telemetry.OpKindUserSubOp,
			Subject: "user.subop:nested",
		})
		if err != nil {
			t.Errorf("start nested op: %v", err)
			return
		}
		nested.End(pb.Status_SUCCESS, "")
	})

	frames := ops(rt)
	if len(frames) != 4 {
		t.Fatalf("op frames: got %d, want 4 (http start, nested start, nested end, http end)", len(frames))
	}
	httpStart, nestedStart := frames[0], frames[1]
	if nestedStart.GetParentOpId() != httpStart.GetOpId() {
		t.Errorf("nested parent: got %q, want the http op %q — the handler did not see the operation context",
			nestedStart.GetParentOpId(), httpStart.GetOpId())
	}
	if nestedStart.GetTraceId() != httpStart.GetTraceId() {
		t.Errorf("nested trace: got %q, want %q — the request fell apart into two trees",
			nestedStart.GetTraceId(), httpStart.GetTraceId())
	}
}

// countingBody reports how many times the request body was read.
type countingBody struct {
	reader *strings.Reader
	mu     sync.Mutex
	reads  int
}

func newCountingBody(payload string) *countingBody {
	return &countingBody{reader: strings.NewReader(payload)}
}

func (b *countingBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	b.reads++
	b.mu.Unlock()
	return b.reader.Read(p)
}

func (b *countingBody) Close() error { return nil }

func (b *countingBody) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reads
}

func TestCaptureOffNeverTouchesTheRequestBody(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	body := newCountingBody(`{"amount":10}`)
	r := httptest.NewRequest(http.MethodPost, "/pay", body)
	serve(integ, r, ok)

	if body.count() != 0 {
		t.Errorf("request body was read %d times with capture off — the bytes would be thrown away", body.count())
	}
	if got := len(payloads(rt)); got != 0 {
		t.Errorf("payloads: got %d, want 0", got)
	}
}

func TestCaptureOnRecordsBodiesAndStillDeliversThem(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeAll)

	const request = `{"amount":10}`
	body := newCountingBody(request)
	r := httptest.NewRequest(http.MethodPost, "/pay", body)

	var seen string
	w := serve(integ, r, func(w http.ResponseWriter, r *http.Request) {
		read, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("handler read body: %v", err)
		}
		seen = string(read)
		if _, err := w.Write([]byte(`{"ok":true}`)); err != nil {
			t.Errorf("handler write: %v", err)
		}
	})

	if seen != request {
		t.Errorf("handler body: got %q, want %q", seen, request)
	}
	if w.Body.String() != `{"ok":true}` {
		t.Errorf("response body: got %q", w.Body.String())
	}

	byDirection := map[uint32]string{}
	for _, att := range payloads(rt) {
		byDirection[att.GetDirection()] = string(att.GetBytes())
	}
	if byDirection[telemetry.DirectionIn] != request {
		t.Errorf("captured request: got %q, want %q", byDirection[telemetry.DirectionIn], request)
	}
	if byDirection[telemetry.DirectionOut] != `{"ok":true}` {
		t.Errorf("captured response: got %q, want %q", byDirection[telemetry.DirectionOut], `{"ok":true}`)
	}
}

func TestPanickingHandlerFailsTheOperationAndKeepsPanicking(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic must reach the caller")
			}
		}()
		serve(integ, httptest.NewRequest(http.MethodGet, "/boom", nil), func(http.ResponseWriter, *http.Request) {
			panic("boom")
		})
	}()

	_, end := startEnd(t, rt)
	if end.GetStatus() != pb.Status_ERROR {
		t.Errorf("status: got %v, want ERROR", end.GetStatus())
	}
}

// spy is a plain http.ResponseWriter — no optional interfaces of its own — that
// counts the calls the wrappers below delegate to it.
type spy struct {
	rec       *httptest.ResponseRecorder
	mu        sync.Mutex
	flushes   int
	hijacks   int
	readFroms int
}

func newSpy() *spy { return &spy{rec: httptest.NewRecorder()} }

func (s *spy) Header() http.Header         { return s.rec.Header() }
func (s *spy) Write(b []byte) (int, error) { return s.rec.Write(b) }
func (s *spy) WriteHeader(code int)        { s.rec.WriteHeader(code) }

func (s *spy) counts() (flushes, hijacks, readFroms int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushes, s.hijacks, s.readFroms
}

func (s *spy) flush() {
	s.mu.Lock()
	s.flushes++
	s.mu.Unlock()
}

func (s *spy) hijack() (net.Conn, *bufio.ReadWriter, error) {
	s.mu.Lock()
	s.hijacks++
	s.mu.Unlock()
	return nil, nil, nil
}

func (s *spy) readFrom(src io.Reader) (int64, error) {
	s.mu.Lock()
	s.readFroms++
	s.mu.Unlock()
	return io.Copy(s.rec, src)
}

// Writers with every combination of the optional interfaces a wrapper must
// neither swallow nor invent.
type flushWriter struct{ *spy }

func (w flushWriter) Flush() { w.flush() }

type hijackWriter struct{ *spy }

func (w hijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }

type readFromWriter struct{ *spy }

func (w readFromWriter) ReadFrom(src io.Reader) (int64, error) { return w.readFrom(src) }

type flushHijackWriter struct{ *spy }

func (w flushHijackWriter) Flush()                                       { w.flush() }
func (w flushHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }

type flushReadFromWriter struct{ *spy }

func (w flushReadFromWriter) Flush()                                { w.flush() }
func (w flushReadFromWriter) ReadFrom(src io.Reader) (int64, error) { return w.readFrom(src) }

type hijackReadFromWriter struct{ *spy }

func (w hijackReadFromWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }
func (w hijackReadFromWriter) ReadFrom(src io.Reader) (int64, error)        { return w.readFrom(src) }

type fullWriter struct{ *spy }

func (w fullWriter) Flush()                                       { w.flush() }
func (w fullWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }
func (w fullWriter) ReadFrom(src io.Reader) (int64, error)        { return w.readFrom(src) }

func TestResponseWrapperMirrorsTheOptionalInterfaces(t *testing.T) {
	cases := []struct {
		name                    string
		build                   func(*spy) http.ResponseWriter
		flush, hijack, readFrom bool
	}{
		{name: "plain", build: func(s *spy) http.ResponseWriter { return s }},
		{name: "flusher", build: func(s *spy) http.ResponseWriter { return flushWriter{s} }, flush: true},
		{name: "hijacker", build: func(s *spy) http.ResponseWriter { return hijackWriter{s} }, hijack: true},
		{name: "readerfrom", build: func(s *spy) http.ResponseWriter { return readFromWriter{s} }, readFrom: true},
		{name: "flusher+hijacker", build: func(s *spy) http.ResponseWriter { return flushHijackWriter{s} }, flush: true, hijack: true},
		{name: "flusher+readerfrom", build: func(s *spy) http.ResponseWriter { return flushReadFromWriter{s} }, flush: true, readFrom: true},
		{name: "hijacker+readerfrom", build: func(s *spy) http.ResponseWriter { return hijackReadFromWriter{s} }, hijack: true, readFrom: true},
		{name: "all three", build: func(s *spy) http.ResponseWriter { return fullWriter{s} }, flush: true, hijack: true, readFrom: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			integ, _ := newIntegration(t, telemetry.ModeNone)
			original := newSpy()
			underlying := tc.build(original)

			integ.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				flusher, isFlusher := w.(http.Flusher)
				if isFlusher != tc.flush {
					t.Errorf("http.Flusher: got %v, want %v", isFlusher, tc.flush)
				}
				hijacker, isHijacker := w.(http.Hijacker)
				if isHijacker != tc.hijack {
					t.Errorf("http.Hijacker: got %v, want %v", isHijacker, tc.hijack)
				}
				readerFrom, isReaderFrom := w.(io.ReaderFrom)
				if isReaderFrom != tc.readFrom {
					t.Errorf("io.ReaderFrom: got %v, want %v", isReaderFrom, tc.readFrom)
				}

				w.Header().Set("X-Test", "1")
				if unwrapped := w.(interface{ Unwrap() http.ResponseWriter }).Unwrap(); unwrapped != underlying {
					t.Error("Unwrap does not return the original writer — http.ResponseController cannot reach it")
				}
				if isFlusher {
					flusher.Flush()
				}
				if isHijacker {
					if _, _, err := hijacker.Hijack(); err != nil {
						t.Errorf("hijack: %v", err)
					}
				}
				if isReaderFrom {
					if _, err := readerFrom.ReadFrom(strings.NewReader("body")); err != nil {
						t.Errorf("readfrom: %v", err)
					}
				}
			})).ServeHTTP(underlying, httptest.NewRequest(http.MethodGet, "/x", nil))

			flushes, hijacks, readFroms := original.counts()
			if want := boolToInt(tc.flush); flushes != want {
				t.Errorf("flushes reaching the original writer: got %d, want %d", flushes, want)
			}
			if want := boolToInt(tc.hijack); hijacks != want {
				t.Errorf("hijacks reaching the original writer: got %d, want %d", hijacks, want)
			}
			// With capture off ReadFrom must stay on the original writer's fast
			// path: rerouting it through Write would kill sendfile.
			if want := boolToInt(tc.readFrom); readFroms != want {
				t.Errorf("ReadFrom reaching the original writer: got %d, want %d", readFroms, want)
			}
			if original.rec.Header().Get("X-Test") != "1" {
				t.Error("Header() did not reach the original writer")
			}
		})
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestReadFromIsRecordedWhileCaptureIsOn(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeAll)

	original := newSpy()
	integ.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.(io.ReaderFrom).ReadFrom(strings.NewReader("streamed")); err != nil {
			t.Errorf("readfrom: %v", err)
		}
	})).ServeHTTP(fullWriter{original}, httptest.NewRequest(http.MethodGet, "/stream", nil))

	if original.rec.Body.String() != "streamed" {
		t.Errorf("body: got %q, want %q", original.rec.Body.String(), "streamed")
	}
	if _, _, readFroms := original.counts(); readFroms != 0 {
		t.Error("ReadFrom skipped the recorder while capture was on")
	}
	var out string
	for _, att := range payloads(rt) {
		if att.GetDirection() == telemetry.DirectionOut {
			out = string(att.GetBytes())
		}
	}
	if out != "streamed" {
		t.Errorf("captured response: got %q, want %q", out, "streamed")
	}
}

func TestHijackedConnectionSucceeds(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	integ.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, _, err := w.(http.Hijacker).Hijack(); err != nil {
			t.Errorf("hijack: %v", err)
		}
	})).ServeHTTP(hijackWriter{newSpy()}, httptest.NewRequest(http.MethodGet, "/ws", nil))

	_, end := startEnd(t, rt)
	if end.GetStatus() != pb.Status_SUCCESS {
		t.Errorf("status: got %v, want SUCCESS — a hijacked connection is not a timeout", end.GetStatus())
	}
}

func TestResponseCaptureStopsAtThePayloadLimit(t *testing.T) {
	integ, rt := newIntegrationWithLimit(t, telemetry.ModeAll, 8)

	serve(integ, httptest.NewRequest(http.MethodGet, "/big", nil), func(w http.ResponseWriter, _ *http.Request) {
		for range 4 {
			if _, err := w.Write([]byte("0123456789")); err != nil {
				t.Errorf("write: %v", err)
			}
		}
	})

	for _, att := range payloads(rt) {
		if att.GetDirection() != telemetry.DirectionOut {
			continue
		}
		if got := string(att.GetBytes()); got != "01234567" {
			t.Errorf("captured response: got %q, want the first 8 bytes", got)
		}
	}
}

func TestRequestWithoutBodyIsCapturedAsNothing(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeAll)

	serve(integ, httptest.NewRequest(http.MethodGet, "/x", nil), ok)

	for _, att := range payloads(rt) {
		if att.GetDirection() == telemetry.DirectionIn {
			t.Errorf("a bodyless request produced an inbound payload: %q", att.GetBytes())
		}
	}
}

func TestLoggerIsReachableByAdapters(t *testing.T) {
	integ, _ := newIntegration(t, telemetry.ModeNone)
	if integ.Logger() == nil {
		t.Error("adapters in their own module report a failed span through this logger")
	}
}
