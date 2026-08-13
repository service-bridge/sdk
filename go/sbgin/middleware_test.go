package sbgin_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"github.com/service-bridge/sdk/go/sbgin"
	"github.com/service-bridge/sdk/go/sbhttp"
)

func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	m.Run()
}

// testRuntime is the client as the integration sees it. It reaches into the
// SDK's internal packages, which is fine for a test in the same repository
// tree; the package's own code depends on sbhttp alone.
type testRuntime struct {
	rec      *telemetry.Recorder
	decls    *registry.Declarations
	restarts int
}

func (r *testRuntime) Recorder() *telemetry.Recorder        { return r.rec }
func (r *testRuntime) Declarations() *registry.Declarations { return r.decls }
func (r *testRuntime) RestartRegistry()                     { r.restarts++ }

func newIntegration(t *testing.T, httpMode telemetry.Mode) (*sbhttp.Integration, *testRuntime) {
	t.Helper()
	return newIntegrationWithLimit(t, httpMode, telemetry.DefaultPayloadMaxBytes)
}

func newIntegrationWithLimit(t *testing.T, httpMode telemetry.Mode, payloadMaxBytes int32) (*sbhttp.Integration, *testRuntime) {
	t.Helper()
	policy := telemetry.NewPolicy()
	modes := telemetry.DefaultModes()
	modes.HTTP = httpMode
	modes.PayloadMaxBytes = payloadMaxBytes
	policy.Set(modes)

	rt := &testRuntime{
		rec:   telemetry.NewRecorder(telemetry.NewRing(telemetry.DefaultBudgets()), policy),
		decls: registry.NewDeclarations(),
	}
	integ, err := sbhttp.New(rt, sbhttp.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatalf("new integration: %v", err)
	}
	return integ, rt
}

func ops(rt *testRuntime) []*pb.OpReport {
	batch := rt.rec.Ring().Peek(1024)
	out := make([]*pb.OpReport, 0, len(batch.Ops))
	for _, item := range batch.Ops {
		out = append(out, item.Msg)
	}
	return out
}

func payloads(rt *testRuntime) []*pb.PayloadAttachment {
	batch := rt.rec.Ring().Peek(1024)
	out := make([]*pb.PayloadAttachment, 0, len(batch.Payloads))
	for _, item := range batch.Payloads {
		out = append(out, item.Msg)
	}
	return out
}

func startEnd(t *testing.T, rt *testRuntime) (start, end *pb.OpReport) {
	t.Helper()
	frames := ops(rt)
	if len(frames) != 2 {
		t.Fatalf("op frames: got %d, want 2 (one START + one END)", len(frames))
	}
	if frames[0].GetOpId() != frames[1].GetOpId() {
		t.Fatalf("END frame closes a different operation")
	}
	return frames[0], frames[1]
}

// engineWith builds an engine whose only route is method+pattern.
func engineWith(integ *sbhttp.Integration, method, pattern string, handler gin.HandlerFunc) *gin.Engine {
	engine := gin.New()
	engine.Use(sbgin.Middleware(integ))
	engine.Handle(method, pattern, handler)
	return engine
}

func noop(*gin.Context) {}

func TestRequestEmitsExactlyOneHandleOperation(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)
	engine := engineWith(integ, http.MethodGet, "/users/:id", noop)

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/users/42", nil))

	start, end := startEnd(t, rt)
	if start.GetChannel() != pb.Channel_HTTP {
		t.Errorf("channel: got %v, want HTTP", start.GetChannel())
	}
	if start.GetKind() != uint32(telemetry.OpKindHTTPHandle) {
		t.Errorf("kind: got %d, want %d (handle)", start.GetKind(), telemetry.OpKindHTTPHandle)
	}
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
		handler gin.HandlerFunc
		status  pb.Status
		message string
	}{
		{"no write is 200", noop, pb.Status_SUCCESS, ""},
		{"below 400 succeeds", func(c *gin.Context) { c.Status(http.StatusNoContent) }, pb.Status_SUCCESS, ""},
		{"404 fails", func(c *gin.Context) { c.Status(http.StatusNotFound) }, pb.Status_ERROR, "HTTP 404"},
		{"500 fails", func(c *gin.Context) { c.String(http.StatusInternalServerError, "nope") }, pb.Status_ERROR, "HTTP 500"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			integ, rt := newIntegration(t, telemetry.ModeNone)
			engine := engineWith(integ, http.MethodGet, "/x", tc.handler)
			engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

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
	engine := engineWith(integ, http.MethodGet, "/slow", func(c *gin.Context) {
		close(entered)
		<-c.Request.Context().Done()
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		engine.ServeHTTP(w, r)
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

func TestBusinessKey(t *testing.T) {
	t.Run("from header", func(t *testing.T) {
		integ, rt := newIntegration(t, telemetry.ModeNone)
		engine := engineWith(integ, http.MethodPost, "/orders", noop)
		r := httptest.NewRequest(http.MethodPost, "/orders", nil)
		r.Header.Set("Idempotency-Key", "order-7")
		engine.ServeHTTP(httptest.NewRecorder(), r)

		start, _ := startEnd(t, rt)
		if start.GetBusinessKey() != "order-7" {
			t.Errorf("business key: got %q, want %q", start.GetBusinessKey(), "order-7")
		}
	})
	t.Run("from method and path", func(t *testing.T) {
		integ, rt := newIntegration(t, telemetry.ModeNone)
		engine := engineWith(integ, http.MethodPost, "/orders", noop)
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/orders", nil))

		start, _ := startEnd(t, rt)
		if want := "POST /orders"; start.GetBusinessKey() != want {
			t.Errorf("business key: got %q, want %q", start.GetBusinessKey(), want)
		}
	})
}

func TestTraceHeader(t *testing.T) {
	t.Run("adopted", func(t *testing.T) {
		integ, rt := newIntegration(t, telemetry.ModeNone)
		engine := engineWith(integ, http.MethodGet, "/x", noop)

		incoming := telemetry.TraceContext{TraceID: uuid.New(), ParentOpID: uuid.New()}
		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		r.Header.Set(telemetry.HeaderName, telemetry.FormatHeader(incoming))
		engine.ServeHTTP(httptest.NewRecorder(), r)

		start, _ := startEnd(t, rt)
		if start.GetTraceId() != incoming.TraceID.String() {
			t.Errorf("trace id: got %q, want %q", start.GetTraceId(), incoming.TraceID)
		}
		if start.GetParentOpId() != incoming.ParentOpID.String() {
			t.Errorf("parent op id: got %q, want %q", start.GetParentOpId(), incoming.ParentOpID)
		}
	})
	t.Run("malformed starts a new root", func(t *testing.T) {
		integ, rt := newIntegration(t, telemetry.ModeNone)
		engine := engineWith(integ, http.MethodGet, "/x", noop)

		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		r.Header.Set(telemetry.HeaderName, "garbage")
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Errorf("a broken trace header must not fail the request: got %d", w.Code)
		}
		start, _ := startEnd(t, rt)
		if start.GetParentOpId() != "" {
			t.Errorf("parent op id: got %q, want empty (root)", start.GetParentOpId())
		}
	})
}

func TestHandlerRunsUnderTheOperationContext(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)
	engine := engineWith(integ, http.MethodGet, "/x", func(c *gin.Context) {
		_, nested, err := rt.rec.Start(c.Request.Context(), telemetry.OpSpec{
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

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	frames := ops(rt)
	if len(frames) != 4 {
		t.Fatalf("op frames: got %d, want 4", len(frames))
	}
	if frames[1].GetParentOpId() != frames[0].GetOpId() {
		t.Errorf("nested parent: got %q, want the http op %q — the handler did not see the operation context",
			frames[1].GetParentOpId(), frames[0].GetOpId())
	}
	if frames[1].GetTraceId() != frames[0].GetTraceId() {
		t.Errorf("nested trace: got %q, want %q", frames[1].GetTraceId(), frames[0].GetTraceId())
	}
}

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
	engine := engineWith(integ, http.MethodPost, "/pay", noop)

	body := newCountingBody(`{"amount":10}`)
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/pay", body))

	if body.count() != 0 {
		t.Errorf("request body was read %d times with capture off", body.count())
	}
	if got := len(payloads(rt)); got != 0 {
		t.Errorf("payloads: got %d, want 0", got)
	}
}

func TestCaptureOnRecordsBodiesAndStillDeliversThem(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeAll)

	const request = `{"amount":10}`
	var seen string
	engine := engineWith(integ, http.MethodPost, "/pay", func(c *gin.Context) {
		read, err := io.ReadAll(c.Request.Body)
		if err != nil {
			t.Errorf("handler read body: %v", err)
		}
		seen = string(read)
		c.String(http.StatusOK, `{"ok":true}`)
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/pay", newCountingBody(request)))

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

func TestWriterKeepsFlusherAndHijacker(t *testing.T) {
	integ, _ := newIntegration(t, telemetry.ModeNone)
	engine := engineWith(integ, http.MethodGet, "/stream", func(c *gin.Context) {
		if _, isFlusher := c.Writer.(http.Flusher); !isFlusher {
			t.Error("wrapped writer lost http.Flusher — streaming responses break")
		}
		if _, isHijacker := c.Writer.(http.Hijacker); !isHijacker {
			t.Error("wrapped writer lost http.Hijacker — websocket upgrades break")
		}
		c.Writer.Flush()
	})

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/stream", nil))
}

func TestPublishDeclaresRoutesAsMethodAndPattern(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	engine := gin.New()
	engine.Use(sbgin.Middleware(integ))
	engine.GET("/users/:id", noop)
	engine.POST("/users", noop)
	api := engine.Group("/api")
	api.GET("/health", noop)

	if err := sbgin.Publish(integ, engine, sbhttp.Endpoint{Host: "10.0.0.4", Port: 8080}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	req := rt.decls.BuildRegisterRequest()
	want := []string{"GET /api/health", "POST /users", "GET /users/:id"}
	if len(req.GetIncoming()) != len(want) {
		t.Fatalf("declared routes: got %d, want %d", len(req.GetIncoming()), len(want))
	}
	for i, in := range req.GetIncoming() {
		if in.GetType() != pb.MethodType_METHOD_TYPE_HTTP {
			t.Errorf("route %q declared as %v, want METHOD_TYPE_HTTP", in.GetName(), in.GetType())
		}
		if len(in.GetOutputSchemaJson()) > 0 || in.GetContractHash() != "" {
			t.Errorf("route %q carries a schema or a contract hash; the runtime rejects that", in.GetName())
		}
		if in.GetName() != want[i] {
			t.Errorf("route %d: got %q, want %q", i, in.GetName(), want[i])
		}
	}
	if got := req.GetHttpEndpoint(); got != "10.0.0.4:8080" {
		t.Errorf("http endpoint: got %q, want %q", got, "10.0.0.4:8080")
	}
	if rt.restarts != 1 {
		t.Errorf("registry restarts: got %d, want 1", rt.restarts)
	}
}

func TestPublishRejectsMissingRouter(t *testing.T) {
	integ, _ := newIntegration(t, telemetry.ModeNone)
	if err := sbgin.Publish(integ, nil, sbhttp.Endpoint{Port: 80}); err == nil {
		t.Error("a nil engine must be rejected")
	}
	if err := sbgin.Publish(nil, gin.New(), sbhttp.Endpoint{Port: 80}); err == nil {
		t.Error("a nil integration must be rejected")
	}
}

func TestWriteStringIsCaptured(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeAll)
	engine := engineWith(integ, http.MethodGet, "/text", func(c *gin.Context) {
		if _, err := c.Writer.WriteString("plain"); err != nil {
			t.Errorf("write string: %v", err)
		}
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/text", nil))

	if w.Body.String() != "plain" {
		t.Errorf("body: got %q, want %q", w.Body.String(), "plain")
	}
	var out string
	for _, att := range payloads(rt) {
		if att.GetDirection() == telemetry.DirectionOut {
			out = string(att.GetBytes())
		}
	}
	if out != "plain" {
		t.Errorf("captured response: got %q, want %q", out, "plain")
	}
}

func TestHijackedConnectionSucceeds(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	finished := make(chan struct{})
	engine := engineWith(integ, http.MethodGet, "/ws", func(c *gin.Context) {
		conn, _, err := c.Writer.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		if err := conn.Close(); err != nil {
			t.Errorf("close hijacked conn: %v", err)
		}
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		engine.ServeHTTP(w, r)
	}))
	defer srv.Close()

	//nolint:bodyclose // the server hijacks and closes the connection
	if _, err := srv.Client().Get(srv.URL + "/ws"); err == nil {
		t.Fatal("a hijacked and closed connection must fail on the client")
	}
	<-finished

	_, end := startEnd(t, rt)
	if end.GetStatus() != pb.Status_SUCCESS {
		t.Errorf("status: got %v, want SUCCESS — a hijacked connection is not a timeout", end.GetStatus())
	}
}

func TestPublishRejectsBadEndpoint(t *testing.T) {
	integ, _ := newIntegration(t, telemetry.ModeNone)
	if err := sbgin.Publish(integ, gin.New(), sbhttp.Endpoint{Port: 0}); err == nil {
		t.Error("port 0 must be rejected")
	}
}

func TestResponseCaptureStopsAtThePayloadLimit(t *testing.T) {
	integ, rt := newIntegrationWithLimit(t, telemetry.ModeAll, 8)
	engine := engineWith(integ, http.MethodGet, "/big", func(c *gin.Context) {
		for range 4 {
			if _, err := c.Writer.Write([]byte("0123456789")); err != nil {
				t.Errorf("write: %v", err)
			}
		}
	})

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/big", nil))

	for _, att := range payloads(rt) {
		if att.GetDirection() != telemetry.DirectionOut {
			continue
		}
		if got := string(att.GetBytes()); got != "01234567" {
			t.Errorf("captured response: got %q, want the first 8 bytes", got)
		}
	}
}
