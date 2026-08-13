package sbhttp_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"github.com/service-bridge/sdk/go/sbhttp"
)

// declaredRoutes returns the HTTP route names the registration frame carries.
func declaredRoutes(t *testing.T, rt *testRuntime) []string {
	t.Helper()
	req := rt.decls.BuildRegisterRequest()
	names := make([]string, 0, len(req.GetIncoming()))
	for _, in := range req.GetIncoming() {
		if in.GetType() != pb.MethodType_METHOD_TYPE_HTTP {
			t.Fatalf("route %q declared as %v, want METHOD_TYPE_HTTP", in.GetName(), in.GetType())
		}
		if len(in.GetOutputSchemaJson()) > 0 || in.GetContractHash() != "" {
			t.Fatalf("route %q carries a schema or a contract hash; the runtime rejects that", in.GetName())
		}
		names = append(names, in.GetName())
	}
	return names
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestMuxRoutesAreDeclaredAsMethodAndPattern(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	mux := sbhttp.NewMux()
	mux.HandleFunc("GET /users/{id}", ok)
	mux.HandleFunc("POST /users", ok)
	mux.HandleFunc("DELETE /users", ok)
	mux.Handle("/health", http.HandlerFunc(ok))

	if err := integ.PublishMux(mux, sbhttp.Endpoint{Host: "10.0.0.4", Port: 8080}); err != nil {
		t.Fatalf("publish mux: %v", err)
	}

	want := []string{"* /health", "DELETE /users", "POST /users", "GET /users/{id}"}
	if got := declaredRoutes(t, rt); !equalStrings(got, want) {
		t.Errorf("declared routes: got %v, want %v", got, want)
	}
}

func TestMuxServesThroughTheStandardMultiplexer(t *testing.T) {
	mux := sbhttp.NewMux()
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte("hi")); err != nil {
			t.Errorf("write: %v", err)
		}
	})

	integ, _ := newIntegration(t, telemetry.ModeNone)
	w := serve(integ, httptest.NewRequest(http.MethodGet, "/hello", nil), mux.ServeHTTP)
	if w.Body.String() != "hi" {
		t.Errorf("body: got %q, want %q", w.Body.String(), "hi")
	}
}

func TestChiRoutesAreDeclaredIncludingSubRouters(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	router := chi.NewRouter()
	router.Use(integ.Middleware)
	router.Get("/health", ok)
	router.Route("/api", func(r chi.Router) {
		r.Get("/users/{id}", ok)
		r.Post("/users", ok)
	})

	if err := integ.PublishChi(router, sbhttp.Endpoint{Host: "10.0.0.4", Port: 8080}); err != nil {
		t.Fatalf("publish chi: %v", err)
	}

	want := []string{"POST /api/users", "GET /api/users/{id}", "GET /health"}
	if got := declaredRoutes(t, rt); !equalStrings(got, want) {
		t.Errorf("declared routes: got %v, want %v", got, want)
	}
}

func TestChiRouterRunsTheMiddleware(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	router := chi.NewRouter()
	router.Use(integ.Middleware)
	router.Get("/users/{id}", ok)

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/users/7", nil))

	start, _ := startEnd(t, rt)
	if want := "http.handle:GET//users/7"; start.GetSubject() != want {
		t.Errorf("subject: got %q, want %q", start.GetSubject(), want)
	}
}

func TestPublishTwiceDeclaresEachRouteOnce(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	mux := sbhttp.NewMux()
	mux.HandleFunc("GET /users", ok)

	for range 2 {
		if err := integ.PublishMux(mux, sbhttp.Endpoint{Host: "10.0.0.4", Port: 8080}); err != nil {
			t.Fatalf("publish mux: %v", err)
		}
	}
	if got := declaredRoutes(t, rt); len(got) != 1 {
		t.Errorf("declared routes: got %v, want one row — a duplicate name rolls the registration back", got)
	}
}

func TestPublishWritesEndpointAndRestartsTheRegistry(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	if err := integ.Publish(nil, sbhttp.Endpoint{Host: "10.0.0.4", Port: 8080}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := rt.decls.BuildRegisterRequest().GetHttpEndpoint(); got != "10.0.0.4:8080" {
		t.Errorf("http endpoint: got %q, want %q", got, "10.0.0.4:8080")
	}
	if rt.restarts != 1 {
		t.Errorf("registry restarts: got %d, want 1", rt.restarts)
	}
}

func TestPublishWithoutHostFallsBackToLoopback(t *testing.T) {
	integ, rt := newIntegration(t, telemetry.ModeNone)

	if err := integ.Publish(nil, sbhttp.Endpoint{Port: 3000}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := rt.decls.BuildRegisterRequest().GetHttpEndpoint(); got != "127.0.0.1:3000" {
		t.Errorf("http endpoint: got %q, want %q", got, "127.0.0.1:3000")
	}
}

func TestPublishRejectsBadInput(t *testing.T) {
	integ, _ := newIntegration(t, telemetry.ModeNone)

	if err := integ.Publish(nil, sbhttp.Endpoint{Port: 0}); !errors.Is(err, sbhttp.ErrPort) {
		t.Errorf("port 0: got %v, want ErrPort", err)
	}
	if err := integ.Publish(nil, sbhttp.Endpoint{Port: 70000}); !errors.Is(err, sbhttp.ErrPort) {
		t.Errorf("port 70000: got %v, want ErrPort", err)
	}
	if err := integ.PublishMux(nil, sbhttp.Endpoint{Port: 80}); !errors.Is(err, sbhttp.ErrNoRouter) {
		t.Errorf("nil mux: got %v, want ErrNoRouter", err)
	}
	if err := integ.PublishChi(nil, sbhttp.Endpoint{Port: 80}); !errors.Is(err, sbhttp.ErrNoRouter) {
		t.Errorf("nil chi router: got %v, want ErrNoRouter", err)
	}
	if err := integ.Publish([]sbhttp.Route{{Method: "GET", Pattern: ""}}, sbhttp.Endpoint{Port: 80}); err == nil {
		t.Error("a route without a pattern must be rejected")
	}
}
