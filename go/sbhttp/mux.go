package sbhttp

import (
	"net/http"
	"strings"
	"sync"
)

// anyMethod names a pattern registered without a method: net/http matches it
// for every verb, so no single method describes it.
const anyMethod = "*"

// Route is one declared endpoint of the application's HTTP server. Patterns
// are kept exactly as the framework holds them — the console renders them, and
// normalising here would make the declared name stop matching what the
// framework reports at request time.
type Route struct {
	Method  string
	Pattern string
}

// Mux is a thin http.ServeMux that remembers the patterns it was given.
// net/http has no way to enumerate a mux, so a wrapper at registration time is
// the only place the route list exists.
type Mux struct {
	mux *http.ServeMux

	mu     sync.Mutex
	routes []Route
}

// NewMux builds a mux over a fresh http.ServeMux.
func NewMux() *Mux { return &Mux{mux: http.NewServeMux()} }

// Handle registers a handler and records the pattern.
func (m *Mux) Handle(pattern string, handler http.Handler) {
	m.remember(pattern)
	m.mux.Handle(pattern, handler)
}

// HandleFunc registers a handler function and records the pattern.
func (m *Mux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	m.remember(pattern)
	m.mux.HandleFunc(pattern, handler)
}

// ServeHTTP routes through the underlying http.ServeMux.
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) { m.mux.ServeHTTP(w, r) }

// Routes returns the recorded routes in registration order.
func (m *Mux) Routes() []Route {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Route, len(m.routes))
	copy(out, m.routes)
	return out
}

func (m *Mux) remember(pattern string) {
	method, rest := splitPattern(pattern)
	m.mu.Lock()
	m.routes = append(m.routes, Route{Method: method, Pattern: rest})
	m.mu.Unlock()
}

// splitPattern reads a Go 1.22 pattern, "[METHOD ][HOST]/[PATH]", the way
// net/http does: everything before the first space or tab is the method. The
// host, when present, stays in the pattern because that is what the route is.
func splitPattern(pattern string) (method, rest string) {
	if i := strings.IndexAny(pattern, " \t"); i >= 0 {
		return pattern[:i], strings.TrimLeft(pattern[i+1:], " \t")
	}
	return anyMethod, pattern
}
