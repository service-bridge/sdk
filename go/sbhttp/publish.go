package sbhttp

import (
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// loopbackHost is the address published when none was given.
const loopbackHost = "127.0.0.1"

// Endpoint is where the application's own HTTP server can be reached.
type Endpoint struct {
	// Host is the address peers and the console dial. Empty falls back to
	// loopback with a one-time warning: an address guessed from the process's
	// environment is wrong in a container almost every time, and a silent guess
	// is worse than an explicit requirement.
	Host string
	// Port is the port the server listens on. It is required — a server bound
	// to :0 does not know it at declaration time, so the caller passes it.
	Port int
}

// Publish declares routes and the endpoint of the application's HTTP server.
// It is safe before the client starts: the endpoint settles in the
// declarations and rides the first registration. After start it reopens the
// registry stream, so the runtime sees the address at once.
//
// Repeat calls are idempotent — a route already declared is not declared twice,
// which would put two rows with the same name in one registration frame.
func (i *Integration) Publish(routes []Route, ep Endpoint) error {
	if err := i.declare(routes); err != nil {
		return fmt.Errorf("sbhttp: publish: %w", err)
	}
	if ep.Port <= 0 || ep.Port > 65535 {
		return fmt.Errorf("sbhttp: publish: port %d: %w", ep.Port, ErrPort)
	}
	host := ep.Host
	if host == "" {
		i.hostWarn.Do(func() {
			i.log.Warn("sbhttp: http advertise host not set, publishing loopback; pass Endpoint.Host to be reachable across hosts",
				"host", loopbackHost)
		})
		host = loopbackHost
	}
	i.decls.SetHTTPEndpoint(net.JoinHostPort(host, strconv.Itoa(ep.Port)))
	i.rt.RestartRegistry()
	return nil
}

// PublishMux declares every route registered on m and publishes ep.
func (i *Integration) PublishMux(m *Mux, ep Endpoint) error {
	if m == nil {
		return fmt.Errorf("sbhttp: publish mux: %w", ErrNoRouter)
	}
	return i.Publish(m.Routes(), ep)
}

// PublishChi walks a chi router — including its sub-routers — and publishes ep.
// chi keeps its own route tree, so enumeration goes through chi.Walk rather
// than the net/http path.
func (i *Integration) PublishChi(r chi.Routes, ep Endpoint) error {
	if r == nil {
		return fmt.Errorf("sbhttp: publish chi: %w", ErrNoRouter)
	}
	var routes []Route
	err := chi.Walk(r, func(method, pattern string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		routes = append(routes, Route{Method: method, Pattern: pattern})
		return nil
	})
	if err != nil {
		return fmt.Errorf("sbhttp: publish chi: walk routes: %w", err)
	}
	return i.Publish(routes, ep)
}

// declare adds the routes to the registration frame, skipping the ones already
// there. The order is normalised because chi walks a map of methods and would
// otherwise put a different frame on the wire every start.
func (i *Integration) declare(routes []Route) error {
	ordered := make([]Route, len(routes))
	copy(ordered, routes)
	sort.SliceStable(ordered, func(a, b int) bool {
		if ordered[a].Pattern != ordered[b].Pattern {
			return ordered[a].Pattern < ordered[b].Pattern
		}
		return ordered[a].Method < ordered[b].Method
	})

	for _, route := range ordered {
		key := route.Method + " " + route.Pattern
		i.mu.Lock()
		_, dup := i.seen[key]
		if !dup {
			i.seen[key] = struct{}{}
		}
		i.mu.Unlock()
		if dup {
			continue
		}
		if err := i.decls.AddHTTPRoute(route.Method, route.Pattern); err != nil {
			return fmt.Errorf("declare route %q: %w", key, err)
		}
	}
	return nil
}
