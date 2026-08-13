package servicebridge

// Option mutates the client configuration at construction time.
type Option func(*config)

type config struct {
	advertiseHost     string
	advertisePort     int
	callerOnly        bool
	dataDir           string
	reconnectAttempts int
}

func defaultConfig() config {
	return config{
		dataDir:           "./.servicebridge",
		reconnectAttempts: 3,
	}
}

// WithAdvertise sets the address other instances dial for direct RPC. Without
// it the inbound Call server stays down and the instance is caller-only.
func WithAdvertise(host string, port int) Option {
	return func(c *config) {
		c.advertiseHost = host
		c.advertisePort = port
	}
}

// WithCallerOnly declares the instance as outbound-only: it registers no
// handlers and never opens an inbound listener.
func WithCallerOnly() Option {
	return func(c *config) { c.callerOnly = true }
}

// WithDataDir sets the directory holding the local outbox database.
func WithDataDir(dir string) Option {
	return func(c *config) { c.dataDir = dir }
}

// WithReconnectAttempts caps consecutive reconnect attempts before the client
// gives up. Zero means unlimited.
func WithReconnectAttempts(n int) Option {
	return func(c *config) { c.reconnectAttempts = n }
}
