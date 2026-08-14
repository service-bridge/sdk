//go:build e2e

// Package e2e drives the Go SDK against a live runtime. Nothing here is
// mocked: every assertion is either an observable effect on another SDK
// instance or a row the runtime wrote to Postgres.
package e2e

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
)

// The per-domain service identities the suite runs under. A domain owns three
// service rows (e2e-<domain>-1/2/3), provisioned by
// sdk/scripts/bootstrap-e2e-keys.sh. Domains exist because the runtime accepts
// one Events.Subscribe stream per instance: two clients that both subscribe
// must be two different services, or the second one gets AlreadyExists.
const (
	domainRPC      = "go-rpc"
	domainEvents   = "go-events"
	domainJobs     = "go-jobs"
	domainWorkflow = "go-workflow"
	domainMisc     = "go-misc"
	domainXLang    = "go-xlang"
)

const (
	// connectTimeout bounds Client.Start. It covers provisioning, the control
	// stream and the first registry snapshot.
	connectTimeout = 20 * time.Second
	// discoveryTimeout bounds how long a peer may take to show up in another
	// instance's service map.
	discoveryTimeout = 15 * time.Second
	// deliveryTimeout bounds an event, a job fire or a workflow run.
	deliveryTimeout = 30 * time.Second
	// rowTimeout bounds a row the runtime writes asynchronously — telemetry is
	// batched on the SDK side and flushed by the transport, so an operation row
	// lands well after the call it describes returned.
	rowTimeout = 30 * time.Second
)

// suite is the environment every test reads. It is populated once by TestMain
// and never mutated afterwards.
var suite struct {
	// runtimeURL is the runtime's gRPC control-plane address, host:port.
	runtimeURL string
	// keys maps the .env.e2e variable name to the bootstrap key.
	keys map[string]string
	// sdkRepo is the directory holding .env.e2e — the Node SDK repository root.
	// The cross-language agent is launched against <sdkRepo>/node/src.
	sdkRepo string
	// protoFile is the shared contract both SDKs load, so a hash mismatch is a
	// real disagreement between the two implementations and never a difference
	// between two source files.
	protoFile string
	// runID separates one `go test` run from the next. Every name a test mints
	// carries it, so a second run reuses no event name, method name or
	// partition key from the first.
	runID string
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e setup failed: %v\n", err)
		// A missing runtime or missing keys is a failure, not a reason to skip:
		// a skipped suite reports the same green as a passing one.
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func setup() error {
	envPath, err := locateEnvFile()
	if err != nil {
		return err
	}
	suite.sdkRepo = filepath.Dir(envPath)

	vars, err := parseEnvFile(envPath)
	if err != nil {
		return err
	}
	suite.runtimeURL = vars["SERVICEBRIDGE_URL"]
	if suite.runtimeURL == "" {
		return fmt.Errorf("%s carries no SERVICEBRIDGE_URL: run `bash scripts/bootstrap-e2e-keys.sh` in %s", envPath, suite.sdkRepo)
	}
	suite.keys = vars

	for _, domain := range []string{domainRPC, domainEvents, domainJobs, domainWorkflow, domainMisc, domainXLang} {
		for i := 1; i <= 3; i++ {
			if vars[keyVar(domain, i)] == "" {
				return fmt.Errorf("%s carries no %s: add domain %q to DOMAINS in scripts/bootstrap-e2e-keys.sh and rerun it",
					envPath, keyVar(domain, i), domain)
			}
		}
	}

	if err := dialRuntime(suite.runtimeURL); err != nil {
		return err
	}
	if err := checkDatabase(); err != nil {
		return err
	}

	root, err := repoDir()
	if err != nil {
		return err
	}
	suite.protoFile = filepath.Join(root, "testdata", "e2e.proto")
	if _, err := os.Stat(suite.protoFile); err != nil {
		return fmt.Errorf("shared contract %s is missing: %w", suite.protoFile, err)
	}
	suite.runID = fmt.Sprintf("%x", rand.Uint32())
	return nil
}

// repoDir is the directory holding this package's sources. Tests run with it as
// their working directory, but a caller may override it, so it is resolved once
// and used for every path built from it.
func repoDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	return wd, nil
}

// locateEnvFile finds .env.e2e. It is gitignored and lives at the repository
// root, shared by the Node and Go suites, so the search walks up from the
// working directory; the `sdk/` candidate covers a checkout that nests this
// repository under a workspace directory.
func locateEnvFile() (string, error) {
	if explicit := os.Getenv("SB_E2E_ENV_FILE"); explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("SB_E2E_ENV_FILE=%s is not readable: %w", explicit, err)
		}
		return explicit, nil
	}
	dir, err := repoDir()
	if err != nil {
		return "", err
	}
	for range 10 {
		for _, candidate := range []string{
			filepath.Join(dir, ".env.e2e"),
			filepath.Join(dir, "sdk", ".env.e2e"),
		} {
			if _, err := os.Stat(candidate); err == nil {
				return candidate, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf(".env.e2e not found above the working directory or in a sibling sdk/ checkout: " +
		"run `bash scripts/bootstrap-e2e-keys.sh` in the sdk repository, or point SB_E2E_ENV_FILE at the file")
}

func parseEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	out := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return out, nil
}

func dialRuntime(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("runtime is not listening on %s: %w\n"+
			"       start it: cd runtime && go build -o /tmp/sb-runtime ./cmd/runtime && "+
			"nohup /tmp/sb-runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/service-bridge?sslmode=disable > /tmp/sb-runtime.log 2>&1 & disown", addr, err)
	}
	return conn.Close()
}

func keyVar(domain string, index int) string {
	return fmt.Sprintf("SB_E2E_%s_%d", strings.ToUpper(strings.ReplaceAll(domain, "-", "_")), index)
}

// serviceName is the runtime-side name of a domain identity. Tests assert
// against it whenever they need to name a callee or read its rows back.
func serviceName(domain string, index int) string {
	return fmt.Sprintf("e2e-%s-%d", domain, index)
}

func bootstrapKey(t *testing.T, domain string, index int) string {
	t.Helper()
	key := suite.keys[keyVar(domain, index)]
	if key == "" {
		t.Fatalf("%s is not set: rerun `bash scripts/bootstrap-e2e-keys.sh`", keyVar(domain, index))
	}
	return key
}

// logger keeps SDK logs out of passing tests. SB_E2E_VERBOSE=1 routes them to
// the test log, which is what a failing delivery path needs.
func logger(t *testing.T) *slog.Logger {
	t.Helper()
	if os.Getenv("SB_E2E_VERBOSE") == "1" {
		return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// newClient builds an unstarted client on a domain identity. Its outbox lives
// in a per-test temporary directory, so no run inherits another run's buffered
// events.
func newClient(t *testing.T, domain string, index int, opts ...servicebridge.Option) *servicebridge.Client {
	t.Helper()
	base := []servicebridge.Option{
		servicebridge.WithDataDir(t.TempDir()),
		servicebridge.WithLogger(logger(t)),
		// Bounded so a runtime that went away fails the test instead of holding
		// it until the go test timeout.
		servicebridge.WithReconnectAttempts(3),
	}
	c, err := servicebridge.New(suite.runtimeURL, bootstrapKey(t, domain, index), append(base, opts...)...)
	if err != nil {
		t.Fatalf("new client on %s: %v", serviceName(domain, index), err)
	}
	return c
}

// start brings a client up and registers its shutdown. Every test that starts a
// client goes through here: a client left running holds an Events.Subscribe
// stream that the next test on the same identity would be refused.
func start(ctx context.Context, t *testing.T, c *servicebridge.Client) {
	t.Helper()
	startCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := c.Start(startCtx); err != nil {
		t.Fatalf("start client: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		if err := c.Stop(stopCtx); err != nil {
			t.Logf("stop client: %v", err)
		}
	})
}

var nameCounter atomic.Uint64

// uniqueName mints a dotted name valid for event names, RPC methods and
// workflow names: the runtime accepts `[a-z0-9_-]+(\.[a-z0-9_-]+)*`. The run
// identifier is part of it, so two consecutive runs of the suite never share a
// name and no test can inherit rows left by the previous one.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s.r%s.n%d", prefix, suite.runID, nameCounter.Add(1))
}

// uniqueID mints a dash-joined identifier for partition, idempotency and
// business keys. Partition keys in particular must never be literals: the
// runtime holds back newer events on a key while an older delivery on it is
// still in flight, so a reused key inherits stalls from earlier runs.
func uniqueID(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, suite.runID, nameCounter.Add(1))
}

// waitFor polls cond until it holds. It replaces a fixed sleep everywhere:
// runtime timings move with machine load, and a sleep long enough to be safe on
// a loaded machine wastes the same time on an idle one.
func waitFor(ctx context.Context, t *testing.T, timeout time.Duration, what string, cond func(context.Context) (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := cond(ctx)
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: context ended: %v (last error: %v)", what, ctx.Err(), lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if lastErr != nil {
		t.Fatalf("timed out after %s waiting for %s (last error: %v)", timeout, what, lastErr)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// methodVisible reports whether the client's mesh view carries the method on
// the named service. It is the readiness signal a caller needs: a call issued
// before the callee is in the snapshot has no candidate to route to.
func methodVisible(c *servicebridge.Client, service, method string) bool {
	for _, m := range c.ServiceMap().Methods {
		if m.ServiceName == service && m.Name == method {
			return true
		}
	}
	return false
}

func waitForMethod(ctx context.Context, t *testing.T, c *servicebridge.Client, service, method string) {
	t.Helper()
	waitFor(ctx, t, discoveryTimeout, fmt.Sprintf("method %s on %s to reach the service map", method, service),
		func(context.Context) (bool, error) { return methodVisible(c, service, method), nil })
}

// testContext bounds a whole test. Every blocking SDK call takes it, so a hung
// runtime surfaces as a named failure rather than a panic from the go test
// watchdog.
func testContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
