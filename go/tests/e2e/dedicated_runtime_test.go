//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// dedicatedRuntime spawns an isolated runtime process against its own
// PostgreSQL database, for tests that need to kill and restart the runtime
// without disturbing the ambient one this whole suite (and other agents
// working in the repo) share on :14444/:14445.
//
// Mirrors node/tests/e2e/_helpers/dedicated-runtime.ts: a fresh DB built with
// `-migrate`, the runtime CA and `services` rows copied from the main DB so
// the .env.e2e bootstrap keys are recognized, ports pinned via
// runtime_settings, and a plain TCP-connect poll for readiness. Uses `docker
// exec psql` like db_test.go rather than adding a Postgres driver to go.mod
// for a test-only need.

// defaultPGPassword matches the local dev Postgres container's credentials
// (see runtime/docs and scripts/bootstrap-e2e-keys.sh's POSTGRES_DSN default).
// It has no bearing on `docker exec psql`, which authenticates inside the
// container without a password — only the dedicated runtime binary, dialing
// Postgres over TCP from outside the container, needs it.
const defaultPGPassword = "servicebridge"

func pgPort() string {
	if v := os.Getenv("SB_E2E_PG_PORT"); v != "" {
		return v
	}
	return "5433"
}

// runtimeRepoDir resolves the sibling runtime/ checkout: sdk/go/tests/e2e is
// four directories below the workspace root that holds both sdk/ and
// runtime/.
func runtimeRepoDir() (string, error) {
	root, err := repoDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, "..", "..", "..", "..", "runtime")
	if _, err := os.Stat(filepath.Join(dir, "cmd", "runtime", "main.go")); err != nil {
		return "", fmt.Errorf("runtime checkout not found at %s: %w", dir, err)
	}
	return dir, nil
}

// buildDedicatedRuntimeBinary compiles the runtime binary to outputPath.
func buildDedicatedRuntimeBinary(t *testing.T, outputPath string) {
	t.Helper()
	dir, err := runtimeRepoDir()
	if err != nil {
		t.Fatalf("locate runtime checkout: %v", err)
	}
	cmd := exec.Command("go", "build", "-o", outputPath, "./cmd/runtime")
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build runtime: %v: %s", err, stderr.String())
	}
}

// runPSQLDB runs sql against dbName in the same container db_test.go reads
// the ambient runtime's database through.
func runPSQLDB(ctx context.Context, dbName, sql string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", pgContainer(),
		"psql", "-U", defaultPGUser, "-d", dbName,
		"-v", "ON_ERROR_STOP=1", "-q", "-t", "-A")
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("psql -d %s: %w: %s", dbName, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func queryRowsFromDB(ctx context.Context, dbName, query string) ([]map[string]any, error) {
	wrapped := fmt.Sprintf("SELECT coalesce(jsonb_agg(t), '[]'::jsonb)::text FROM (\n%s\n) t;", query)
	out, err := runPSQLDB(ctx, dbName, wrapped)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rows); err != nil {
		return nil, fmt.Errorf("decode query answer %q: %w", strings.TrimSpace(out), err)
	}
	return rows, nil
}

// randomSuffix mints a short hex suffix for an isolated database name.
func randomSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func createIsolatedDatabase(ctx context.Context, name string) error {
	_, err := runPSQLDB(ctx, "postgres", fmt.Sprintf(`CREATE DATABASE %q`, name))
	if err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

func dropIsolatedDatabase(ctx context.Context, name string) error {
	_, err := runPSQLDB(ctx, "postgres", fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, name))
	if err != nil {
		return fmt.Errorf("drop database %s: %w", name, err)
	}
	return nil
}

func runMigrateOnly(ctx context.Context, binaryPath, dsn string) error {
	cmd := exec.CommandContext(ctx, binaryPath, "-pg-url", dsn, "-migrate")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("migrate-only: %w\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	return nil
}

// byteaLiteral turns a bytea column value as jsonb_agg encodes it ("\xHEX")
// into a SQL expression that reconstructs the same bytes. jsonb_agg never
// encodes bytea any other way, so a value that doesn't start with \x means the
// column wasn't bytea and the caller passed the wrong helper.
func byteaLiteral(t *testing.T, v any) string {
	t.Helper()
	s, ok := v.(string)
	if !ok || !strings.HasPrefix(s, "\\x") {
		t.Fatalf("byteaLiteral: value %v is not a jsonb-encoded bytea", v)
	}
	return fmt.Sprintf("decode(%s, 'hex')", lit(t, strings.TrimPrefix(s, "\\x")))
}

func boolLiteral(v any) string {
	if b, _ := v.(bool); b {
		return "true"
	}
	return "false"
}

// seedRuntimeCaInto copies the runtime_ca row (id=1) from the main DB into
// dbName, so the isolated runtime trusts the same CA as the .env.e2e bootstrap
// keys — the CA lives in Postgres, not on disk.
func seedRuntimeCaInto(ctx context.Context, t *testing.T, dbName string) {
	t.Helper()
	rows, err := queryRowsFromDB(ctx, defaultPGDatabase,
		`SELECT id, cert_der, key_der, created_at::text AS created_at FROM runtime_ca WHERE id = 1`)
	if err != nil {
		t.Fatalf("read runtime_ca: %v", err)
	}
	if len(rows) == 0 {
		return
	}
	row := rows[0]
	sql := fmt.Sprintf(
		`INSERT INTO runtime_ca (id, cert_der, key_der, created_at)
		 VALUES (%v, %s, %s, %s) ON CONFLICT (id) DO NOTHING`,
		int(row["id"].(float64)), byteaLiteral(t, row["cert_der"]), byteaLiteral(t, row["key_der"]),
		lit(t, str(row, "created_at")))
	if _, err := runPSQLDB(ctx, dbName, sql); err != nil {
		t.Fatalf("seed runtime_ca into %s: %v", dbName, err)
	}
}

// seedServicesInto copies every row of the main DB's `services` table into
// dbName, so the isolated runtime recognizes the same .env.e2e bootstrap keys
// as the ambient one.
func seedServicesInto(ctx context.Context, t *testing.T, dbName string) {
	t.Helper()
	rows, err := queryRowsFromDB(ctx, defaultPGDatabase, `
		SELECT id, name, key_id, secret_hash, status, persist_ops, persist_logs, persist_metrics,
		       cap_rpc_handle, cap_event_handle, cap_workflow_handle, cap_job_handle,
		       cap_rpc_call, cap_event_publish, cap_workflow_run, created_at::text AS created_at
		  FROM services`)
	if err != nil {
		t.Fatalf("read services: %v", err)
	}
	for _, row := range rows {
		sql := fmt.Sprintf(`
			INSERT INTO services
				(id, name, key_id, secret_hash, status, persist_ops, persist_logs, persist_metrics,
				 cap_rpc_handle, cap_event_handle, cap_workflow_handle, cap_job_handle,
				 cap_rpc_call, cap_event_publish, cap_workflow_run, created_at)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
			ON CONFLICT (id) DO NOTHING`,
			lit(t, str(row, "id")), lit(t, str(row, "name")), byteaLiteral(t, row["key_id"]),
			lit(t, str(row, "secret_hash")), lit(t, str(row, "status")),
			boolLiteral(row["persist_ops"]), boolLiteral(row["persist_logs"]), boolLiteral(row["persist_metrics"]),
			boolLiteral(row["cap_rpc_handle"]), boolLiteral(row["cap_event_handle"]),
			boolLiteral(row["cap_workflow_handle"]), boolLiteral(row["cap_job_handle"]),
			boolLiteral(row["cap_rpc_call"]), boolLiteral(row["cap_event_publish"]), boolLiteral(row["cap_workflow_run"]),
			lit(t, str(row, "created_at")))
		if _, err := runPSQLDB(ctx, dbName, sql); err != nil {
			t.Fatalf("seed service %s into %s: %v", str(row, "name"), dbName, err)
		}
	}
}

func updateSettingsInto(ctx context.Context, t *testing.T, dbName string, settings map[string]string) {
	t.Helper()
	for key, value := range settings {
		sql := fmt.Sprintf(`UPDATE runtime_settings SET value = %s, updated_at = now() WHERE key = %s`,
			lit(t, value), lit(t, key))
		if _, err := runPSQLDB(ctx, dbName, sql); err != nil {
			t.Fatalf("set %s in %s: %v", key, dbName, err)
		}
	}
}

func waitTCPReady(ctx context.Context, t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 2*time.Second)
		if err == nil {
			_ = conn.Close()
			// A fresh accept doesn't mean the gRPC server inside is fully wired
			// yet; a short grace period matches node's dedicated-runtime.ts.
			time.Sleep(300 * time.Millisecond)
			return
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("gRPC port %d not ready: context ended: %v (last error: %v)", port, ctx.Err(), lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("gRPC port %d not ready after %s: %v", port, timeout, lastErr)
}

// dedicatedRuntime is a handle on one isolated runtime process.
type dedicatedRuntime struct {
	t          *testing.T
	binaryPath string
	dbName     string
	dsn        string
	GRPCPort   int
	UIPort     int
	URL        string

	cmd    *exec.Cmd
	killed bool
}

type spawnDedicatedRuntimeOpts struct {
	Name          string
	GRPCPort      int
	UIPort        int
	BinaryPath    string
	ExtraSettings map[string]string
}

// spawnDedicatedRuntime creates a fresh database, applies migrations, seeds
// the CA and service rows from the main DB, pins the requested ports, starts
// the runtime binary and waits for it to accept connections.
func spawnDedicatedRuntime(ctx context.Context, t *testing.T, opts spawnDedicatedRuntimeOpts) *dedicatedRuntime {
	t.Helper()

	dbName := fmt.Sprintf("test_go_dedirt_%s_%s", opts.Name, randomSuffix())
	if err := createIsolatedDatabase(ctx, dbName); err != nil {
		t.Fatalf("%v", err)
	}
	runtimeDir, err := runtimeRepoDir()
	if err != nil {
		t.Fatalf("locate runtime checkout: %v", err)
	}
	dsn := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", defaultPGUser, defaultPGPassword, pgPort(), dbName)

	if err := runMigrateOnly(ctx, opts.BinaryPath, dsn); err != nil {
		_ = dropIsolatedDatabase(ctx, dbName)
		t.Fatalf("migrate isolated db %s: %v", dbName, err)
	}
	seedRuntimeCaInto(ctx, t, dbName)
	seedServicesInto(ctx, t, dbName)

	settings := map[string]string{
		"network.grpc_port": strconv.Itoa(opts.GRPCPort),
		"network.ui_port":   strconv.Itoa(opts.UIPort),
	}
	for k, v := range opts.ExtraSettings {
		settings[k] = v
	}
	updateSettingsInto(ctx, t, dbName, settings)

	rt := &dedicatedRuntime{
		t:          t,
		binaryPath: opts.BinaryPath,
		dbName:     dbName,
		dsn:        dsn,
		GRPCPort:   opts.GRPCPort,
		UIPort:     opts.UIPort,
		URL:        fmt.Sprintf("localhost:%d", opts.GRPCPort),
	}
	rt.spawnProcess(runtimeDir)
	waitTCPReady(ctx, t, opts.GRPCPort, 60*time.Second)
	return rt
}

func (rt *dedicatedRuntime) spawnProcess(runtimeDir string) {
	cmd := exec.Command(rt.binaryPath, "-pg-url", rt.dsn)
	cmd.Dir = runtimeDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		rt.t.Fatalf("start dedicated runtime: %v", err)
	}
	rt.cmd = cmd
	rt.killed = false
}

// Kill stops the current process and waits for it to exit. It does not drop
// the database or restart — call Restart for that.
func (rt *dedicatedRuntime) Kill() {
	rt.t.Helper()
	if rt.killed || rt.cmd == nil {
		return
	}
	rt.killed = true
	_ = rt.cmd.Process.Kill()
	done := make(chan struct{})
	go func() {
		_ = rt.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		rt.t.Log("dedicated runtime did not exit within 10s of being killed")
	}
}

// Restart spawns a fresh process against the same database and ports and
// waits until it accepts connections again.
func (rt *dedicatedRuntime) Restart(ctx context.Context) {
	rt.t.Helper()
	if !rt.killed {
		rt.Kill()
	}
	runtimeDir, err := runtimeRepoDir()
	if err != nil {
		rt.t.Fatalf("locate runtime checkout: %v", err)
	}
	rt.spawnProcess(runtimeDir)
	waitTCPReady(ctx, rt.t, rt.GRPCPort, 60*time.Second)
}

// Cleanup kills the process if still running and drops the isolated database.
// Safe to call more than once.
func (rt *dedicatedRuntime) Cleanup() {
	rt.Kill()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := dropIsolatedDatabase(ctx, rt.dbName); err != nil {
		rt.t.Logf("drop isolated database %s: %v", rt.dbName, err)
	}
}
