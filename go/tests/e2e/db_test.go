//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The runtime's own tables are the only place the effects of a call can be
// observed from outside both SDKs: an operation row, a job execution, a workflow
// step. The suite reads them with psql rather than a Postgres driver — adding a
// driver would put a test-only dependency in the SDK's go.mod, and the
// development database only exists inside a container anyway.

const (
	defaultPGContainer = "servicebridge2-pg"
	defaultPGUser      = "servicebridge"
	defaultPGDatabase  = "service-bridge"
	// dollarTag delimits string literals in generated SQL. Every value a test
	// interpolates is a name it minted itself; the tag is asserted absent so a
	// value can never close the quote.
	dollarTag = "$sbe2e$"
)

func pgContainer() string {
	if v := os.Getenv("SB_E2E_PG_CONTAINER"); v != "" {
		return v
	}
	return defaultPGContainer
}

// checkDatabase proves the suite can read the runtime's tables before any test
// runs, so a missing container is one clear message instead of one failure per
// assertion.
func checkDatabase() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := runPSQL(ctx, "SELECT count(*)::text FROM operations")
	if err != nil {
		return fmt.Errorf("cannot read the runtime database through container %q: %w\n"+
			"       start it, or point SB_E2E_PG_CONTAINER at the right container name", pgContainer(), err)
	}
	if strings.TrimSpace(out) == "" {
		return fmt.Errorf("container %q answered nothing for a count query", pgContainer())
	}
	return nil
}

func runPSQL(ctx context.Context, sql string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", pgContainer(),
		"psql", "-U", defaultPGUser, "-d", defaultPGDatabase,
		"-v", "ON_ERROR_STOP=1", "-q", "-t", "-A")
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("psql: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// queryRows runs a SELECT and returns its rows as decoded JSON objects.
// jsonb_agg keeps the whole answer on one value, so no column separator can be
// confused with data.
func queryRows(ctx context.Context, query string) ([]map[string]any, error) {
	wrapped := fmt.Sprintf("SELECT coalesce(jsonb_agg(t), '[]'::jsonb)::text FROM (\n%s\n) t;", query)
	out, err := runPSQL(ctx, wrapped)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rows); err != nil {
		return nil, fmt.Errorf("decode query answer %q: %w", strings.TrimSpace(out), err)
	}
	return rows, nil
}

func mustQuery(ctx context.Context, t *testing.T, query string) []map[string]any {
	t.Helper()
	rows, err := queryRows(ctx, query)
	if err != nil {
		t.Fatalf("query failed: %v\nquery: %s", err, query)
	}
	return rows
}

// lit renders a value as a SQL string literal. Dollar quoting needs no escaping
// rules, and a value carrying the tag is a bug in the test rather than an input
// to sanitise.
func lit(t *testing.T, value string) string {
	t.Helper()
	if strings.Contains(value, dollarTag) {
		t.Fatalf("value %q contains the SQL quoting tag and cannot be used in a query", value)
	}
	return dollarTag + value + dollarTag
}

// waitRows polls a query until it returns at least min rows and hands them back.
// Telemetry reaches Postgres through a batching transport, so every row-level
// assertion has to wait for a condition rather than for a duration.
func waitRows(ctx context.Context, t *testing.T, timeout time.Duration, what, query string, min int) []map[string]any {
	t.Helper()
	var found []map[string]any
	waitFor(ctx, t, timeout, what, func(ctx context.Context) (bool, error) {
		rows, err := queryRows(ctx, query)
		if err != nil {
			return false, err
		}
		if len(rows) >= min {
			found = rows
			return true, nil
		}
		return false, nil
	})
	return found
}

// str reads a text column. A NULL and an absent column both answer "", which is
// what every caller here compares against.
func str(row map[string]any, column string) string {
	v, ok := row[column]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprint(v)
	}
	return s
}

func num(t *testing.T, row map[string]any, column string) float64 {
	t.Helper()
	v, ok := row[column]
	if !ok || v == nil {
		t.Fatalf("column %q is absent or null in row %v", column, row)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("column %q is %T, not a number, in row %v", column, v, row)
	}
	return f
}

// serviceIDOf resolves the runtime's identifier for a service name. Bootstrap
// revokes and recreates the row on every provisioning, so the live one is the
// active one.
func serviceIDOf(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	rows := mustQuery(ctx, t, fmt.Sprintf(
		`SELECT id::text AS id FROM services WHERE name = %s AND status = 'active' ORDER BY created_at DESC LIMIT 1`,
		lit(t, name)))
	if len(rows) != 1 {
		t.Fatalf("service %s has %d active rows, expected exactly one", name, len(rows))
	}
	return str(rows[0], "id")
}
