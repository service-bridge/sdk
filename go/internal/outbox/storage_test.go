package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func openTemp(t *testing.T) *Storage {
	t.Helper()
	st, err := Open(context.Background(), Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return st
}

func record(id string, enqueuedAt int64) Record {
	return Record{
		ID:           id,
		Name:         "order.created",
		Payload:      []byte("proto-" + id),
		PayloadJSON:  []byte(`{"id":"` + id + `"}`),
		ContractHash: "hash-1",
		Headers:      map[string]string{"k": "v"},
		OccurredAtMs: enqueuedAt,
		EnqueuedAtMs: enqueuedAt,
	}
}

func TestOpenCreatesSchemaAndStampsVersion(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	var version int
	if err := st.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, SchemaVersion)
	}
	var mode string
	if err := st.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	if st.Rows() != 0 {
		t.Fatalf("Rows() = %d on a fresh file, want 0", st.Rows())
	}
}

func TestOpenRejectsForeignSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	st, err := Open(ctx, Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, "PRAGMA user_version = 99"); err != nil {
		t.Fatalf("stamp foreign version: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = Open(ctx, Config{Dir: dir})
	if err == nil {
		t.Fatal("Open accepted a file written by another schema version")
	}
	var versionErr *SchemaVersionError
	if !errors.As(err, &versionErr) {
		t.Fatalf("error %v is not a *SchemaVersionError", err)
	}
	if versionErr.Found != 99 || versionErr.Want != SchemaVersion {
		t.Fatalf("SchemaVersionError = %+v, want found 99 want %d", versionErr, SchemaVersion)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("message %q does not name the directory to delete", err.Error())
	}
}

func TestCrashRecoveryReturnsInflightRowsToPending(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	st, err := Open(ctx, Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 3 {
		if err := st.Enqueue(ctx, record(fmt.Sprintf("id-%d", i), int64(i)), 0); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	claimed, err := st.ClaimDue(ctx, 100, 10)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d rows, want 3", len(claimed))
	}
	inflight, err := st.CountByStatus(ctx, StatusInflight)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if inflight != 3 {
		t.Fatalf("inflight = %d before the crash, want 3", inflight)
	}
	// Close stands in for the process dying with a batch claimed: the rows were
	// never confirmed by the runtime.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(ctx, Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	inflight, err = reopened.CountByStatus(ctx, StatusInflight)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if inflight != 0 {
		t.Fatalf("inflight = %d after recovery, want 0", inflight)
	}
	again, err := reopened.ClaimDue(ctx, 100, 10)
	if err != nil {
		t.Fatalf("ClaimDue after recovery: %v", err)
	}
	if len(again) != 3 {
		t.Fatalf("claimed %d rows after recovery, want 3", len(again))
	}
	if reopened.Rows() != 3 {
		t.Fatalf("Rows() = %d after reopen, want 3", reopened.Rows())
	}
}

func TestEnqueueRoundTripsEveryField(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	want := Record{
		ID:              "evt-1",
		Name:            "order.created",
		Payload:         []byte{0x01, 0x02},
		PayloadJSON:     []byte(`{"a":1}`),
		ContractHash:    "hash-9",
		PartitionKey:    "cust-7",
		IdempotencyKey:  "idem-3",
		Headers:         map[string]string{"tenant": "acme"},
		OccurredAtMs:    1_700_000_000_000,
		EnqueuedAtMs:    1_700_000_000_500,
		NextAttemptAtMs: 0,
		Trace:           "0199a0f2-0000-7000-8000-000000000001-0199a0f2-0000-7000-8000-000000000002",
	}
	if err := st.Enqueue(ctx, want, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	claimed, err := st.ClaimDue(ctx, want.EnqueuedAtMs, 10)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d rows, want 1", len(claimed))
	}
	got := claimed[0]
	if got.ID != want.ID || got.Name != want.Name || string(got.Payload) != string(want.Payload) ||
		string(got.PayloadJSON) != string(want.PayloadJSON) || got.ContractHash != want.ContractHash ||
		got.PartitionKey != want.PartitionKey || got.IdempotencyKey != want.IdempotencyKey ||
		got.OccurredAtMs != want.OccurredAtMs || got.EnqueuedAtMs != want.EnqueuedAtMs ||
		got.Trace != want.Trace {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}
	if got.Headers["tenant"] != "acme" {
		t.Fatalf("headers = %v, want tenant=acme", got.Headers)
	}
}

func TestEnqueueRefusesAtCapUnderConcurrency(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	const maxRows = 20
	const publishers = 8
	const perPublisher = 10

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		full   int
		stored int
	)
	for p := range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perPublisher {
				err := st.Enqueue(ctx, record(fmt.Sprintf("id-%d-%d", p, i), int64(i)), maxRows)
				mu.Lock()
				switch {
				case err == nil:
					stored++
				case errors.Is(err, ErrFull):
					full++
				default:
					t.Errorf("Enqueue: %v", err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if stored != maxRows {
		t.Fatalf("stored %d rows, want exactly the cap %d", stored, maxRows)
	}
	if full != publishers*perPublisher-maxRows {
		t.Fatalf("refused %d publishes, want %d", full, publishers*perPublisher-maxRows)
	}
	if st.Rows() != maxRows {
		t.Fatalf("Rows() = %d, want %d", st.Rows(), maxRows)
	}
}

func TestClaimDueHonoursOrderAndNextAttempt(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	for _, id := range []string{"c", "a", "b"} {
		rec := record(id, map[string]int64{"a": 10, "b": 20, "c": 30}[id])
		if err := st.Enqueue(ctx, rec, 0); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	deferred := record("d", 5)
	deferred.NextAttemptAtMs = 1_000
	if err := st.Enqueue(ctx, deferred, 0); err != nil {
		t.Fatalf("Enqueue deferred: %v", err)
	}

	claimed, err := st.ClaimDue(ctx, 999, 10)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	got := make([]string, len(claimed))
	for i, r := range claimed {
		got[i] = r.ID
	}
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("claimed %v, want a,b,c in enqueue order without the deferred row", got)
	}

	due, ok, err := st.NextDueAt(ctx)
	if err != nil {
		t.Fatalf("NextDueAt: %v", err)
	}
	if !ok || due != 1_000 {
		t.Fatalf("NextDueAt = (%d, %v), want (1000, true)", due, ok)
	}
}

func TestDrainQueryPlanDoesNotSort(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	// A backlog large enough that a sort would be the plan's dominant cost.
	for i := range 10_000 {
		if err := st.Enqueue(ctx, record(fmt.Sprintf("id-%05d", i), int64(i)), 0); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if _, err := st.db.ExecContext(ctx, "ANALYZE"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}

	plan, err := st.queryPlan(ctx, selectDueSQL, int64(1<<40), 100)
	if err != nil {
		t.Fatalf("queryPlan: %v", err)
	}
	if len(plan) == 0 {
		t.Fatal("empty query plan")
	}
	joined := strings.Join(plan, " | ")
	if strings.Contains(strings.ToUpper(joined), "TEMP B-TREE") {
		t.Fatalf("drain query sorts the backlog: %s", joined)
	}
	if !strings.Contains(joined, "event_outbox_pending_order_idx") {
		t.Fatalf("drain query does not walk the ordering index: %s", joined)
	}
}

func TestClaimAndCompleteCostOneTransactionEach(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	const batch = 100
	for i := range batch {
		if err := st.Enqueue(ctx, record(fmt.Sprintf("id-%03d", i), int64(i)), 0); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	before := st.commitCount()
	claimed, err := st.ClaimDue(ctx, int64(batch), batch)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != batch {
		t.Fatalf("claimed %d rows, want %d", len(claimed), batch)
	}
	if got := st.commitCount() - before; got != 1 {
		t.Fatalf("ClaimDue committed %d transactions for %d rows, want 1", got, batch)
	}

	var result Result
	for i, r := range claimed {
		switch i % 3 {
		case 0:
			result.Done = append(result.Done, r.ID)
		case 1:
			result.Retry = append(result.Retry, Retry{ID: r.ID, Attempts: 1, LastError: "boom", NextAttemptAtMs: 5_000})
		default:
			result.Failed = append(result.Failed, Failure{ID: r.ID, Attempts: 1, LastError: "invalid"})
		}
	}

	before = st.commitCount()
	if err := st.Complete(ctx, result); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := st.commitCount() - before; got != 1 {
		t.Fatalf("Complete committed %d transactions for %d rows, want 1", got, batch)
	}

	if st.Rows() != batch-len(result.Done) {
		t.Fatalf("Rows() = %d, want %d", st.Rows(), batch-len(result.Done))
	}
	pending, err := st.CountByStatus(ctx, StatusPending)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if pending != len(result.Retry) {
		t.Fatalf("pending = %d, want %d", pending, len(result.Retry))
	}
	failed, err := st.CountByStatus(ctx, StatusFailed)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if failed != len(result.Failed) {
		t.Fatalf("failed = %d, want %d", failed, len(result.Failed))
	}
}

func TestCompleteReArmsRowForItsNextAttempt(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	if err := st.Enqueue(ctx, record("id-1", 1), 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := st.ClaimDue(ctx, 10, 10); err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	err := st.Complete(ctx, Result{Retry: []Retry{{
		ID: "id-1", Attempts: 3, LastError: "runtime unreachable", NextAttemptAtMs: 60_000,
	}}})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	rec, status, err := st.Load(ctx, "id-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if status != StatusPending {
		t.Fatalf("status = %q, want %q", status, StatusPending)
	}
	if rec.Attempts != 3 || rec.NextAttemptAtMs != 60_000 || rec.LastError != "runtime unreachable" {
		t.Fatalf("re-armed row = %+v", rec)
	}
	if got, err := st.ClaimDue(ctx, 59_999, 10); err != nil || len(got) != 0 {
		t.Fatalf("ClaimDue before the next attempt returned %d rows (err %v), want 0", len(got), err)
	}
	if got, err := st.ClaimDue(ctx, 60_000, 10); err != nil || len(got) != 1 {
		t.Fatalf("ClaimDue at the next attempt returned %d rows (err %v), want 1", len(got), err)
	}
}

func TestClaimDueRejectsNonPositiveLimit(t *testing.T) {
	st := openTemp(t)
	if _, err := st.ClaimDue(context.Background(), 0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ClaimDue(limit=0) error = %v, want ErrInvalidConfig", err)
	}
}

func TestOpenRejectsEmptyDir(t *testing.T) {
	if _, err := Open(context.Background(), Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Open with no Dir returned %v, want ErrInvalidConfig", err)
	}
}

func TestClosedStorageRefusesWork(t *testing.T) {
	st, err := Open(context.Background(), Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	ctx := context.Background()
	if err := st.Enqueue(ctx, record("id-1", 1), 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrClosed", err)
	}
	if _, err := st.ClaimDue(ctx, 1, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("ClaimDue after Close = %v, want ErrClosed", err)
	}
	if _, _, err := st.NextDueAt(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("NextDueAt after Close = %v, want ErrClosed", err)
	}
	if _, err := st.CountByStatus(ctx, StatusPending); !errors.Is(err, ErrClosed) {
		t.Fatalf("CountByStatus after Close = %v, want ErrClosed", err)
	}
	if _, _, err := st.Load(ctx, "id-1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Load after Close = %v, want ErrClosed", err)
	}
}

func TestNextDueAtReportsNothingPending(t *testing.T) {
	st := openTemp(t)
	_, ok, err := st.NextDueAt(context.Background())
	if err != nil {
		t.Fatalf("NextDueAt: %v", err)
	}
	if ok {
		t.Fatal("NextDueAt reported work on an empty outbox")
	}
}

func TestCompleteWithNothingToDoSkipsTheTransaction(t *testing.T) {
	st := openTemp(t)
	before := st.commitCount()
	if err := st.Complete(context.Background(), Result{}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if st.commitCount() != before {
		t.Fatal("Complete opened a transaction for an empty result")
	}
}

func TestLoadReportsMissingRow(t *testing.T) {
	st := openTemp(t)
	_, _, err := st.Load(context.Background(), "absent")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Load of an absent row = %v, want sql.ErrNoRows", err)
	}
}

func TestPathPointsAtTheConfiguredFile(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(context.Background(), Config{Dir: dir, FileName: "events.db"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	if st.Path() != filepath.Join(dir, "events.db") {
		t.Fatalf("Path() = %q, want %q", st.Path(), filepath.Join(dir, "events.db"))
	}
}

func TestFireAndForgetFlagRoundTrips(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	rec := record("id-1", 1)
	rec.FireAndForget = true
	if err := st.Enqueue(ctx, rec, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	loaded, _, err := st.Load(ctx, "id-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !loaded.FireAndForget {
		t.Fatal("the no-wait flag was lost")
	}
	claimed, err := st.ClaimDue(ctx, 10, 10)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 || !claimed[0].FireAndForget {
		t.Fatalf("claimed row lost the no-wait flag: %+v", claimed)
	}
}

func TestEnqueueWithoutHeadersStoresAnEmptyObject(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	rec := record("id-1", 1)
	rec.Headers = nil
	if err := st.Enqueue(ctx, rec, 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	loaded, _, err := st.Load(ctx, "id-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Headers == nil || len(loaded.Headers) != 0 {
		t.Fatalf("headers = %v, want an empty map", loaded.Headers)
	}
}

func TestOpenReportsAnUnusableDirectory(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	if _, err := Open(context.Background(), Config{Dir: filepath.Join(blocker, "outbox")}); err == nil {
		t.Fatal("Open accepted a path that cannot hold a directory")
	}
}

func TestOpenReportsACorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, DefaultFileName), []byte("this is not a database"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	if _, err := Open(context.Background(), Config{Dir: dir}); err == nil {
		t.Fatal("Open accepted a file that is not a database")
	}
}
