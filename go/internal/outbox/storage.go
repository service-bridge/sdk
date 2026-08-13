// Package outbox owns the SDK's local durable buffer: the SQLite file that
// lets a published event survive a process restart and a runtime outage.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	// modernc.org/sqlite is a pure-Go translation of SQLite. A cgo driver would
	// make every consumer of this SDK give up cross-compilation.
	_ "modernc.org/sqlite"
)

// SchemaVersion is stamped into the file via PRAGMA user_version. A file
// carrying any other version is rejected on open: the SDK does not migrate
// outbox schemas.
const SchemaVersion = 1

// DefaultFileName is the database file created inside the data directory.
const DefaultFileName = "sdk.db"

// Row statuses. A row is pending until a drain claims it, inflight while one
// batch is on the wire, failed once a terminal rejection parks it.
const (
	StatusPending  = "pending"
	StatusInflight = "inflight"
	StatusFailed   = "failed"
)

// ErrFull is returned by Enqueue when the outbox already holds MaxRows rows.
var ErrFull = errors.New("outbox: full")

// ErrClosed is returned by every method called after Close.
var ErrClosed = errors.New("outbox: closed")

// ErrInvalidConfig is returned by Open when the config misses a required field.
var ErrInvalidConfig = errors.New("outbox: invalid config")

// SchemaVersionError reports a database file written by an SDK with a different
// outbox schema. It carries the path so the message can name what to delete.
type SchemaVersionError struct {
	Path  string
	Found int
	Want  int
}

func (e *SchemaVersionError) Error() string {
	return fmt.Sprintf(
		"outbox: %s holds schema v%d, this SDK requires v%d: outbox schemas are not migrated, "+
			"stop the service, delete %s and start again; events still buffered in the old file are lost",
		e.Path, e.Found, e.Want, filepath.Dir(e.Path))
}

// Record is one buffered event: the envelope the runtime will receive plus the
// delivery bookkeeping the drain path owns.
type Record struct {
	ID           string
	Name         string
	Payload      []byte
	PayloadJSON  []byte
	ContractHash string
	PartitionKey string
	// IdempotencyKey is scoped to the publishing service by the runtime.
	IdempotencyKey string
	// FireAndForget mirrors the envelope flag. Rows written here always carry
	// false — the no-wait path goes straight to the runtime — but the column
	// keeps the stored row a complete image of the envelope.
	FireAndForget bool
	Headers       map[string]string
	OccurredAtMs  int64
	EnqueuedAtMs  int64
	Attempts      int32
	LastError     string
	// NextAttemptAtMs is the unix-ms before which no drain may claim the row.
	NextAttemptAtMs int64
	// Trace is the X-SB-Trace header captured at publish time.
	Trace string
}

// Retry re-arms one row for a later attempt.
type Retry struct {
	ID              string
	Attempts        int32
	LastError       string
	NextAttemptAtMs int64
}

// Failure parks one row no retry can fix.
type Failure struct {
	ID        string
	Attempts  int32
	LastError string
}

// Result is what one drain batch decided, applied by Complete in one
// transaction.
type Result struct {
	Done   []string
	Retry  []Retry
	Failed []Failure
}

// Config configures Open.
type Config struct {
	// Dir holds the database file. Created if missing.
	Dir string
	// FileName defaults to DefaultFileName.
	FileName string
}

// Storage is the outbox database. Safe for concurrent use; the file is closed
// explicitly by Close.
type Storage struct {
	db   *sql.DB
	path string

	insertStmt  *sql.Stmt
	selectStmt  *sql.Stmt
	claimStmt   *sql.Stmt
	deleteStmt  *sql.Stmt
	retryStmt   *sql.Stmt
	failStmt    *sql.Stmt
	nextDueStmt *sql.Stmt

	// enqueueMu makes the cap check and the insert one critical section.
	// Without it two concurrent publishes both read a row count below the cap
	// and both insert.
	enqueueMu sync.Mutex

	mu      sync.Mutex
	rows    int
	commits uint64
	closed  bool
}

const createSchemaSQL = `
CREATE TABLE IF NOT EXISTS event_outbox (
  id                 TEXT    PRIMARY KEY,
  name               TEXT    NOT NULL,
  payload            BLOB    NOT NULL,
  payload_json       BLOB    NOT NULL DEFAULT x'',
  contract_hash      TEXT    NOT NULL DEFAULT '',
  partition_key      TEXT    NOT NULL DEFAULT '',
  idempotency_key    TEXT    NOT NULL DEFAULT '',
  fire_and_forget    INTEGER NOT NULL DEFAULT 0,
  headers            TEXT    NOT NULL DEFAULT '{}',
  occurred_at_ms     INTEGER NOT NULL,
  enqueued_at_ms     INTEGER NOT NULL,
  status             TEXT    NOT NULL CHECK(status IN ('pending','inflight','failed')),
  attempts           INTEGER NOT NULL DEFAULT 0,
  last_error         TEXT    NOT NULL DEFAULT '',
  next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
  x_sb_trace         TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS event_outbox_pending_order_idx
  ON event_outbox(enqueued_at_ms, id, next_attempt_at_ms)
  WHERE status='pending';
`

// selectDueSQL reads the next claimable batch. Its column order matches
// event_outbox_pending_order_idx, so SQLite walks that index in ORDER BY order
// and LIMIT ends the scan. An index covering only the filter would make the
// engine sort the whole backlog before taking the batch, and LIMIT could not
// stop it early.
const selectDueSQL = `SELECT id, name, payload, payload_json, contract_hash, partition_key,
       idempotency_key, fire_and_forget, headers, occurred_at_ms, enqueued_at_ms,
       attempts, last_error, next_attempt_at_ms, x_sb_trace
  FROM event_outbox
  WHERE status='pending' AND next_attempt_at_ms <= ?
  ORDER BY enqueued_at_ms ASC, id ASC
  LIMIT ?`

const insertSQL = `INSERT INTO event_outbox
  (id, name, payload, payload_json, contract_hash, partition_key, idempotency_key,
   fire_and_forget, headers, occurred_at_ms, enqueued_at_ms, status,
   attempts, last_error, next_attempt_at_ms, x_sb_trace)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`

// claimSQL and deleteSQL take the whole id set as one JSON array, so a batch of
// any size reuses one prepared statement instead of compiling a fresh IN-list.
const claimSQL = `UPDATE event_outbox SET status='inflight'
  WHERE id IN (SELECT value FROM json_each(?))`

const deleteSQL = `DELETE FROM event_outbox
  WHERE id IN (SELECT value FROM json_each(?))`

const retrySQL = `UPDATE event_outbox
  SET status='pending', attempts=?, last_error=?, next_attempt_at_ms=?
  WHERE id=?`

const failSQL = `UPDATE event_outbox
  SET status='failed', attempts=?, last_error=?
  WHERE id=?`

const nextDueSQL = `SELECT MIN(next_attempt_at_ms) FROM event_outbox WHERE status='pending'`

// Open creates or opens the outbox database, enforces the schema version and
// returns rows left inflight by a killed process to pending.
func Open(ctx context.Context, cfg Config) (*Storage, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("outbox: open: empty Dir: %w", ErrInvalidConfig)
	}
	name := cfg.FileName
	if name == "" {
		name = DefaultFileName
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("outbox: open: create %s: %w", cfg.Dir, err)
	}
	path := filepath.Join(cfg.Dir, name)

	// WAL keeps a drain's writes from blocking a concurrent publish read;
	// synchronous=NORMAL is the WAL-safe setting that survives a process crash
	// (only a machine power loss can lose the last commits).
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("outbox: open %s: %w", path, err)
	}
	// One writer, one connection: SQLite serializes writers anyway, and a
	// single long-lived connection keeps the pragmas applied for the whole
	// lifetime of the file handle.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	s := &Storage{db: db, path: path}
	if err := s.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Storage) init(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("outbox: open %s: connect: %w", s.path, err)
	}
	if err := s.checkVersion(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("outbox: open %s: create schema: %w", s.path, err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", SchemaVersion)); err != nil {
		return fmt.Errorf("outbox: open %s: stamp version: %w", s.path, err)
	}
	// Crash recovery: a killed process leaves its claimed batch inflight. Those
	// rows were never confirmed by the runtime, so they go back to pending.
	if _, err := s.db.ExecContext(ctx,
		`UPDATE event_outbox SET status=? WHERE status=?`, StatusPending, StatusInflight); err != nil {
		return fmt.Errorf("outbox: open %s: recover inflight: %w", s.path, err)
	}
	if err := s.countRows(ctx); err != nil {
		return err
	}
	return s.prepareAll(ctx)
}

// checkVersion rejects a file another schema wrote. Migrating it would mean
// carrying every past shape of the table forever; the file is a local buffer,
// not a system of record, so it is deleted instead.
func (s *Storage) checkVersion(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("outbox: open %s: read version: %w", s.path, err)
	}
	if version == SchemaVersion {
		return nil
	}
	if version == 0 {
		var name string
		err := s.db.QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name='event_outbox'`).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("outbox: open %s: inspect schema: %w", s.path, err)
		}
	}
	return &SchemaVersionError{Path: s.path, Found: version, Want: SchemaVersion}
}

func (s *Storage) countRows(ctx context.Context) error {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_outbox`).Scan(&n); err != nil {
		return fmt.Errorf("outbox: open %s: count rows: %w", s.path, err)
	}
	s.rows = n
	return nil
}

func (s *Storage) prepareAll(ctx context.Context) error {
	targets := []struct {
		dst **sql.Stmt
		sql string
	}{
		{&s.insertStmt, insertSQL},
		{&s.selectStmt, selectDueSQL},
		{&s.claimStmt, claimSQL},
		{&s.deleteStmt, deleteSQL},
		{&s.retryStmt, retrySQL},
		{&s.failStmt, failSQL},
		{&s.nextDueStmt, nextDueSQL},
	}
	for _, t := range targets {
		stmt, err := s.db.PrepareContext(ctx, t.sql)
		if err != nil {
			return fmt.Errorf("outbox: open %s: prepare: %w", s.path, err)
		}
		*t.dst = stmt
	}
	return nil
}

// Path returns the database file.
func (s *Storage) Path() string { return s.path }

// Rows returns the buffered row count without touching the database: the cap
// check runs on every publish and COUNT(*) is O(rows).
func (s *Storage) Rows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows
}

// Enqueue appends one record. maxRows caps the buffer; zero means unlimited.
// The cap check and the insert share one transaction and one lock, so
// concurrent publishes cannot both slip past a full outbox.
func (s *Storage) Enqueue(ctx context.Context, rec Record, maxRows int) error {
	headers, err := json.Marshal(headersOrEmpty(rec.Headers))
	if err != nil {
		return fmt.Errorf("outbox: enqueue %s: encode headers: %w", rec.ID, err)
	}

	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()

	if maxRows > 0 && s.Rows() >= maxRows {
		return fmt.Errorf("outbox: enqueue %s: %d rows at cap %d: %w", rec.ID, s.Rows(), maxRows, ErrFull)
	}

	err = s.tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.StmtContext(ctx, s.insertStmt).ExecContext(ctx,
			rec.ID, rec.Name, rec.Payload, rec.PayloadJSON, rec.ContractHash,
			rec.PartitionKey, rec.IdempotencyKey, boolToInt(rec.FireAndForget),
			string(headers), rec.OccurredAtMs, rec.EnqueuedAtMs,
			rec.Attempts, rec.LastError, rec.NextAttemptAtMs, rec.Trace)
		return err
	})
	if err != nil {
		return fmt.Errorf("outbox: enqueue %s: %w", rec.ID, err)
	}
	s.addRows(1)
	return nil
}

// ClaimDue takes up to limit rows due at nowMs and marks them inflight in the
// same transaction, so a crash between the read and the send is recoverable.
func (s *Storage) ClaimDue(ctx context.Context, nowMs int64, limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("outbox: claim due: limit %d must be positive: %w", limit, ErrInvalidConfig)
	}
	var claimed []Record
	err := s.tx(ctx, func(tx *sql.Tx) error {
		recs, err := scanRecords(ctx, tx.StmtContext(ctx, s.selectStmt), nowMs, limit)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		ids, err := idListJSON(recordIDs(recs))
		if err != nil {
			return err
		}
		if _, err := tx.StmtContext(ctx, s.claimStmt).ExecContext(ctx, ids); err != nil {
			return err
		}
		claimed = recs
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: claim due: %w", err)
	}
	return claimed, nil
}

// Complete applies one batch outcome in a single transaction: successes leave
// in one statement, retried rows are re-armed, terminal rejections are parked.
// A row-per-transaction loop would append a WAL commit record per event.
func (s *Storage) Complete(ctx context.Context, res Result) error {
	if len(res.Done) == 0 && len(res.Retry) == 0 && len(res.Failed) == 0 {
		return nil
	}
	err := s.tx(ctx, func(tx *sql.Tx) error {
		if len(res.Done) > 0 {
			ids, err := idListJSON(res.Done)
			if err != nil {
				return err
			}
			if _, err := tx.StmtContext(ctx, s.deleteStmt).ExecContext(ctx, ids); err != nil {
				return err
			}
		}
		retry := tx.StmtContext(ctx, s.retryStmt)
		for _, r := range res.Retry {
			if _, err := retry.ExecContext(ctx, r.Attempts, r.LastError, r.NextAttemptAtMs, r.ID); err != nil {
				return err
			}
		}
		fail := tx.StmtContext(ctx, s.failStmt)
		for _, f := range res.Failed {
			if _, err := fail.ExecContext(ctx, f.Attempts, f.LastError, f.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("outbox: complete batch: %w", err)
	}
	s.addRows(-len(res.Done))
	return nil
}

// NextDueAt returns when the earliest pending row becomes claimable. The second
// result is false when nothing is pending, which lets an idle drain park on its
// wake signal alone instead of polling.
func (s *Storage) NextDueAt(ctx context.Context) (int64, bool, error) {
	if s.isClosed() {
		return 0, false, fmt.Errorf("outbox: next due: %w", ErrClosed)
	}
	var at sql.NullInt64
	if err := s.nextDueStmt.QueryRowContext(ctx).Scan(&at); err != nil {
		return 0, false, fmt.Errorf("outbox: next due: %w", err)
	}
	if !at.Valid {
		return 0, false, nil
	}
	return at.Int64, true, nil
}

// Load returns one row by id regardless of status. Used to inspect state; the
// drain path never reads a single row.
func (s *Storage) Load(ctx context.Context, id string) (Record, string, error) {
	if s.isClosed() {
		return Record{}, "", fmt.Errorf("outbox: load %s: %w", id, ErrClosed)
	}
	const q = `SELECT id, name, payload, payload_json, contract_hash, partition_key,
       idempotency_key, fire_and_forget, headers, occurred_at_ms, enqueued_at_ms,
       attempts, last_error, next_attempt_at_ms, x_sb_trace, status
  FROM event_outbox WHERE id = ?`
	row := s.db.QueryRowContext(ctx, q, id)
	var (
		rec           Record
		headers       string
		fireAndForget int
		status        string
	)
	err := row.Scan(&rec.ID, &rec.Name, &rec.Payload, &rec.PayloadJSON, &rec.ContractHash,
		&rec.PartitionKey, &rec.IdempotencyKey, &fireAndForget, &headers, &rec.OccurredAtMs,
		&rec.EnqueuedAtMs, &rec.Attempts, &rec.LastError, &rec.NextAttemptAtMs, &rec.Trace, &status)
	if err != nil {
		return Record{}, "", fmt.Errorf("outbox: load %s: %w", id, err)
	}
	rec.FireAndForget = fireAndForget != 0
	if err := json.Unmarshal([]byte(headers), &rec.Headers); err != nil {
		return Record{}, "", fmt.Errorf("outbox: load %s: decode headers: %w", id, err)
	}
	return rec, status, nil
}

// CountByStatus reports how many rows sit in one status.
func (s *Storage) CountByStatus(ctx context.Context, status string) (int, error) {
	if s.isClosed() {
		return 0, fmt.Errorf("outbox: count %s: %w", status, ErrClosed)
	}
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_outbox WHERE status = ?`, status).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("outbox: count %s: %w", status, err)
	}
	return n, nil
}

// Close releases the prepared statements and the file handle. Idempotent.
func (s *Storage) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	for _, stmt := range []*sql.Stmt{
		s.insertStmt, s.selectStmt, s.claimStmt,
		s.deleteStmt, s.retryStmt, s.failStmt, s.nextDueStmt,
	} {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("outbox: close %s: %w", s.path, err)
	}
	return nil
}

// tx runs fn in one transaction. Every write in this package goes through it,
// which is what makes commits a truthful count of transactions.
func (s *Storage) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	if s.isClosed() {
		return ErrClosed
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	s.mu.Lock()
	s.commits++
	s.mu.Unlock()
	return nil
}

func (s *Storage) addRows(delta int) {
	s.mu.Lock()
	s.rows += delta
	s.mu.Unlock()
}

func (s *Storage) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// commitCount reports how many transactions this storage committed. Test-only
// hook: it is the only way to assert that a batch costs exactly one commit.
//
// @internal
func (s *Storage) commitCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

// queryPlan returns the EXPLAIN QUERY PLAN detail lines for a statement.
// Test-only hook: the drain query's cost hinges on the plan holding no sort.
//
// @internal
func (s *Storage) queryPlan(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		return nil, fmt.Errorf("outbox: query plan: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var plan []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			return nil, fmt.Errorf("outbox: query plan: scan: %w", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: query plan: %w", err)
	}
	return plan, nil
}

func scanRecords(ctx context.Context, stmt *sql.Stmt, nowMs int64, limit int) ([]Record, error) {
	rows, err := stmt.QueryContext(ctx, nowMs, limit)
	if err != nil {
		return nil, fmt.Errorf("select due: %w", err)
	}
	defer func() { _ = rows.Close() }()

	recs := make([]Record, 0, limit)
	for rows.Next() {
		var (
			rec           Record
			headers       string
			fireAndForget int
		)
		err := rows.Scan(&rec.ID, &rec.Name, &rec.Payload, &rec.PayloadJSON, &rec.ContractHash,
			&rec.PartitionKey, &rec.IdempotencyKey, &fireAndForget, &headers,
			&rec.OccurredAtMs, &rec.EnqueuedAtMs, &rec.Attempts, &rec.LastError,
			&rec.NextAttemptAtMs, &rec.Trace)
		if err != nil {
			return nil, fmt.Errorf("select due: scan: %w", err)
		}
		rec.FireAndForget = fireAndForget != 0
		// The writer is this package and it always stores a marshalled map, so
		// unparseable headers mean the file was corrupted from outside.
		if err := json.Unmarshal([]byte(headers), &rec.Headers); err != nil {
			return nil, fmt.Errorf("select due: decode headers of %s: %w", rec.ID, err)
		}
		recs = append(recs, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select due: %w", err)
	}
	return recs, nil
}

func recordIDs(recs []Record) []string {
	ids := make([]string, len(recs))
	for i, r := range recs {
		ids[i] = r.ID
	}
	return ids
}

func idListJSON(ids []string) (string, error) {
	b, err := json.Marshal(ids)
	if err != nil {
		return "", fmt.Errorf("encode id list: %w", err)
	}
	return string(b), nil
}

func headersOrEmpty(h map[string]string) map[string]string {
	if h == nil {
		return map[string]string{}
	}
	return h
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
