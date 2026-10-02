package sqq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/matthewmueller/jobq"
	"github.com/matthewmueller/jobq/internal/sqlq"
	_ "modernc.org/sqlite"
)

type Payload = jobq.Payload
type Job[T Payload] = jobq.Job[T]
type Handler[T Payload] = jobq.Handler[T]
type Stats = jobq.Stats

// Config configures how a registered queue is consumed
type Config struct {
	config *sqlq.Config
}

// Concurrency sets the maximum number of jobs this process runs at once for
// the queue. Defaults to 1.
func (c *Config) Concurrency(n int) *Config {
	c.config.Concurrency(n)
	return c
}

// Retries sets how many times a failed job is retried. Defaults to 0.
func (c *Config) Retries(n int) *Config {
	c.config.Retries(n)
	return c
}

// Timeout bounds how long a handler may run before its context is cancelled
// and the attempt fails. Defaults to no timeout.
func (c *Config) Timeout(d time.Duration) *Config {
	c.config.Timeout(d)
	return c
}

// Permanent marks err as non-retryable, so the job fails immediately
func Permanent(err error) error {
	return jobq.Permanent(err)
}

// schema is idempotent. Times are Unix milliseconds.
const schema = `
CREATE TABLE IF NOT EXISTS sqq_jobs (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	queue        TEXT    NOT NULL,
	payload      TEXT    NOT NULL,
	state        TEXT    NOT NULL DEFAULT 'pending',
	attempts     INTEGER NOT NULL DEFAULT 0,
	max_retries  INTEGER NOT NULL DEFAULT 0,
	last_error   TEXT,
	run_at       INTEGER NOT NULL,
	locked_until INTEGER,
	created_at   INTEGER NOT NULL,
	updated_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS sqq_jobs_pending ON sqq_jobs (queue, run_at, id) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS sqq_jobs_running ON sqq_jobs (queue, locked_until) WHERE state = 'running';
`

// Dial opens the SQLite database at path (a file path or file: URI) and
// creates the jobs table if needed. Processes sharing the database must run
// on the same host, and the file must not be on a network filesystem.
func Dial(ctx context.Context, log *slog.Logger, path string) (*Queues, error) {
	if strings.Contains(path, ":memory:") || strings.Contains(path, "mode=memory") {
		return nil, fmt.Errorf("sqq: in-memory databases aren't supported because each connection would get its own database")
	}
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("sqq: unable to open %q: %w", path, err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqq: unable to create schema: %w", err)
	}
	log = log.With("package", "sqq")
	return &Queues{
		db:   db,
		log:  log,
		sqlq: sqlq.New("sqq", &store{db}, log, 500*time.Millisecond),
	}, nil
}

// dsn configures each connection for concurrent use: WAL so readers don't
// block the writer, a busy timeout so writers wait for the lock instead of
// failing, and immediate transactions so a transaction never has to upgrade
// from a read lock to a write lock, which can deadlock.
func dsn(path string) string {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"
}

// Queues produces and consumes jobs backed by SQLite
type Queues struct {
	db   *sql.DB
	log  *slog.Logger
	sqlq *sqlq.Worker
}

// Queue registers a handler for the queue named by T. Register handlers
// before calling Start.
func (q *Queues) Queue[T Payload](handler Handler[T]) *Config {
	return &Config{q.sqlq.Register(handler)}
}

// Push enqueues the payload onto the queue it names
func (q *Queues) Push[T Payload](ctx context.Context, payload T) error {
	if err := insert(ctx, q.db, payload); err != nil {
		return err
	}
	// SQLite can't notify other processes, but this process's idle workers
	// can start right away. Other processes find the job when they poll.
	q.sqlq.Notify(payload.Queue())
	return nil
}

// PushTx enqueues the payload within tx, so the job only exists if tx commits
func (q *Queues) PushTx[T Payload](ctx context.Context, tx *sql.Tx, payload T) error {
	return insert(ctx, tx, payload)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insert(ctx context.Context, db execer, payload Payload) error {
	queue := payload.Queue()
	if queue == "" {
		return fmt.Errorf("sqq: %T has an empty queue name", payload)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("sqq: unable to encode %q payload: %w", queue, err)
	}
	now := time.Now().UnixMilli()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sqq_jobs (queue, payload, run_at, created_at, updated_at) VALUES (?1, ?2, ?3, ?3, ?3)
	`, queue, string(data), now); err != nil {
		return fmt.Errorf("sqq: unable to push to %q: %w", queue, err)
	}
	return nil
}

// Revive moves the queue's failed jobs back to pending with fresh attempts
func (q *Queues) Revive(ctx context.Context, queue string) error {
	res, err := q.db.ExecContext(ctx, `
		UPDATE sqq_jobs SET state = 'pending', attempts = 0, run_at = ?2, last_error = NULL, updated_at = ?2
		WHERE queue = ?1 AND state = 'failed'
	`, queue, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("sqq: unable to revive %q: %w", queue, err)
	}
	count, _ := res.RowsAffected()
	q.log.Info("revived failed jobs", "queue", queue, "count", count)
	q.sqlq.Notify(queue)
	return nil
}

// Stats counts the queue's pending, running and failed jobs
func (q *Queues) Stats(ctx context.Context, queue string) (*Stats, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT state, count(*) FROM sqq_jobs WHERE queue = ? GROUP BY state`, queue)
	if err != nil {
		return nil, fmt.Errorf("sqq: unable to get stats for %q: %w", queue, err)
	}
	defer rows.Close()
	stats := new(Stats)
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("sqq: unable to scan stats for %q: %w", queue, err)
		}
		switch state {
		case "pending":
			stats.Pending = count
		case "running":
			stats.Running = count
		case "failed":
			stats.Failed = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqq: unable to get stats for %q: %w", queue, err)
	}
	return stats, nil
}

// Start processes jobs for all registered queues. It blocks until ctx is
// cancelled and running handlers have returned. Database errors are logged
// and retried rather than returned.
func (q *Queues) Start(ctx context.Context) error {
	return q.sqlq.Start(ctx)
}

// Close closes the database
func (q *Queues) Close() error {
	return q.db.Close()
}

// store implements sqlq.Store for SQLite. SQLite runs one writer at a
// time and a write statement takes the write lock before it reads, so a
// single UPDATE can't hand the same job to two workers, even across
// processes.
type store struct {
	db *sql.DB
}

var _ sqlq.Store = (*store)(nil)

func (s *store) Claim(ctx context.Context, queue string, retries int, lease time.Duration) (*sqlq.Job, error) {
	now := time.Now()
	j := new(sqlq.Job)
	var createdAt int64
	err := s.db.QueryRowContext(ctx, `
		UPDATE sqq_jobs SET
			state = 'running',
			attempts = attempts + 1,
			max_retries = ?2,
			locked_until = ?3,
			updated_at = ?4
		WHERE id = (
			SELECT id FROM sqq_jobs
			WHERE queue = ?1 AND (
				(state = 'pending' AND run_at <= ?4) OR
				(state = 'running' AND locked_until < ?4)
			)
			ORDER BY run_at, id
			LIMIT 1
		)
		RETURNING id, payload, attempts, created_at
	`, queue, retries, now.Add(lease).UnixMilli(), now.UnixMilli()).Scan(&j.ID, &j.Payload, &j.Attempt, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("sqq: unable to claim from %q: %w", queue, err)
	}
	j.CreatedAt = time.UnixMilli(createdAt)
	return j, nil
}

func (s *store) Extend(ctx context.Context, j *sqlq.Job, lease time.Duration) error {
	now := time.Now()
	return s.update(ctx, j, `
		UPDATE sqq_jobs SET locked_until = ?3, updated_at = ?4
		WHERE id = ?1 AND attempts = ?2 AND state = 'running'
	`, now.Add(lease).UnixMilli(), now.UnixMilli())
}

func (s *store) Complete(ctx context.Context, j *sqlq.Job) error {
	return s.update(ctx, j, `
		UPDATE sqq_jobs SET state = 'completed', locked_until = NULL, updated_at = ?3
		WHERE id = ?1 AND attempts = ?2 AND state = 'running'
	`, time.Now().UnixMilli())
}

func (s *store) Release(ctx context.Context, j *sqlq.Job) error {
	return s.update(ctx, j, `
		UPDATE sqq_jobs SET state = 'pending', attempts = attempts - 1, locked_until = NULL, updated_at = ?3
		WHERE id = ?1 AND attempts = ?2 AND state = 'running'
	`, time.Now().UnixMilli())
}

func (s *store) Retry(ctx context.Context, j *sqlq.Job, delay time.Duration, err error) error {
	now := time.Now()
	return s.update(ctx, j, `
		UPDATE sqq_jobs SET
			state = 'pending',
			run_at = ?4,
			last_error = ?3,
			locked_until = NULL,
			updated_at = ?5
		WHERE id = ?1 AND attempts = ?2 AND state = 'running'
	`, err.Error(), now.Add(delay).UnixMilli(), now.UnixMilli())
}

func (s *store) Fail(ctx context.Context, j *sqlq.Job, err error) error {
	return s.update(ctx, j, `
		UPDATE sqq_jobs SET state = 'failed', last_error = ?3, locked_until = NULL, updated_at = ?4
		WHERE id = ?1 AND attempts = ?2 AND state = 'running'
	`, err.Error(), time.Now().UnixMilli())
}

// update runs query with the job's ID and attempt as ?1 and ?2
func (s *store) update(ctx context.Context, j *sqlq.Job, query string, args ...any) error {
	if _, err := s.db.ExecContext(ctx, query, append([]any{j.ID, j.Attempt}, args...)...); err != nil {
		return fmt.Errorf("sqq: unable to update job %d: %w", j.ID, err)
	}
	return nil
}
