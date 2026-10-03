package pgq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewmueller/jobq"
	"github.com/matthewmueller/jobq/internal/backoff"
	"github.com/matthewmueller/jobq/internal/sqlq"
)

type Payload = jobq.Payload
type Job[T Payload] = jobq.Job[T]
type Handler[T Payload] = jobq.Handler[T]
type Stats = jobq.Stats

// Laned is implemented by payloads that run one at a time per lane. Jobs in
// the same lane never run at the same time, even across processes. Jobs in
// different lanes, and jobs without a lane, run concurrently. Jobs usually run
// in the order they were pushed, but a job waiting to retry doesn't hold up
// the rest of its lane.
type Laned = sqlq.Laned

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

// schema is idempotent. The advisory lock serializes concurrent Dials, since
// CREATE ... IF NOT EXISTS can race across processes. The lane index allows
// one running job per lane. The trigger notifies listening workers whenever a
// job becomes pending, on commit.
const schema = `
SELECT pg_advisory_xact_lock(hashtext('pgq_jobs'));
CREATE TABLE IF NOT EXISTS pgq_jobs (
	id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	queue        text        NOT NULL,
	payload      jsonb       NOT NULL,
	lane         text,
	state        text        NOT NULL DEFAULT 'pending',
	attempts     int         NOT NULL DEFAULT 0,
	max_retries  int         NOT NULL DEFAULT 0,
	last_error   text,
	run_at       timestamptz NOT NULL DEFAULT now(),
	locked_until timestamptz,
	created_at   timestamptz NOT NULL DEFAULT now(),
	updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pgq_jobs_pending ON pgq_jobs (queue, run_at, id) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS pgq_jobs_running ON pgq_jobs (queue, locked_until) WHERE state = 'running';
-- Added in v0.0.4
ALTER TABLE pgq_jobs ADD COLUMN IF NOT EXISTS lane text;
CREATE UNIQUE INDEX IF NOT EXISTS pgq_jobs_lane ON pgq_jobs (queue, lane) WHERE state = 'running' AND lane IS NOT NULL;
CREATE OR REPLACE FUNCTION pgq_notify() RETURNS trigger AS $$
BEGIN
	PERFORM pg_notify('pgq_jobs', NEW.queue);
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER pgq_jobs_notify
	AFTER INSERT OR UPDATE OF state ON pgq_jobs
	FOR EACH ROW WHEN (NEW.state = 'pending')
	EXECUTE FUNCTION pgq_notify();
`

// Dial connects to PostgreSQL and creates the jobs table if needed
func Dial(ctx context.Context, log *slog.Logger, url string) (*Queues, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("pgq: unable to connect: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgq: unable to create schema: %w", err)
	}
	log = log.With("package", "pgq")
	return &Queues{
		pool: pool,
		log:  log,
		// Notifications wake idle workers, so polling is only a fallback
		sqlq: sqlq.New("pgq", &store{pool}, log, 5*time.Second),
	}, nil
}

// Queues produces and consumes jobs backed by PostgreSQL
type Queues struct {
	pool *pgxpool.Pool
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
	return insert(ctx, q.pool, payload)
}

// PushTx enqueues the payload within tx, so the job only exists if tx commits
func (q *Queues) PushTx[T Payload](ctx context.Context, tx pgx.Tx, payload T) error {
	return insert(ctx, tx, payload)
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func insert(ctx context.Context, db execer, payload Payload) error {
	queue := payload.Queue()
	if queue == "" {
		return fmt.Errorf("pgq: %T has an empty queue name", payload)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pgq: unable to encode %q payload: %w", queue, err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO pgq_jobs (queue, payload, lane) VALUES ($1, $2, $3)`, queue, data, sqlq.Lane(payload)); err != nil {
		return fmt.Errorf("pgq: unable to push to %q: %w", queue, err)
	}
	return nil
}

// Revive moves the queue's failed jobs back to pending with fresh attempts
func (q *Queues) Revive(ctx context.Context, queue string) error {
	tag, err := q.pool.Exec(ctx, `
		UPDATE pgq_jobs SET state = 'pending', attempts = 0, run_at = now(), last_error = NULL, updated_at = now()
		WHERE queue = $1 AND state = 'failed'
	`, queue)
	if err != nil {
		return fmt.Errorf("pgq: unable to revive %q: %w", queue, err)
	}
	q.log.Info("revived failed jobs", "queue", queue, "count", tag.RowsAffected())
	return nil
}

// Stats counts the queue's pending, running and failed jobs
func (q *Queues) Stats(ctx context.Context, queue string) (*Stats, error) {
	rows, err := q.pool.Query(ctx, `SELECT state, count(*) FROM pgq_jobs WHERE queue = $1 GROUP BY state`, queue)
	if err != nil {
		return nil, fmt.Errorf("pgq: unable to get stats for %q: %w", queue, err)
	}
	defer rows.Close()
	stats := new(Stats)
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("pgq: unable to scan stats for %q: %w", queue, err)
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
		return nil, fmt.Errorf("pgq: unable to get stats for %q: %w", queue, err)
	}
	return stats, nil
}

// Start processes jobs for all registered queues. It blocks until ctx is
// cancelled and running handlers have returned. Database errors are logged
// and retried rather than returned.
func (q *Queues) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { q.listen(ctx) })
	err := q.sqlq.Start(ctx)
	cancel()
	wg.Wait()
	return err
}

// listen wakes idle workers when jobs become pending, reconnecting until ctx
// is cancelled. Workers fall back to polling while it's disconnected.
func (q *Queues) listen(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		err := q.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		failures++
		q.log.Warn("listener disconnected, polling until it reconnects", "error", err)
		sleep(ctx, min(backoff.Delay(failures), 30*time.Second))
	}
}

// listenOnce listens on a dedicated connection, since LISTEN is tied to the
// session and pooled connections are shared
func (q *Queues) listenOnce(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, q.pool.Config().ConnConfig)
	if err != nil {
		return fmt.Errorf("pgq: unable to connect listener: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	if _, err := conn.Exec(ctx, "LISTEN pgq_jobs"); err != nil {
		return fmt.Errorf("pgq: unable to listen: %w", err)
	}
	// Catch up on anything pushed while we weren't listening
	q.sqlq.NotifyAll()
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("pgq: unable to wait for notifications: %w", err)
		}
		q.sqlq.Notify(notification.Payload)
	}
}

// sleep waits for d or until ctx is cancelled
func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// Close releases the database connections
func (q *Queues) Close() error {
	q.pool.Close()
	return nil
}

// store implements sqlq.Store for PostgreSQL. Competing workers skip
// each other's locked rows when claiming.
type store struct {
	pool *pgxpool.Pool
}

var _ sqlq.Store = (*store)(nil)

// Claim skips jobs whose lane already has a running job, including one whose
// lease expired, so it's reclaimed before the rest of its lane runs. Two
// workers can still claim jobs in the same lane at once. The lane index fails
// the second, which then claims again and skips that lane.
func (s *store) Claim(ctx context.Context, queue string, retries int, lease time.Duration) (*sqlq.Job, error) {
	for {
		j, err := s.claim(ctx, queue, retries, lease)
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
			continue
		}
		return j, err
	}
}

func (s *store) claim(ctx context.Context, queue string, retries int, lease time.Duration) (*sqlq.Job, error) {
	j := new(sqlq.Job)
	err := s.pool.QueryRow(ctx, `
		UPDATE pgq_jobs SET
			state = 'running',
			attempts = attempts + 1,
			max_retries = $2,
			locked_until = now() + $3 * interval '1 millisecond',
			updated_at = now()
		WHERE id = (
			SELECT id FROM pgq_jobs j
			WHERE queue = $1 AND (
				(state = 'pending' AND run_at <= now()) OR
				(state = 'running' AND locked_until < now())
			) AND (lane IS NULL OR NOT EXISTS (
				SELECT 1 FROM pgq_jobs r
				WHERE r.queue = j.queue AND r.lane = j.lane AND r.state = 'running' AND r.id <> j.id
			))
			ORDER BY run_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, payload, attempts, created_at
	`, queue, retries, lease.Milliseconds()).Scan(&j.ID, &j.Payload, &j.Attempt, &j.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("pgq: unable to claim from %q: %w", queue, err)
	}
	return j, nil
}

func (s *store) Extend(ctx context.Context, j *sqlq.Job, lease time.Duration) error {
	return s.update(ctx, j, `
		UPDATE pgq_jobs SET locked_until = now() + $3 * interval '1 millisecond', updated_at = now()
		WHERE id = $1 AND attempts = $2 AND state = 'running'
	`, lease.Milliseconds())
}

func (s *store) Complete(ctx context.Context, j *sqlq.Job) error {
	return s.update(ctx, j, `
		UPDATE pgq_jobs SET state = 'completed', locked_until = NULL, updated_at = now()
		WHERE id = $1 AND attempts = $2 AND state = 'running'
	`)
}

func (s *store) Release(ctx context.Context, j *sqlq.Job) error {
	return s.update(ctx, j, `
		UPDATE pgq_jobs SET state = 'pending', attempts = attempts - 1, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND attempts = $2 AND state = 'running'
	`)
}

func (s *store) Retry(ctx context.Context, j *sqlq.Job, delay time.Duration, err error) error {
	return s.update(ctx, j, `
		UPDATE pgq_jobs SET
			state = 'pending',
			run_at = now() + $4 * interval '1 millisecond',
			last_error = $3,
			locked_until = NULL,
			updated_at = now()
		WHERE id = $1 AND attempts = $2 AND state = 'running'
	`, err.Error(), delay.Milliseconds())
}

func (s *store) Fail(ctx context.Context, j *sqlq.Job, err error) error {
	return s.update(ctx, j, `
		UPDATE pgq_jobs SET state = 'failed', last_error = $3, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND attempts = $2 AND state = 'running'
	`, err.Error())
}

// update runs sql with the job's ID and attempt as $1 and $2
func (s *store) update(ctx context.Context, j *sqlq.Job, sql string, args ...any) error {
	if _, err := s.pool.Exec(ctx, sql, append([]any{j.ID, j.Attempt}, args...)...); err != nil {
		return fmt.Errorf("pgq: unable to update job %d: %w", j.ID, err)
	}
	return nil
}
