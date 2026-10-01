package pgq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewmueller/jobq"
	"github.com/matthewmueller/jobq/internal/backoff"
)

type Payload = jobq.Payload
type Job[T Payload] = jobq.Job[T]
type Handler[T Payload] = jobq.Handler[T]
type Stats = jobq.Stats

// Permanent marks err as non-retryable, so the job fails immediately
func Permanent(err error) error {
	return jobq.Permanent(err)
}

// schema is idempotent. The advisory lock serializes concurrent Dials, since
// CREATE ... IF NOT EXISTS can race across processes.
const schema = `
SELECT pg_advisory_xact_lock(hashtext('pgq_jobs'));
CREATE TABLE IF NOT EXISTS pgq_jobs (
	id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	queue        text        NOT NULL,
	payload      jsonb       NOT NULL,
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
	return &Queues{
		pool:  pool,
		log:   log.With("package", "pgq"),
		lease: 30 * time.Second,
		poll:  500 * time.Millisecond,
	}, nil
}

// Queues produces and consumes jobs backed by PostgreSQL
type Queues struct {
	pool    *pgxpool.Pool
	log     *slog.Logger
	lease   time.Duration // how long a claimed job is reserved before others may reclaim it
	poll    time.Duration // how long an idle worker waits before checking for jobs again
	configs []*Config
}

// Config configures how a registered queue is consumed
type Config struct {
	queue       string
	concurrency int
	retries     int
	timeout     time.Duration
	handle      func(ctx context.Context, j *job) error
}

// Concurrency sets the maximum number of jobs this process runs at once for
// the queue. Defaults to 1.
func (c *Config) Concurrency(n int) *Config {
	c.concurrency = n
	return c
}

// Retries sets how many times a failed job is retried. Defaults to 0.
func (c *Config) Retries(n int) *Config {
	c.retries = n
	return c
}

// Timeout bounds how long a handler may run before its context is cancelled
// and the attempt fails. Defaults to no timeout.
func (c *Config) Timeout(d time.Duration) *Config {
	c.timeout = d
	return c
}

// job is the untyped persisted job
type job struct {
	id        int64
	payload   []byte
	attempt   int
	createdAt time.Time
}

// Queue registers a handler for the queue named by T. Register handlers
// before calling Start.
func (q *Queues) Queue[T Payload](handler Handler[T]) *Config {
	var zero T
	config := &Config{
		queue:       zero.Queue(),
		concurrency: 1,
		handle: func(ctx context.Context, j *job) error {
			var data T
			if err := json.Unmarshal(j.payload, &data); err != nil {
				return jobq.Permanent(fmt.Errorf("pgq: unable to decode job %d: %w", j.id, err))
			}
			return handler(ctx, &Job[T]{
				ID:        strconv.FormatInt(j.id, 10),
				Data:      data,
				Attempt:   j.attempt,
				CreatedAt: j.createdAt,
			})
		},
	}
	q.configs = append(q.configs, config)
	return config
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
	if _, err := db.Exec(ctx, `INSERT INTO pgq_jobs (queue, payload) VALUES ($1, $2)`, queue, data); err != nil {
		return fmt.Errorf("pgq: unable to push to %q: %w", queue, err)
	}
	return nil
}

// Redrive moves the queue's failed jobs back to pending with fresh attempts
func (q *Queues) Redrive(ctx context.Context, queue string) error {
	tag, err := q.pool.Exec(ctx, `
		UPDATE pgq_jobs SET state = 'pending', attempts = 0, run_at = now(), last_error = NULL, updated_at = now()
		WHERE queue = $1 AND state = 'failed'
	`, queue)
	if err != nil {
		return fmt.Errorf("pgq: unable to redrive %q: %w", queue, err)
	}
	q.log.Info("redrove failed jobs", "queue", queue, "count", tag.RowsAffected())
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
	if err := q.validate(); err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, config := range q.configs {
		for range config.concurrency {
			wg.Go(func() { q.work(ctx, config) })
		}
	}
	wg.Wait()
	return nil
}

// Close releases the database connections
func (q *Queues) Close() error {
	q.pool.Close()
	return nil
}

func (q *Queues) validate() error {
	var errs []error
	seen := map[string]bool{}
	for _, c := range q.configs {
		if c.queue == "" {
			errs = append(errs, fmt.Errorf("pgq: queue name must not be empty"))
		}
		if c.concurrency < 1 {
			errs = append(errs, fmt.Errorf("pgq: %q concurrency must be at least 1", c.queue))
		}
		if c.retries < 0 {
			errs = append(errs, fmt.Errorf("pgq: %q retries must not be negative", c.queue))
		}
		if c.timeout < 0 {
			errs = append(errs, fmt.Errorf("pgq: %q timeout must not be negative", c.queue))
		}
		if seen[c.queue] {
			errs = append(errs, fmt.Errorf("pgq: %q is registered more than once", c.queue))
		}
		seen[c.queue] = true
	}
	return errors.Join(errs...)
}

// work claims and runs jobs until ctx is cancelled. Database errors back off
// and retry so a blip doesn't stop the worker.
func (q *Queues) work(ctx context.Context, c *Config) {
	failures := 0
	for ctx.Err() == nil {
		j, err := q.claim(ctx, c)
		if err == nil && j != nil {
			err = q.run(ctx, c, j)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			q.log.Error("worker error", "queue", c.queue, "error", err)
			sleep(ctx, min(backoff.Delay(failures), 30*time.Second))
			continue
		}
		failures = 0
		if j == nil {
			sleep(ctx, q.poll)
		}
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

// claim reserves the next available job, or returns nil if there are none.
// Jobs whose lease expired (e.g. their worker crashed) are reclaimed.
func (q *Queues) claim(ctx context.Context, c *Config) (*job, error) {
	j := new(job)
	err := q.pool.QueryRow(ctx, `
		UPDATE pgq_jobs SET
			state = 'running',
			attempts = attempts + 1,
			max_retries = $2,
			locked_until = now() + $3 * interval '1 millisecond',
			updated_at = now()
		WHERE id = (
			SELECT id FROM pgq_jobs
			WHERE queue = $1 AND (
				(state = 'pending' AND run_at <= now()) OR
				(state = 'running' AND locked_until < now())
			)
			ORDER BY run_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, payload, attempts, created_at
	`, c.queue, c.retries, q.lease.Milliseconds()).Scan(&j.id, &j.payload, &j.attempt, &j.createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("pgq: unable to claim from %q: %w", c.queue, err)
	}
	return j, nil
}

// run calls the handler and records the outcome
func (q *Queues) run(ctx context.Context, c *Config, j *job) error {
	// A reclaimed job that already used up its attempts (e.g. it keeps
	// crashing the process) is failed rather than run again.
	if j.attempt > c.retries+1 {
		return q.finish(ctx, c, j, errors.New("pgq: lease expired"))
	}
	stop := q.heartbeat(ctx, j)
	err := q.call(ctx, c, j)
	stop()
	return q.finish(ctx, c, j, err)
}

// call invokes the handler with the configured timeout, converting panics
// into errors
func (q *Queues) call(ctx context.Context, c *Config, j *job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			q.log.Error("handler panicked", "queue", c.queue, "job", j.id, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("pgq: handler panicked: %v", r)
		}
	}()
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	return c.handle(ctx, j)
}

// heartbeat extends the job's lease while the handler runs
func (q *Queues) heartbeat(ctx context.Context, j *job) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(q.lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Errors are retried on the next tick, well before the lease expires
				if _, err := q.pool.Exec(ctx, `
					UPDATE pgq_jobs SET locked_until = now() + $3 * interval '1 millisecond', updated_at = now()
					WHERE id = $1 AND attempts = $2 AND state = 'running'
				`, j.id, j.attempt, q.lease.Milliseconds()); err != nil && ctx.Err() == nil {
					q.log.Warn("unable to extend lease", "job", j.id, "error", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// finish records the handler's result. Every update is fenced on the attempt
// number so a worker whose lease expired can't clobber a newer claim.
func (q *Queues) finish(ctx context.Context, c *Config, j *job, err error) error {
	// Record the result even if we're shutting down
	shutdown := ctx.Err() != nil
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	log := q.log.With("queue", c.queue, "job", j.id, "attempt", j.attempt)
	switch {
	case err == nil:
		return q.update(ctx, j, `
			UPDATE pgq_jobs SET state = 'completed', locked_until = NULL, updated_at = now()
			WHERE id = $1 AND attempts = $2 AND state = 'running'
		`)
	case jobq.IsPermanent(err):
		// Honored even during shutdown, since it should never be retried
		return q.fail(ctx, log, j, err)
	case shutdown:
		// Interrupted by shutdown, so release the job without using up an attempt
		return q.update(ctx, j, `
			UPDATE pgq_jobs SET state = 'pending', attempts = attempts - 1, locked_until = NULL, updated_at = now()
			WHERE id = $1 AND attempts = $2 AND state = 'running'
		`)
	case j.attempt <= c.retries:
		delay := backoff.Delay(j.attempt)
		log.Warn("job failed, retrying", "error", err, "delay", delay)
		return q.update(ctx, j, `
			UPDATE pgq_jobs SET
				state = 'pending',
				run_at = now() + $4 * interval '1 millisecond',
				last_error = $3,
				locked_until = NULL,
				updated_at = now()
			WHERE id = $1 AND attempts = $2 AND state = 'running'
		`, err.Error(), delay.Milliseconds())
	default:
		return q.fail(ctx, log, j, err)
	}
}

// fail permanently fails the job, leaving it in the dead-letter state
func (q *Queues) fail(ctx context.Context, log *slog.Logger, j *job, err error) error {
	log.Error("job failed", "error", err, "permanent", jobq.IsPermanent(err))
	return q.update(ctx, j, `
		UPDATE pgq_jobs SET state = 'failed', last_error = $3, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND attempts = $2 AND state = 'running'
	`, err.Error())
}

func (q *Queues) update(ctx context.Context, j *job, sql string, args ...any) error {
	if _, err := q.pool.Exec(ctx, sql, append([]any{j.id, j.attempt}, args...)...); err != nil {
		return fmt.Errorf("pgq: unable to update job %d: %w", j.id, err)
	}
	return nil
}
