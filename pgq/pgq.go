package pgq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthewmueller/jobq"
	"golang.org/x/sync/errgroup"
)

type Payload = jobq.Payload
type Job[T Payload] = jobq.Job[T]
type Handler[T Payload] = jobq.Handler[T]

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
func Dial(ctx context.Context, url string) (*Queues, error) {
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
		lease: 30 * time.Second,
		poll:  500 * time.Millisecond,
	}, nil
}

// Queues produces and consumes jobs backed by PostgreSQL
type Queues struct {
	pool    *pgxpool.Pool
	lease   time.Duration // how long a claimed job is reserved before others may reclaim it
	poll    time.Duration // how long an idle worker waits before checking for jobs again
	configs []*Config
}

// Config configures how a registered queue is consumed
type Config struct {
	queue       string
	concurrency int
	retries     int
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
				return fmt.Errorf("pgq: unable to decode job %d: %w", j.id, err)
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
	queue := payload.Queue()
	if queue == "" {
		return fmt.Errorf("pgq: %T has an empty queue name", payload)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pgq: unable to encode %q payload: %w", queue, err)
	}
	if _, err := q.pool.Exec(ctx, `INSERT INTO pgq_jobs (queue, payload) VALUES ($1, $2)`, queue, data); err != nil {
		return fmt.Errorf("pgq: unable to push to %q: %w", queue, err)
	}
	return nil
}

// Start processes jobs for all registered queues. It blocks until ctx is
// cancelled and running handlers have returned.
func (q *Queues) Start(ctx context.Context) error {
	if err := q.validate(); err != nil {
		return err
	}
	eg, ctx := errgroup.WithContext(ctx)
	for _, config := range q.configs {
		for range config.concurrency {
			eg.Go(func() error { return q.work(ctx, config) })
		}
	}
	return eg.Wait()
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
		if seen[c.queue] {
			errs = append(errs, fmt.Errorf("pgq: %q is registered more than once", c.queue))
		}
		seen[c.queue] = true
	}
	return errors.Join(errs...)
}

// work claims and runs jobs until ctx is cancelled
func (q *Queues) work(ctx context.Context, c *Config) error {
	for {
		j, err := q.claim(ctx, c)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if j == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(q.poll):
			}
			continue
		}
		if err := q.run(ctx, c, j); err != nil {
			return err
		}
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
	err := call(ctx, c, j)
	stop()
	return q.finish(ctx, c, j, err)
}

// call invokes the handler, converting panics into errors
func call(ctx context.Context, c *Config, j *job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pgq: handler panicked: %v", r)
		}
	}()
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
				q.pool.Exec(ctx, `
					UPDATE pgq_jobs SET locked_until = now() + $3 * interval '1 millisecond', updated_at = now()
					WHERE id = $1 AND attempts = $2 AND state = 'running'
				`, j.id, j.attempt, q.lease.Milliseconds())
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
	switch {
	case err == nil:
		return q.update(ctx, j, `
			UPDATE pgq_jobs SET state = 'completed', locked_until = NULL, updated_at = now()
			WHERE id = $1 AND attempts = $2 AND state = 'running'
		`)
	case shutdown:
		// Interrupted by shutdown, so release the job without using up an attempt
		return q.update(ctx, j, `
			UPDATE pgq_jobs SET state = 'pending', attempts = attempts - 1, locked_until = NULL, updated_at = now()
			WHERE id = $1 AND attempts = $2 AND state = 'running'
		`)
	case j.attempt <= c.retries:
		return q.update(ctx, j, `
			UPDATE pgq_jobs SET
				state = 'pending',
				run_at = now() + $4 * interval '1 millisecond',
				last_error = $3,
				locked_until = NULL,
				updated_at = now()
			WHERE id = $1 AND attempts = $2 AND state = 'running'
		`, err.Error(), backoff(j.attempt).Milliseconds())
	default:
		return q.update(ctx, j, `
			UPDATE pgq_jobs SET state = 'failed', last_error = $3, locked_until = NULL, updated_at = now()
			WHERE id = $1 AND attempts = $2 AND state = 'running'
		`, err.Error())
	}
}

func (q *Queues) update(ctx context.Context, j *job, sql string, args ...any) error {
	if _, err := q.pool.Exec(ctx, sql, append([]any{j.id, j.attempt}, args...)...); err != nil {
		return fmt.Errorf("pgq: unable to update job %d: %w", j.id, err)
	}
	return nil
}

// backoff returns how long to wait before retrying a failed attempt
func backoff(attempt int) time.Duration {
	return 0
}
