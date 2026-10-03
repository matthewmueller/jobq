// Package sqlq processes jobs from queues stored in a SQL table, leaving the
// SQL itself to each backend
package sqlq

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

	"github.com/matthewmueller/jobq"
	"github.com/matthewmueller/jobq/internal/backoff"
)

// Store is the SQL a backend implements. Every method that touches a claimed
// job must be fenced on its ID and attempt, so a worker whose lease expired
// can't clobber a newer claim.
type Store interface {
	// Claim reserves the next available job for lease, or returns nil if there
	// are none. Jobs whose lease expired (e.g. their worker crashed) are
	// reclaimed.
	Claim(ctx context.Context, queue string, retries int, lease time.Duration) (*Job, error)
	// Extend renews a running job's lease
	Extend(ctx context.Context, job *Job, lease time.Duration) error
	// Complete marks the job as done
	Complete(ctx context.Context, job *Job) error
	// Release returns the job to pending without using up an attempt
	Release(ctx context.Context, job *Job) error
	// Retry returns the job to pending after delay, recording err
	Retry(ctx context.Context, job *Job, delay time.Duration, err error) error
	// Fail moves the job to the failed (dead-letter) state, recording err
	Fail(ctx context.Context, job *Job, err error) error
}

// Laned is implemented by payloads that run one at a time per lane. Jobs in
// the same lane never run at the same time, even across processes. Jobs in
// different lanes, and jobs without a lane, run concurrently. Jobs usually run
// in the order they were pushed, but a job waiting to retry doesn't hold up
// the rest of its lane.
type Laned interface {
	Lane() string
}

// Lane returns the payload's lane, or nil if it doesn't have one
func Lane(payload jobq.Payload) *string {
	laned, ok := payload.(Laned)
	if !ok {
		return nil
	}
	lane := laned.Lane()
	if lane == "" {
		return nil
	}
	return &lane
}

// Job is an untyped claimed job
type Job struct {
	ID        int64
	Payload   []byte
	Attempt   int
	CreatedAt time.Time
}

// New returns a Worker that processes jobs from store. The name prefixes
// errors, e.g. "pgq". Idle workers check for jobs every poll, or sooner when
// notified.
func New(name string, store Store, log *slog.Logger, poll time.Duration) *Worker {
	return &Worker{
		name:  name,
		store: store,
		log:   log,
		lease: 30 * time.Second,
		poll:  poll,
	}
}

// Worker runs registered handlers against a Store, using each queue's
// concurrency worth of goroutines to claim and process jobs
type Worker struct {
	name    string
	store   Store
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
	handle      func(ctx context.Context, j *Job) error
	wake        chan struct{} // wakes one idle worker
}

// signal wakes one idle worker for the queue. Signals collapse while no
// worker is waiting, and never block.
func (c *Config) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
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

// Register a handler for the queue named by T. Register handlers before
// calling Start.
func (w *Worker) Register[T jobq.Payload](handler jobq.Handler[T]) *Config {
	var zero T
	config := &Config{
		queue:       zero.Queue(),
		concurrency: 1,
		wake:        make(chan struct{}, 1),
		handle: func(ctx context.Context, j *Job) error {
			var data T
			if err := json.Unmarshal(j.Payload, &data); err != nil {
				return jobq.Permanent(fmt.Errorf("%s: unable to decode job %d: %w", w.name, j.ID, err))
			}
			return handler(ctx, &jobq.Job[T]{
				ID:        strconv.FormatInt(j.ID, 10),
				Data:      data,
				Attempt:   j.Attempt,
				CreatedAt: j.CreatedAt,
			})
		},
	}
	w.configs = append(w.configs, config)
	return config
}

// Notify wakes an idle worker for the queue, e.g. when a job was pushed.
// Queues this process doesn't handle are ignored.
func (w *Worker) Notify(queue string) {
	for _, c := range w.configs {
		if c.queue == queue {
			c.signal()
		}
	}
}

// NotifyAll wakes an idle worker for every queue, e.g. after notifications
// may have been missed
func (w *Worker) NotifyAll() {
	for _, c := range w.configs {
		c.signal()
	}
}

// Start processes jobs for all registered queues. It blocks until ctx is
// cancelled and running handlers have returned. Database errors are logged
// and retried rather than returned.
func (w *Worker) Start(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, config := range w.configs {
		for range config.concurrency {
			wg.Go(func() { w.work(ctx, config) })
		}
	}
	wg.Wait()
	return nil
}

func (w *Worker) validate() error {
	var errs []error
	seen := map[string]bool{}
	for _, c := range w.configs {
		if c.queue == "" {
			errs = append(errs, fmt.Errorf("%s: queue name must not be empty", w.name))
		}
		if c.concurrency < 1 {
			errs = append(errs, fmt.Errorf("%s: %q concurrency must be at least 1", w.name, c.queue))
		}
		if c.retries < 0 {
			errs = append(errs, fmt.Errorf("%s: %q retries must not be negative", w.name, c.queue))
		}
		if c.timeout < 0 {
			errs = append(errs, fmt.Errorf("%s: %q timeout must not be negative", w.name, c.queue))
		}
		if seen[c.queue] {
			errs = append(errs, fmt.Errorf("%s: %q is registered more than once", w.name, c.queue))
		}
		seen[c.queue] = true
	}
	return errors.Join(errs...)
}

// work claims and runs jobs until ctx is cancelled. Database errors back off
// and retry so a blip doesn't stop the worker.
func (w *Worker) work(ctx context.Context, c *Config) {
	failures := 0
	for ctx.Err() == nil {
		j, err := w.store.Claim(ctx, c.queue, c.retries, w.lease)
		if err == nil && j != nil {
			// There may be more jobs, so get another worker checking too
			c.signal()
			err = w.run(ctx, c, j)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			w.log.Error("worker error", "queue", c.queue, "error", err)
			sleep(ctx, min(backoff.Delay(failures), 30*time.Second))
			continue
		}
		failures = 0
		if j == nil {
			idle(ctx, c, w.poll)
		}
	}
}

// idle waits until the queue is signaled, poll elapses or ctx is cancelled
func idle(ctx context.Context, c *Config, poll time.Duration) {
	timer := time.NewTimer(poll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-c.wake:
	case <-timer.C:
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

// run calls the handler and records the outcome
func (w *Worker) run(ctx context.Context, c *Config, j *Job) error {
	// A reclaimed job that already used up its attempts (e.g. it keeps
	// crashing the process) is failed rather than run again.
	if j.Attempt > c.retries+1 {
		return w.finish(ctx, c, j, fmt.Errorf("%s: lease expired", w.name))
	}
	stop := w.heartbeat(ctx, j)
	err := w.call(ctx, c, j)
	stop()
	return w.finish(ctx, c, j, err)
}

// call invokes the handler with the configured timeout, converting panics
// into errors
func (w *Worker) call(ctx context.Context, c *Config, j *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("handler panicked", "queue", c.queue, "job", j.ID, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("%s: handler panicked: %v", w.name, r)
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
func (w *Worker) heartbeat(ctx context.Context, j *Job) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Errors are retried on the next tick, well before the lease expires
				if err := w.store.Extend(ctx, j, w.lease); err != nil && ctx.Err() == nil {
					w.log.Warn("unable to extend lease", "job", j.ID, "error", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// finish records the handler's result
func (w *Worker) finish(ctx context.Context, c *Config, j *Job, err error) error {
	// Record the result even if we're shutting down
	shutdown := ctx.Err() != nil
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	log := w.log.With("queue", c.queue, "job", j.ID, "attempt", j.Attempt)
	switch {
	case err == nil:
		return w.store.Complete(ctx, j)
	case jobq.IsPermanent(err):
		// Honored even during shutdown, since it should never be retried
		log.Error("job failed", "error", err, "permanent", true)
		return w.store.Fail(ctx, j, err)
	case shutdown:
		// Interrupted by shutdown, so release the job without using up an attempt
		return w.store.Release(ctx, j)
	case j.Attempt <= c.retries:
		delay := backoff.Delay(j.Attempt)
		log.Warn("job failed, retrying", "error", err, "delay", delay)
		if retryErr := w.store.Retry(ctx, j, delay, err); retryErr != nil {
			return retryErr
		}
		// Wake a worker when the retry is due rather than at the next poll
		time.AfterFunc(delay, c.signal)
		return nil
	default:
		log.Error("job failed", "error", err, "permanent", false)
		return w.store.Fail(ctx, j, err)
	}
}
