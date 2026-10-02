package jetq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matthewmueller/jobq"
	"github.com/matthewmueller/jobq/internal/backoff"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Payload = jobq.Payload
type Job[T Payload] = jobq.Job[T]
type Handler[T Payload] = jobq.Handler[T]
type Stats = jobq.Stats

// Permanent marks err as non-retryable, so the job goes straight to the
// dead-letter stream
func Permanent(err error) error {
	return jobq.Permanent(err)
}

const (
	jobsStream = "JOBQ"
	deadStream = "JOBQ_DEAD"
)

func jobsSubject(queue string) string { return "jobq.jobs." + queue }
func deadSubject(queue string) string { return "jobq.dead." + queue }

// consumerName is the queue's durable consumer. Consumer names can't contain
// dots, so they become underscores.
func consumerName(queue string) string { return strings.ReplaceAll(queue, ".", "_") }

// Dial connects to NATS and creates or updates the JOBQ and JOBQ_DEAD
// streams. Each payload's queue is the subject jobq.jobs.<Payload.Queue()>.
func Dial(ctx context.Context, log *slog.Logger, url string) (*Queues, error) {
	nc, err := nats.Connect(url, nats.Name("jobq"))
	if err != nil {
		return nil, fmt.Errorf("jetq: unable to connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetq: unable to use jetstream: %w", err)
	}
	// Work-queue retention deletes each message once it's acknowledged
	jobs, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      jobsStream,
		Subjects:  []string{jobsSubject(">")},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetq: unable to create the %s stream: %w", jobsStream, err)
	}
	dead, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      deadStream,
		Subjects:  []string{deadSubject(">")},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetq: unable to create the %s stream: %w", deadStream, err)
	}
	return &Queues{
		nc:    nc,
		js:    js,
		jobs:  jobs,
		dead:  dead,
		log:   log.With("package", "jetq"),
		lease: 30 * time.Second,
		wait:  time.Second,
	}, nil
}

// Queues produces and consumes jobs backed by NATS JetStream
type Queues struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	jobs    jetstream.Stream
	dead    jetstream.Stream
	log     *slog.Logger
	lease   time.Duration // how long a delivered message is reserved before it's redelivered
	wait    time.Duration // how long a pull waits for messages, which also bounds shutdown. Pulls are cheap, so keep it short.
	configs []*Config
}

// Config configures how a registered queue is consumed
type Config struct {
	queue       string
	concurrency int
	retries     int
	timeout     time.Duration
	handle      func(ctx context.Context, msg *message) error
	consumer    jetstream.Consumer // created on Start
}

// Concurrency sets the maximum number of jobs this process runs at once for
// the queue. Defaults to 1.
func (c *Config) Concurrency(n int) *Config {
	c.concurrency = n
	return c
}

// Retries sets how many times a failed job is retried. Defaults to 0.
// Redeliveries after a shutdown or crash count as attempts.
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

// message is a received message and its metadata
type message struct {
	msg       jetstream.Msg
	id        string
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
		handle: func(ctx context.Context, msg *message) error {
			var data T
			if err := json.Unmarshal(msg.msg.Data(), &data); err != nil {
				return jobq.Permanent(fmt.Errorf("jetq: unable to decode message %s: %w", msg.id, err))
			}
			return handler(ctx, &Job[T]{
				ID:        msg.id,
				Data:      data,
				Attempt:   msg.attempt,
				CreatedAt: msg.createdAt,
			})
		},
	}
	q.configs = append(q.configs, config)
	return config
}

// Push enqueues the payload onto the queue it names
func (q *Queues) Push[T Payload](ctx context.Context, payload T) error {
	queue := payload.Queue()
	if err := validate(queue); err != nil {
		return err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("jetq: unable to encode %q payload: %w", queue, err)
	}
	if _, err := q.js.Publish(ctx, jobsSubject(queue), data); err != nil {
		return fmt.Errorf("jetq: unable to push to %q: %w", queue, err)
	}
	return nil
}

// Revive moves the queue's dead-lettered jobs back onto it with fresh
// attempts
func (q *Queues) Revive(ctx context.Context, queue string) error {
	if err := validate(queue); err != nil {
		return err
	}
	consumer, err := q.dead.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject:     deadSubject(queue),
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: time.Minute,
	})
	if err != nil {
		return fmt.Errorf("jetq: unable to revive %q: %w", queue, err)
	}
	defer q.dead.DeleteConsumer(context.WithoutCancel(ctx), consumer.CachedInfo().Name)
	count := 0
	for {
		batch, err := consumer.FetchNoWait(100)
		if err != nil {
			return fmt.Errorf("jetq: unable to revive %q: %w", queue, err)
		}
		fetched := 0
		for msg := range batch.Messages() {
			fetched++
			if _, err := q.js.Publish(ctx, jobsSubject(queue), msg.Data()); err != nil {
				return fmt.Errorf("jetq: unable to revive %q: %w", queue, err)
			}
			if err := msg.DoubleAck(ctx); err != nil {
				return fmt.Errorf("jetq: unable to revive %q: %w", queue, err)
			}
			count++
		}
		if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
			return fmt.Errorf("jetq: unable to revive %q: %w", queue, err)
		}
		if fetched == 0 {
			break
		}
	}
	q.log.Info("revived failed jobs", "queue", queue, "count", count)
	return nil
}

// Stats counts the queue's pending, running and dead-lettered jobs. Jobs
// waiting out a retry backoff or released at shutdown count as running until
// they're redelivered.
func (q *Queues) Stats(ctx context.Context, queue string) (*Stats, error) {
	if err := validate(queue); err != nil {
		return nil, err
	}
	total, err := count(ctx, q.jobs, jobsSubject(queue))
	if err != nil {
		return nil, err
	}
	failed, err := count(ctx, q.dead, deadSubject(queue))
	if err != nil {
		return nil, err
	}
	running := 0
	consumer, err := q.jobs.Consumer(ctx, consumerName(queue))
	if err == nil {
		info, err := consumer.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("jetq: unable to get stats for %q: %w", queue, err)
		}
		running = info.NumAckPending
	} else if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, fmt.Errorf("jetq: unable to get stats for %q: %w", queue, err)
	}
	return &Stats{
		Pending: total - running,
		Running: running,
		Failed:  failed,
	}, nil
}

// count returns how many messages the stream holds for subject
func count(ctx context.Context, stream jetstream.Stream, subject string) (int, error) {
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		return 0, fmt.Errorf("jetq: unable to count %s: %w", subject, err)
	}
	return int(info.State.Subjects[subject]), nil
}

// Start processes jobs for all registered queues. It blocks until ctx is
// cancelled and running handlers have returned. NATS errors while running
// are logged and retried rather than returned.
func (q *Queues) Start(ctx context.Context) error {
	if err := q.validate(); err != nil {
		return err
	}
	for _, c := range q.configs {
		consumer, err := q.jobs.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:       consumerName(c.queue),
			FilterSubject: jobsSubject(c.queue),
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       q.lease,
			MaxDeliver:    -1,
		})
		if err != nil {
			return fmt.Errorf("jetq: unable to create consumer for %q: %w", c.queue, err)
		}
		c.consumer = consumer
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

// Close closes the NATS connection
func (q *Queues) Close() error {
	q.nc.Close()
	return nil
}

func (q *Queues) validate() error {
	var errs []error
	consumers := map[string]string{}
	for _, c := range q.configs {
		if err := validate(c.queue); err != nil {
			errs = append(errs, err)
		}
		if c.concurrency < 1 {
			errs = append(errs, fmt.Errorf("jetq: %q concurrency must be at least 1", c.queue))
		}
		if c.retries < 0 {
			errs = append(errs, fmt.Errorf("jetq: %q retries must not be negative", c.queue))
		}
		if c.timeout < 0 {
			errs = append(errs, fmt.Errorf("jetq: %q timeout must not be negative", c.queue))
		}
		name := consumerName(c.queue)
		if other, ok := consumers[name]; ok && other == c.queue {
			errs = append(errs, fmt.Errorf("jetq: %q is registered more than once", c.queue))
		} else if ok {
			errs = append(errs, fmt.Errorf("jetq: %q and %q would share the consumer %q", other, c.queue, name))
		}
		consumers[name] = c.queue
	}
	return errors.Join(errs...)
}

// validate checks the queue name is dot-separated tokens that are safe in a
// subject
func validate(queue string) error {
	for token := range strings.SplitSeq(queue, ".") {
		if token == "" {
			return fmt.Errorf("jetq: queue name %q must be dot-separated tokens of letters, numbers, hyphens and underscores", queue)
		}
		for _, r := range token {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
				return fmt.Errorf("jetq: queue name %q must be dot-separated tokens of letters, numbers, hyphens and underscores", queue)
			}
		}
	}
	return nil
}

// work receives and runs jobs until ctx is cancelled. NATS errors back off
// and retry so a blip doesn't stop the worker.
func (q *Queues) work(ctx context.Context, c *Config) {
	failures := 0
	for ctx.Err() == nil {
		msg, err := q.receive(ctx, c)
		if err == nil && msg != nil {
			err = q.run(ctx, c, msg)
		}
		if err != nil {
			failures++
			q.log.Error("worker error", "queue", c.queue, "error", err)
			sleep(ctx, min(backoff.Delay(failures), 30*time.Second))
			continue
		}
		failures = 0
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

// receive pulls the next message, or returns nil if there are none. Pulls
// aren't cancelled on shutdown, because the server may already be delivering
// to them, which would leave the message unacknowledged until its lease
// expired.
func (q *Queues) receive(ctx context.Context, c *Config) (*message, error) {
	msg, err := c.consumer.Next(jetstream.FetchMaxWait(q.wait))
	if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("jetq: unable to receive from %q: %w", c.queue, err)
	}
	if ctx.Err() != nil {
		// Shutting down, so release the message right away
		if err := msg.Nak(); err != nil {
			return nil, fmt.Errorf("jetq: unable to release message: %w", err)
		}
		return nil, nil
	}
	meta, err := msg.Metadata()
	if err != nil {
		return nil, fmt.Errorf("jetq: unable to read message metadata from %q: %w", c.queue, err)
	}
	return &message{
		msg:       msg,
		id:        strconv.FormatUint(meta.Sequence.Stream, 10),
		attempt:   int(meta.NumDelivered),
		createdAt: meta.Timestamp,
	}, nil
}

// run calls the handler and records the outcome
func (q *Queues) run(ctx context.Context, c *Config, msg *message) error {
	stop := q.heartbeat(ctx, c, msg)
	err := q.call(ctx, c, msg)
	stop()
	return q.finish(ctx, c, msg, err)
}

// call invokes the handler with the configured timeout, converting panics
// into errors
func (q *Queues) call(ctx context.Context, c *Config, msg *message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			q.log.Error("handler panicked", "queue", c.queue, "job", msg.id, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("jetq: handler panicked: %v", r)
		}
	}()
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	return c.handle(ctx, msg)
}

// heartbeat extends the message's lease while the handler runs
func (q *Queues) heartbeat(ctx context.Context, c *Config, msg *message) (stop func()) {
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
				if err := msg.msg.InProgress(); err != nil && ctx.Err() == nil {
					q.log.Warn("unable to extend lease", "queue", c.queue, "job", msg.id, "error", err)
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
func (q *Queues) finish(ctx context.Context, c *Config, msg *message, err error) error {
	// Record the result even if we're shutting down
	shutdown := ctx.Err() != nil
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	log := q.log.With("queue", c.queue, "job", msg.id, "attempt", msg.attempt)
	switch {
	case err == nil:
		if err := msg.msg.DoubleAck(ctx); err != nil {
			return fmt.Errorf("jetq: unable to ack message %s: %w", msg.id, err)
		}
		return nil
	case jobq.IsPermanent(err):
		// Honored even during shutdown, since it should never be retried
		log.Error("job failed", "error", err, "permanent", true)
		return q.deadLetter(ctx, c, msg, err)
	case shutdown:
		// Interrupted by shutdown, so release the message right away
		if err := msg.msg.Nak(); err != nil {
			return fmt.Errorf("jetq: unable to release message %s: %w", msg.id, err)
		}
		return nil
	case msg.attempt <= c.retries:
		delay := backoff.Delay(msg.attempt)
		log.Warn("job failed, retrying", "error", err, "delay", delay)
		if err := msg.msg.NakWithDelay(delay); err != nil {
			return fmt.Errorf("jetq: unable to retry message %s: %w", msg.id, err)
		}
		return nil
	default:
		log.Error("job failed", "error", err, "permanent", false)
		return q.deadLetter(ctx, c, msg, err)
	}
}

// deadLetter moves a failed message to the dead-letter stream, recording the
// error and attempts in its headers
func (q *Queues) deadLetter(ctx context.Context, c *Config, msg *message, err error) error {
	header := nats.Header{}
	// Header values can't contain newlines
	header.Set("Jobq-Error", strings.ReplaceAll(err.Error(), "\n", " "))
	header.Set("Jobq-Attempts", strconv.Itoa(msg.attempt))
	if _, err := q.js.PublishMsg(ctx, &nats.Msg{
		Subject: deadSubject(c.queue),
		Data:    msg.msg.Data(),
		Header:  header,
	}); err != nil {
		return fmt.Errorf("jetq: unable to dead-letter message %s: %w", msg.id, err)
	}
	if err := msg.msg.DoubleAck(ctx); err != nil {
		return fmt.Errorf("jetq: unable to ack message %s: %w", msg.id, err)
	}
	return nil
}
