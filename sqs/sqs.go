package sqs

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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/matthewmueller/jobq"
	"github.com/matthewmueller/jobq/internal/backoff"
)

type Payload = jobq.Payload
type Job[T Payload] = jobq.Job[T]
type Handler[T Payload] = jobq.Handler[T]
type Stats = jobq.Stats

// Permanent marks err as non-retryable, so the message goes straight to the
// dead-letter queue
func Permanent(err error) error {
	return jobq.Permanent(err)
}

// Dial connects to SQS. The url is the account's queue prefix (e.g.
// https://sqs.us-west-2.amazonaws.com/123456789012) and each payload's queue
// lives at url/<Payload.Queue()>. Queues must already exist. Credentials come
// from the default AWS config (e.g. AWS_PROFILE).
func Dial(ctx context.Context, log *slog.Logger, url string) (*Queues, error) {
	var options []func(*config.LoadOptions) error
	if region, ok := regionOf(url); ok {
		options = append(options, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("sqs: unable to load aws config: %w", err)
	}
	return &Queues{
		client:     awssqs.NewFromConfig(cfg),
		log:        log.With("package", "sqs"),
		url:        strings.TrimSuffix(url, "/"),
		visibility: 30 * time.Second,
		wait:       5 * time.Second,
	}, nil
}

// regionOf extracts the region from https://sqs.<region>.amazonaws.com/...
func regionOf(url string) (string, bool) {
	_, host, ok := strings.Cut(url, "://sqs.")
	if !ok {
		return "", false
	}
	region, _, ok := strings.Cut(host, ".amazonaws.com")
	return region, ok && region != ""
}

// Queues produces and consumes jobs backed by SQS
type Queues struct {
	client     *awssqs.Client
	log        *slog.Logger
	url        string
	visibility time.Duration // how long a received message is hidden from other consumers
	wait       time.Duration // how long a receive long-polls for messages, which also bounds shutdown
	configs    []*Config
}

// Config configures how a registered queue is consumed. Retries are governed
// by the SQS queue's redrive policy.
type Config struct {
	queue       string
	concurrency int
	timeout     time.Duration
	handle      func(ctx context.Context, msg *message) error
	redrive     *redrive // resolved on Start, nil without a dead-letter queue
}

// Concurrency sets the maximum number of jobs this process runs at once for
// the queue. Defaults to 1.
func (c *Config) Concurrency(n int) *Config {
	c.concurrency = n
	return c
}

// Timeout bounds how long a handler may run before its context is cancelled
// and the attempt fails. Defaults to no timeout. SQS limits how long a message
// can stay hidden to 12 hours.
func (c *Config) Timeout(d time.Duration) *Config {
	c.timeout = d
	return c
}

// message is the untyped received message
type message struct {
	id        string
	receipt   string
	body      string
	attempt   int
	createdAt time.Time
}

// redrive is a queue's dead-letter configuration
type redrive struct {
	queueARN    string
	dlqARN      string
	dlqURL      string
	maxReceives int
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
			if err := json.Unmarshal([]byte(msg.body), &data); err != nil {
				return jobq.Permanent(fmt.Errorf("sqs: unable to decode message %s: %w", msg.id, err))
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
		return fmt.Errorf("sqs: unable to encode %q payload: %w", queue, err)
	}
	if _, err := q.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    aws.String(q.queueURL(queue)),
		MessageBody: aws.String(string(data)),
	}); err != nil {
		return fmt.Errorf("sqs: unable to push to %q: %w", queue, err)
	}
	return nil
}

// Redrive starts moving the queue's dead-lettered messages back onto it. The
// move happens asynchronously in SQS and only covers the messages SQS counts
// when it starts, so messages dead-lettered moments ago may need another
// Redrive.
func (q *Queues) Redrive(ctx context.Context, queue string) error {
	r, err := q.redrive(ctx, queue)
	if err != nil {
		return err
	} else if r == nil {
		return fmt.Errorf("sqs: %q has no dead-letter queue", queue)
	}
	if _, err := q.client.StartMessageMoveTask(ctx, &awssqs.StartMessageMoveTaskInput{
		SourceArn: aws.String(r.dlqARN),
		// Required for messages that were dead-lettered by Permanent rather
		// than by the redrive policy
		DestinationArn: aws.String(r.queueARN),
	}); err != nil {
		return fmt.Errorf("sqs: unable to redrive %q: %w", queue, err)
	}
	q.log.Info("started redrive", "queue", queue)
	return nil
}

// Stats approximates the queue's pending, running and dead-lettered messages.
// Messages waiting out a retry backoff count as running.
func (q *Queues) Stats(ctx context.Context, queue string) (*Stats, error) {
	attrs, err := q.attributes(ctx, q.queueURL(queue),
		types.QueueAttributeNameApproximateNumberOfMessages,
		types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		types.QueueAttributeNameRedrivePolicy,
	)
	if err != nil {
		return nil, err
	}
	stats := &Stats{
		Pending: count(attrs, types.QueueAttributeNameApproximateNumberOfMessages) +
			count(attrs, types.QueueAttributeNameApproximateNumberOfMessagesDelayed),
		Running: count(attrs, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible),
	}
	r, err := parseRedrive(attrs)
	if err != nil || r == nil {
		return stats, err
	}
	dlq, err := q.attributes(ctx, r.dlqURL, types.QueueAttributeNameApproximateNumberOfMessages)
	if err != nil {
		return nil, err
	}
	stats.Failed = count(dlq, types.QueueAttributeNameApproximateNumberOfMessages)
	return stats, nil
}

func count(attrs map[string]string, name types.QueueAttributeName) int {
	n, _ := strconv.Atoi(attrs[string(name)])
	return n
}

// Start processes jobs for all registered queues. It blocks until ctx is
// cancelled and running handlers have returned. SQS errors while running are
// logged and retried rather than returned.
func (q *Queues) Start(ctx context.Context) error {
	if err := q.validate(); err != nil {
		return err
	}
	for _, c := range q.configs {
		r, err := q.redrive(ctx, c.queue)
		if err != nil {
			return err
		} else if r == nil {
			q.log.Warn("queue has no dead-letter queue, so failed jobs retry until they expire", "queue", c.queue)
		}
		c.redrive = r
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

// Close releases resources. The SQS client holds none that need closing.
func (q *Queues) Close() error {
	return nil
}

func (q *Queues) queueURL(queue string) string {
	return q.url + "/" + queue
}

func (q *Queues) validate() error {
	var errs []error
	seen := map[string]bool{}
	for _, c := range q.configs {
		if err := validate(c.queue); err != nil {
			errs = append(errs, err)
		}
		if c.concurrency < 1 {
			errs = append(errs, fmt.Errorf("sqs: %q concurrency must be at least 1", c.queue))
		}
		if c.timeout < 0 || c.timeout > 12*time.Hour {
			errs = append(errs, fmt.Errorf("sqs: %q timeout must be between 0 and 12 hours", c.queue))
		}
		if seen[c.queue] {
			errs = append(errs, fmt.Errorf("sqs: %q is registered more than once", c.queue))
		}
		seen[c.queue] = true
	}
	return errors.Join(errs...)
}

// validate checks the queue name is a valid standard SQS queue name
func validate(queue string) error {
	if queue == "" || len(queue) > 80 {
		return fmt.Errorf("sqs: queue name %q must be 1 to 80 characters", queue)
	}
	for _, r := range queue {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return fmt.Errorf("sqs: queue name %q may only contain letters, numbers, hyphens and underscores", queue)
		}
	}
	return nil
}

func (q *Queues) attributes(ctx context.Context, url string, names ...types.QueueAttributeName) (map[string]string, error) {
	out, err := q.client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: names,
	})
	if err != nil {
		return nil, fmt.Errorf("sqs: unable to get attributes of %s: %w", url, err)
	}
	return out.Attributes, nil
}

// redrive looks up the queue's dead-letter configuration, returning nil if it
// has no redrive policy
func (q *Queues) redrive(ctx context.Context, queue string) (*redrive, error) {
	attrs, err := q.attributes(ctx, q.queueURL(queue), types.QueueAttributeNameQueueArn, types.QueueAttributeNameRedrivePolicy)
	if err != nil {
		return nil, err
	}
	return parseRedrive(attrs)
}

func parseRedrive(attrs map[string]string) (*redrive, error) {
	policy := attrs[string(types.QueueAttributeNameRedrivePolicy)]
	if policy == "" {
		return nil, nil
	}
	var p struct {
		DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
		MaxReceiveCount     json.Number `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(policy), &p); err != nil {
		return nil, fmt.Errorf("sqs: unable to parse redrive policy %q: %w", policy, err)
	}
	maxReceives, err := strconv.Atoi(p.MaxReceiveCount.String())
	if err != nil {
		return nil, fmt.Errorf("sqs: unable to parse max receive count in %q: %w", policy, err)
	}
	// arn:aws:sqs:<region>:<account>:<name>
	parts := strings.Split(p.DeadLetterTargetArn, ":")
	if len(parts) != 6 {
		return nil, fmt.Errorf("sqs: unexpected dead-letter queue arn %q", p.DeadLetterTargetArn)
	}
	return &redrive{
		queueARN:    attrs[string(types.QueueAttributeNameQueueArn)],
		dlqARN:      p.DeadLetterTargetArn,
		dlqURL:      fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s", parts[3], parts[4], parts[5]),
		maxReceives: maxReceives,
	}, nil
}

// work receives and runs jobs until ctx is cancelled. SQS errors back off and
// retry so a blip doesn't stop the worker.
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

// receive long-polls for the next message, or returns nil if there are none.
// In-flight polls finish during shutdown, because cancelling the request
// doesn't stop SQS from handing it a message, which would then stay hidden
// until its visibility timeout expired.
func (q *Queues) receive(ctx context.Context, c *Config) (*message, error) {
	msg, err := q.poll(context.WithoutCancel(ctx), c)
	if err != nil || msg == nil {
		return nil, err
	}
	if ctx.Err() != nil {
		// Shutting down, so release the message right away
		return nil, q.setVisibility(context.WithoutCancel(ctx), c, msg, 0)
	}
	return msg, nil
}

func (q *Queues) poll(ctx context.Context, c *Config) (*message, error) {
	out, err := q.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.queueURL(c.queue)),
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   int32(q.visibility.Seconds()),
		WaitTimeSeconds:     int32(q.wait.Seconds()),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameSentTimestamp,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("sqs: unable to receive from %q: %w", c.queue, err)
	}
	if len(out.Messages) == 0 {
		return nil, nil
	}
	m := out.Messages[0]
	attempt, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil {
		return nil, fmt.Errorf("sqs: unable to parse receive count for message %s: %w", aws.ToString(m.MessageId), err)
	}
	sent, err := strconv.ParseInt(m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("sqs: unable to parse sent timestamp for message %s: %w", aws.ToString(m.MessageId), err)
	}
	return &message{
		id:        aws.ToString(m.MessageId),
		receipt:   aws.ToString(m.ReceiptHandle),
		body:      aws.ToString(m.Body),
		attempt:   attempt,
		createdAt: time.UnixMilli(sent),
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
			err = fmt.Errorf("sqs: handler panicked: %v", r)
		}
	}()
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	return c.handle(ctx, msg)
}

// heartbeat extends the message's visibility while the handler runs
func (q *Queues) heartbeat(ctx context.Context, c *Config, msg *message) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(q.visibility / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Errors are retried on the next tick, well before visibility expires
				if err := q.setVisibility(ctx, c, msg, q.visibility); err != nil && ctx.Err() == nil {
					q.log.Warn("unable to extend visibility", "queue", c.queue, "job", msg.id, "error", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// finish records the handler's result. Failed messages become visible again
// after a backoff, and the queue's redrive policy decides when to give up.
func (q *Queues) finish(ctx context.Context, c *Config, msg *message, err error) error {
	// Record the result even if we're shutting down
	shutdown := ctx.Err() != nil
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	log := q.log.With("queue", c.queue, "job", msg.id, "attempt", msg.attempt)
	switch {
	case err == nil:
		return q.delete(ctx, c, msg)
	case jobq.IsPermanent(err):
		// Honored even during shutdown, since it should never be retried
		return q.deadLetter(ctx, log, c, msg, err)
	case shutdown:
		// Interrupted by shutdown, so release the message right away
		return q.setVisibility(ctx, c, msg, 0)
	default:
		delay := backoff.Delay(msg.attempt)
		if c.redrive != nil && msg.attempt >= c.redrive.maxReceives {
			log.Error("job failed, moving to dead-letter queue", "error", err)
		} else {
			log.Warn("job failed, retrying", "error", err, "delay", delay)
		}
		return q.setVisibility(ctx, c, msg, delay)
	}
}

// deadLetter moves a permanently failed message to the dead-letter queue, or
// drops it if there isn't one
func (q *Queues) deadLetter(ctx context.Context, log *slog.Logger, c *Config, msg *message, err error) error {
	if c.redrive == nil {
		log.Error("job failed permanently, dropping it since there's no dead-letter queue", "error", err)
		return q.delete(ctx, c, msg)
	}
	log.Error("job failed permanently, moving to dead-letter queue", "error", err)
	if _, err := q.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    aws.String(c.redrive.dlqURL),
		MessageBody: aws.String(msg.body),
	}); err != nil {
		return fmt.Errorf("sqs: unable to dead-letter message %s: %w", msg.id, err)
	}
	return q.delete(ctx, c, msg)
}

func (q *Queues) delete(ctx context.Context, c *Config, msg *message) error {
	if _, err := q.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueURL(c.queue)),
		ReceiptHandle: aws.String(msg.receipt),
	}); err != nil {
		return fmt.Errorf("sqs: unable to delete message %s: %w", msg.id, err)
	}
	return nil
}

func (q *Queues) setVisibility(ctx context.Context, c *Config, msg *message, timeout time.Duration) error {
	if _, err := q.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(q.queueURL(c.queue)),
		ReceiptHandle:     aws.String(msg.receipt),
		VisibilityTimeout: int32(timeout.Seconds()),
	}); err != nil {
		return fmt.Errorf("sqs: unable to change visibility of message %s: %w", msg.id, err)
	}
	return nil
}
