package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/matthewmueller/jobq"
	"golang.org/x/sync/errgroup"
)

type Payload = jobq.Payload
type Job[T Payload] = jobq.Job[T]
type Handler[T Payload] = jobq.Handler[T]

// Dial connects to SQS. The url is the account's queue prefix (e.g.
// https://sqs.us-west-2.amazonaws.com/123456789012) and each payload's queue
// lives at url/<Payload.Queue()>. Queues must already exist. Credentials come
// from the default AWS config (e.g. AWS_PROFILE).
func Dial(ctx context.Context, url string) (*Queues, error) {
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
	handle      func(ctx context.Context, msg *message) error
}

// Concurrency sets the maximum number of jobs this process runs at once for
// the queue. Defaults to 1.
func (c *Config) Concurrency(n int) *Config {
	c.concurrency = n
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
				return fmt.Errorf("sqs: unable to decode message %s: %w", msg.id, err)
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

// work receives and runs jobs until ctx is cancelled
func (q *Queues) work(ctx context.Context, c *Config) error {
	for ctx.Err() == nil {
		msg, err := q.receive(ctx, c)
		if err != nil {
			return err
		}
		if msg == nil {
			continue
		}
		if err := q.run(ctx, c, msg); err != nil {
			return err
		}
	}
	return nil
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
	err := call(ctx, c, msg)
	stop()
	return q.finish(ctx, c, msg, err)
}

// call invokes the handler, converting panics into errors
func call(ctx context.Context, c *Config, msg *message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sqs: handler panicked: %v", r)
		}
	}()
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
				q.setVisibility(ctx, c, msg, q.visibility)
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
	switch {
	case err == nil:
		if _, err := q.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
			QueueUrl:      aws.String(q.queueURL(c.queue)),
			ReceiptHandle: aws.String(msg.receipt),
		}); err != nil {
			return fmt.Errorf("sqs: unable to delete message %s: %w", msg.id, err)
		}
		return nil
	case shutdown:
		// Interrupted by shutdown, so release the message right away
		return q.setVisibility(ctx, c, msg, 0)
	default:
		return q.setVisibility(ctx, c, msg, backoff(msg.attempt))
	}
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

// backoff returns how long to wait before retrying a failed attempt. Retries
// are unbounded until the redrive policy moves the message to a dead-letter
// queue, so back off exponentially to avoid hot-looping.
func backoff(attempt int) time.Duration {
	if attempt > 10 {
		return 15 * time.Minute
	}
	return min(time.Second<<(attempt-1), 15*time.Minute)
}
