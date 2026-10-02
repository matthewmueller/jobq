package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/matryer/is"
	"github.com/matthewmueller/jobq/sqs"
)

// queueURL is the full URL of an existing SQS queue reserved for tests
func queueURL() string {
	return os.Getenv("SQS_QUEUE")
}

// testQueue is the test queue's name. Every test payload uses it, since the
// test credentials only have access to a single queue.
func testQueue() string {
	return queueURL()[strings.LastIndex(queueURL(), "/")+1:]
}

// logger writes to the test's output, shown for failed or verbose tests
func logger(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), nil))
}

// dial connects a Queues, skipping the test if SQS isn't configured
func dial(t testing.TB) *sqs.Queues {
	t.Helper()
	if queueURL() == "" {
		t.Skip("SQS_QUEUE not set")
	}
	base := strings.TrimSuffix(queueURL(), "/"+testQueue())
	queues, err := sqs.Dial(context.Background(), logger(t), base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { queues.Close() })
	return queues
}

// client returns a raw SQS client for inspecting the test queue and drains it
// along with its dead-letter queue. Call after dial.
func client(t testing.TB) *awssqs.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := awssqs.NewFromConfig(cfg)
	drain(t, client, queueURL())
	if url := dlqURL(t, client); url != "" {
		drain(t, client, url)
	}
	return client
}

// dlqURL returns the test queue's dead-letter queue URL, or "" without one
func dlqURL(t testing.TB, client *awssqs.Client) string {
	t.Helper()
	out, err := client.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL()),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := out.Attributes[string(types.QueueAttributeNameRedrivePolicy)]
	if policy == "" {
		return ""
	}
	var p struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}
	if err := json.Unmarshal([]byte(policy), &p); err != nil {
		t.Fatal(err)
	}
	name := p.DeadLetterTargetArn[strings.LastIndex(p.DeadLetterTargetArn, ":")+1:]
	return strings.TrimSuffix(queueURL(), testQueue()) + name
}

// requireDLQ returns the dead-letter queue URL, skipping the test without one
func requireDLQ(t testing.TB, client *awssqs.Client) string {
	t.Helper()
	url := dlqURL(t, client)
	if url == "" {
		t.Skip("test queue has no redrive policy")
	}
	return url
}

// drain deletes and returns every visible message in the queue
func drain(t testing.TB, client *awssqs.Client, url string) []types.Message {
	t.Helper()
	ctx := context.Background()
	var drained []types.Message
	for {
		out, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Messages) == 0 {
			return drained
		}
		for _, msg := range out.Messages {
			if _, err := client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
				QueueUrl:      aws.String(url),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				t.Fatal(err)
			}
		}
		drained = append(drained, out.Messages...)
	}
}

// arrived waits until a message is in the queue, leaving it visible. SQS
// counts are approximate, so this receives the message to be sure.
func arrived(t testing.TB, client *awssqs.Client, url string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:        aws.String(url),
			WaitTimeSeconds: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Messages) == 0 {
			continue
		}
		if _, err := client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(url),
			ReceiptHandle:     out.Messages[0].ReceiptHandle,
			VisibilityTimeout: 0,
		}); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatalf("timed out waiting for a message in %s", url)
}

// start runs the queues in the background. Calling stop cancels and waits.
func start(t testing.TB, queues *sqs.Queues) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- queues.Start(ctx) }()
	wait := sync.OnceValue(func() error {
		cancel()
		return <-errc
	})
	// Stop the workers even if the test fails before calling stop
	t.Cleanup(func() {
		if err := wait(); err != nil {
			t.Error(err)
		}
	})
	return func() {
		t.Helper()
		if err := wait(); err != nil {
			t.Fatal(err)
		}
	}
}

func receive[T any](t testing.TB, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting to receive")
		panic("unreachable")
	}
}

type createUser struct {
	Name string
}

func (createUser) Queue() string { return testQueue() }

type users struct {
	jobs chan *sqs.Job[createUser]
}

func (u *users) Create(ctx context.Context, job *sqs.Job[createUser]) error {
	u.jobs <- job
	return nil
}

func TestPushAndProcess(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	u := &users{jobs: make(chan *sqs.Job[createUser], 1)}
	queues.Queue(u.Create)
	is.NoErr(queues.Push(ctx, createUser{Name: "alice"}))
	stop := start(t, queues)
	job := receive(t, u.jobs)
	stop()
	is.Equal(job.Data.Name, "alice")
	is.True(job.ID != "")
	is.Equal(job.Attempt, 1)
	is.True(time.Since(job.CreatedAt) < time.Minute)
	is.Equal(len(drain(t, client, queueURL())), 0) // deleted once handled
}

func TestPushWithoutRegistration(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	producer := dial(t)
	consumer := dial(t)
	client := client(t)
	is.NoErr(producer.Push(ctx, createUser{Name: "bob"}))
	u := &users{jobs: make(chan *sqs.Job[createUser], 1)}
	consumer.Queue(u.Create)
	stop := start(t, consumer)
	job := receive(t, u.jobs)
	stop()
	is.Equal(job.Data.Name, "bob")
	is.Equal(len(drain(t, client, queueURL())), 0)
}

func TestPushPointer(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	u := &users{jobs: make(chan *sqs.Job[createUser], 2)}
	queues.Queue(u.Create)
	is.NoErr(queues.Push(ctx, &createUser{Name: "dave"}))
	is.NoErr(queues.Push(ctx, createUser{Name: "erin"}))
	stop := start(t, queues)
	// SQS doesn't guarantee order
	names := map[string]bool{
		receive(t, u.jobs).Data.Name: true,
		receive(t, u.jobs).Data.Name: true,
	}
	stop()
	is.Equal(names, map[string]bool{"dave": true, "erin": true})
	is.Equal(len(drain(t, client, queueURL())), 0)
}

type sendEmail struct {
	To string
}

func (sendEmail) Queue() string { return testQueue() }

type mailer struct {
	succeedOn int
	attempts  chan int
}

func (m *mailer) Send(ctx context.Context, job *sqs.Job[sendEmail]) error {
	m.attempts <- job.Attempt
	if job.Attempt == m.succeedOn {
		return nil
	}
	return errors.New("smtp unavailable")
}

func TestRetries(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	m := &mailer{succeedOn: 3, attempts: make(chan int, 10)}
	queues.Queue(m.Send)
	is.NoErr(queues.Push(ctx, sendEmail{To: "a@example.com"}))
	stop := start(t, queues)
	is.Equal(receive(t, m.attempts), 1)
	is.Equal(receive(t, m.attempts), 2)
	is.Equal(receive(t, m.attempts), 3)
	stop()
	is.Equal(len(drain(t, client, queueURL())), 0)
}

type resizeImage struct {
	Path string
}

func (resizeImage) Queue() string { return testQueue() }

type resizer struct {
	started chan string
	release chan struct{}
}

func (r *resizer) Resize(ctx context.Context, job *sqs.Job[resizeImage]) error {
	r.started <- job.Data.Path
	<-r.release
	return nil
}

func TestConcurrency(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	r := &resizer{started: make(chan string, 4), release: make(chan struct{})}
	queues.Queue(r.Resize).Concurrency(3)
	for _, path := range []string{"a.png", "b.png", "c.png", "d.png"} {
		is.NoErr(queues.Push(ctx, resizeImage{Path: path}))
	}
	stop := start(t, queues)
	// Three jobs run at once
	receive(t, r.started)
	receive(t, r.started)
	receive(t, r.started)
	// The fourth waits for a free worker
	select {
	case <-r.started:
		is.Fail() // expected at most 3 concurrent jobs
	case <-time.After(2 * time.Second):
	}
	close(r.release)
	receive(t, r.started)
	stop()
	is.Equal(len(drain(t, client, queueURL())), 0)
}

type chargeCard struct {
	N int
}

func (chargeCard) Queue() string { return testQueue() }

type billing struct {
	mu   sync.Mutex
	seen map[string]int
	done chan struct{}
}

func (b *billing) Charge(ctx context.Context, job *sqs.Job[chargeCard]) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen[job.ID]++
	b.done <- struct{}{}
	return nil
}

func TestCompetingWorkers(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	a := dial(t)
	b := dial(t)
	client := client(t)
	bill := &billing{seen: map[string]int{}, done: make(chan struct{}, 40)}
	a.Queue(bill.Charge).Concurrency(4)
	b.Queue(bill.Charge).Concurrency(4)
	for i := range 20 {
		is.NoErr(a.Push(ctx, chargeCard{N: i}))
	}
	stopA := start(t, a)
	stopB := start(t, b)
	for range 20 {
		receive(t, bill.done)
	}
	stopA()
	stopB()
	is.Equal(len(drain(t, client, queueURL())), 0)
	bill.mu.Lock()
	defer bill.mu.Unlock()
	is.Equal(len(bill.seen), 20)
	for _, n := range bill.seen {
		is.Equal(n, 1) // each job handled exactly once
	}
}

type longTask struct{}

func (longTask) Queue() string { return testQueue() }

type worker struct {
	started chan struct{}
}

func (w *worker) Run(ctx context.Context, job *sqs.Job[longTask]) error {
	close(w.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestShutdown(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	w := &worker{started: make(chan struct{})}
	queues.Queue(w.Run)
	is.NoErr(queues.Push(ctx, longTask{}))
	stop := start(t, queues)
	receive(t, w.started)
	stop()
	// Released right away rather than waiting out the visibility timeout
	msgs := drain(t, client, queueURL())
	is.Equal(len(msgs), 1)
	is.Equal(msgs[0].Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)], "2")
}

type badName struct{}

func (badName) Queue() string { return "session.run" }

type invalid struct{}

func (invalid) Run(ctx context.Context, job *sqs.Job[badName]) error { return nil }

func TestInvalidConfig(t *testing.T) {
	is := is.New(t)
	queues := dial(t)
	w := &worker{started: make(chan struct{})}
	queues.Queue(w.Run).Concurrency(0)
	queues.Queue(w.Run).Timeout(13 * time.Hour)
	queues.Queue(invalid{}.Run)
	err := queues.Start(context.Background())
	is.True(err != nil)
	is.Equal(err.Error(), `sqs: "`+testQueue()+`" concurrency must be at least 1
sqs: "`+testQueue()+`" timeout must be between 0 and 12 hours
sqs: "`+testQueue()+`" is registered more than once
sqs: queue name "session.run" may only contain letters, numbers, hyphens and underscores`)
}

func TestPushInvalidName(t *testing.T) {
	is := is.New(t)
	queues := dial(t)
	err := queues.Push(context.Background(), badName{})
	is.True(err != nil)
	is.Equal(err.Error(), `sqs: queue name "session.run" may only contain letters, numbers, hyphens and underscores`)
}

type slowTask struct{}

func (slowTask) Queue() string { return testQueue() }

type sleeper struct {
	errs chan error
}

// Run times out on the first attempt and succeeds on the second
func (s *sleeper) Run(ctx context.Context, job *sqs.Job[slowTask]) error {
	if job.Attempt > 1 {
		s.errs <- nil
		return nil
	}
	<-ctx.Done()
	s.errs <- ctx.Err()
	return ctx.Err()
}

func TestTimeout(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	s := &sleeper{errs: make(chan error, 2)}
	queues.Queue(s.Run).Timeout(50 * time.Millisecond)
	is.NoErr(queues.Push(ctx, slowTask{}))
	stop := start(t, queues)
	is.Equal(receive(t, s.errs), context.DeadlineExceeded)
	is.Equal(receive(t, s.errs), nil) // retried
	stop()
	is.Equal(len(drain(t, client, queueURL())), 0)
}

func TestStats(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	is.NoErr(queues.Push(ctx, createUser{Name: "alice"}))
	is.NoErr(queues.Push(ctx, createUser{Name: "bob"}))
	// SQS counts are approximate and eventually consistent
	deadline := time.Now().Add(time.Minute)
	for {
		stats, err := queues.Stats(ctx, testQueue())
		is.NoErr(err)
		if stats.Pending == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected 2 pending messages, got %+v", stats)
		}
		time.Sleep(time.Second)
	}
	is.Equal(len(drain(t, client, queueURL())), 2)
}

type importFile struct {
	Path string
}

func (importFile) Queue() string { return testQueue() }

type importer struct {
	mu    sync.Mutex
	calls int
	done  chan int
}

// Import fails permanently on the first call and succeeds afterwards
func (i *importer) Import(ctx context.Context, job *sqs.Job[importFile]) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	i.done <- i.calls
	if i.calls == 1 {
		return sqs.Permanent(errors.New("unsupported format"))
	}
	return nil
}

func TestPermanent(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	dlq := requireDLQ(t, client)
	i := &importer{done: make(chan int, 2)}
	queues.Queue(i.Import)
	is.NoErr(queues.Push(ctx, importFile{Path: "a.xls"}))
	stop := start(t, queues)
	is.Equal(receive(t, i.done), 1)
	stop()
	is.Equal(len(drain(t, client, queueURL())), 0)
	msgs := drain(t, client, dlq)
	is.Equal(len(msgs), 1)
	is.Equal(aws.ToString(msgs[0].Body), `{"Path":"a.xls"}`)
}

func TestRevive(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t)
	client := client(t)
	dlq := requireDLQ(t, client)
	i := &importer{done: make(chan int, 2)}
	queues.Queue(i.Import)
	is.NoErr(queues.Push(ctx, importFile{Path: "a.xls"}))
	stop := start(t, queues)
	is.Equal(receive(t, i.done), 1)
	// A move task only moves the messages SQS counts when it starts. Counts
	// are approximate and lag, so a message dead-lettered moments ago can be
	// missed. Keep reviving until the message comes back.
	arrived(t, client, dlq)
	for attempt := 1; ; attempt++ {
		if err := queues.Revive(ctx, testQueue()); err != nil {
			t.Log(err) // e.g. the previous move task is still running
		}
		select {
		case calls := <-i.done:
			is.Equal(calls, 2)
		case <-time.After(15 * time.Second):
			if attempt < 8 {
				continue
			}
			t.Fatal("timed out waiting for the revived message")
		}
		break
	}
	stop()
	is.Equal(len(drain(t, client, queueURL())), 0)
}

type RunSession struct {
	SessionID string
}

func (RunSession) Queue() string { return "session-run" }

type InterruptSession struct {
	SessionID string
}

func (InterruptSession) Queue() string { return "session-interrupt" }

type session struct{}

func (s *session) Run(ctx context.Context, job *sqs.Job[RunSession]) error {
	return nil
}

func (s *session) Interrupt(ctx context.Context, job *sqs.Job[InterruptSession]) error {
	return nil
}

func Example() {
	ctx := context.Background()
	// Payloads map to existing queues under this prefix, e.g.
	// https://sqs.us-west-2.amazonaws.com/123456789012/session-run
	queues, err := sqs.Dial(ctx, slog.Default(), "https://sqs.us-west-2.amazonaws.com/123456789012")
	if err != nil {
		panic(err)
	}
	defer queues.Close()

	// Configure consumers. Payload types are inferred from the handlers.
	// Retries come from each queue's redrive policy.
	session := &session{}
	queues.Queue(session.Run).
		Concurrency(4)
	queues.Queue(session.Interrupt).
		Concurrency(5)

	// Produce jobs from anywhere with *sqs.Queues. No queue names needed.
	if err := queues.Push(ctx, RunSession{SessionID: "123"}); err != nil {
		panic(err)
	}

	// Start blocks until ctx is cancelled
	if err := queues.Start(ctx); err != nil {
		panic(err)
	}
}
