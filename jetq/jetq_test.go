package jetq_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/matthewmueller/jobq/jetq"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// serve starts an embedded NATS server with JetStream and returns its URL
func serve(t testing.TB) string {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server isn't ready")
	}
	return ns.ClientURL()
}

// logger writes to the test's output, shown for failed or verbose tests
func logger(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), nil))
}

// dial connects a Queues to the server at url
func dial(t testing.TB, url string) *jetq.Queues {
	t.Helper()
	queues, err := jetq.Dial(context.Background(), logger(t), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { queues.Close() })
	return queues
}

// client returns a raw JetStream client for setting up and inspecting jobs
func client(t testing.TB, url string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// start runs the queues in the background. Calling stop cancels and waits.
func start(t testing.TB, queues *jetq.Queues) (stop func()) {
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
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting to receive")
		panic("unreachable")
	}
}

// waitFor polls until the queue's stats match
func waitFor(t testing.TB, queues *jetq.Queues, queue string, want jetq.Stats) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats, err := queues.Stats(context.Background(), queue)
		if err != nil {
			t.Fatal(err)
		}
		if *stats == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %+v in %q, got %+v", want, queue, *stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// deadLetter returns the last dead-lettered message for the queue
func deadLetter(t testing.TB, js jetstream.JetStream, queue string) *jetstream.RawStreamMsg {
	t.Helper()
	ctx := context.Background()
	stream, err := js.Stream(ctx, "JOBQ_DEAD")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, "jobq.dead."+queue)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

type createUser struct {
	Name string
}

func (createUser) Queue() string { return "test.create_user" }

type users struct {
	jobs chan *jetq.Job[createUser]
}

func (u *users) Create(ctx context.Context, job *jetq.Job[createUser]) error {
	u.jobs <- job
	return nil
}

func TestPushAndProcess(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	u := &users{jobs: make(chan *jetq.Job[createUser], 1)}
	queues.Queue(u.Create)
	is.NoErr(queues.Push(ctx, createUser{Name: "alice"}))
	stop := start(t, queues)
	job := receive(t, u.jobs)
	is.Equal(job.Data.Name, "alice")
	is.Equal(job.ID, "1")
	is.Equal(job.Attempt, 1)
	is.True(!job.CreatedAt.IsZero())
	waitFor(t, queues, "test.create_user", jetq.Stats{}) // acked and removed
	stop()
}

func TestPushWithoutRegistration(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	producer := dial(t, url)
	consumer := dial(t, url)
	is.NoErr(producer.Push(ctx, createUser{Name: "bob"}))
	u := &users{jobs: make(chan *jetq.Job[createUser], 1)}
	consumer.Queue(u.Create)
	stop := start(t, consumer)
	job := receive(t, u.jobs)
	is.Equal(job.Data.Name, "bob")
	waitFor(t, consumer, "test.create_user", jetq.Stats{})
	stop()
}

func TestPushPointer(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	u := &users{jobs: make(chan *jetq.Job[createUser], 2)}
	queues.Queue(u.Create)
	is.NoErr(queues.Push(ctx, &createUser{Name: "dave"}))
	is.NoErr(queues.Push(ctx, createUser{Name: "erin"}))
	stop := start(t, queues)
	is.Equal(receive(t, u.jobs).Data.Name, "dave")
	is.Equal(receive(t, u.jobs).Data.Name, "erin")
	waitFor(t, queues, "test.create_user", jetq.Stats{})
	stop()
}

type sendEmail struct {
	To string
}

func (sendEmail) Queue() string { return "test.send_email" }

type mailer struct {
	succeedOn int
	attempts  chan int
}

func (m *mailer) Send(ctx context.Context, job *jetq.Job[sendEmail]) error {
	m.attempts <- job.Attempt
	if job.Attempt == m.succeedOn {
		return nil
	}
	return errors.New("smtp unavailable")
}

func TestRetries(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	m := &mailer{succeedOn: 3, attempts: make(chan int, 10)}
	queues.Queue(m.Send).Retries(2)
	is.NoErr(queues.Push(ctx, sendEmail{To: "a@example.com"}))
	stop := start(t, queues)
	is.Equal(receive(t, m.attempts), 1)
	is.Equal(receive(t, m.attempts), 2)
	is.Equal(receive(t, m.attempts), 3)
	waitFor(t, queues, "test.send_email", jetq.Stats{})
	stop()
}

func TestRetriesExhausted(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	queues := dial(t, url)
	m := &mailer{succeedOn: -1, attempts: make(chan int, 10)}
	queues.Queue(m.Send).Retries(1)
	is.NoErr(queues.Push(ctx, sendEmail{To: "a@example.com"}))
	stop := start(t, queues)
	waitFor(t, queues, "test.send_email", jetq.Stats{Failed: 1})
	stop()
	is.Equal(len(m.attempts), 2)
	msg := deadLetter(t, client(t, url), "test.send_email")
	is.Equal(msg.Header.Get("Jobq-Error"), "smtp unavailable")
	is.Equal(msg.Header.Get("Jobq-Attempts"), "2")
	is.Equal(string(msg.Data), `{"To":"a@example.com"}`)
}

type resizeImage struct {
	Path string
}

func (resizeImage) Queue() string { return "test.resize_image" }

type resizer struct {
	started chan string
	release chan struct{}
}

func (r *resizer) Resize(ctx context.Context, job *jetq.Job[resizeImage]) error {
	r.started <- job.Data.Path
	<-r.release
	return nil
}

func TestConcurrency(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
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
	case <-time.After(300 * time.Millisecond):
	}
	close(r.release)
	receive(t, r.started)
	waitFor(t, queues, "test.resize_image", jetq.Stats{})
	stop()
}

type chargeCard struct {
	N int
}

func (chargeCard) Queue() string { return "test.charge_card" }

type billing struct {
	mu    sync.Mutex
	seen  map[string]int
	total int
}

func (b *billing) Charge(ctx context.Context, job *jetq.Job[chargeCard]) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen[job.ID]++
	b.total++
	return nil
}

func TestCompetingWorkers(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	a := dial(t, url)
	b := dial(t, url)
	bill := &billing{seen: map[string]int{}}
	a.Queue(bill.Charge).Concurrency(4)
	b.Queue(bill.Charge).Concurrency(4)
	for i := range 50 {
		is.NoErr(a.Push(ctx, chargeCard{N: i}))
	}
	stopA := start(t, a)
	stopB := start(t, b)
	waitFor(t, a, "test.charge_card", jetq.Stats{})
	stopA()
	stopB()
	bill.mu.Lock()
	defer bill.mu.Unlock()
	is.Equal(bill.total, 50)
	is.Equal(len(bill.seen), 50)
	for _, n := range bill.seen {
		is.Equal(n, 1) // each job handled exactly once
	}
}

type longTask struct{}

func (longTask) Queue() string { return "test.long_task" }

type worker struct {
	started chan struct{}
}

func (w *worker) Run(ctx context.Context, job *jetq.Job[longTask]) error {
	close(w.started)
	<-ctx.Done()
	return ctx.Err()
}

type recorder struct {
	attempts chan int
}

func (r *recorder) Run(ctx context.Context, job *jetq.Job[longTask]) error {
	r.attempts <- job.Attempt
	return nil
}

func TestShutdown(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	queues := dial(t, url)
	w := &worker{started: make(chan struct{})}
	queues.Queue(w.Run)
	is.NoErr(queues.Push(ctx, longTask{}))
	stop := start(t, queues)
	receive(t, w.started)
	stop()
	// Released right away rather than waiting out the 30s lease. The
	// interrupted delivery counts as an attempt, but isn't dead-lettered.
	next := dial(t, url)
	r := &recorder{attempts: make(chan int, 1)}
	next.Queue(r.Run)
	stop = start(t, next)
	is.Equal(receive(t, r.attempts), 2)
	waitFor(t, next, "test.long_task", jetq.Stats{})
	stop()
}

type importFile struct {
	Path string
}

func (importFile) Queue() string { return "test.import_file" }

type importer struct {
	attempts chan int
}

func (i *importer) Import(ctx context.Context, job *jetq.Job[importFile]) error {
	i.attempts <- job.Attempt
	return jetq.Permanent(errors.New("unsupported format"))
}

func TestPermanent(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	queues := dial(t, url)
	i := &importer{attempts: make(chan int, 10)}
	queues.Queue(i.Import).Retries(3)
	is.NoErr(queues.Push(ctx, importFile{Path: "a.xls"}))
	stop := start(t, queues)
	waitFor(t, queues, "test.import_file", jetq.Stats{Failed: 1})
	stop()
	is.Equal(len(i.attempts), 1) // not retried
	msg := deadLetter(t, client(t, url), "test.import_file")
	is.Equal(msg.Header.Get("Jobq-Error"), "unsupported format")
}

func TestDecodeError(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	queues := dial(t, url)
	js := client(t, url)
	u := &users{jobs: make(chan *jetq.Job[createUser], 1)}
	queues.Queue(u.Create).Retries(3)
	_, err := js.Publish(ctx, "jobq.jobs.test.create_user", []byte(`"not an object"`))
	is.NoErr(err)
	stop := start(t, queues)
	waitFor(t, queues, "test.create_user", jetq.Stats{Failed: 1})
	stop()
	is.Equal(len(u.jobs), 0)
	msg := deadLetter(t, js, "test.create_user")
	is.True(strings.Contains(msg.Header.Get("Jobq-Error"), "unable to decode"))
	is.Equal(msg.Header.Get("Jobq-Attempts"), "1") // not retried
}

type slowTask struct{}

func (slowTask) Queue() string { return "test.slow_task" }

type sleeper struct {
	errs chan error
}

func (s *sleeper) Run(ctx context.Context, job *jetq.Job[slowTask]) error {
	<-ctx.Done()
	s.errs <- ctx.Err()
	return ctx.Err()
}

func TestTimeout(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	queues := dial(t, url)
	s := &sleeper{errs: make(chan error, 1)}
	queues.Queue(s.Run).Timeout(50 * time.Millisecond)
	is.NoErr(queues.Push(ctx, slowTask{}))
	stop := start(t, queues)
	is.Equal(receive(t, s.errs), context.DeadlineExceeded)
	waitFor(t, queues, "test.slow_task", jetq.Stats{Failed: 1})
	stop()
	msg := deadLetter(t, client(t, url), "test.slow_task")
	is.Equal(msg.Header.Get("Jobq-Error"), "context deadline exceeded")
}

type flaky struct {
	mu    sync.Mutex
	calls int
	done  chan int
}

// Send fails the first call and succeeds afterwards
func (f *flaky) Send(ctx context.Context, job *jetq.Job[sendEmail]) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == 1 {
		return errors.New("smtp unavailable")
	}
	f.done <- job.Attempt
	return nil
}

func TestRevive(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	f := &flaky{done: make(chan int, 1)}
	queues.Queue(f.Send)
	is.NoErr(queues.Push(ctx, sendEmail{To: "a@example.com"}))
	stop := start(t, queues)
	waitFor(t, queues, "test.send_email", jetq.Stats{Failed: 1})
	is.NoErr(queues.Revive(ctx, "test.send_email"))
	is.Equal(receive(t, f.done), 1) // attempts start over
	waitFor(t, queues, "test.send_email", jetq.Stats{})
	stop()
}

func TestStats(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	is.NoErr(queues.Push(ctx, createUser{Name: "alice"}))
	is.NoErr(queues.Push(ctx, createUser{Name: "bob"}))
	stats, err := queues.Stats(ctx, "test.create_user")
	is.NoErr(err)
	is.Equal(stats, &jetq.Stats{Pending: 2})
	stats, err = queues.Stats(ctx, "test.unknown")
	is.NoErr(err)
	is.Equal(stats, &jetq.Stats{})
}

type dotted struct{}

func (dotted) Queue() string { return "a.b" }

type underscored struct{}

func (underscored) Queue() string { return "a_b" }

type wildcard struct{}

func (wildcard) Queue() string { return "a.*" }

type handlers struct{}

func (handlers) Dotted(ctx context.Context, job *jetq.Job[dotted]) error           { return nil }
func (handlers) Underscored(ctx context.Context, job *jetq.Job[underscored]) error { return nil }
func (handlers) Wildcard(ctx context.Context, job *jetq.Job[wildcard]) error       { return nil }

func TestInvalidConfig(t *testing.T) {
	is := is.New(t)
	queues := dial(t, serve(t))
	w := &worker{started: make(chan struct{})}
	queues.Queue(w.Run).Concurrency(0)
	queues.Queue(w.Run).Retries(-1).Timeout(-time.Second)
	queues.Queue(handlers{}.Dotted)
	queues.Queue(handlers{}.Underscored)
	queues.Queue(handlers{}.Wildcard)
	err := queues.Start(context.Background())
	is.True(err != nil)
	is.Equal(err.Error(), `jetq: "test.long_task" concurrency must be at least 1
jetq: "test.long_task" retries must not be negative
jetq: "test.long_task" timeout must not be negative
jetq: "test.long_task" is registered more than once
jetq: "a.b" and "a_b" would share the consumer "a_b"
jetq: queue name "a.*" must be dot-separated tokens of letters, numbers, hyphens and underscores`)
}

type emptyToken struct{}

func (emptyToken) Queue() string { return "a..b" }

func TestPushInvalidName(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	err := queues.Push(ctx, wildcard{})
	is.True(err != nil)
	is.Equal(err.Error(), `jetq: queue name "a.*" must be dot-separated tokens of letters, numbers, hyphens and underscores`)
	err = queues.Push(ctx, emptyToken{})
	is.True(err != nil)
	is.Equal(err.Error(), `jetq: queue name "a..b" must be dot-separated tokens of letters, numbers, hyphens and underscores`)
}

type RunSession struct {
	SessionID string
}

func (RunSession) Queue() string { return "session.run" }

type session struct{}

func (s *session) Run(ctx context.Context, job *jetq.Job[RunSession]) error {
	fmt.Println("running session", job.Data.SessionID)
	return nil
}

func Example() {
	ctx := context.Background()
	// Creates the JOBQ and JOBQ_DEAD streams if needed
	queues, err := jetq.Dial(ctx, slog.Default(), "nats://localhost:4222")
	if err != nil {
		panic(err)
	}
	defer queues.Close()

	// Configure consumers. Payload types are inferred from the handlers.
	session := &session{}
	queues.Queue(session.Run).
		Concurrency(4).
		Retries(3)

	// Produce jobs from anywhere with *jetq.Queues. No queue names needed.
	if err := queues.Push(ctx, RunSession{SessionID: "123"}); err != nil {
		panic(err)
	}

	// Start blocks until ctx is cancelled
	if err := queues.Start(ctx); err != nil {
		panic(err)
	}
}

func TestPushIn(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	u := &users{jobs: make(chan *jetq.Job[createUser], 1)}
	queues.Queue(u.Create)
	stop := start(t, queues)
	pushed := time.Now()
	is.NoErr(queues.PushIn(ctx, time.Second, createUser{Name: "alice"}))
	select {
	case <-u.jobs:
		t.Fatal("delayed job ran early")
	case <-time.After(500 * time.Millisecond):
	}
	is.Equal(receive(t, u.jobs).Data.Name, "alice")
	is.True(time.Since(pushed) >= time.Second)
	stop()
}

func TestPushAt(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	u := &users{jobs: make(chan *jetq.Job[createUser], 1)}
	queues.Queue(u.Create)
	stop := start(t, queues)
	at := time.Now().Add(time.Second)
	is.NoErr(queues.PushAt(ctx, at, createUser{Name: "alice"}))
	is.Equal(receive(t, u.jobs).Data.Name, "alice")
	is.True(!time.Now().Before(at))
	// A time that has passed runs right away
	pushed := time.Now()
	is.NoErr(queues.PushAt(ctx, pushed.Add(-time.Hour), createUser{Name: "bob"}))
	is.Equal(receive(t, u.jobs).Data.Name, "bob")
	is.True(time.Since(pushed) < time.Second)
	stop()
}

func TestDelayedStats(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	queues := dial(t, serve(t))
	u := &users{jobs: make(chan *jetq.Job[createUser], 1)}
	queues.Queue(u.Create)
	is.NoErr(queues.PushIn(ctx, time.Second, createUser{Name: "alice"}))
	waitFor(t, queues, "test.create_user", jetq.Stats{Pending: 1})
	stop := start(t, queues)
	is.Equal(receive(t, u.jobs).Data.Name, "alice")
	// The schedule is removed once it fires
	waitFor(t, queues, "test.create_user", jetq.Stats{})
	stop()
}

func TestDelayedUpgrade(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	url := serve(t)
	// The stream as created by v0.0.4 and earlier
	_, err := client(t, url).CreateStream(ctx, jetstream.StreamConfig{
		Name:      "JOBQ",
		Subjects:  []string{"jobq.jobs.>"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
	is.NoErr(err)
	queues := dial(t, url)
	u := &users{jobs: make(chan *jetq.Job[createUser], 1)}
	queues.Queue(u.Create)
	stop := start(t, queues)
	is.NoErr(queues.PushIn(ctx, time.Second, createUser{Name: "alice"}))
	is.Equal(receive(t, u.jobs).Data.Name, "alice")
	stop()
}
