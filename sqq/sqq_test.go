package sqq_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/matthewmueller/jobq/sqq"
)

// file returns a fresh database path for the test
func file(t testing.TB) string {
	return filepath.Join(t.TempDir(), "jobs.db")
}

// logger writes to the test's output, shown for failed or verbose tests
func logger(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), nil))
}

// dial opens a Queues on the database at path
func dial(t testing.TB, path string) *sqq.Queues {
	t.Helper()
	queues, err := sqq.Dial(context.Background(), logger(t), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { queues.Close() })
	return queues
}

// database returns a raw connection for inspecting jobs. Call after dial so
// the table exists.
func database(t testing.TB, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// start runs the queues in the background. Calling stop cancels and waits.
func start(t testing.TB, queues *sqq.Queues) (stop func()) {
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

type row struct {
	State     string
	Attempts  int
	LastError *string
}

// waitFor polls until the queue has count jobs, all in the given state
func waitFor(t testing.TB, db *sql.DB, queue, state string, count int) []row {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows := jobs(t, db, queue)
		matched := 0
		for _, r := range rows {
			if r.State == state {
				matched++
			}
		}
		if matched == count && len(rows) == count {
			return rows
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d %q jobs in %q, got %+v", count, state, queue, rows)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func jobs(t testing.TB, db *sql.DB, queue string) []row {
	t.Helper()
	rs, err := db.QueryContext(context.Background(), `SELECT state, attempts, last_error FROM sqq_jobs WHERE queue = ? ORDER BY id`, queue)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var rows []row
	for rs.Next() {
		var r row
		if err := rs.Scan(&r.State, &r.Attempts, &r.LastError); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// crashed inserts a job that looks like its worker died mid-run
func crashed(t testing.TB, db *sql.DB, payload sqq.Payload, attempts int) {
	t.Helper()
	now := time.Now().UnixMilli()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO sqq_jobs (queue, payload, state, attempts, locked_until, run_at, created_at, updated_at)
		VALUES (?1, '{}', 'running', ?2, ?3, ?4, ?4, ?4)
	`, payload.Queue(), attempts, now-1000, now)
	if err != nil {
		t.Fatal(err)
	}
}

type createUser struct {
	Name string
}

func (createUser) Queue() string { return "test.create_user" }

type users struct {
	jobs chan *sqq.Job[createUser]
}

func (u *users) Create(ctx context.Context, job *sqq.Job[createUser]) error {
	u.jobs <- job
	return nil
}

func TestPushAndProcess(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	u := &users{jobs: make(chan *sqq.Job[createUser], 1)}
	queues.Queue(u.Create)
	is.NoErr(queues.Push(ctx, createUser{Name: "alice"}))
	stop := start(t, queues)
	job := receive(t, u.jobs)
	is.Equal(job.Data.Name, "alice")
	is.True(job.ID != "")
	is.Equal(job.Attempt, 1)
	is.True(!job.CreatedAt.IsZero())
	waitFor(t, db, "test.create_user", "completed", 1)
	stop()
}

func TestPushWithoutRegistration(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	producer := dial(t, path)
	consumer := dial(t, path)
	db := database(t, path)
	is.NoErr(producer.Push(ctx, createUser{Name: "bob"}))
	u := &users{jobs: make(chan *sqq.Job[createUser], 1)}
	consumer.Queue(u.Create)
	stop := start(t, consumer)
	job := receive(t, u.jobs)
	is.Equal(job.Data.Name, "bob")
	waitFor(t, db, "test.create_user", "completed", 1)
	stop()
}

func TestPushPointer(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	u := &users{jobs: make(chan *sqq.Job[createUser], 2)}
	queues.Queue(u.Create)
	is.NoErr(queues.Push(ctx, &createUser{Name: "dave"}))
	is.NoErr(queues.Push(ctx, createUser{Name: "erin"}))
	stop := start(t, queues)
	is.Equal(receive(t, u.jobs).Data.Name, "dave")
	is.Equal(receive(t, u.jobs).Data.Name, "erin")
	waitFor(t, db, "test.create_user", "completed", 2)
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

func (m *mailer) Send(ctx context.Context, job *sqq.Job[sendEmail]) error {
	m.attempts <- job.Attempt
	if job.Attempt == m.succeedOn {
		return nil
	}
	return errors.New("smtp unavailable")
}

func TestRetries(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	m := &mailer{succeedOn: 3, attempts: make(chan int, 10)}
	queues.Queue(m.Send).Retries(2)
	is.NoErr(queues.Push(ctx, sendEmail{To: "a@example.com"}))
	stop := start(t, queues)
	is.Equal(receive(t, m.attempts), 1)
	is.Equal(receive(t, m.attempts), 2)
	is.Equal(receive(t, m.attempts), 3)
	rows := waitFor(t, db, "test.send_email", "completed", 1)
	is.Equal(rows[0].Attempts, 3)
	stop()
}

func TestRetriesExhausted(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	m := &mailer{succeedOn: -1, attempts: make(chan int, 10)}
	queues.Queue(m.Send).Retries(1)
	is.NoErr(queues.Push(ctx, sendEmail{To: "a@example.com"}))
	stop := start(t, queues)
	rows := waitFor(t, db, "test.send_email", "failed", 1)
	stop()
	is.Equal(rows[0].Attempts, 2)
	is.True(rows[0].LastError != nil)
	is.Equal(*rows[0].LastError, "smtp unavailable")
	is.Equal(len(m.attempts), 2)
}

type resizeImage struct {
	Path string
}

func (resizeImage) Queue() string { return "test.resize_image" }

type resizer struct {
	started chan string
	release chan struct{}
}

func (r *resizer) Resize(ctx context.Context, job *sqq.Job[resizeImage]) error {
	r.started <- job.Data.Path
	<-r.release
	return nil
}

func TestConcurrency(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
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
	waitFor(t, db, "test.resize_image", "completed", 4)
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

func (b *billing) Charge(ctx context.Context, job *sqq.Job[chargeCard]) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen[job.ID]++
	b.total++
	return nil
}

func TestCompetingWorkers(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	a := dial(t, path)
	b := dial(t, path)
	db := database(t, path)
	bill := &billing{seen: map[string]int{}}
	a.Queue(bill.Charge).Concurrency(4)
	b.Queue(bill.Charge).Concurrency(4)
	for i := range 50 {
		is.NoErr(a.Push(ctx, chargeCard{N: i}))
	}
	stopA := start(t, a)
	stopB := start(t, b)
	waitFor(t, db, "test.charge_card", "completed", 50)
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

func (w *worker) Run(ctx context.Context, job *sqq.Job[longTask]) error {
	close(w.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestShutdown(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	w := &worker{started: make(chan struct{})}
	queues.Queue(w.Run).Retries(3)
	is.NoErr(queues.Push(ctx, longTask{}))
	stop := start(t, queues)
	receive(t, w.started)
	stop()
	rows := jobs(t, db, "test.long_task")
	is.Equal(len(rows), 1)
	is.Equal(rows[0].State, "pending")
	is.Equal(rows[0].Attempts, 0)
}

func TestReclaimExpiredLease(t *testing.T) {
	is := is.New(t)
	path := file(t)
	queues := dial(t, path)
	db := database(t, path)
	m := &mailer{succeedOn: 2, attempts: make(chan int, 10)}
	queues.Queue(m.Send).Retries(1)
	crashed(t, db, sendEmail{}, 1)
	stop := start(t, queues)
	is.Equal(receive(t, m.attempts), 2)
	waitFor(t, db, "test.send_email", "completed", 1)
	stop()
}

func TestReclaimExhausted(t *testing.T) {
	is := is.New(t)
	path := file(t)
	queues := dial(t, path)
	db := database(t, path)
	m := &mailer{succeedOn: 3, attempts: make(chan int, 10)}
	queues.Queue(m.Send).Retries(1)
	crashed(t, db, sendEmail{}, 2)
	stop := start(t, queues)
	rows := waitFor(t, db, "test.send_email", "failed", 1)
	stop()
	is.Equal(*rows[0].LastError, "sqq: lease expired")
	is.Equal(len(m.attempts), 0)
}

func TestInvalidConfig(t *testing.T) {
	is := is.New(t)
	path := file(t)
	queues := dial(t, path)
	w := &worker{started: make(chan struct{})}
	queues.Queue(w.Run).Concurrency(0)
	queues.Queue(w.Run).Retries(-1).Timeout(-time.Second)
	err := queues.Start(context.Background())
	is.True(err != nil)
	is.Equal(err.Error(), `sqq: "test.long_task" concurrency must be at least 1
sqq: "test.long_task" retries must not be negative
sqq: "test.long_task" timeout must not be negative
sqq: "test.long_task" is registered more than once`)
}

func TestMemoryRejected(t *testing.T) {
	is := is.New(t)
	_, err := sqq.Dial(context.Background(), logger(t), ":memory:")
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "in-memory databases aren't supported"))
}

type importFile struct {
	Path string
}

func (importFile) Queue() string { return "test.import_file" }

type importer struct {
	attempts chan int
}

func (i *importer) Import(ctx context.Context, job *sqq.Job[importFile]) error {
	i.attempts <- job.Attempt
	return sqq.Permanent(errors.New("unsupported format"))
}

func TestPermanent(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	i := &importer{attempts: make(chan int, 10)}
	queues.Queue(i.Import).Retries(3)
	is.NoErr(queues.Push(ctx, importFile{Path: "a.xls"}))
	stop := start(t, queues)
	rows := waitFor(t, db, "test.import_file", "failed", 1)
	stop()
	is.Equal(rows[0].Attempts, 1) // not retried
	is.Equal(*rows[0].LastError, "unsupported format")
	is.Equal(len(i.attempts), 1)
}

func TestDecodeError(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	u := &users{jobs: make(chan *sqq.Job[createUser], 1)}
	queues.Queue(u.Create).Retries(3)
	_, err := db.ExecContext(ctx, `INSERT INTO sqq_jobs (queue, payload, run_at, created_at, updated_at) VALUES ('test.create_user', '"not an object"', 0, 0, 0)`)
	is.NoErr(err)
	stop := start(t, queues)
	rows := waitFor(t, db, "test.create_user", "failed", 1)
	stop()
	is.Equal(rows[0].Attempts, 1) // not retried
	is.True(strings.Contains(*rows[0].LastError, "unable to decode"))
	is.Equal(len(u.jobs), 0)
}

type slowTask struct{}

func (slowTask) Queue() string { return "test.slow_task" }

type sleeper struct {
	errs chan error
}

func (s *sleeper) Run(ctx context.Context, job *sqq.Job[slowTask]) error {
	<-ctx.Done()
	s.errs <- ctx.Err()
	return ctx.Err()
}

func TestTimeout(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	s := &sleeper{errs: make(chan error, 1)}
	queues.Queue(s.Run).Timeout(50 * time.Millisecond)
	is.NoErr(queues.Push(ctx, slowTask{}))
	stop := start(t, queues)
	is.Equal(receive(t, s.errs), context.DeadlineExceeded)
	rows := waitFor(t, db, "test.slow_task", "failed", 1)
	stop()
	is.Equal(*rows[0].LastError, "context deadline exceeded")
}

func TestPushTx(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	// Rolled back, so never enqueued
	tx, err := db.BeginTx(ctx, nil)
	is.NoErr(err)
	is.NoErr(queues.PushTx(ctx, tx, createUser{Name: "mallory"}))
	is.NoErr(tx.Rollback())
	// Committed
	tx, err = db.BeginTx(ctx, nil)
	is.NoErr(err)
	is.NoErr(queues.PushTx(ctx, tx, createUser{Name: "carol"}))
	is.NoErr(tx.Commit())
	u := &users{jobs: make(chan *sqq.Job[createUser], 2)}
	queues.Queue(u.Create)
	stop := start(t, queues)
	job := receive(t, u.jobs)
	is.Equal(job.Data.Name, "carol")
	waitFor(t, db, "test.create_user", "completed", 1)
	stop()
}

type flaky struct {
	mu    sync.Mutex
	calls int
	done  chan int
}

// Send fails the first call and succeeds afterwards
func (f *flaky) Send(ctx context.Context, job *sqq.Job[sendEmail]) error {
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
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	db := database(t, path)
	f := &flaky{done: make(chan int, 1)}
	queues.Queue(f.Send)
	is.NoErr(queues.Push(ctx, sendEmail{To: "a@example.com"}))
	stop := start(t, queues)
	waitFor(t, db, "test.send_email", "failed", 1)
	stats, err := queues.Stats(ctx, "test.send_email")
	is.NoErr(err)
	is.Equal(stats.Failed, 1)
	is.NoErr(queues.Revive(ctx, "test.send_email"))
	is.Equal(receive(t, f.done), 1) // attempts start over
	rows := waitFor(t, db, "test.send_email", "completed", 1)
	stop()
	is.Equal(rows[0].LastError, nil)
}

func TestStats(t *testing.T) {
	is := is.New(t)
	path := file(t)
	ctx := context.Background()
	queues := dial(t, path)
	database(t, path)
	is.NoErr(queues.Push(ctx, createUser{Name: "alice"}))
	is.NoErr(queues.Push(ctx, createUser{Name: "bob"}))
	stats, err := queues.Stats(ctx, "test.create_user")
	is.NoErr(err)
	is.Equal(stats, &sqq.Stats{Pending: 2})
	stats, err = queues.Stats(ctx, "test.unknown")
	is.NoErr(err)
	is.Equal(stats, &sqq.Stats{})
}

type RunSession struct {
	SessionID string
}

func (RunSession) Queue() string { return "session.run" }

type InterruptSession struct {
	SessionID string
}

func (InterruptSession) Queue() string { return "session.interrupt" }

type session struct {
	done context.CancelFunc // stops the example once the job is handled
}

func (s *session) Run(ctx context.Context, job *sqq.Job[RunSession]) error {
	fmt.Println("running session", job.Data.SessionID)
	s.done()
	return nil
}

func (s *session) Interrupt(ctx context.Context, job *sqq.Job[InterruptSession]) error {
	fmt.Println("interrupting session", job.Data.SessionID)
	s.done()
	return nil
}

func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir, err := os.MkdirTemp("", "sqq")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	queues, err := sqq.Dial(ctx, slog.Default(), filepath.Join(dir, "jobs.db"))
	if err != nil {
		panic(err)
	}
	defer queues.Close()

	// Configure consumers. Payload types are inferred from the handlers.
	session := &session{done: cancel}
	queues.Queue(session.Run).
		Concurrency(4).
		Retries(3)
	queues.Queue(session.Interrupt).
		Concurrency(5)

	// Produce jobs from anywhere with *sqq.Queues. No queue names needed.
	if err := queues.Push(ctx, RunSession{SessionID: "123"}); err != nil {
		panic(err)
	}

	// Start blocks until ctx is cancelled
	if err := queues.Start(ctx); err != nil {
		panic(err)
	}
	// Output: running session 123
}

func ExampleQueues_Push() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := os.MkdirTemp("", "sqq")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "jobs.db")

	// A producer pushes jobs without registering any handlers
	producer, err := sqq.Dial(ctx, slog.Default(), path)
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	if err := producer.Push(ctx, InterruptSession{SessionID: "456"}); err != nil {
		panic(err)
	}

	// A consumer, typically another process, handles them
	consumer, err := sqq.Dial(ctx, slog.Default(), path)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	session := &session{done: cancel}
	consumer.Queue(session.Interrupt)
	if err := consumer.Start(ctx); err != nil {
		panic(err)
	}
	// Output: interrupting session 456
}

type SendWelcome struct {
	Email string
}

func (SendWelcome) Queue() string { return "welcome.send" }

type welcome struct {
	done context.CancelFunc
}

func (w *welcome) Send(ctx context.Context, job *sqq.Job[SendWelcome]) error {
	if job.Attempt < 3 {
		fmt.Println("attempt", job.Attempt, "failed")
		return errors.New("smtp unavailable")
	}
	fmt.Println("attempt", job.Attempt, "sent to", job.Data.Email)
	w.done()
	return nil
}

func ExampleConfig_Retries() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir, err := os.MkdirTemp("", "sqq")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	queues, err := sqq.Dial(ctx, slog.Default(), filepath.Join(dir, "jobs.db"))
	if err != nil {
		panic(err)
	}
	defer queues.Close()

	// Failed jobs are retried up to twice, so the third attempt succeeds
	welcome := &welcome{done: cancel}
	queues.Queue(welcome.Send).Retries(2)

	if err := queues.Push(ctx, SendWelcome{Email: "alice@example.com"}); err != nil {
		panic(err)
	}
	if err := queues.Start(ctx); err != nil {
		panic(err)
	}
	// Output:
	// attempt 1 failed
	// attempt 2 failed
	// attempt 3 sent to alice@example.com
}
