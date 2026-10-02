# jobq

Typed background job queues for Go, backed by PostgreSQL (`pgq`), SQLite (`sqq`), NATS JetStream (`jetq`) or Amazon SQS (`sqs`). Payload types name their own queue and handlers are plain typed methods, so producers and consumers can't disagree about queue names or payload shapes.

## Features

- Payload types are inferred from handler signatures: `queues.Queue(session.Run)`
- Push from any process without registering handlers: `queues.Push(ctx, RunSession{...})`
- Per-queue concurrency, retries with exponential backoff, and handler timeouts
- Dead-letter queues with `Revive` and `Stats`
- `Permanent(err)` to skip retries for errors that will never succeed
- Transactional push for PostgreSQL and SQLite with `PushTx`
- Safe across processes: `FOR UPDATE SKIP LOCKED` leases in PostgreSQL, single-writer claims in SQLite, acknowledgements in JetStream, visibility timeouts in SQS

## Install

```sh
go get github.com/matthewmueller/jobq
```

Requires Go 1.27 or later for generic methods.

## Example

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/matthewmueller/jobq/pgq"
)

// RunSession is the contract between producers and consumers
type RunSession struct {
	SessionID string
}

func (RunSession) Queue() string { return "session-run" }

type session struct{}

func (s *session) Run(ctx context.Context, job *pgq.Job[RunSession]) error {
	// job.Data.SessionID, job.ID, job.Attempt
	return nil
}

func main() {
	ctx := context.Background()
	queues, err := pgq.Dial(ctx, slog.Default(), "postgres://localhost:5432/app")
	if err != nil {
		panic(err)
	}
	defer queues.Close()

	// Configure consumers
	session := &session{}
	queues.Queue(session.Run).
		Concurrency(4).
		Retries(3).
		Timeout(time.Minute)

	// Produce from anywhere with *pgq.Queues
	if err := queues.Push(ctx, RunSession{SessionID: "123"}); err != nil {
		panic(err)
	}

	// Blocks until ctx is cancelled
	if err := queues.Start(ctx); err != nil {
		panic(err)
	}
}
```

Switching to SQLite changes the import and `Dial`; the rest of the API is identical:

```go
queues, err := sqq.Dial(ctx, slog.Default(), "jobs.db")
```

Switching to NATS JetStream works the same way. `Dial` creates the streams it needs:

```go
queues, err := jetq.Dial(ctx, slog.Default(), "nats://localhost:4222")
```

Switching to SQS also changes the import and `Dial`. Retries come from the queue's redrive policy instead of `.Retries(n)`:

```go
queues, err := sqs.Dial(ctx, slog.Default(), "https://sqs.us-west-2.amazonaws.com/123456789012")
queues.Queue(session.Run).Concurrency(4).Timeout(time.Minute)
```

## API Reference

All backends share this API:

| Method | Description |
| --- | --- |
| `Dial(ctx, log, url)` | Connect. `pgq` and `sqq` create their jobs table and `jetq` its streams if needed. |
| `queues.Queue(handler)` | Register a handler, returning a `*Config` |
| `config.Concurrency(n)` | Jobs this process runs at once for the queue (default 1) |
| `config.Timeout(d)` | Cancel the handler's context after `d` and fail the attempt (default none) |
| `queues.Push(ctx, payload)` | Enqueue onto `payload.Queue()` |
| `queues.Start(ctx)` | Process registered queues until `ctx` is cancelled |
| `queues.Revive(ctx, queue)` | Move dead-lettered jobs back onto the queue |
| `queues.Stats(ctx, queue)` | Pending, running and failed (dead-lettered) counts |
| `Permanent(err)` | Mark an error as non-retryable |
| `jobq.IsPermanent(err)` | Report whether an error was marked with `Permanent` |

Backend specific:

| Method | Description |
| --- | --- |
| `pgq`, `sqq`, `jetq` `config.Retries(n)` | Retry failed jobs `n` times (default 0) |
| `pgq` `queues.PushTx(ctx, tx, payload)` | Enqueue inside a `pgx.Tx`, so the job only exists if `tx` commits |
| `sqq` `queues.PushTx(ctx, tx, payload)` | Enqueue inside a `*sql.Tx`, so the job only exists if `tx` commits |

### Delivery semantics

All backends deliver **at least once**. A job can run again if a worker crashes, loses its lease, or the process shuts down mid-job, so make handlers idempotent. `Job.ID` is stable across retries.

Returning `nil` completes a job. Returning an error retries it after a backoff of 1s, 2s, 4s… up to 15 minutes. Once retries are exhausted, or the error is `Permanent`, the job is dead-lettered.

### PostgreSQL

- Failed jobs stay in `pgq_jobs` with `state = 'failed'` and `last_error`. `Revive` resets them to pending with fresh attempts.
- Completed jobs are kept with `state = 'completed'`. Delete them periodically, e.g. `DELETE FROM pgq_jobs WHERE state = 'completed' AND updated_at < now() - interval '7 days'`.
- Workers share the connection pool. Size it for the total concurrency with `pool_max_conns` in the URL, e.g. `postgres://…/app?pool_max_conns=20`.

### SQLite

- `Dial` takes a file path or `file:` URI and enables WAL mode, a 5 second busy timeout and immediate transactions. In-memory databases are rejected, since each pooled connection would get its own database.
- Processes sharing a database must run on the same host, and the file must not be on a network filesystem.
- SQLite runs one writer at a time, so claims serialize. That's plenty for most job volumes, but use `pgq` for high write throughput.
- For `PushTx`, open your own `*sql.DB` with `?_pragma=busy_timeout(5000)&_txlock=immediate` so transactions wait for the write lock instead of failing.
- Failed and completed jobs stay in `sqq_jobs`, as with PostgreSQL. Times are Unix milliseconds, e.g. `DELETE FROM sqq_jobs WHERE state = 'completed' AND updated_at < unixepoch('now', '-7 days') * 1000`.

### NATS JetStream

- `Dial` creates or updates two streams with work-queue retention, so each message is deleted once it's acknowledged. `JOBQ` holds jobs on `jobq.jobs.<queue>`, and `JOBQ_DEAD` holds dead-lettered jobs on `jobq.dead.<queue>`.
- `Start` creates a durable pull consumer per registered queue, named after the queue with dots replaced by underscores (`session.run` becomes `session_run`). Queue names are dot-separated tokens of letters, numbers, hyphens and underscores.
- The connection needs permission to publish to `jobq.>` and to manage those streams and consumers.
- Attempts are JetStream deliveries, so a delivery interrupted by a shutdown or crash uses up an attempt. A job that crashes the whole process (a panic in a handler is recovered and doesn't) is redelivered indefinitely.
- Dead-lettered jobs carry `Jobq-Error` and `Jobq-Attempts` headers. Inspect them with `nats stream view JOBQ_DEAD`.

### SQS

Each payload's queue lives at `<url>/<Payload.Queue()>` and must already exist. Queue names may only contain letters, numbers, hyphens and underscores. For each queue:

1. Create the queue and a dead-letter queue, e.g. `session-run` and `session-run-dlq`.
2. Set a redrive policy on the queue pointing at the dead-letter queue. `maxReceiveCount` is the total number of attempts.
3. Grant the application:
   - On the queue: `sqs:SendMessage`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes`, `sqs:StartMessageMoveTask`
   - On the dead-letter queue: `sqs:SendMessage`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`

`Start` fails if a queue doesn't exist, and warns if it has no redrive policy, since failed jobs would then retry until they expire.

### Alarms

- Dead-letter depth above zero: `Stats(...).Failed`, or CloudWatch `ApproximateNumberOfMessagesVisible` on the SQS dead-letter queue.
- Age of the oldest pending job: CloudWatch `ApproximateAgeOfOldestMessage` for SQS, or the oldest `run_at` over pending rows for PostgreSQL and SQLite.

## Development

```sh
make test
```

`sqq` tests use temporary files and `jetq` tests run an embedded NATS server, so neither needs setup. `pgq` tests use `DATABASE_URL`, defaulting to `postgres://localhost:5432/jobq_test`. `sqs` tests use `SQS_QUEUE`, the URL of a dedicated test queue, and are skipped when it's unset. The dead-letter tests also need a redrive policy on that queue.
