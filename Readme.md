# jobq

Typed background job queues for Go, backed by PostgreSQL (`pgq`) or Amazon SQS (`sqs`). Payload types name their own queue and handlers are plain typed methods, so producers and consumers can't disagree about queue names or payload shapes.

## Features

- Payload types are inferred from handler signatures: `queues.Queue(session.Run)`
- Push from any process without registering handlers: `queues.Push(ctx, RunSession{...})`
- Per-queue concurrency, retries with exponential backoff, and handler timeouts
- Dead-letter queues with `Redrive` and `Stats`
- `Permanent(err)` to skip retries for errors that will never succeed
- Transactional push for PostgreSQL with `PushTx`
- Safe across processes: `FOR UPDATE SKIP LOCKED` leases in PostgreSQL, visibility timeouts in SQS

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

Switching to SQS changes the import and `Dial`. Retries come from the queue's redrive policy instead of `.Retries(n)`:

```go
queues, err := sqs.Dial(ctx, slog.Default(), "https://sqs.us-west-2.amazonaws.com/123456789012")
queues.Queue(session.Run).Concurrency(4).Timeout(time.Minute)
```

## API Reference

Both backends share this API:

| Method | Description |
| --- | --- |
| `Dial(ctx, log, url)` | Connect. `pgq` creates the `pgq_jobs` table if needed. |
| `queues.Queue(handler)` | Register a handler, returning a `*Config` |
| `config.Concurrency(n)` | Jobs this process runs at once for the queue (default 1) |
| `config.Timeout(d)` | Cancel the handler's context after `d` and fail the attempt (default none) |
| `queues.Push(ctx, payload)` | Enqueue onto `payload.Queue()` |
| `queues.Start(ctx)` | Process registered queues until `ctx` is cancelled |
| `queues.Redrive(ctx, queue)` | Move dead-lettered jobs back onto the queue |
| `queues.Stats(ctx, queue)` | Pending, running and failed (dead-lettered) counts |
| `Permanent(err)` | Mark an error as non-retryable |
| `jobq.IsPermanent(err)` | Report whether an error was marked with `Permanent` |

Backend specific:

| Method | Description |
| --- | --- |
| `pgq` `config.Retries(n)` | Retry failed jobs `n` times (default 0) |
| `pgq` `queues.PushTx(ctx, tx, payload)` | Enqueue inside a `pgx.Tx`, so the job only exists if `tx` commits |

### Delivery semantics

Both backends deliver **at least once**. A job can run again if a worker crashes, loses its lease, or the process shuts down mid-job, so make handlers idempotent. `Job.ID` is stable across retries.

Returning `nil` completes a job. Returning an error retries it after a backoff of 1s, 2s, 4s… up to 15 minutes. Once retries are exhausted, or the error is `Permanent`, the job is dead-lettered.

### PostgreSQL

- Failed jobs stay in `pgq_jobs` with `state = 'failed'` and `last_error`. `Redrive` resets them to pending with fresh attempts.
- Completed jobs are kept with `state = 'completed'`. Delete them periodically, e.g. `DELETE FROM pgq_jobs WHERE state = 'completed' AND updated_at < now() - interval '7 days'`.
- Workers share the connection pool. Size it for the total concurrency with `pool_max_conns` in the URL, e.g. `postgres://…/app?pool_max_conns=20`.

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
- Age of the oldest pending job: CloudWatch `ApproximateAgeOfOldestMessage` for SQS, or `now() - min(run_at)` over pending rows for PostgreSQL.

## Development

```sh
make test
```

`pgq` tests use `DATABASE_URL`, defaulting to `postgres://localhost:5432/jobq_test`. `sqs` tests use `SQS_QUEUE`, the URL of a dedicated test queue, and are skipped when it's unset. The dead-letter tests also need a redrive policy on that queue.
