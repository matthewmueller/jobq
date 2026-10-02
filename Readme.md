# jobq

Typed background job queues for Go, backed by PostgreSQL, SQLite, NATS JetStream or Amazon SQS. Payload types name their own queue and handlers are plain typed methods, so producers and consumers can't disagree about queue names or payloads.

## Features

- Payload types are inferred from handlers: `queues.Queue(session.Run)`
- Push from any process without registering handlers: `queues.Push(ctx, RunSession{...})`
- Per-queue concurrency, retries with backoff, and timeouts
- Dead-letter queues with `Revive` and `Stats`

## Install

```sh
go get github.com/matthewmueller/jobq
```

Requires Go 1.27 or later.

## Example

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/matthewmueller/jobq/pgq"
)

// Session queues
type Session struct{
  // Dependencies
}

type Run struct {
	SessionID string
}

func (Run) Queue() string {
  return "session.run"
}

func (s *Session) Run(ctx context.Context, job *pgq.Job[RunSession]) error {
	// job.Data.SessionID, job.ID, job.Attempt
	return nil
}

type Interrupt struct {
	SessionID string
}

func (Interrupt) Queue() string {
  return "session.interrupt"
}

func (s *Session) Interrupt(ctx context.Context, job *pgq.Job[Interrupt]) error {
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

  {
    session := &Session{}
    // Configure "session.run" queue
    queues.Queue(session.Run).Concurrency(4).Retries(3).Timeout(time.Minute)
    // Configure "session.interrupt" queue
    queues.Queue(session.Interrupt).Concurrency(4).Retries(3).Timeout(time.Minute)
  }

  // Push some work onto the "session.run" queue
	if err := queues.Push(ctx, RunSession{SessionID: "123"}); err != nil {
		panic(err)
	}

	// Blocks until ctx is cancelled
	if err := queues.Start(ctx); err != nil {
		panic(err)
	}
}
```

## Backends

Every backend has the same API. Switching changes the import and `Dial`:

| Package | Dial                                                                 | Notes                                                                 |
| ------- | -------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `pgq`   | `pgq.Dial(ctx, log, "postgres://localhost/app")`                     | Creates `pgq_jobs`. Wakes workers with LISTEN/NOTIFY. PostgreSQL 14+. |
| `sqq`   | `sqq.Dial(ctx, log, "jobs.db")`                                      | Creates `sqq_jobs`. Processes must share one host. No `:memory:`.     |
| `jetq`  | `jetq.Dial(ctx, log, "nats://localhost:4222")`                       | Creates the `JOBQ` and `JOBQ_DEAD` streams and a consumer per queue.  |
| `sqs`   | `sqs.Dial(ctx, log, "https://sqs.<region>.amazonaws.com/<account>")` | Queues and dead-letter queues must already exist.                     |

## API Reference

| Method                            | Description                                         |
| --------------------------------- | --------------------------------------------------- |
| `queues.Queue(handler)`           | Register a handler for its payload's queue          |
| `config.Concurrency(n)`           | Jobs this process runs at once (default 1)          |
| `config.Retries(n)`               | Retries after a failure (default 0). Not on `sqs`.  |
| `config.Timeout(d)`               | Cancel the handler after `d` (default none)         |
| `queues.Push(ctx, payload)`       | Enqueue onto `payload.Queue()`                      |
| `queues.PushTx(ctx, tx, payload)` | Enqueue inside a transaction (`pgq` and `sqq` only) |
| `queues.Start(ctx)`               | Process registered queues until `ctx` is cancelled  |
| `queues.Revive(ctx, queue)`       | Move dead-lettered jobs back onto the queue         |
| `queues.Stats(ctx, queue)`        | Pending, running and failed counts                  |
| `Permanent(err)`                  | Fail without retrying                               |

Jobs are delivered **at least once**, so make handlers idempotent. Returning an error retries the job after 1s, 2s, 4s… up to 15 minutes. Once retries run out, or the error is `Permanent`, the job is dead-lettered.

### Backend notes

- **PostgreSQL:** completed jobs stay in `pgq_jobs`, so delete them periodically. `LISTEN` uses one extra connection per process running workers, which must be direct or session-pooled (PgBouncer's transaction pooling doesn't support it).
- **SQLite:** other processes find new jobs by polling every 500ms. Don't put the file on a network filesystem.
- **NATS JetStream:** queue names are dot-separated tokens. A delivery interrupted by a shutdown or crash counts as an attempt.
- **SQS:** queue names may only use letters, numbers, hyphens and underscores. Each queue needs a dead-letter queue and permissions; see [docs/sqs.md](docs/sqs.md) for Terraform that sets them up.

## Development

```sh
make test
```

`sqq` and `jetq` tests need no setup. `pgq` tests use `DATABASE_URL` (default `postgres://localhost:5432/jobq_test`). `sqs` tests use `SQS_QUEUE` and skip when it's unset.
