# jobq

Dead-simple job queue for Go. Supports:

- PostgreSQL
- SQLite
- NATS JetStream
- Amazon SQS

## Features

- Payload typed and inferred from their handlers: `queues.Queue(session.Run)`
- Push from any process: `queues.Push(ctx, RunSession{...})`
- Delay jobs: `queues.PushIn(ctx, time.Hour, RunSession{...})`
- Per-queue concurrency, retries with backoff, and timeouts
- Dead-letter queues with support for `Revive` and `Stats`
- Lanes run related jobs one at a time (`pgq` and `sqq`)

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

## Lanes

Jobs in the same lane never run at the same time, even across processes. Add a `Lane` method to the payload:

```go
// Lane runs a session's jobs one at a time
func (r Run) Lane() string {
	return r.SessionID
}
```

Jobs in different lanes, and jobs without a lane, run concurrently up to each queue's concurrency. Jobs usually run in the order they were pushed, but a delayed job or one waiting to retry doesn't hold up the rest of its lane. Lanes are supported by `pgq` and `sqq`.

## Delayed Jobs

Push a job to run later with `PushIn` or `PushAt`:

```go
// Run in an hour
queues.PushIn(ctx, time.Hour, Run{SessionID: "123"})

// Run tomorrow at 9am, or right away if that time has passed
queues.PushAt(ctx, tomorrow9am, Run{SessionID: "123"})
```

Delayed jobs count as pending in `Stats`. `pgq` and `sqq` also have `PushTxIn` and `PushTxAt`. On `jetq` and `sqs`, delays are rounded up to the second.

## Backends

Every backend has the same API. Switching changes the import and `Dial`:

| Package | Dial                                                                 | Notes                                                                 |
| ------- | -------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `pgq`   | `pgq.Dial(ctx, log, "postgres://localhost/app")`                     | Creates `pgq_jobs`. Wakes workers with LISTEN/NOTIFY. PostgreSQL 14+. |
| `sqq`   | `sqq.Dial(ctx, log, "jobs.db")`                                      | Creates `sqq_jobs`. Processes must share one host. No `:memory:`.     |
| `jetq`  | `jetq.Dial(ctx, log, "nats://localhost:4222")`                       | Creates the `JOBQ` and `JOBQ_DEAD` streams and a consumer per queue.  |
| `sqs`   | `sqs.Dial(ctx, log, "https://sqs.<region>.amazonaws.com/<account>")` | Queues and dead-letter queues must already exist.                     |

## API Reference

| Method                            | Description                                                    |
| --------------------------------- | -------------------------------------------------------------- |
| `queues.Queue(handler)`           | Register a handler for its payload's queue                     |
| `config.Concurrency(n)`           | Jobs this process runs at once (default 1)                     |
| `config.Retries(n)`               | Retries after a failure (default 0). Not on `sqs`.             |
| `config.Timeout(d)`               | Cancel the handler after `d` (default none)                    |
| `queues.Push(ctx, payload)`       | Enqueue onto `payload.Queue()`                                 |
| `queues.PushIn(ctx, d, payload)`  | Enqueue to run after `d`. Up to 15 minutes on `sqs`.           |
| `queues.PushAt(ctx, t, payload)`  | Enqueue to run at `t`. Up to 15 minutes ahead on `sqs`.        |
| `queues.PushTx(ctx, tx, payload)` | Enqueue inside a transaction (`pgq` and `sqq` only)            |
| `queues.PushTxIn/PushTxAt(...)`   | Delayed `PushTx` (`pgq` and `sqq` only)                        |
| `payload.Lane()`                  | Run jobs in the same lane one at a time (`pgq` and `sqq` only) |
| `queues.Start(ctx)`               | Process registered queues until `ctx` is cancelled             |
| `queues.Revive(ctx, queue)`       | Move dead-lettered jobs back onto the queue                    |
| `queues.Stats(ctx, queue)`        | Pending, running and failed counts                             |
| `Permanent(err)`                  | Fail without retrying                                          |

Jobs are delivered **at least once**, so make handlers idempotent. Returning an error retries the job after 1s, 2s, 4s… up to 15 minutes. Once retries run out, or the error is `Permanent`, the job is dead-lettered.

### Backend notes

- **PostgreSQL:** completed jobs stay in `pgq_jobs`, so delete them periodically. `LISTEN` uses one extra connection per process running workers, which must be direct or session-pooled (PgBouncer's transaction pooling doesn't support it).
- **SQLite:** other processes find new jobs by polling every 500ms. Don't put the file on a network filesystem.
- **NATS JetStream:** queue names are dot-separated tokens. A delivery interrupted by a shutdown or crash counts as an attempt. Delayed jobs need NATS server 2.12+.
- **SQS:** queue names may only use letters, numbers, hyphens and underscores. Delays are limited to 15 minutes. Each queue needs a dead-letter queue and permissions; see [docs/sqs.md](docs/sqs.md) for Terraform that sets them up.

## Development

```sh
make test
```

`sqq` and `jetq` tests need no setup. `pgq` tests use `DATABASE_URL` (default `postgres://localhost:5432/jobq_test`). `sqs` tests use `SQS_QUEUE` and skip when it's unset.
