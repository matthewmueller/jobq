# 0.1.0 / 2026-10-10

- add: `pgq.Load(ctx, log, pool)` to run on your app's `*pgxpool.Pool`. `Close` leaves the pool open.

  ```go
  pool, err := pgxpool.New(ctx, url)
  defer pool.Close()
  queues, err := pgq.Load(ctx, log, pool)
  ```

- add: `pgq.New(log, pool)` and `queues.Migrate(ctx)` to create `pgq_jobs` alongside your own migrations

# 0.0.5 / 2026-10-04

- add delayed jobs with PushIn and PushAt

# 0.0.4 / 2026-10-03

- add support for adding lanes to the payload (sqlite and postgres only)

# 0.0.3 / 2026-10-02

- add listen/notify wake-ups to pgq
- add nats jetstream backend (jetq)

# 0.0.2 / 2026-10-01

- add sqlite backend (sqq) on a shared sql worker engine
- add support for DLQ, add a logger

# 0.0.1 / 2026-10-01

- initial commit
