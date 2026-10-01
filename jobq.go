package jobq

import (
	"context"
	"time"
)

// Payload is implemented by job data. The queue name comes from the payload
// type itself, so producers and consumers always agree on it. Queue must work
// on the zero value, so prefer a value receiver.
type Payload interface {
	Queue() string
}

// Job is a unit of work delivered to a handler
type Job[T Payload] struct {
	ID        string
	Data      T
	Attempt   int // 1-based attempt number
	CreatedAt time.Time
}

// Handler processes a job. Returning nil completes the job, returning an error
// records a failed attempt.
type Handler[T Payload] func(ctx context.Context, job *Job[T]) error
