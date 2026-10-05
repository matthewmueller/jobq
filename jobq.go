package jobq

import (
	"context"
	"errors"
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

// Permanent marks err as non-retryable, so the job goes straight to the
// dead-letter queue instead of being retried
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// IsPermanent reports whether err was marked with Permanent
func IsPermanent(err error) bool {
	_, ok := errors.AsType[*permanentError](err)
	return ok
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Stats summarizes a queue. Counts may be approximate depending on the
// backend.
type Stats struct {
	Pending int // waiting to run, including delayed jobs and retries
	Running int // currently claimed by a worker
	Failed  int // in the dead-letter queue
}
