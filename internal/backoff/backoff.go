package backoff

import "time"

// Delay returns how long to wait before retrying after the given 1-based
// attempt: 1s, 2s, 4s, ... capped at 15 minutes
func Delay(attempt int) time.Duration {
	const max = 15 * time.Minute
	if attempt < 1 {
		return time.Second
	} else if attempt > 10 {
		return max
	}
	return min(time.Second<<(attempt-1), max)
}
