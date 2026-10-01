package backoff_test

import (
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/matthewmueller/jobq/internal/backoff"
)

func TestDelay(t *testing.T) {
	is := is.New(t)
	is.Equal(backoff.Delay(0), time.Second)
	is.Equal(backoff.Delay(1), time.Second)
	is.Equal(backoff.Delay(2), 2*time.Second)
	is.Equal(backoff.Delay(3), 4*time.Second)
	is.Equal(backoff.Delay(10), 512*time.Second)
	is.Equal(backoff.Delay(11), 15*time.Minute)
	is.Equal(backoff.Delay(1000), 15*time.Minute)
}
