package jobq_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/matryer/is"
	"github.com/matthewmueller/jobq"
)

func TestPermanent(t *testing.T) {
	is := is.New(t)
	errBad := errors.New("bad input")
	err := fmt.Errorf("importing: %w", jobq.Permanent(errBad))
	is.True(jobq.IsPermanent(err))
	is.True(errors.Is(err, errBad))
	is.Equal(err.Error(), "importing: bad input")
	is.True(!jobq.IsPermanent(errBad))
	is.Equal(jobq.Permanent(nil), nil)
}
