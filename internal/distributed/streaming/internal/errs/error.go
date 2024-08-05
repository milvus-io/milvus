package errs

import (
	"github.com/cockroachdb/errors"
)

// All error in streamingservice package should be marked by streamingservice/errs package.
var (
	ErrClosed   = errors.New("closed")
	ErrCanceled = errors.New("canceled")
)
