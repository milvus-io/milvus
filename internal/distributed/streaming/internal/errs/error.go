package errs

import (
	"github.com/cockroachdb/errors"
)

// All error in streamingservice package should be marked by streamingservice/errs package.
var (
	ErrClosed                   = errors.New("closed")
	ErrCanceledOrDeadlineExceed = errors.New("canceled or deadline exceed")
	ErrUnrecoverable            = errors.New("unrecoverable")
)
