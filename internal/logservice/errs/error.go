package errs

import (
	"github.com/cockroachdb/errors"
)

// All error in logservice package should be marked by logservice/errs package.
var (
	ErrClosed   = errors.New("closed")
	ErrCanceled = errors.New("canceled")
)
