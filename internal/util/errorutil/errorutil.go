package errorutil

import (
	"fmt"
	"strings"
)

// ErrorList for print error log
type ErrorList []error

// TODO shouldn't print all retries, might be too much
// Error method return an string representation of retry error list.
func (el ErrorList) Error() string {
	var builder strings.Builder
	builder.WriteString("All attempts results:\n")
	for index, err := range el {
		// if early termination happens
		if err == nil {
			break
		}
		builder.WriteString(fmt.Sprintf("attempt #%d:%s\n", index+1, err.Error()))
	}
	return builder.String()
}

type unrecoverableError struct {
	error
}

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return unrecoverableError{err}
}

// IsUnRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsUnRecoverable(err error) bool {
	_, isUnrecoverable := err.(unrecoverableError)
	return isUnrecoverable
}
