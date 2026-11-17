// Licensed under the Apache License, Version 2.0.
package credentials

import "fmt"

// credentialError is the compatibility SDK's internal error dialect. Public
// storage adapters translate it to merr at their boundary. Messages must never
// include credential values or raw credential-service response bodies.
type credentialError struct {
	message string
	cause   error
}

func (e *credentialError) Error() string      { return e.message }
func (e *credentialError) Unwrap() error      { return e.cause }
func newCredentialError(message string) error { return &credentialError{message: message} }
func credentialErrorf(format string, args ...any) error {
	return &credentialError{message: fmt.Sprintf(format, args...)}
}

func wrapCredentialError(cause error, message string) error {
	return &credentialError{message: message, cause: cause}
}
