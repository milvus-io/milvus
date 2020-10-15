
package errors

import (
	"fmt"
	"io"
)

// New returns an error with the supplied message.
// New also records the stack trace at the point it was called.
func New(message string) error {
	return &fundamental{
		msg:   message,
		stack: callers(),
	}
}

// Errorf formats according to a format specifier and returns the string
// as a value that satisfies error.
// Errorf also records the stack trace at the point it was called.
func Errorf(format string, args ...interface{}) error {
	return &fundamental{
		msg:   fmt.Sprintf(format, args...),
		stack: callers(),
	}
}

// StackTraceAware is an optimization to avoid repetitive traversals of an error chain.
// HasStack checks for this marker first.
// Annotate/Wrap and Annotatef/Wrapf will produce this marker.
type StackTraceAware interface {
	HasStack() bool
}

// HasStack tells whether a StackTracer exists in the error chain
func HasStack(err error) bool {
	if errWithStack, ok := err.(StackTraceAware); ok {
		return errWithStack.HasStack()
	}
	return GetStackTracer(err) != nil
}

// fundamental is an error that has a message and a stack, but no caller.
type fundamental struct {
	msg string
	*stack
}

func (f *fundamental) Error() string { return f.msg }

func (f *fundamental) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, f.msg)
			f.stack.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, f.msg)
	case 'q':
		fmt.Fprintf(s, "%q", f.msg)
	}
}

// WithStack annotates err with a stack trace at the point WithStack was called.
// If err is nil, WithStack returns nil.
//
// For most use cases this is deprecated and AddStack should be used (which will ensure just one stack trace).
// However, one may want to use this in some situations, for example to create a 2nd trace across a goroutine.
func WithStack(err error) error {
	if err == nil {
		return nil
	}

	return &withStack{
		err,
		callers(),
	}
}

// AddStack is similar to WithStack.
// However, it will first check with HasStack to see if a stack trace already exists in the causer chain before creating another one.
func AddStack(err error) error {
	if HasStack(err) {
		return err
	}
	return WithStack(err)
}

type withStack struct {
	error
	*stack
}

func (w *withStack) Cause() error { return w.error }

func (w *withStack) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.Cause())
			w.stack.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, w.Error())
	case 'q':
		fmt.Fprintf(s, "%q", w.Error())
	}
}

// Wrap returns an error annotating err with a stack trace
// at the point Wrap is called, and the supplied message.
// If err is nil, Wrap returns nil.
//
// For most use cases this is deprecated in favor of Annotate.
// Annotate avoids creating duplicate stack traces.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	hasStack := HasStack(err)
	err = &withMessage{
		cause:         err,
		msg:           message,
		causeHasStack: hasStack,
	}
	return &withStack{
		err,
		callers(),
	}
}

// Wrapf returns an error annotating err with a stack trace
// at the point Wrapf is call, and the format specifier.
// If err is nil, Wrapf returns nil.
//
// For most use cases this is deprecated in favor of Annotatef.
// Annotatef avoids creating duplicate stack traces.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	hasStack := HasStack(err)
	err = &withMessage{
		cause:         err,
		msg:           fmt.Sprintf(format, args...),
		causeHasStack: hasStack,
	}
	return &withStack{
		err,
		callers(),
	}
}

// WithMessage annotates err with a new message.
// If err is nil, WithMessage returns nil.
func WithMessage(err error, message string) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause:         err,
		msg:           message,
		causeHasStack: HasStack(err),
	}
}

type withMessage struct {
	cause         error
	msg           string
	causeHasStack bool
}

func (w *withMessage) Error() string  { return w.msg + ": " + w.cause.Error() }
func (w *withMessage) Cause() error   { return w.cause }
func (w *withMessage) HasStack() bool { return w.causeHasStack }

func (w *withMessage) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n", w.Cause())
			io.WriteString(s, w.msg)
			return
		}
		fallthrough
	case 's', 'q':
		io.WriteString(s, w.Error())
	}
}

// Cause returns the underlying cause of the error, if possible.
// An error value has a cause if it implements the following
// interface:
//
//     type causer interface {
//            Cause() error
//     }
//
// If the error does not implement Cause, the original error will
// be returned. If the error is nil, nil will be returned without further
// investigation.
func Cause(err error) error {
	cause := Unwrap(err)
	if cause == nil {
		return err
	}
	return Cause(cause)
}

// Unwrap uses causer to return the next error in the chain or nil.
// This goes one-level deeper, whereas Cause goes as far as possible
func Unwrap(err error) error {
	type causer interface {
		Cause() error
	}
	if unErr, ok := err.(causer); ok {
		return unErr.Cause()
	}
	return nil
}

// Find an error in the chain that matches a test function.
// returns nil if no error is found.
func Find(origErr error, test func(error) bool) error {
	var foundErr error
	WalkDeep(origErr, func(err error) bool {
		if test(err) {
			foundErr = err
			return true
		}
		return false
	})
	return foundErr
}
