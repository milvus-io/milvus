package rocksmq

import "fmt"

type Result int

const (
	Ok Result = iota
	UnknownError
	InvalidConfiguration
)

type Error struct {
	msg    string
	result Result
}

func (e *Error) Result() Result {
	return e.result
}

func (e *Error) Error() string {
	return e.msg
}

func newError(result Result, msg string) error {
	return &Error{
		msg:    fmt.Sprintf("%s: %s", msg, getResultStr(result)),
		result: result,
	}
}

func getResultStr(r Result) string {
	switch r {
	case Ok:
		return "OK"
	case UnknownError:
		return "UnknownError"
	case InvalidConfiguration:
		return "InvalidConfiguration"
	default:
		return fmt.Sprintf("Result(%d)", r)
	}
}
