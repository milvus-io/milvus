package status

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/milvus-io/milvus/internal/proto/logpb"
)

var _ error = (*LogError)(nil)

// LogError is the error type for log internal module.
type (
	LogError logpb.LogError
	LogCode  logpb.LogCode
)

// Error implements LogError as error.
func (e *LogError) Error() string {
	return fmt.Sprintf("code: %s, cause: %s", e.Code.String(), e.Cause)
}

// AsPBError convert LogError to logpb.LogError.
func (e *LogError) AsPBError() *logpb.LogError {
	return (*logpb.LogError)(e)
}

// IsWrongLogNode returns true if the error is caused by wrong lognode.
// Client should report these error to coord and block until new assignment term coming.
func (e *LogError) IsWrongLogNode() bool {
	return e.Code == logpb.LogCode_LOG_CODE_UNMATCHED_CHANNEL_TERM || // channel term not match
		e.Code == logpb.LogCode_LOG_CODE_CHANNEL_NOT_EXIST || // channel do not exist on lognode
		e.Code == logpb.LogCode_LOG_CODE_CHANNEL_FENCED // channel fenced on these node.
}

// NewOnShutdownError creates a new LogError with code LOG_CODE_ON_SHUTDOWN.
func NewOnShutdownError(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_ON_SHUTDOWN, format, args...)
}

// NewUnknownError creates a new LogError with code LOG_CODE_UNKNOWN.
func NewUnknownError(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_UNKNOWN, format, args...)
}

// NewInvalidRequestSeq creates a new LogError with code LOG_CODE_INVALID_REQUEST_SEQ.
func NewInvalidRequestSeq(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_INVALID_REQUEST_SEQ, format, args...)
}

// NewChannelExist creates a new LogError with code LogCode_LOG_CODE_CHANNEL_EXIST.
func NewChannelExist(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_CHANNEL_EXIST, format, args...)
}

// NewChannelNotExist creates a new LogError with code LOG_CODE_CHANNEL_NOT_EXIST.
func NewChannelNotExist(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_CHANNEL_NOT_EXIST, format, args...)
}

// NewUnmatchedChannelTerm creates a new LogError with code LogCode_LOG_CODE_UNMATCHED_CHANNEL_TERM.
func NewUnmatchedChannelTerm(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_UNMATCHED_CHANNEL_TERM, format, args...)
}

// NewIgnoreOperation creates a new LogError with code LOG_CODE_IGNORED_OPERATION.
func NewIgnoreOperation(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_IGNORED_OPERATION, format, args...)
}

// NewInner creates a new LogError with code LOG_CODE_INNER.
func NewInner(format string, args ...interface{}) *LogError {
	return New(logpb.LogCode_LOG_CODE_INNER, format, args...)
}

// New creates a new LogError with the given code and cause.
func New(code logpb.LogCode, format string, args ...interface{}) *LogError {
	if len(args) == 0 {
		return &LogError{
			Code:  code,
			Cause: format,
		}
	}
	return &LogError{
		Code:  code,
		Cause: redact.Sprintf(format, args...).StripMarkers(),
	}
}

// getCause returns the cause of the LogError.
func getCause(cause ...interface{}) string {
	if len(cause) == 0 {
		return ""
	}
	switch c := cause[0].(type) {
	case string:
		return c
	case *string:
		return *c
	case fmt.Stringer:
		return c.String()
	case error:
		return c.Error()
	default:
		return fmt.Sprintf("%+v", c)
	}
}

// As implements LogError as error.
func AsLogError(err error) *LogError {
	if err == nil {
		return nil
	}

	// If the error is a LogError, return it directly.
	var e *LogError
	if errors.As(err, &e) {
		return e
	}

	// If the error is LogStatus,
	var st *LogStatus
	if errors.As(err, &st) {
		e = st.TryIntoLogError()
		if e != nil {
			return e
		}
	}

	// Return a default LogError.
	return &LogError{
		Code:  logpb.LogCode_LOG_CODE_UNKNOWN,
		Cause: err.Error(),
	}
}
