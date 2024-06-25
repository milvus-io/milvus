package status

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
)

var _ error = (*StreamingError)(nil)

// StreamingError is the error type for log internal module.
type (
	StreamingError streamingpb.StreamingError
	StreamingCode  streamingpb.StreamingCode
)

// Error implements StreamingError as error.
func (e *StreamingError) Error() string {
	return fmt.Sprintf("code: %s, cause: %s", e.Code.String(), e.Cause)
}

// AsPBError convert StreamingError to streamingpb.StreamingError.
func (e *StreamingError) AsPBError() *streamingpb.StreamingError {
	return (*streamingpb.StreamingError)(e)
}

// IsWrongStreamingNode returns true if the error is caused by wrong streamingnode.
// Client should report these error to coord and block until new assignment term coming.
func (e *StreamingError) IsWrongStreamingNode() bool {
	return e.Code == streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM || // channel term not match
		e.Code == streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST || // channel do not exist on streamingnode
		e.Code == streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED // channel fenced on these node.
}

// NewOnShutdownError creates a new StreamingError with code STREAMING_CODE_ON_SHUTDOWN.
func NewOnShutdownError(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, format, args...)
}

// NewUnknownError creates a new StreamingError with code STREAMING_CODE_UNKNOWN.
func NewUnknownError(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN, format, args...)
}

// NewInvalidRequestSeq creates a new StreamingError with code STREAMING_CODE_INVALID_REQUEST_SEQ.
func NewInvalidRequestSeq(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_INVALID_REQUEST_SEQ, format, args...)
}

// NewChannelExist creates a new StreamingError with code StreamingCode_STREAMING_CODE_CHANNEL_EXIST.
func NewChannelExist(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_EXIST, format, args...)
}

// NewChannelNotExist creates a new StreamingError with code STREAMING_CODE_CHANNEL_NOT_EXIST.
func NewChannelNotExist(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST, format, args...)
}

// NewUnmatchedChannelTerm creates a new StreamingError with code StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM.
func NewUnmatchedChannelTerm(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM, format, args...)
}

// NewIgnoreOperation creates a new StreamingError with code STREAMING_CODE_IGNORED_OPERATION.
func NewIgnoreOperation(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_IGNORED_OPERATION, format, args...)
}

// NewInner creates a new StreamingError with code STREAMING_CODE_INNER.
func NewInner(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_INNER, format, args...)
}

// New creates a new StreamingError with the given code and cause.
func New(code streamingpb.StreamingCode, format string, args ...interface{}) *StreamingError {
	if len(args) == 0 {
		return &StreamingError{
			Code:  code,
			Cause: format,
		}
	}
	return &StreamingError{
		Code:  code,
		Cause: redact.Sprintf(format, args...).StripMarkers(),
	}
}

// getCause returns the cause of the StreamingError.
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

// As implements StreamingError as error.
func AsStreamingError(err error) *StreamingError {
	if err == nil {
		return nil
	}

	// If the error is a StreamingError, return it directly.
	var e *StreamingError
	if errors.As(err, &e) {
		return e
	}

	// If the error is StreamingStatus,
	var st *StreamingStatus
	if errors.As(err, &st) {
		e = st.TryIntoStreamingError()
		if e != nil {
			return e
		}
	}

	// Return a default StreamingError.
	return &StreamingError{
		Code:  streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN,
		Cause: err.Error(),
	}
}
