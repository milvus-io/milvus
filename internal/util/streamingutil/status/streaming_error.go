package status

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"

	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ error = (*StreamingError)(nil)

// StreamingError is the error type for streaming internal module.
// Should be used at logic layer.
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
// Client for producing and consuming should report these error to coord and block until new assignment term coming.
func (e *StreamingError) IsWrongStreamingNode() bool {
	return e.Code == streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM || // channel term not match
		e.Code == streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST || // channel do not exist on streamingnode
		e.Code == streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED // channel fenced on these node.
}

// IsSkippedOperation returns true if the operation is ignored or skipped.
func (e *StreamingError) IsSkippedOperation() bool {
	return e.Code == streamingpb.StreamingCode_STREAMING_CODE_IGNORED_OPERATION ||
		e.Code == streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM
}

// IsUnrecoverable returns true if the error is unrecoverable.
// Stop resuming retry and report to user.
func (e *StreamingError) IsUnrecoverable() bool {
	return e.Code == streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE || e.IsTxnUnavilable()
}

// IsTxnUnavilable returns true if the transaction is unavailable.
func (e *StreamingError) IsTxnUnavilable() bool {
	return e.Code == streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED ||
		e.Code == streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE
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

// NewChannelFenced creates a new StreamingError with code STREAMING_CODE_CHANNEL_FENCED.
// TODO: Unused by now, add it after enable wal fence.
func NewChannelFenced(channel string) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED, "%s fenced", channel)
}

// NewChannelNotExist creates a new StreamingError with code STREAMING_CODE_CHANNEL_NOT_EXIST.
func NewChannelNotExist(channel string) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST, "%s not exist", channel)
}

// NewUnmatchedChannelTerm creates a new StreamingError with code StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM.
func NewUnmatchedChannelTerm(channel string, expectedTerm int64, currentTerm int64) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM, "channel %s at term %d is expected, but current term is %d", channel, expectedTerm, currentTerm)
}

// NewIgnoreOperation creates a new StreamingError with code STREAMING_CODE_IGNORED_OPERATION.
func NewIgnoreOperation(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_IGNORED_OPERATION, format, args...)
}

// NewInner creates a new StreamingError with code STREAMING_CODE_INNER.
func NewInner(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_INNER, format, args...)
}

// NewInvaildArgument creates a new StreamingError with code STREAMING_CODE_INVAILD_ARGUMENT.
func NewInvaildArgument(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT, format, args...)
}

// NewTransactionExpired creates a new StreamingError with code STREAMING_CODE_TRANSACTION_EXPIRED.
func NewTransactionExpired(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, format, args...)
}

// NewInvalidTransactionState creates a new StreamingError with code STREAMING_CODE_INVALID_TRANSACTION_STATE.
func NewInvalidTransactionState(operation string, expectState message.TxnState, currentState message.TxnState) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE, "invalid transaction state for operation %s, expect %s, current %s", operation, expectState, currentState)
}

// NewUnrecoverableError creates a new StreamingError with code STREAMING_CODE_UNRECOVERABLE.
func NewUnrecoverableError(format string, args ...interface{}) *StreamingError {
	return New(streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, format, args...)
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
	var st *StreamingClientStatus
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
