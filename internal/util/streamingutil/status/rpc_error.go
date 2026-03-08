package status

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

var streamingErrorToGRPCStatus = map[streamingpb.StreamingCode]codes.Code{
	streamingpb.StreamingCode_STREAMING_CODE_OK:                        codes.OK,
	streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST:         codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED:            codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN:               codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_INVALID_REQUEST_SEQ:       codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM:    codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_IGNORED_OPERATION:         codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_INNER:                     codes.Internal,
	streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT:          codes.InvalidArgument,
	streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED:       codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE: codes.FailedPrecondition,
	streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN:                   codes.Unknown,
}

// NewGRPCStatusFromStreamingError converts StreamingError to grpc status.
// Should be called at server-side.
func NewGRPCStatusFromStreamingError(e *StreamingError) *status.Status {
	if e == nil || e.Code == streamingpb.StreamingCode_STREAMING_CODE_OK {
		return status.New(codes.OK, "")
	}

	code, ok := streamingErrorToGRPCStatus[e.Code]
	if !ok {
		code = codes.Unknown
	}

	// Attach streaming error to detail.
	st := status.New(code, "")
	newST, err := st.WithDetails(e.AsPBError())
	if err != nil {
		return status.New(code, fmt.Sprintf("convert streaming error failed, detail: %s", e.Cause))
	}
	return newST
}

// StreamingClientStatus is a wrapper of grpc status.
// Should be used in client side.
type StreamingClientStatus struct {
	*status.Status
	method string
}

// ConvertStreamingError convert error to StreamingStatus.
// Used in client side.
func ConvertStreamingError(method string, err error) error {
	if err == nil {
		return nil
	}
	if errors.IsAny(err, context.DeadlineExceeded, context.Canceled, io.EOF) {
		return err
	}
	rpcStatus := status.Convert(err)
	e := &StreamingClientStatus{
		Status: rpcStatus,
		method: method,
	}
	return e
}

// TryIntoStreamingError try to convert StreamingStatus to StreamingError.
func (s *StreamingClientStatus) TryIntoStreamingError() *StreamingError {
	if s == nil {
		return nil
	}
	for _, detail := range s.Details() {
		if detail, ok := detail.(*streamingpb.StreamingError); ok {
			return New(detail.Code, detail.Cause)
		}
	}
	return nil
}

// For converting with status.Status.
// !!! DO NOT Delete this method. IsCanceled function use it.
func (s *StreamingClientStatus) GRPCStatus() *status.Status {
	if s == nil {
		return nil
	}
	return s.Status
}

// Error implements StreamingStatus as error.
func (s *StreamingClientStatus) Error() string {
	if streamingErr := s.TryIntoStreamingError(); streamingErr != nil {
		return fmt.Sprintf("%s; streaming error: code = %s, cause = %s; rpc error: code = %s, desc = %s", s.method, streamingErr.Code.String(), streamingErr.Cause, s.Code(), s.Message())
	}
	return fmt.Sprintf("%s; rpc error: code = %s, desc = %s", s.method, s.Code(), s.Message())
}
