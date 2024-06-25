package status

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
)

// StreamingStatus is a wrapper of grpc status.
// Should be used in client side.
type StreamingStatus struct {
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
	e := &StreamingStatus{
		Status: rpcStatus,
		method: method,
	}
	return e
}

// ToError converts the StreamingStatus to an error.
func (e *StreamingStatus) ToError() error {
	if e == nil {
		return nil
	}
	return e
}

// Method returns the method of StreamingStatus.
func (s *StreamingStatus) Method() string {
	return s.method
}

// TryIntoStreamingError try to convert StreamingStatus to StreamingError.
func (s *StreamingStatus) TryIntoStreamingError() *StreamingError {
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
func (s *StreamingStatus) GRPCStatus() *status.Status {
	if s == nil {
		return nil
	}
	return s.Status
}

// Error implements StreamingStatus as error.
func (s *StreamingStatus) Error() string {
	if streamingErr := s.TryIntoStreamingError(); streamingErr != nil {
		return fmt.Sprintf("%s; log error: code = %s, cause = %s; rpc error: code = %s, desc = %s", s.method, streamingErr.Code.String(), streamingErr.Cause, s.Code(), s.Message())
	}
	return fmt.Sprintf("%s; rpc error: code = %s, desc = %s", s.method, s.Code(), s.Message())
}
