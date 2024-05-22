package status

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"google.golang.org/grpc/status"
)

// LogStatus is a wrapper of grpc status.
// Should be used in client side.
type LogStatus struct {
	*status.Status
	method string
}

// ConvertLogError convert error to LogStatus.
// Used in client side.
func ConvertLogError(method string, err error) error {
	if err == nil {
		return nil
	}
	if errors.IsAny(err, context.DeadlineExceeded, context.Canceled, io.EOF) {
		return err
	}
	rpcStatus := status.Convert(err)
	e := &LogStatus{
		Status: rpcStatus,
		method: method,
	}
	return e
}

// ToError converts the LogStatus to an error.
func (e *LogStatus) ToError() error {
	if e == nil {
		return nil
	}
	return e
}

// Method returns the method of LogStatus.
func (s *LogStatus) Method() string {
	return s.method
}

// TryIntoLogError try to convert LogStatus to LogError.
func (s *LogStatus) TryIntoLogError() *LogError {
	if s == nil {
		return nil
	}
	for _, detail := range s.Details() {
		if detail, ok := detail.(*logpb.LogError); ok {
			return New(detail.Code, detail.Cause)
		}
	}
	return nil
}

// For converting with status.Status.
func (s *LogStatus) GRPCStatus() *status.Status {
	if s == nil {
		return nil
	}
	return s.Status
}

// Error implements LogStatus as error.
func (s *LogStatus) Error() string {
	if err := s.TryIntoLogError(); err != nil {
		return fmt.Sprintf("%s; log error: code = %s, cause = %s; rpc error: code = %s, desc = %s", s.method, err.Code.String(), err.Cause, s.Code(), s.Message())
	}
	return fmt.Sprintf("%s; rpc error: code = %s, desc = %s", s.method, s.Code(), s.Message())
}
