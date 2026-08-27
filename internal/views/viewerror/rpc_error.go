package viewerror

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var viewCodeToGRPCCode = map[viewpb.ViewCode]codes.Code{
	viewpb.ViewCode_VIEW_CODE_OK:               codes.OK,
	viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED: codes.FailedPrecondition,
	viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND:   codes.NotFound,
	viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN:      codes.Unavailable,
	viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY:      codes.FailedPrecondition,
	viewpb.ViewCode_VIEW_CODE_UNKNOWN:          codes.Unknown,
}

// NewGRPCStatusFromViewError converts a QueryView error into a gRPC status with ViewError detail.
func NewGRPCStatusFromViewError(err *ViewError) *status.Status {
	if err == nil || err.Code == viewpb.ViewCode_VIEW_CODE_OK {
		return status.New(codes.OK, "")
	}
	code, ok := viewCodeToGRPCCode[err.Code]
	if !ok {
		code = codes.Unknown
	}
	st, detailErr := status.New(code, "query view operation failed").WithDetails(err.AsPBError())
	if detailErr != nil {
		return status.New(code, fmt.Sprintf("query view operation failed: %s", err.Cause))
	}
	return st
}

// ViewClientStatus keeps the original RPC status and exposes QueryView details.
type ViewClientStatus struct {
	*status.Status
	method string
}

// ConvertViewError converts a client-side gRPC error without changing context and EOF errors.
func ConvertViewError(method string, err error) error {
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded, io.EOF) {
		return err
	}
	return &ViewClientStatus{Status: status.Convert(err), method: method}
}

// TryIntoViewError extracts the ViewError status detail, if present.
func (s *ViewClientStatus) TryIntoViewError() *ViewError {
	if s == nil || s.Status == nil {
		return nil
	}
	for _, detail := range s.Details() {
		if wireErr, ok := detail.(*viewpb.ViewError); ok {
			return New(wireErr.GetCode(), wireErr.GetCause())
		}
	}
	return nil
}

// GRPCStatus returns the underlying gRPC status.
func (s *ViewClientStatus) GRPCStatus() *status.Status {
	if s == nil {
		return nil
	}
	return s.Status
}

func (s *ViewClientStatus) Error() string {
	if s == nil || s.Status == nil {
		return "query view RPC status is nil"
	}
	if viewErr := s.TryIntoViewError(); viewErr != nil {
		return fmt.Sprintf("%s: %s; %s", s.method, s.Err(), viewErr)
	}
	return fmt.Sprintf("%s: %s", s.method, s.Err())
}

// Unwrap exposes the underlying gRPC status error to standard error traversal.
func (s *ViewClientStatus) Unwrap() error {
	if s == nil || s.Status == nil {
		return nil
	}
	return s.Err()
}
