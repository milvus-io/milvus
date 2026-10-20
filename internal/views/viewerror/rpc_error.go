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

var viewCodeToGRPCStatus = map[viewpb.ViewCode]codes.Code{
	viewpb.ViewCode_VIEW_CODE_OK:               codes.OK,
	viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED: codes.FailedPrecondition,
	viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND:   codes.NotFound,
	viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN:      codes.Unavailable,
	viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY:      codes.FailedPrecondition,
	viewpb.ViewCode_VIEW_CODE_UNKNOWN:          codes.Unknown,
}

// NewGRPCStatusFromViewError converts ViewError to grpc status.
// Should be called at server-side.
func NewGRPCStatusFromViewError(e *ViewError) *status.Status {
	if e == nil || e.Code == viewpb.ViewCode_VIEW_CODE_OK {
		return status.New(codes.OK, "")
	}

	code, ok := viewCodeToGRPCStatus[e.Code]
	if !ok {
		code = codes.Unknown
	}

	st := status.New(code, "")
	newST, err := st.WithDetails(e.AsPBError())
	if err != nil {
		return status.New(code, fmt.Sprintf("convert view error failed, detail: %s", e.Cause))
	}
	return newST
}

// ViewClientStatus is a wrapper of grpc status.
// Should be used in client side.
type ViewClientStatus struct {
	*status.Status
	method string
}

// ConvertViewError converts error to ViewClientStatus.
// Used in client side.
func ConvertViewError(method string, err error) error {
	if err == nil {
		return nil
	}
	if errors.IsAny(err, context.DeadlineExceeded, context.Canceled, io.EOF) {
		return err
	}
	rpcStatus := status.Convert(err)
	return &ViewClientStatus{
		Status: rpcStatus,
		method: method,
	}
}

// TryIntoViewError tries to extract a ViewError from the gRPC status details.
func (s *ViewClientStatus) TryIntoViewError() *ViewError {
	if s == nil {
		return nil
	}
	for _, detail := range s.Details() {
		if detail, ok := detail.(*viewpb.ViewError); ok {
			return New(detail.Code, detail.Cause)
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

// Error implements error.
func (s *ViewClientStatus) Error() string {
	if viewErr := s.TryIntoViewError(); viewErr != nil {
		return fmt.Sprintf("%s; view error: code = %s, cause = %s; rpc error: code = %s, desc = %s",
			s.method, viewErr.Code.String(), viewErr.Cause, s.Code(), s.Message())
	}
	return fmt.Sprintf("%s; rpc error: code = %s, desc = %s", s.method, s.Code(), s.Message())
}
