package viewerror

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ViewError is the in-process representation of a QueryView protocol error.
type ViewError viewpb.ViewError

func (e *ViewError) Error() string {
	if e == nil {
		return "query view error is nil"
	}
	return fmt.Sprintf("query view error: code=%s, cause=%s", e.Code.String(), e.Cause)
}

// AsPBError converts the error to its wire representation.
func (e *ViewError) AsPBError() *viewpb.ViewError {
	return (*viewpb.ViewError)(e)
}

// IsRetryable reports whether rerunning Phase 1 may produce a usable view.
func (e *ViewError) IsRetryable() bool {
	return e != nil && (e.Code == viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED ||
		e.Code == viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND ||
		e.Code == viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN ||
		e.Code == viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY)
}

// New creates a QueryView error with a safely formatted cause.
func New(code viewpb.ViewCode, format string, args ...any) *ViewError {
	cause := format
	if len(args) > 0 {
		cause = redact.Sprintf(format, args...).StripMarkers()
	}
	return &ViewError{Code: code, Cause: cause}
}

func NewViewInvalidated(format string, args ...any) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED, format, args...)
}

func NewViewNotFound(format string, args ...any) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, format, args...)
}

func NewOnShutdownError(format string, args ...any) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN, format, args...)
}

func NewNotPrimaryError(format string, args ...any) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY, format, args...)
}

func NewUnknownError(format string, args ...any) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_UNKNOWN, format, args...)
}

// TryAsViewError extracts a QueryView error from an in-process error or converted gRPC status.
// It returns nil when the error has no QueryView detail.
func TryAsViewError(err error) *ViewError {
	if err == nil {
		return nil
	}
	var viewErr *ViewError
	if errors.As(err, &viewErr) {
		return viewErr
	}
	var clientStatus *ViewClientStatus
	if errors.As(err, &clientStatus) {
		return clientStatus.TryIntoViewError()
	}
	return nil
}

// AsViewError converts any error to a ViewError. Plain errors use UNKNOWN;
// callers that need to distinguish absence of wire detail use TryAsViewError.
func AsViewError(err error) *ViewError {
	if err == nil {
		return nil
	}
	if viewErr := TryAsViewError(err); viewErr != nil {
		return viewErr
	}
	return NewUnknownError(err.Error())
}
