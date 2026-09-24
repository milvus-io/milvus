package viewerror

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ error = (*ViewError)(nil)

// ViewError is the error type for view-related operations.
// Should be used at logic layer.
type (
	ViewError ViewErrorInner
	ViewCode  viewpb.ViewCode
)

// ViewErrorInner is the underlying type aliased from proto.
type ViewErrorInner = viewpb.ViewError

// Error implements error.
func (e *ViewError) Error() string {
	return fmt.Sprintf("code: %s, cause: %s", e.Code.String(), e.Cause)
}

// AsPBError converts ViewError to viewpb.ViewError.
func (e *ViewError) AsPBError() *viewpb.ViewError {
	return (*viewpb.ViewError)(e)
}

// IsViewInvalidated returns true if the view version is no longer valid.
func (e *ViewError) IsViewInvalidated() bool {
	return e.Code == viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED
}

// IsViewNotFound returns true if the view version was not found on the node.
func (e *ViewError) IsViewNotFound() bool {
	return e.Code == viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND
}

// IsOnShutdown returns true if the node is shutting down.
func (e *ViewError) IsOnShutdown() bool {
	return e.Code == viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN
}

// IsNotPrimary returns true if the node is not the primary replica for this shard.
func (e *ViewError) IsNotPrimary() bool {
	return e.Code == viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY
}

// IsRetryable returns true if the error should trigger a retry from Phase 1.
func (e *ViewError) IsRetryable() bool {
	return e.IsViewInvalidated() || e.IsViewNotFound() || e.IsOnShutdown() || e.IsNotPrimary()
}

// NewViewInvalidated creates a ViewError with VIEW_CODE_VIEW_INVALIDATED.
func NewViewInvalidated(format string, args ...interface{}) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED, format, args...)
}

// NewViewNotFound creates a ViewError with VIEW_CODE_VIEW_NOT_FOUND.
func NewViewNotFound(format string, args ...interface{}) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, format, args...)
}

// NewOnShutdownError creates a ViewError with VIEW_CODE_ON_SHUTDOWN.
func NewOnShutdownError(format string, args ...interface{}) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN, format, args...)
}

// NewNotPrimaryError creates a ViewError with VIEW_CODE_NOT_PRIMARY.
func NewNotPrimaryError(format string, args ...interface{}) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY, format, args...)
}

// NewUnknownError creates a ViewError with VIEW_CODE_UNKNOWN.
func NewUnknownError(format string, args ...interface{}) *ViewError {
	return New(viewpb.ViewCode_VIEW_CODE_UNKNOWN, format, args...)
}

// New creates a new ViewError with the given code and cause.
func New(code viewpb.ViewCode, format string, args ...interface{}) *ViewError {
	if len(args) == 0 {
		return &ViewError{
			Code:  code,
			Cause: format,
		}
	}
	return &ViewError{
		Code:  code,
		Cause: redact.Sprintf(format, args...).StripMarkers(),
	}
}

// AsViewError converts an error to a ViewError.
// If the error is already a ViewError, returns it directly.
// Otherwise wraps it as VIEW_CODE_UNKNOWN.
func AsViewError(err error) *ViewError {
	if err == nil {
		return nil
	}

	var e *ViewError
	if errors.As(err, &e) {
		return e
	}
	var clientStatus *ViewClientStatus
	if errors.As(err, &clientStatus) {
		if viewErr := clientStatus.TryIntoViewError(); viewErr != nil {
			return viewErr
		}
	}

	return &ViewError{
		Code:  viewpb.ViewCode_VIEW_CODE_UNKNOWN,
		Cause: err.Error(),
	}
}
