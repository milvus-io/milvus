package analyzerservice

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// StreamingError projects execution failures into the SN's native error domain.
// RPC responses never carry an application Status.
func StreamingError(err error) error {
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	var se *status.StreamingError
	if errors.As(err, &se) {
		return err
	}
	code := streamingpb.StreamingCode_STREAMING_CODE_INNER
	if errors.Is(err, merr.ErrCollectionSchemaVersionNotReady) {
		code = streamingpb.StreamingCode_STREAMING_CODE_SCHEMA_VERSION_MISMATCH
	} else if merr.GetErrorType(err) == merr.InputError {
		code = streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT
	}
	return &status.StreamingError{Code: code, Cause: err.Error()}
}

// PublicError is used only at the public Proxy response boundary.
func PublicError(err error) error {
	if err == nil || status.IsCanceled(err) {
		return err
	}
	se := status.AsStreamingError(err)
	if se.IsInvalidArgument() {
		return merr.WrapErrParameterInvalidErr(err, "run analyzer")
	}
	if se.IsSchemaVersionMismatch() {
		return merr.Wrap(merr.ErrCollectionSchemaVersionNotReady, se.Cause)
	}
	if grpcstatus.Code(err) == codes.Unimplemented {
		return merr.Wrap(merr.ErrServiceUnimplemented, err.Error())
	}
	return merr.WrapErrServiceUnavailableErr(err, "run analyzer")
}

func Retryable(err error) bool {
	if err == nil || status.IsCanceled(err) {
		return false
	}
	se := status.AsStreamingError(err)
	return se.IsWrongStreamingNode() || se.IsOnShutdown() || se.Code == streamingpb.StreamingCode_STREAMING_CODE_INNER || grpcstatus.Code(err) == codes.Unavailable
}
