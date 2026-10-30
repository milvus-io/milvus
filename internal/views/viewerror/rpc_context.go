package viewerror

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/status"

	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// AttachRPCContext adds structured gRPC details without replacing the original
// error chain: readiness, session discovery and cancellation still inspect it.
// WithDetails serializes a snapshot, so later changes to info cannot affect err.
func AttachRPCContext(err error, info *viewpb.RPCRequestContext) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
			st = status.FromContextError(err)
		} else {
			var ve *ViewError
			var se *streamingstatus.StreamingError
			if errors.As(err, &se) {
				st = streamingstatus.NewGRPCStatusFromStreamingError(se)
			} else if merr.IsMilvusError(err) || errors.As(err, &ve) {
				st = NewGRPCStatusFromError(err)
			} else {
				st = status.Convert(err)
			}
		}
	}
	detailed, detailErr := st.WithDetails(info)
	if detailErr != nil {
		return err
	}
	return &rpcErrorWithContext{error: err, status: detailed}
}

type rpcErrorWithContext struct {
	error
	status *status.Status
}

func (e *rpcErrorWithContext) Unwrap() error              { return e.error }
func (e *rpcErrorWithContext) GRPCStatus() *status.Status { return e.status }

// RPCContextFromDetails uses the last matching detail: the local client's
// actual target takes precedence over context supplied by a remote peer.
func RPCContextFromDetails(details []any) *viewpb.RPCRequestContext {
	for i := len(details) - 1; i >= 0; i-- {
		if detail, ok := details[i].(*viewpb.RPCRequestContext); ok {
			return detail
		}
	}
	return nil
}
