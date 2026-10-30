package queryclient

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// toMilvusError is the collection-level client boundary, after all internal
// retries. Restore structured business errors before interpreting protocol codes;
// an UNKNOWN ViewError can still carry a precise business error such as 2042.
func toMilvusError(err error) (result error) {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		details := st.Details()
		if info := viewerror.RPCContextFromDetails(details); info != nil {
			defer func() { result = formatRPCContext(result, info) }()
		}
		for _, detail := range details {
			if business, ok := detail.(*commonpb.Status); ok && !merr.Ok(business) {
				return restoreBusinessError(business)
			}
		}
		for _, detail := range details {
			switch e := detail.(type) {
			case *viewpb.ViewError:
				return viewMilvusError(e.Code, e.Cause)
			case *streamingpb.StreamingError:
				return streamingMilvusError(e.Code, e.Cause)
			}
		}
		if errors.Is(err, io.EOF) {
			return merr.WrapErrServiceUnavailableErr(io.EOF, "query connection closed")
		}
		switch st.Code() {
		case codes.Canceled:
			return contextMilvusError(context.Canceled)
		case codes.DeadlineExceeded:
			return contextMilvusError(context.DeadlineExceeded)
		case codes.Unavailable:
			return merr.Wrap(merr.ErrServiceUnavailable, st.Message())
		case codes.Unimplemented:
			return merr.Wrap(merr.ErrServiceUnimplemented, st.Message())
		default:
			// InvalidArgument here describes an internal RPC request, not
			// necessarily user input. User errors must carry a business status.
			return merr.Wrap(merr.ErrServiceInternal, st.Message())
		}
	}
	// LocalWAL and injected local providers need the same mapping as remote RPCs.
	var ve *viewerror.ViewError
	if errors.As(err, &ve) {
		return viewMilvusError(ve.Code, ve.Cause)
	}
	var se *streamingstatus.StreamingError
	if errors.As(err, &se) {
		return streamingMilvusError(se.Code, se.Cause)
	}
	if merr.IsMilvusError(err) {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return contextMilvusError(context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return contextMilvusError(context.DeadlineExceeded)
	}
	if errors.Is(err, io.EOF) {
		return merr.WrapErrServiceUnavailableErr(err, "query connection closed")
	}
	return merr.WrapErrServiceInternalErr(err, "query execution failed")
}

func restoreBusinessError(business *commonpb.Status) error {
	err := merr.Error(business)
	switch business.GetCode() {
	case merr.CanceledCode:
		return merr.Mark(err, context.Canceled)
	case merr.TimeoutCode:
		return merr.Mark(err, context.DeadlineExceeded)
	default:
		return err
	}
}

func contextMilvusError(cause error) error {
	// Reconstitute the existing public code without retaining an RPC wrapper,
	// while keeping errors.Is(..., context.Canceled/DeadlineExceeded) meaningful.
	return merr.Mark(merr.Error(merr.Status(cause)), cause)
}

func viewMilvusError(code viewpb.ViewCode, cause string) error {
	switch code {
	case viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, viewpb.ViewCode_VIEW_CODE_VIEW_INVALIDATED:
		return merr.Wrap(merr.ErrQueryViewNotReady, cause)
	case viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN, viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY:
		return merr.Wrap(merr.ErrServiceUnavailable, cause)
	default:
		return merr.Wrap(merr.ErrServiceInternal, cause)
	}
}

func streamingMilvusError(code streamingpb.StreamingCode, cause string) error {
	switch code {
	case streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
		streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED,
		streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM,
		streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN:
		return merr.Wrap(merr.ErrServiceUnavailable, cause)
	case streamingpb.StreamingCode_STREAMING_CODE_RATE_LIMIT_REJECTED:
		return merr.Wrap(merr.ErrServiceRateLimit, cause)
	default:
		// Transaction/write errors and malformed channel metadata are internal
		// failures in a query client; do not blame the caller's query parameters.
		return merr.Wrap(merr.ErrServiceInternal, cause)
	}
}

// Formatting belongs only to the public boundary; internal retries keep details.
func formatRPCContext(err error, info *viewpb.RPCRequestContext) error {
	var operation string
	switch info.Method {
	case viewpb.ViewQueryService_SearchOnView_FullMethodName:
		operation = "search"
	case viewpb.ViewQueryService_QueryOnView_FullMethodName:
		operation = "query"
	case viewpb.ViewQueryService_StatisticsOnView_FullMethodName:
		operation = "get statistics"
	case viewpb.QueryPlanService_GetQueryPlan_FullMethodName:
		operation = "get query plan"
	case viewpb.QueryPlanService_GetMVCCTimestamp_FullMethodName:
		operation = "get MVCC timestamp"
	default:
		return err
	}
	switch target := info.Target.(type) {
	case *viewpb.RPCRequestContext_QueryNode:
		return merr.Wrapf(err, "fail to %s on QueryNode %d", operation, target.QueryNode.GetNodeId())
	case *viewpb.RPCRequestContext_StreamingNode:
		node := target.StreamingNode
		return merr.Wrapf(err, "fail to %s on StreamingNode %d", operation, node.GetNodeId())
	default:
		return err
	}
}
