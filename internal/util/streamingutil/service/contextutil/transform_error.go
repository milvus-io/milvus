package contextutil

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// TransformLogErrorReason preserves subscription errors across gRPC transport.
func TransformLogErrorReason(err error) streamingpb.TransformSubscriptionErrorReason {
	switch {
	case errors.Is(err, wal.ErrTransformLogInvalidReadOption):
		return streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_INVALID_OPTION
	case errors.Is(err, wal.ErrTransformLogStartPointTruncated):
		return streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_TRUNCATED
	case errors.Is(err, wal.ErrTransformLogVChannelUnavailable):
		return streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_UNAVAILABLE
	default:
		return streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_UNKNOWN
	}
}

func TransformLogErrorFromProto(resp *streamingpb.TransformSubscriptionError) error {
	switch resp.GetReason() {
	case streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_INVALID_OPTION:
		return wal.ErrTransformLogInvalidReadOption
	case streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_TRUNCATED:
		return wal.ErrTransformLogStartPointTruncated
	case streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_UNAVAILABLE:
		return wal.ErrTransformLogVChannelUnavailable
	default:
		if resp.GetError() != nil {
			return (*status.StreamingError)(resp.GetError())
		}
		return status.NewUnknownError("transform subscription error")
	}
}
