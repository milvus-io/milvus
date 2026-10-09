package transformlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRetryableSubscriptionError(t *testing.T) {
	for _, err := range []error{
		context.Canceled, context.DeadlineExceeded,
		grpcstatus.Error(codes.Canceled, "owner closed"), grpcstatus.Error(codes.DeadlineExceeded, "owner timed out"),
		status.AsStreamingError(context.Canceled), status.AsStreamingError(context.DeadlineExceeded),
		status.NewOnShutdownError("owner closed"), status.NewChannelFenced("p"),
		status.NewChannelNotExist("p"), status.NewUnmatchedChannelTerm("p", 1, 2),
		merr.Wrap(status.NewChannelFenced("p"), "old owner"),
	} {
		require.True(t, isRetryableSubscriptionError(err), "%v", err)
	}
	for _, err := range []error{
		nil, wal.ErrTransformLogInvalidReadOption, wal.ErrTransformLogVChannelUnavailable, wal.ErrTransformLogStartPointTruncated,
		status.NewUnknownError("corrupt retained chunk"), status.NewUnknownError("read failed: context canceled"),
		status.NewInvalidArgument("invalid start"), status.NewUnrecoverableError("context canceled"),
	} {
		require.False(t, isRetryableSubscriptionError(err), "%v", err)
	}
}
