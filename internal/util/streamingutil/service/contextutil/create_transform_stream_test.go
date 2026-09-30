package contextutil

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestTransformStreamMetadata(t *testing.T) {
	req := &streamingpb.CreateTransformStreamRequest{Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 7}}
	ctx := WithCreateTransformStream(context.Background(), req)
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	actual, err := GetCreateTransformStream(metadata.NewIncomingContext(context.Background(), md))
	require.NoError(t, err)
	require.True(t, proto.Equal(req, actual))
	for _, ctx := range []context.Context{context.Background(), metadata.NewIncomingContext(context.Background(), metadata.MD{}), metadata.NewIncomingContext(context.Background(), metadata.Pairs(createTransformStreamKey, "!")), metadata.NewIncomingContext(context.Background(), metadata.Pairs(createTransformStreamKey, base64.StdEncoding.EncodeToString([]byte{255})))} {
		_, err := GetCreateTransformStream(ctx)
		require.Error(t, err)
	}
}

func TestTransformSubscriptionErrorRoundTrip(t *testing.T) {
	for _, err := range []error{wal.ErrTransformLogInvalidReadOption, wal.ErrTransformLogStartPointTruncated, wal.ErrTransformLogVChannelUnavailable} {
		encoded := &streamingpb.TransformSubscriptionError{Reason: TransformLogErrorReason(err), Error: status.AsStreamingError(err).AsPBError()}
		require.ErrorIs(t, TransformLogErrorFromProto(encoded), err)
	}
	err := status.NewUnrecoverableError("corrupt history")
	actual := TransformLogErrorFromProto(&streamingpb.TransformSubscriptionError{Error: err.AsPBError()})
	require.True(t, status.AsStreamingError(actual).IsUnrecoverable())
	require.NotNil(t, TransformLogErrorFromProto(&streamingpb.TransformSubscriptionError{}))
	require.Equal(t, streamingpb.TransformSubscriptionErrorReason_TRANSFORM_SUBSCRIPTION_ERROR_REASON_UNKNOWN, TransformLogErrorReason(context.Canceled))
}
