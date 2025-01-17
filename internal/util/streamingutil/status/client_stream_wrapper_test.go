package status

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/google.golang.org/mock_grpc"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
)

func TestClientStreamWrapper(t *testing.T) {
	s := mock_grpc.NewMockClientStream(t)
	s.EXPECT().SendMsg(mock.Anything).Return(NewGRPCStatusFromStreamingError(NewOnShutdownError("test")).Err())
	s.EXPECT().RecvMsg(mock.Anything).Return(NewGRPCStatusFromStreamingError(NewOnShutdownError("test")).Err())
	w := NewClientStreamWrapper("method", s)

	err := w.SendMsg(context.Background())
	assert.NotNil(t, err)
	streamingErr := AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, streamingErr.Code)
	assert.Contains(t, streamingErr.Cause, "test")

	err = w.RecvMsg(context.Background())
	assert.NotNil(t, err)
	streamingErr = AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, streamingErr.Code)
	assert.Contains(t, streamingErr.Cause, "test")

	assert.Nil(t, NewClientStreamWrapper("method", nil))
}
