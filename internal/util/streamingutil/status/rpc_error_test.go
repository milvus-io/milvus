package status

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestStreamingStatus(t *testing.T) {
	err := ConvertStreamingError("test", nil)
	assert.Nil(t, err)
	err = ConvertStreamingError("test", errors.Wrap(context.DeadlineExceeded, "test"))
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	err = ConvertStreamingError("test", errors.New("test"))
	assert.NotNil(t, err)
	streamingErr := AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN, streamingErr.Code)
	assert.Contains(t, streamingErr.Cause, "test; rpc error: code = Unknown, desc = test")

	err = ConvertStreamingError("test", NewGRPCStatusFromStreamingError(NewOnShutdownError("test")).Err())
	assert.NotNil(t, err)
	streamingErr = AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, streamingErr.Code)
	assert.Contains(t, streamingErr.Cause, "test")
	assert.Contains(t, err.Error(), "streaming error")
}

func TestNewGRPCStatusFromStreamingError(t *testing.T) {
	st := NewGRPCStatusFromStreamingError(nil)
	assert.Equal(t, codes.OK, st.Code())

	st = NewGRPCStatusFromStreamingError(
		NewOnShutdownError("test"),
	)
	assert.Equal(t, codes.FailedPrecondition, st.Code())

	st = NewGRPCStatusFromStreamingError(
		New(10086, "test"),
	)
	assert.Equal(t, codes.Unknown, st.Code())
}
