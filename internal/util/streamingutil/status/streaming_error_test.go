package status

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestStreamingError(t *testing.T) {
	streamingErr := NewOnShutdownError("test")
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_ON_SHUTDOWN, cause: test")
	assert.False(t, streamingErr.IsWrongStreamingNode())
	pbErr := streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, pbErr.Code)

	streamingErr = NewUnknownError("test")
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_UNKNOWN, cause: test")
	assert.False(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN, pbErr.Code)

	streamingErr = NewInvalidRequestSeq("test")
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_INVALID_REQUEST_SEQ, cause: test")
	assert.False(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_REQUEST_SEQ, pbErr.Code)

	streamingErr = NewChannelNotExist("test")
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_CHANNEL_NOT_EXIST, cause: test")
	assert.True(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST, pbErr.Code)

	streamingErr = NewUnmatchedChannelTerm("test", 1, 2)
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_UNMATCHED_CHANNEL_TERM, cause: channel test")
	assert.True(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM, pbErr.Code)

	streamingErr = NewIgnoreOperation("test")
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_IGNORED_OPERATION, cause: test")
	assert.False(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_IGNORED_OPERATION, pbErr.Code)

	streamingErr = NewInner("test")
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_INNER, cause: test")
	assert.False(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INNER, pbErr.Code)

	streamingErr = NewOnShutdownError("test, %d", 1)
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_ON_SHUTDOWN, cause: test, 1")
	assert.False(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, pbErr.Code)

	streamingErr = NewResourceAcquired("test, %d", 1)
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_RESOURCE_ACQUIRED, cause: test, 1")
	assert.False(t, streamingErr.IsWrongStreamingNode())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_RESOURCE_ACQUIRED, pbErr.Code)

	streamingErr = NewTransactionExpired("test, %d", 1)
	assert.Contains(t, streamingErr.Error(), "code: STREAMING_CODE_TRANSACTION_EXPIRED, cause: test, 1")
	assert.True(t, streamingErr.IsTxnExpired())
	assert.True(t, streamingErr.IsTxnUnavilable())
	assert.True(t, streamingErr.IsUnrecoverable())
	pbErr = streamingErr.AsPBError()
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, pbErr.Code)
}
