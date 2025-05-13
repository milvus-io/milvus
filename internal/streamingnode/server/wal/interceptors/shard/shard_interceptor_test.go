package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

func TestShardInterceptor(t *testing.T) {
	mockErr := errors.New("mock error")

	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()
	ctx := context.Background()
	appender := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	}

	vchannel := "v1"
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{1},
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable()
	shardManager.EXPECT().CheckIfCollectionCanBeCreated(mock.Anything).Return(nil)
	shardManager.EXPECT().CreateCollection(mock.Anything).Return()
	msgID, err := i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfCollectionCanBeCreated(mock.Anything).Unset()
	shardManager.EXPECT().CheckIfCollectionCanBeCreated(mock.Anything).Return(mockErr)
	shardManager.EXPECT().CreateCollection(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	msg = message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.DropCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable()
	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Return(nil)
	shardManager.EXPECT().DropCollection(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Unset()
	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Return(mockErr)
	shardManager.EXPECT().DropCollection(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	msg = message.NewCreatePartitionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.CreatePartitionMessageHeader{
			CollectionId: 1,
			PartitionId:  1,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable()
	shardManager.EXPECT().CheckIfPartitionCanBeCreated(mock.Anything, mock.Anything).Return(nil)
	shardManager.EXPECT().CreatePartition(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfPartitionCanBeCreated(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().CheckIfPartitionCanBeCreated(mock.Anything, mock.Anything).Return(mockErr)
	shardManager.EXPECT().CreatePartition(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	msg = message.NewDropPartitionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.DropPartitionMessageHeader{
			CollectionId: 1,
			PartitionId:  1,
		}).
		WithBody(&msgpb.DropPartitionRequest{}).
		MustBuildMutable()
	shardManager.EXPECT().CheckIfPartitionExists(mock.Anything, mock.Anything).Return(nil)
	shardManager.EXPECT().DropPartition(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfPartitionExists(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().CheckIfPartitionExists(mock.Anything, mock.Anything).Return(mockErr)
	shardManager.EXPECT().DropPartition(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	msg = message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&messagespb.CreateSegmentMessageHeader{
			CollectionId: 1,
			PartitionId:  1,
			SegmentId:    1,
		}).
		WithBody(&messagespb.CreateSegmentMessageBody{}).
		MustBuildMutable()
	shardManager.EXPECT().CheckIfSegmentCanBeCreated(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	shardManager.EXPECT().CreateSegment(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfSegmentCanBeCreated(mock.Anything, mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().CheckIfSegmentCanBeCreated(mock.Anything, mock.Anything, mock.Anything).Return(mockErr)
	shardManager.EXPECT().CreateSegment(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	msg = message.NewFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&messagespb.FlushMessageHeader{
			CollectionId: 1,
			PartitionId:  1,
			SegmentId:    1,
		}).
		WithBody(&messagespb.FlushMessageBody{}).
		MustBuildMutable()
	shardManager.EXPECT().CheckIfSegmentCanBeFlushed(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	shardManager.EXPECT().FlushSegment(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfSegmentCanBeFlushed(mock.Anything, mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().CheckIfSegmentCanBeFlushed(mock.Anything, mock.Anything, mock.Anything).Return(mockErr)
	shardManager.EXPECT().FlushSegment(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	ctx = utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{})
	msg = message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&messagespb.ManualFlushMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&messagespb.ManualFlushMessageBody{}).
		MustBuildMutable().WithTimeTick(1)
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Return(nil, nil)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Return(nil, mockErr)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	msg = message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 1,
					Rows:        1,
					BinarySize:  100,
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(1)

	shardManager.EXPECT().AssignSegment(mock.Anything).Return(&shards.AssignSegmentResult{SegmentID: 1, Acknowledge: atomic.NewInt32(1)}, nil)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().AssignSegment(mock.Anything).Unset()
	shardManager.EXPECT().AssignSegment(mock.Anything).Return(nil, mockErr)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	msg = message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.DeleteMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable().WithTimeTick(1)

	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Unset()
	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Return(nil)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Unset()
	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Return(mockErr)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)
}
