package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func newTestSplitShardMutableMessage() message.MutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId: 1,
			SplitTaskId:  100,
			Targets: []*message.SplitShardTarget{
				{Vchannel: "v2", KeyRange: &message.KeyRange{Upper: []byte{0x80}}},
				{Vchannel: "v3", KeyRange: &message.KeyRange{Lower: []byte{0x80}}},
			},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()
}

func newTestShardInterceptor(t *testing.T) (interceptors.Interceptor, *mock_shards.MockShardManager) {
	core, _ := observer.New(zapcore.WarnLevel)
	logger := &log.MLogger{Logger: zap.New(core)}
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(logger).Maybe()
	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	t.Cleanup(i.Close)
	return i, shardManager
}

func TestShardInterceptorSplitShardMessage(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1)).Return(nil).Once()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(int64(1), uint64(100)).Return([]int64{7}, nil).Once()
	shardManager.EXPECT().SplitShard(mock.Anything).Once()

	var appendedMsg message.MutableMessage
	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appendedMsg = msg
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.NotNil(t, appendedMsg)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
	// the auto-flushed segment ids are embedded into the split message header,
	// the single seal record for T_switch.
	header := message.MustAsMutableSplitShardMessageV2(appendedMsg).Header()
	assert.Equal(t, []int64{7}, header.GetFlushedSegmentIds())
}

func newTestCreateVChannelMutableMessage(vchannel string, collectionID int64) message.MutableMessage {
	return message.NewCreateVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId:        collectionID,
			PartitionIds:        []int64{2},
			SplitTaskId:         100,
			SplitSourceVchannel: "v1",
			KeyRange:            &message.KeyRange{Upper: []byte{0x80}},
		}).
		WithBody(&message.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmedUseMessageID()
}

func TestShardInterceptorCreateVChannelMessage(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfCollectionCanBeCreated(int64(7)).Return(nil).Once()
	shardManager.EXPECT().CreateVChannel(mock.Anything).Once()

	appended := false
	msgID, err := i.DoAppend(context.Background(), newTestCreateVChannelMutableMessage("v2", 7),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
	assert.True(t, msgID.EQ(rmq.NewRmqID(1)))
}

func TestShardInterceptorCreateVChannelMessageAppendFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfCollectionCanBeCreated(int64(7)).Return(nil).Once()

	_, err := i.DoAppend(context.Background(), newTestCreateVChannelMutableMessage("v2", 7),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, errors.New("mock append error")
		})
	assert.Error(t, err)
}

func TestShardInterceptorCreateVChannelMessageOnExistingCollection(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	// the collection already exists on this pchannel: the genesis is still
	// appended and applied (idempotent), only a warning is logged.
	shardManager.EXPECT().CheckIfCollectionCanBeCreated(int64(7)).Return(errors.New("already exists")).Once()
	shardManager.EXPECT().CreateVChannel(mock.Anything).Once()

	appended := false
	_, err := i.DoAppend(context.Background(), newTestCreateVChannelMutableMessage("v2", 7),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	assert.NoError(t, err)
	assert.True(t, appended)
}

func TestShardInterceptorSplitShardMessageOnFencedVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	// idempotent: the vchannel is already fenced by a previous split message.
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1)).Return(shards.ErrVChannelFenced).Once()

	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on a fenced vchannel")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsShardFenced())
}

func TestShardInterceptorSplitShardMessageOnUnknownCollection(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1)).Return(shards.ErrCollectionNotFound).Once()

	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on an unknown collection")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
	assert.False(t, status.AsStreamingError(err).IsShardFenced())
}

func TestShardInterceptorSplitShardMessageFlushFailure(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1)).Return(nil).Once()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(int64(1), uint64(100)).Return(nil, errors.New("mock flush error")).Once()

	msgID, err := i.DoAppend(context.Background(), newTestSplitShardMutableMessage(),
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called when the flush fails")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.Error(t, err)
}

func TestShardInterceptorInsertOnFencedVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1)).Return(shards.ErrVChannelFenced).Once()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 1, Rows: 1, BinarySize: 100},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(100)

	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on a fenced vchannel")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsShardFenced())
}

func TestShardInterceptorDeleteOnFencedVChannel(t *testing.T) {
	i, shardManager := newTestShardInterceptor(t)
	shardManager.EXPECT().CheckIfVChannelCanBeWritten(int64(1)).Return(shards.ErrVChannelFenced).Once()

	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.DeleteMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable().WithTimeTick(100)

	msgID, err := i.DoAppend(context.Background(), msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			assert.Fail(t, "the append should not be called on a fenced vchannel")
			return nil, nil
		})
	assert.Nil(t, msgID)
	assert.True(t, status.AsStreamingError(err).IsShardFenced())
}
