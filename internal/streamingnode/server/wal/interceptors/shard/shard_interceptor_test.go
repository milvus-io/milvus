package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestShardInterceptorLogsOmittedSchemaVersionAsNotProvided(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	logger := &log.MLogger{Logger: zap.New(core)}
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(logger).Maybe()
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
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

	insertHdrMatcher := mock.MatchedBy(func(h *message.InsertMessageHeader) bool {
		return h != nil && h.GetCollectionId() == int64(1) && h.SchemaVersion == nil
	})
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(5), shards.ErrCollectionSchemaVersionNotMatch)

	msgID, err := i.DoAppend(context.Background(), msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.Error(t, err)
	assert.Nil(t, msgID)

	entries := logs.FilterMessage("insertMessage schema version mismatch").All()
	assert.Len(t, entries, 1)
	assert.Equal(t, false, entries[0].ContextMap()["schemaVersionProvided"])
}

func TestShardInterceptorReportsExplicitZeroSchemaVersionInMismatchError(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(log.With()).Maybe()
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()

	zero := proto.Int32(0)
	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 1,
					Rows:        1,
					BinarySize:  100,
				},
			},
			SchemaVersion: zero,
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(1)

	insertHdrMatcher := mock.MatchedBy(func(h *message.InsertMessageHeader) bool {
		return h != nil && h.GetCollectionId() == int64(1) && h.SchemaVersion != nil && h.GetSchemaVersion() == 0
	})
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(5), shards.ErrCollectionSchemaVersionNotMatch)

	msgID, err := i.DoAppend(context.Background(), msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "input schema version: 0")
	assert.Nil(t, msgID)
}

func TestShardInterceptorPassesExplicitNonZeroSchemaVersion(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(log.With()).Maybe()
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 1,
					Rows:        1,
					BinarySize:  100,
				},
			},
			SchemaVersion: proto.Int32(3),
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(1)

	insertHdrMatcher := mock.MatchedBy(func(h *message.InsertMessageHeader) bool {
		return h != nil && h.GetCollectionId() == int64(1) && h.SchemaVersion != nil && h.GetSchemaVersion() == 3
	})
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(3), nil)
	shardManager.EXPECT().AssignSegment(mock.Anything).Return(&shards.AssignSegmentResult{SegmentID: 1, Acknowledge: atomic.NewInt32(1)}, nil)

	msgID, err := i.DoAppend(context.Background(), msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)
}

func TestShardInterceptorPassesExplicitZeroSchemaVersion(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(log.With()).Maybe()
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()

	zero := proto.Int32(0)
	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 1,
					Rows:        1,
					BinarySize:  100,
				},
			},
			SchemaVersion: zero,
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(1)

	insertHdrMatcher := mock.MatchedBy(func(h *message.InsertMessageHeader) bool {
		return h != nil && h.GetCollectionId() == int64(1) && h.SchemaVersion != nil && h.GetSchemaVersion() == 0
	})
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(0), nil)
	shardManager.EXPECT().AssignSegment(mock.Anything).Return(&shards.AssignSegmentResult{SegmentID: 1, Acknowledge: atomic.NewInt32(1)}, nil)

	msgID, err := i.DoAppend(context.Background(), msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)
}

func TestShardInterceptor(t *testing.T) {
	mockErr := errors.New("mock error")

	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(log.With()).Maybe()
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
	shardManager.EXPECT().CheckIfPartitionCanBeCreated(mock.Anything).Return(nil)
	shardManager.EXPECT().CreatePartition(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfPartitionCanBeCreated(mock.Anything).Unset()
	shardManager.EXPECT().CheckIfPartitionCanBeCreated(mock.Anything).Return(mockErr)
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
	shardManager.EXPECT().CheckIfPartitionExists(mock.Anything).Return(nil)
	shardManager.EXPECT().DropPartition(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfPartitionExists(mock.Anything).Unset()
	shardManager.EXPECT().CheckIfPartitionExists(mock.Anything).Return(mockErr)
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
	shardManager.EXPECT().CheckIfSegmentCanBeCreated(mock.Anything, mock.Anything).Return(nil)
	shardManager.EXPECT().CreateSegment(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfSegmentCanBeCreated(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().CheckIfSegmentCanBeCreated(mock.Anything, mock.Anything).Return(mockErr)
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
	shardManager.EXPECT().CheckIfSegmentCanBeFlushed(mock.Anything, mock.Anything).Return(nil)
	shardManager.EXPECT().FlushSegment(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfSegmentCanBeFlushed(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().CheckIfSegmentCanBeFlushed(mock.Anything, mock.Anything).Return(mockErr)
	shardManager.EXPECT().FlushSegment(mock.Anything).Return()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	// Flush from old arch should always be allowed.
	msgID, err = i.DoAppend(utility.WithFlushFromOldArch(ctx), msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

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

	insertHdrMatcher := mock.MatchedBy(func(h *message.InsertMessageHeader) bool {
		return h != nil && h.GetCollectionId() == int64(1) && h.SchemaVersion == nil
	})

	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(0), nil).Once()
	shardManager.EXPECT().AssignSegment(mock.Anything).Return(&shards.AssignSegmentResult{SegmentID: 1, Acknowledge: atomic.NewInt32(1)}, nil).Once()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(0), nil).Once()
	shardManager.EXPECT().AssignSegment(mock.Anything).Return(nil, mockErr).Once()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	// ErrCollectionNotFound from schema version check must surface as an unrecoverable insert error.
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(-1), shards.ErrCollectionNotFound).Once()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	// ErrCollectionSchemaNotFound must also become an unrecoverable insert error.
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(-1), shards.ErrCollectionSchemaNotFound).Once()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	// ErrCollectionSchemaVersionNotMatch must surface as a schema-version-mismatch error
	// so the proxy can refresh its cache and retry.
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(5), shards.ErrCollectionSchemaVersionNotMatch).Once()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	// Unexpected error from the schema version check must be propagated as-is.
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(-1), mockErr).Once()
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
	shardManager.EXPECT().ApplyDelete(mock.Anything).Return(nil)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Unset()
	shardManager.EXPECT().CheckIfCollectionExists(mock.Anything).Return(mockErr)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	msg = message.NewSchemaChangeMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&messagespb.SchemaChangeMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&messagespb.SchemaChangeMessageBody{}).
		MustBuildMutable().WithTimeTick(1)
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Return(nil, nil)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Return(nil, mockErr)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)

	msg = message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&messagespb.TruncateCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&messagespb.TruncateCollectionMessageBody{}).
		MustBuildMutable().WithTimeTick(1)
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Return(nil, nil)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Unset()
	shardManager.EXPECT().FlushAndFenceSegmentAllocUntil(mock.Anything, mock.Anything).Return(nil, mockErr)
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.Nil(t, msgID)
}
