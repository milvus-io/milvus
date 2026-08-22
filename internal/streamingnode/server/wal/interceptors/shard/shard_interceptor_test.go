package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

func allocWALSchemaForTest(t *testing.T, collectionID int64, vchannel string, schemaVersion int32) {
	t.Helper()
	key := walFunctionRunnerKey(vchannel)
	require.NoError(t, function.GetManager().Alloc(collectionID, key, &schemapb.CollectionSchema{Version: schemaVersion}))
	t.Cleanup(func() {
		function.GetManager().Release(collectionID, key)
	})
}

func TestMaterializeFunctionFieldsSkipsOmittedVersionWithoutFunctions(t *testing.T) {
	collectionID := int64(99000)
	vchannel := "v1-no-functions"
	allocWALSchemaForTest(t, collectionID, vchannel, 0)

	impl := &shardInterceptor{shardManager: mock_shards.NewMockShardManager(t)}
	validMsg := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{CollectionId: collectionID}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
	msg := message.NewMutableMessageBeforeAppend([]byte("invalid insert body"), validMsg.Properties().ToRawMap())

	insertMsg := message.MustAsMutableInsertMessageV1(msg)
	err := impl.materializeFunctionFields(context.Background(), insertMsg, collectionID, function.LatestFunctionRunnerVersion)
	require.NoError(t, err)
}

func TestShardInterceptorUpdateFunctionRunnersRetainsSchemaWhenFunctionsDropped(t *testing.T) {
	collectionID := int64(99001)
	vchannel := "by-dev-rootcoord-dml_0_99001v0"
	key := walFunctionRunnerKey(vchannel)
	schema := &schemapb.CollectionSchema{
		Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "analyzer_params", Value: "{}"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		}},
	}
	require.NoError(t, function.GetManager().Alloc(collectionID, key, schema))
	defer function.GetManager().Release(collectionID, key)

	ok, err := function.GetManager().RunWithAnalyzer(context.Background(), collectionID, key, 101, func(function.Analyzer) error {
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)

	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(log.With()).Maybe()
	impl := &shardInterceptor{shardManager: shardManager}
	noFunctionSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	noFunctionSchema.Version = 2
	noFunctionSchema.Functions = nil
	impl.updateFunctionRunners(collectionID, vchannel, noFunctionSchema)

	ok, err = function.GetManager().RunWithAnalyzer(context.Background(), collectionID, key, 101, func(function.Analyzer) error {
		return nil
	})
	require.NoError(t, err)
	require.False(t, ok)

	invalidSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	invalidSchema.Version = 3
	invalidSchema.Functions[0].OutputFieldIds = []int64{999}
	require.NotPanics(t, func() {
		impl.updateFunctionRunners(collectionID, vchannel, invalidSchema)
	})
	require.NotPanics(t, func() {
		impl.allocFunctionRunners(collectionID+1, vchannel+"-alloc", invalidSchema)
	})
}

func TestShardInterceptorCreateCollectionAllocatesFunctionRunnersFromLegacySchema(t *testing.T) {
	collectionID := int64(99003)
	vchannel := "by-dev-rootcoord-dml_0_99003v0"
	key := walFunctionRunnerKey(vchannel)
	schema := &schemapb.CollectionSchema{
		Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "analyzer_params", Value: "{}"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		}},
	}
	legacySchema, err := proto.Marshal(schema)
	require.NoError(t, err)

	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().CheckIfCollectionCanBeCreated(collectionID).Return(nil).Once()
	shardManager.EXPECT().CreateCollection(mock.Anything).Return().Once()
	shardManager.EXPECT().Logger().Return(log.With()).Maybe()
	impl := &shardInterceptor{shardManager: shardManager}
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.CreateCollectionMessageHeader{CollectionId: collectionID}).
		WithBody(&msgpb.CreateCollectionRequest{Schema: legacySchema}).
		MustBuildMutable().
		WithTimeTick(1)

	msgID, err := impl.handleCreateCollection(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	require.NoError(t, err)
	require.NotNil(t, msgID)
	defer function.GetManager().Release(collectionID, key)

	ok, err := function.GetManager().RunWithRunner(context.Background(), collectionID, key, 102, func(function.FunctionRunner) error {
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestShardInterceptorRejectsCreateCollectionWithoutSchema(t *testing.T) {
	impl := &shardInterceptor{shardManager: mock_shards.NewMockShardManager(t)}
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("by-dev-rootcoord-dml_0_99004v0").
		WithHeader(&messagespb.CreateCollectionMessageHeader{CollectionId: 99004}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable()

	appended := false
	msgID, err := impl.handleCreateCollection(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return rmq.NewRmqID(1), nil
	})
	require.ErrorContains(t, err, "does not contain collection schema")
	require.Nil(t, msgID)
	require.False(t, appended)
}

func TestShardInterceptorRejectsInvalidLegacySchemaBeforeAppend(t *testing.T) {
	impl := &shardInterceptor{shardManager: mock_shards.NewMockShardManager(t)}
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("by-dev-rootcoord-dml_0_99005v0").
		WithHeader(&messagespb.CreateCollectionMessageHeader{CollectionId: 99005}).
		WithBody(&msgpb.CreateCollectionRequest{Schema: []byte{0xff}}).
		MustBuildMutable()

	appended := false
	require.Panics(t, func() {
		_, _ = impl.handleCreateCollection(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	})
	require.False(t, appended)
}

func TestShardInterceptorDeleteAppliesBeforeAppend(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(log.With()).Maybe()
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()

	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("vchannel").
		WithHeader(&messagespb.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         10,
		}).
		WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable().WithTimeTick(1)

	shardManager.EXPECT().CheckIfCollectionExists(int64(1)).Return(nil)
	shardManager.EXPECT().ApplyDelete(mock.MatchedBy(func(deleteMsg message.MutableDeleteMessageV1) bool {
		return deleteMsg.Header().GetCollectionId() == int64(1) && deleteMsg.Header().GetRows() == uint64(10)
	})).Return(nil)

	msgID, err := i.DoAppend(context.Background(), msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return nil, errors.New("append failed")
	})
	assert.Error(t, err)
	assert.Nil(t, msgID)
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
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Version: 0}}).
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
	allocWALSchemaForTest(t, 1, vchannel, 0)

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
