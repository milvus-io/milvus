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
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func allocWALSchemaForTest(t *testing.T, collectionID int64, vchannel string, schemaVersion int32) {
	t.Helper()
	key := walFunctionRunnerKey(vchannel)
	assert.NoError(t, function.GetManager().Alloc(collectionID, key, &schemapb.CollectionSchema{Version: schemaVersion}))
	t.Cleanup(func() {
		function.GetManager().Release(collectionID, key)
	})
}

func TestMaterializeFunctionFieldsSkipsOmittedVersionWithoutFunctions(t *testing.T) {
	collectionID := int64(99000)
	vchannel := "v1"
	shardManager := mock_shards.NewMockShardManager(t)
	impl := &shardInterceptor{
		shardManager: shardManager,
	}
	msg := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{CollectionId: collectionID}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()

	insertMsg := message.MustAsMutableInsertMessageV1(msg)
	err := impl.materializeFunctionFields(context.Background(), insertMsg, collectionID, function.LatestFunctionRunnerVersion)
	assert.NoError(t, err)
}

func TestShardInterceptorPassesOmittedSchemaVersionToChecker(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
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
}

func TestShardInterceptorReportsExplicitZeroSchemaVersionInMismatchError(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
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

func TestShardInterceptorUpdateFunctionRunnersRetainsSchemaWhenFunctionsDropped(t *testing.T) {
	collectionID := int64(99001)
	vchannel := "by-dev-rootcoord-dml_0_99001v0"
	schema := &schemapb.CollectionSchema{
		Name:    "test",
		Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "256"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
		},
	}
	assert.NoError(t, function.GetManager().Alloc(collectionID, walFunctionRunnerKey(vchannel), schema))
	defer function.GetManager().Release(collectionID, walFunctionRunnerKey(vchannel))

	ok, err := function.GetManager().RunWithAnalyzer(context.Background(), collectionID, walFunctionRunnerKey(vchannel), 101, func(function.Analyzer) error {
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, ok)

	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
	impl := &shardInterceptor{shardManager: shardManager}

	noFunctionSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	noFunctionSchema.Version = 2
	noFunctionSchema.Functions = nil
	impl.updateFunctionRunners(collectionID, vchannel, noFunctionSchema)

	ok, err = function.GetManager().RunWithAnalyzer(context.Background(), collectionID, walFunctionRunnerKey(vchannel), 101, func(function.Analyzer) error {
		return nil
	})
	assert.NoError(t, err)
	assert.False(t, ok)

	invalidSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	invalidSchema.Version = 3
	invalidSchema.Functions[0].OutputFieldIds = []int64{999}
	assert.NotPanics(t, func() {
		impl.updateFunctionRunners(collectionID, vchannel, invalidSchema)
	})
	assert.NotPanics(t, func() {
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
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
	impl := &shardInterceptor{shardManager: shardManager}
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.CreateCollectionMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&msgpb.CreateCollectionRequest{Schema: legacySchema}).
		MustBuildMutable().
		WithTimeTick(1)

	msgID, err := impl.handleCreateCollection(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)
	defer function.GetManager().Release(collectionID, key)

	ok, err := function.GetManager().RunWithRunner(context.Background(), collectionID, key, 102, func(function.FunctionRunner) error {
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestShardInterceptorRejectsCreateCollectionWithoutSchema(t *testing.T) {
	shardManager := mock_shards.NewMockShardManager(t)
	impl := &shardInterceptor{shardManager: shardManager}
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
	assert.ErrorContains(t, err, "does not contain collection schema")
	assert.Nil(t, msgID)
	assert.False(t, appended)
}

func TestShardInterceptorRejectsInvalidLegacySchemaBeforeAppend(t *testing.T) {
	shardManager := mock_shards.NewMockShardManager(t)
	impl := &shardInterceptor{shardManager: shardManager}
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("by-dev-rootcoord-dml_0_99005v0").
		WithHeader(&messagespb.CreateCollectionMessageHeader{CollectionId: 99005}).
		WithBody(&msgpb.CreateCollectionRequest{Schema: []byte{0xff}}).
		MustBuildMutable()

	appended := false
	assert.Panics(t, func() {
		_, _ = impl.handleCreateCollection(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			appended = true
			return rmq.NewRmqID(1), nil
		})
	})
	assert.False(t, appended)
}

func TestShardInterceptorAlterCollectionSkipsPartialSchemaForFunctionManager(t *testing.T) {
	collectionID := int64(99002)
	vchannel := "by-dev-rootcoord-dml_0_99002v0"
	key := walFunctionRunnerKey(vchannel)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	assert.NoError(t, function.GetManager().Alloc(collectionID, key, schema))
	defer function.GetManager().Release(collectionID, key)

	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().AlterCollection(mock.Anything).Return(nil, nil)
	impl := &shardInterceptor{shardManager: shardManager}
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionExternalSpec}},
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: &schemapb.CollectionSchema{ExternalSource: "s3://bucket/object"},
			},
		}).
		MustBuildMutable().WithTimeTick(1)

	msgID, err := impl.handleAlterCollection(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	ok, err := function.GetManager().RunWithRunner(context.Background(), collectionID, key, 100, func(function.FunctionRunner) error {
		return nil
	})
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestShardInterceptorDeleteAppliesBeforeAppend(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
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

func TestShardInterceptorPassesExplicitNonZeroSchemaVersion(t *testing.T) {
	allocWALSchemaForTest(t, 1, "v1", 3)
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
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
	allocWALSchemaForTest(t, 1, "v1", 0)
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
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

func TestShardInterceptorRejectsMissingWALFunctionSnapshot(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("missing-snapshot-v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 99101,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 1, Rows: 1, BinarySize: 100},
			},
			SchemaVersion: proto.Int32(1),
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(1)

	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil)
	msgID, err := i.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
	assert.Nil(t, msgID)
}

func TestShardInterceptorAllowsLegacyInsertWithoutCollectionSchema(t *testing.T) {
	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("legacy-v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 99102,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 1, Rows: 1, BinarySize: 100},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(1)

	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(0), nil)
	shardManager.EXPECT().AssignSegment(mock.Anything).Return(&shards.AssignSegmentResult{SegmentID: 1, Acknowledge: atomic.NewInt32(1)}, nil)
	msgID, err := i.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)
}

func TestShardInterceptor(t *testing.T) {
	mockErr := errors.New("mock error")

	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
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

	// Unexpected error from the schema version check must stop producer retry.
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(insertHdrMatcher).Return(int32(-1), mockErr).Once()
	msgID, err = i.DoAppend(ctx, msg, appender)
	assert.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
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
