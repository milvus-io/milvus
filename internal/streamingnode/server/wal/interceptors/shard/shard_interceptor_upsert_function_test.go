package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type upsertMaterializationManager struct {
	function.FunctionRunnerManager
	materialize func(context.Context, int64, string, int32, function.InsertMessage) (bool, error)
}

func (m *upsertMaterializationManager) Materialize(ctx context.Context, id int64, key string, version int32, msg function.InsertMessage) (bool, error) {
	return m.materialize(ctx, id, key, version, msg)
}

func upsertFunctionFixture() (message.MutableMessage, *schemapb.CollectionSchema) {
	msg := upsertForTest("p_1v0")
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Insert.FieldsData = append(body.Insert.FieldsData, &schemapb.FieldData{FieldId: 101, FieldName: "text", Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"new row"}}}}}})
	typed.OverwriteBody(body)
	header := typed.Header()
	header.Partition.BinarySize = 1
	header.Partition.SegmentAssignment = &message.SegmentAssignment{SegmentId: 999}
	typed.OverwriteHeader(header)
	schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "embedding", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}, Functions: []*schemapb.FunctionSchema{{Id: 1, Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}}}
	return msg, schema
}

func TestUpsertFunctionMaterializationGateAndNestedAdapter(t *testing.T) {
	paramtable.Init()
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		mockey.PatchConvey(name, t, func() {
			item := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization
			value := "false"
			if enabled {
				value = "true"
			}
			old := item.SwapTempValue(value)
			defer item.SwapTempValue(old)
			msg, schema := upsertFunctionFixture()
			initial := message.MustAsMutableUpsertMessageV2(msg).MustBody()
			deletion := proto.Clone(initial.GetDelete()).(*msgpb.DeleteRequest)
			initialSize := proto.Size(initial.GetInsert())
			calls := 0
			manager := &upsertMaterializationManager{materialize: func(ctx context.Context, id int64, key string, version int32, insert function.InsertMessage) (bool, error) {
				calls++
				require.Equal(t, int64(1), id)
				require.Equal(t, WALFunctionRunnerKey("p_1v0"), key)
				require.Equal(t, int32(1), version)
				require.Equal(t, "new row", insert.MustBody().GetFieldsData()[1].GetScalars().GetStringData().GetData()[0])
				body := proto.Clone(insert.MustBody()).(*msgpb.InsertRequest)
				body.FieldsData = append(body.FieldsData, &schemapb.FieldData{FieldId: 102, FieldName: "embedding", Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{{1, 0, 0, 0, 0, 0, 128, 63}}}}}}})
				insert.OverwriteBody(body)
				return true, nil
			}}
			mockey.Mock(function.GetManager).Return(manager).Build()
			base := mock_shards.NewMockShardManager(t)
			shardManager := &upsertSchemaManager{MockShardManager: base, schema: schema}
			base.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
			assignment := &shards.AssignSegmentResult{SegmentID: 123, Acknowledge: atomic.NewInt32(1)}
			base.EXPECT().AssignSegment(mock.MatchedBy(func(request *shards.AssignSegmentRequest) bool {
				current := message.MustAsMutableUpsertMessageV2(msg).MustBody().GetInsert()
				return request.CollectionID == 1 && request.PartitionID == 2 && request.TimeTick == 10 && request.ModifiedMetrics.Rows == 1 && request.ModifiedMetrics.BinarySize == uint64(proto.Size(current))
			})).Return(assignment, nil).Once()
			interceptor := &shardInterceptor{shardManager: shardManager}
			interceptor.initOpTable()
			appends := 0
			id, err := interceptor.DoAppend(t.Context(), msg, func(_ context.Context, appended message.MutableMessage) (message.MessageID, error) {
				appends++
				upsert := message.MustAsMutableUpsertMessageV2(appended)
				require.True(t, proto.Equal(deletion, upsert.MustBody().GetDelete()))
				require.Equal(t, int64(123), upsert.Header().GetPartition().GetSegmentAssignment().GetSegmentId())
				require.Equal(t, uint64(proto.Size(upsert.MustBody().GetInsert())), upsert.Header().GetPartition().GetBinarySize())
				if enabled {
					require.Len(t, upsert.MustBody().GetInsert().GetFieldsData(), 3)
					require.Greater(t, proto.Size(upsert.MustBody().GetInsert()), initialSize)
				} else {
					require.Len(t, upsert.MustBody().GetInsert().GetFieldsData(), 2)
				}
				return walimplstest.NewTestMessageID(1), nil
			})
			require.NoError(t, err)
			require.NotNil(t, id)
			require.Equal(t, 1, appends)
			expectedCalls := 0
			if enabled {
				expectedCalls = 1
			}
			require.Equal(t, expectedCalls, calls)
			require.Zero(t, assignment.Acknowledge.Load())
			require.Equal(t, uint64(1), shardManager.deleteRows)
			require.Equal(t, uint64(proto.Size(deletion)), shardManager.deleteBytes)
		})
	}
}

func TestUpsertFunctionMaterializationPreservesErrorsAndSkipsAppend(t *testing.T) {
	paramtable.Init()
	for _, test := range []struct {
		name string
		err  error
	}{
		{"deferred preparation", &function.PreparationRequiredError{}},
		{"transient unavailable", merr.ErrServiceUnavailable},
		{"permanent resource denial", merr.ErrIoPermissionDenied},
		{"redo", redo.ErrRedo},
	} {
		mockey.PatchConvey(test.name, t, func() {
			item := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization
			old := item.SwapTempValue("true")
			defer item.SwapTempValue(old)
			msg, schema := upsertFunctionFixture()
			before := proto.Clone(message.MustAsMutableUpsertMessageV2(msg).MustBody())
			calls := 0
			manager := &upsertMaterializationManager{materialize: func(context.Context, int64, string, int32, function.InsertMessage) (bool, error) {
				calls++
				return false, test.err
			}}
			mockey.Mock(function.GetManager).Return(manager).Build()
			base := mock_shards.NewMockShardManager(t)
			base.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
			shardManager := &upsertSchemaManager{MockShardManager: base, schema: schema}
			interceptor := &shardInterceptor{shardManager: shardManager}
			interceptor.initOpTable()
			_, err := interceptor.DoAppend(t.Context(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				t.Fatal("materialization failure reached backend append")
				return nil, nil
			})
			require.True(t, errors.Is(err, test.err))
			if test.name != "deferred preparation" {
				require.True(t, status.AsStreamingError(err).IsUnrecoverable())
			} else {
				require.Equal(t, test.err, err)
			}
			require.Equal(t, 1, calls)
			require.True(t, proto.Equal(before, message.MustAsMutableUpsertMessageV2(msg).MustBody()))
			require.Zero(t, shardManager.deleteRows)
			require.Zero(t, shardManager.deleteBytes)
			if test.name == "deferred preparation" {
				var preparation *function.PreparationRequiredError
				require.ErrorAs(t, err, &preparation)
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
		})
	}
}

func TestUpsertFunctionPreparationRunsOutsideAdmission(t *testing.T) {
	paramtable.Init()
	for _, test := range []struct {
		name string
		err  error
	}{
		{"missing dictionary", merr.WrapErrIoKeyNotFound("dictionary")},
		{"dictionary permission", merr.ErrIoPermissionDenied},
		{"temporary dependency", merr.ErrServiceUnavailable},
	} {
		mockey.PatchConvey(test.name, t, func() {
			item := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization
			old := item.SwapTempValue("true")
			defer item.SwapTempValue(old)
			mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
			acquire := mockey.Mock(fileresource.Acquire).To(func(context.Context, []int64, ...string) (*fileresource.Resources, error) {
				return &fileresource.Resources{}, test.err
			}).Build()
			msg, schema := upsertFunctionFixture()
			schema.FileResourceIds = []int64{7}
			schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{{Key: "analyzer_params", Value: `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"dictionary","file_name":"stop.txt"}}]}`}}
			schema.Fields[2].DataType = schemapb.DataType_SparseFloatVector
			schema.Fields[2].TypeParams = nil
			schema.Functions[0].Type = schemapb.FunctionType_BM25
			manager := function.NewFunctionRunnerManager()
			defer manager.Close()
			mockey.Mock(function.GetManager).Return(manager).Build()
			build := mockey.Mock(function.BuildEmbeddingRunner).Return(nil, merr.ErrServiceInternal).Build()
			require.NoError(t, manager.Alloc(1, WALFunctionRunnerKey("p_1v0"), schema))
			base := mock_shards.NewMockShardManager(t)
			base.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
			shardManager := &upsertSchemaManager{MockShardManager: base, schema: schema}
			interceptor := &shardInterceptor{shardManager: shardManager}
			interceptor.initOpTable()
			before := proto.Clone(message.MustAsMutableUpsertMessageV2(msg).MustBody())
			_, err := interceptor.DoAppend(t.Context(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				t.Fatal("unprepared Upsert reached backend append")
				return nil, nil
			})
			var preparation *function.PreparationRequiredError
			require.ErrorAs(t, err, &preparation)
			require.Zero(t, acquire.Times(), "resource fetch must occur after append admission unwinds")
			require.Zero(t, build.Times())
			require.True(t, proto.Equal(before, message.MustAsMutableUpsertMessageV2(msg).MustBody()))
			err = preparation.Prepare(t.Context())
			require.ErrorIs(t, err, test.err)
			require.Equal(t, 1, acquire.Times())
			require.Zero(t, build.Times(), "a failed resource fetch must precede runner construction")
			require.Zero(t, shardManager.deleteRows)
		})
	}
}

func TestUpsertFunctionMaterializationPreservesCallerCancellation(t *testing.T) {
	paramtable.Init()
	mockey.PatchConvey("canceled materialization", t, func() {
		item := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization
		old := item.SwapTempValue("true")
		defer item.SwapTempValue(old)
		msg, schema := upsertFunctionFixture()
		ctx, cancel := context.WithCancel(t.Context())
		manager := &upsertMaterializationManager{materialize: func(context.Context, int64, string, int32, function.InsertMessage) (bool, error) {
			cancel()
			return false, merr.ErrServiceInternal
		}}
		mockey.Mock(function.GetManager).Return(manager).Build()
		base := mock_shards.NewMockShardManager(t)
		base.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(1), nil).Once()
		interceptor := &shardInterceptor{shardManager: &upsertSchemaManager{MockShardManager: base, schema: schema}}
		interceptor.initOpTable()
		_, err := interceptor.DoAppend(ctx, msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Fatal("canceled materialization reached backend append")
			return nil, nil
		})
		require.Equal(t, context.Canceled, err)
		require.False(t, status.AsStreamingError(err).IsUnrecoverable())
	})
}
