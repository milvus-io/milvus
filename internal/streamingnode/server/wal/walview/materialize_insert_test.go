package walview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestMaterializeInsertPreservesWALAndExistingOutputs(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "raw_bm25", Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}, {Key: "max_length", Value: "256"}}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		}, Functions: []*schemapb.FunctionSchema{{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text"}, InputFieldIds: []int64{101}, OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{102}}},
	}
	body := &msgpb.InsertRequest{CollectionID: 99040451, Version: msgpb.InsertDataVersion_ColumnBased, NumRows: 1, FieldsData: []*schemapb.FieldData{
		{FieldId: 101, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"shared special"}}}}}},
	}}
	assignment := &messagespb.PartitionSegmentAssignment{PartitionId: 10, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 20}}
	inputFor := func(body *msgpb.InsertRequest) SegmentInsertMessage {
		raw := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{Partitions: []*messagespb.PartitionSegmentAssignment{assignment}}).WithBody(body).MustBuildMutable().WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(10))
		return SegmentInsertMessage{Message: message.MustAsImmutableInsertMessageV1(raw), Assignment: assignment, TimeTick: 10}
	}
	input := inputFor(body)
	before := proto.Clone(input.Message.MustBody())
	output, err := MaterializeInsertRequest(schema, input)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, input.Message.MustBody()), "query materialization cannot mutate retained WAL")
	require.Equal(t, int64(10), output.PartitionID)
	require.Equal(t, int64(20), output.SegmentID)
	require.Len(t, output.FieldsData, 2)
	require.NotEmpty(t, output.FieldsData[1].GetVectors().GetSparseFloatVector().GetContents()[0])
	patch := mockey.Mock((*function.FunctionRunnerLocalStore).FillEmbeddingData).Return(context.Canceled).Build()
	defer patch.UnPatch()
	materialized, err := MaterializeInsertRequest(schema, inputFor(output))
	require.NoError(t, err)
	require.True(t, proto.Equal(output, materialized))
	require.Zero(t, patch.Times())
	_, err = MaterializeInsertRequest(schema, input)
	require.ErrorIs(t, err, context.Canceled)
	_, err = MaterializeInsertRequest(nil, input)
	require.Error(t, err)
}
