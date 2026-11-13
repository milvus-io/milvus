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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
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
		return SegmentInsertMessage{RowsReader: func() messageutil.InsertRowsReader { rows, _, _ := messageutil.AsInsertRowsReader(raw); return rows }(), Assignment: assignment, TimeTick: 10}
	}
	input := inputFor(body)
	before := proto.Clone(body)
	output, err := MaterializeInsertRequest(schema, input)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, body), "query materialization cannot mutate retained WAL")
	require.Equal(t, int64(10), output.PartitionID)
	require.Equal(t, int64(20), output.SegmentID)
	require.Len(t, output.FieldsData, 2)
	require.NotEmpty(t, output.FieldsData[1].GetVectors().GetSparseFloatVector().GetContents()[0])
	patch := mockey.Mock((*function.FunctionRunnerLocalStore).FillEmbeddingData).Return(context.Canceled).Build()
	defer patch.UnPatch()
	materializedInput := inputFor(output)
	cached, err := materializedInput.RowsReader.ReadRows(context.Background(), schema, nil)
	require.NoError(t, err)
	materialized, err := MaterializeInsertRequest(schema, materializedInput)
	require.NoError(t, err)
	require.True(t, proto.Equal(output, materialized))
	require.Same(t, cached.FieldsData[1].GetVectors().GetSparseFloatVector(), materialized.FieldsData[1].GetVectors().GetSparseFloatVector(), "materialized columns are borrowed read-only")
	materialized.FieldsData[1].FieldName = "private"
	require.True(t, proto.Equal(output, cached), "execution metadata belongs to the consumer")
	require.Zero(t, patch.Times())
	_, err = MaterializeInsertRequest(schema, input)
	require.ErrorIs(t, err, context.Canceled)
	_, err = MaterializeInsertRequest(nil, input)
	require.Error(t, err)
}

func TestInsertOrUpsertMinHashMaterializationAcrossPartitions(t *testing.T) {
	schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}, {Key: "enable_analyzer", Value: "true"}}},
		{FieldID: 102, Name: "hash", DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "512"}}},
	}, Functions: []*schemapb.FunctionSchema{{Name: "minhash", Type: schemapb.FunctionType_MinHash, InputFieldNames: []string{"text"}, InputFieldIds: []int64{101}, OutputFieldNames: []string{"hash"}, OutputFieldIds: []int64{102}, Params: []*commonpb.KeyValuePair{{Key: "num_hashes", Value: "16"}}}}}
	version := int32(1)
	raw := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 20}}, {PartitionId: 11, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 21}}}}).WithBody(&message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 8}}}}, RowIdsEncoding: &message.InsertRows_RowIds{RowIds: &schemapb.
		LongArray{Data: []int64{1, 2}}}, PartitionIndexes: []uint32{0, 1}, Columns: []*schemapb.FieldData{{FieldId: 101, FieldName: "text", Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"first text", "second document"}}}}}}}}}).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
	before := proto.Clone(message.MustAsImmutableUpsertMessageV2(raw).MustBody())
	header := message.MustAsImmutableUpsertMessageV2(raw).Header()
	insert := message.NewInsertMessageBuilderV2().WithVChannel(raw.VChannel()).WithHeader(&message.InsertMessageHeaderV2{CollectionId: header.CollectionId, Partitions: header.Partitions, SchemaVersion: header.SchemaVersion}).WithBody(&message.InsertMessageBody{Rows: message.MustAsImmutableUpsertMessageV2(raw).MustBody().GetRows()}).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
	seen := 0
	for _, input := range []message.ImmutableMessage{raw, insert} {
		require.NoError(t, ForEachSegmentInsertMessage(input, 0, func(selected SegmentInsertMessage) error {
			row, err := MaterializeInsertRequest(schema, selected)
			if err != nil {
				return err
			}
			require.Equal(t, uint64(1), row.NumRows)
			require.Equal(t, []uint64{42}, row.Timestamps)
			var hashes []byte
			for _, column := range row.FieldsData {
				if column.FieldId == 102 {
					hashes = column.GetVectors().GetBinaryVector()
				}
			}
			require.Len(t, hashes, 64)
			seen++
			return nil
		}))
	}
	require.Equal(t, 4, seen)
	require.True(t, proto.Equal(before, message.MustAsImmutableUpsertMessageV2(raw).MustBody()))
}

func BenchmarkUpsertBM25Materialization(b *testing.B) {
	const rows = 128
	schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}, {Key: "enable_analyzer", Value: "true"}}},
		{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}, Functions: []*schemapb.FunctionSchema{{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text"}, InputFieldIds: []int64{101}, OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{102}}}}
	texts := make([]string, rows)
	for i := range texts {
		texts[i] = "shared document for materialization benchmark"
	}
	version := int32(1)
	assignment := &messagespb.PartitionSegmentAssignment{PartitionId: 10, Rows: rows, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 20}}
	raw := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*messagespb.PartitionSegmentAssignment{assignment}}).WithBody(&message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, rows)}}}, RowIdsEncoding: &message.InsertRows_RowIds{RowIds: &schemapb.
		LongArray{Data: make([]int64, rows)}}, Columns: []*schemapb.FieldData{{FieldId: 101, FieldName: "text", Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}}}}}}}}).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ForEachSegmentInsertMessage(raw, 20, func(selected SegmentInsertMessage) error {
			_, err := MaterializeInsertRequest(schema, selected)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}
