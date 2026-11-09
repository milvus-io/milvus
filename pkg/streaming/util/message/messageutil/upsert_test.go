// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package messageutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func testUpsertRows(t *testing.T, nullable bool) (message.ImmutableMessage, *schemapb.CollectionSchema) {
	t.Helper()
	version := int32(1)
	vector := &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}}
	if nullable {
		vector.ValidData = []bool{true, false, true}
		vector.GetFloatVector().Data = []float32{1, 2, 5, 6}
	}
	body := &message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4, 5, 4}}}}, RowIds: []int64{10, 20, 30}, PartitionIndexes: []uint32{0, 1, 0}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: vector}}}}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{99}}}}, Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 30}}}}
	raw := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 2}, {PartitionId: 20, Rows: 1}}, DeleteRows: 1}).WithBody(body).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}, {FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, Nullable: nullable}}}
	return raw, schema
}

func TestUpsertSelectedRowsAndIndependentEffects(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		raw, schema := testUpsertRows(t, nullable)
		before := proto.Clone(message.MustAsImmutableUpsertMessageV2(raw).MustBody())
		rows, partitions, ok := AsInsertRowsReader(raw)
		require.True(t, ok)
		require.Len(t, partitions, 2)
		selections, err := InsertSelections(context.Background(), raw)
		require.NoError(t, err)
		require.Equal(t, [][]int64{{0, 2}, {1}}, selections)
		first, err := rows.ReadRows(context.Background(), schema, selections[0])
		require.NoError(t, err)
		require.Equal(t, []int64{10, 30}, first.RowIDs)
		require.Equal(t, []uint64{42, 42}, first.Timestamps)
		require.Equal(t, []float32{1, 2, 5, 6}, first.FieldsData[0].GetVectors().GetFloatVector().Data)
		require.Equal(t, []int64{4, 4}, first.FieldsData[1].GetScalars().GetLongData().Data)
		second, err := rows.ReadRows(context.Background(), schema, selections[1])
		require.NoError(t, err)
		if nullable {
			require.Empty(t, second.FieldsData[0].GetVectors().GetFloatVector().Data)
			require.Equal(t, []bool{false}, second.FieldsData[0].GetVectors().ValidData)
		} else {
			require.Equal(t, []float32{3, 4}, second.FieldsData[0].GetVectors().GetFloatVector().Data)
		}
		effects, err := DeleteRequests(context.Background(), raw)
		require.NoError(t, err)
		require.Len(t, effects, 1)
		require.Equal(t, int64(30), effects[0].PartitionID)
		require.Equal(t, []int64{99}, effects[0].PrimaryKeys.GetIntId().Data)
		require.Equal(t, []uint64{42}, effects[0].Timestamps)
		require.True(t, proto.Equal(before, message.MustAsImmutableUpsertMessageV2(raw).MustBody()))
	}
}

func TestHistoricalDeleteProjection(t *testing.T) {
	for _, source := range []*msgpb.DeleteRequest{{NumRows: 2, Int64PrimaryKeys: []int64{1, 2}}, {SerializedExprPlan: []byte{1}}} {
		raw := message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.DeleteMessageHeader{}).WithBody(source).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
		effects, err := DeleteRequests(context.Background(), raw)
		require.NoError(t, err)
		expected := len(source.Int64PrimaryKeys)
		if len(source.SerializedExprPlan) > 0 {
			expected = 1
		}
		require.Len(t, effects[0].Timestamps, expected)
		require.Equal(t, uint64(42), effects[0].Base.Timestamp)
		require.Equal(t, "p_1v0", effects[0].ShardName)
	}
}

func TestUpsertNestedColumnsSharePartitionSelection(t *testing.T) {
	raw, schema := testUpsertRows(t, false)
	original := message.MustAsImmutableUpsertMessageV2(raw)
	body := proto.Clone(original.MustBody()).(*message.UpsertMessageBody)
	array := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64}}}
	vectors := &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector}
	for i := 0; i < 3; i++ {
		array.GetArrayData().Data = append(array.GetArrayData().Data, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(i), int64(i + 10)}}}})
		vectors.Data = append(vectors.Data, &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{float32(i), float32(i + 10)}}}})
	}
	body.Rows.Columns = append(body.Rows.Columns, &schemapb.FieldData{FieldId: 102, Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{{FieldId: 103, Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: array}}}}}}, &schemapb.FieldData{FieldId: 104, Type: schemapb.DataType_ArrayOfVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_VectorArray{VectorArray: vectors}}}})
	raw = message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(original.Header()).WithBody(body).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(43))
	reader, _, ok := AsInsertRowsReader(raw)
	require.True(t, ok)
	selected, err := reader.ReadRows(context.Background(), schema, []int64{0, 2})
	require.NoError(t, err)
	nested := selected.FieldsData[1].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data
	require.Len(t, nested, 2)
	require.Equal(t, []int64{0, 10}, nested[0].GetLongData().Data)
	require.Equal(t, []int64{2, 12}, nested[1].GetLongData().Data)
	chosen := selected.FieldsData[2].GetVectors().GetVectorArray().Data
	require.Len(t, chosen, 2)
	require.Equal(t, []float32{0, 10}, chosen[0].GetFloatVector().Data)
	require.Equal(t, []float32{2, 12}, chosen[1].GetFloatVector().Data)
}

func BenchmarkUpsertConsumerScatter(b *testing.B) {
	const rows = 1024
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	for _, partitions := range []int{1, 8} {
		b.Run(fmt.Sprintf("partitions_%d", partitions), func(b *testing.B) {
			version := int32(1)
			header := &message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version}
			body := &message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, rows)}}}, RowIds: make([]int64, rows), Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, rows*128)}}}}}}}}
			for i := 0; i < partitions; i++ {
				header.Partitions = append(header.Partitions, &message.PartitionSegmentAssignment{PartitionId: int64(i + 1), Rows: rows / uint64(partitions)})
			}
			if partitions > 1 {
				body.Rows.PartitionIndexes = make([]uint32, rows)
				for i := range body.Rows.PartitionIndexes {
					body.Rows.PartitionIndexes[i] = uint32(i % partitions)
				}
			}
			raw := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(header).WithBody(body).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader, _, _ := AsInsertRowsReader(raw)
				selections, err := InsertSelections(context.Background(), raw)
				if err != nil {
					b.Fatal(err)
				}
				for _, selection := range selections {
					if _, err := reader.ReadRows(context.Background(), schema, selection); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func transformUpsert(tick uint64) message.ImmutableMessage {
	keys := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"one", "two"}}}}
	return message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 2}}, DeleteRows: 2}).
		WithBody(&message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: keys, RowIds: []int64{1, 2}}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{PrimaryKeys: keys, Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}}}}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
}

func transformDelete(tick uint64, partition int64, key string) message.ImmutableMessage {
	return message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			CollectionID: 1, PartitionID: partition, NumRows: 1,
			PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{key}}}},
		}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
}

func TestUpsertAndMixedTransactionTransformBlocks(t *testing.T) {
	upsertEntry := BuildTransformLogEntry(transformUpsert(30), TransformEntryOption{})
	require.Equal(t, uint64(30), upsertEntry.GetTimeTick())
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(31).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(31))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	builder.Add(transformUpsert(32))
	builder.Add(transformDelete(33, 4, "txn-delete"))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).
		WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(40).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(40))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	txnEntry := BuildTransformLogEntry(txn, TransformEntryOption{})
	for _, entry := range []*streamingpb.TransformLogEntry{upsertEntry, txnEntry} {
		blocks := entry.GetDelete().GetBlocks()
		require.Equal(t, int64(-1), blocks[0].GetPartitionId())
		require.Equal(t, []string{"one", "two"}, blocks[0].GetPrimaryKeys().GetStrId().GetData())
	}
	require.Equal(t, uint64(40), txnEntry.GetTimeTick())
	require.Len(t, txnEntry.GetDelete().GetBlocks(), 2)
	require.Equal(t, int64(4), txnEntry.GetDelete().GetBlocks()[1].GetPartitionId())
	require.Equal(t, []string{"txn-delete"}, txnEntry.GetDelete().GetBlocks()[1].GetPrimaryKeys().GetStrId().GetData())
}

func TestInsertRowsReaderPreservesSparseDimensions(t *testing.T) {
	for _, format := range []string{"legacy", "insert-v2", "upsert"} {
		legacy := format == "legacy"
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("format=%s/nullable=%t", format, nullable), func(t *testing.T) {
				contents := [][]byte{
					typeutil.CreateSparseFloatRow([]uint32{7}, []float32{1}),
					typeutil.CreateSparseFloatRow([]uint32{3}, []float32{2}),
					typeutil.CreateSparseFloatRow([]uint32{15}, []float32{3}),
				}
				vector := &schemapb.VectorField{Dim: 32, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 16, Contents: contents}}}
				if nullable {
					vector.ValidData = []bool{true, false, true}
					vector.GetSparseFloatVector().Contents = [][]byte{contents[0], contents[2]}
				}
				column := &schemapb.FieldData{FieldId: 101, Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: vector}}
				schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "vector", DataType: schemapb.DataType_SparseFloatVector, Nullable: nullable},
				}}
				var raw message.ImmutableMessage
				if legacy {
					body := &msgpb.InsertRequest{CollectionID: 1, NumRows: 3, RowIDs: []int64{10, 20, 30}, FieldsData: []*schemapb.FieldData{column}, Version: msgpb.InsertDataVersion_ColumnBased}
					raw = message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{}).WithBody(body).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
				} else {
					body := &message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}, RowIds: []int64{10, 20, 30}, Columns: []*schemapb.FieldData{column}}}
					if format == "insert-v2" {
						raw = message.NewInsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeaderV2{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 3}}}).WithBody(&message.InsertMessageBody{Rows: body.Rows}).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
					} else {
						raw = message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 3}}}).WithBody(body).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
					}
				}
				sourcePayload := append([]byte(nil), raw.Payload()...)
				sourceColumn := proto.Clone(column)
				var decodedSource proto.Message
				if legacy {
					decodedSource = message.MustAsImmutableInsertMessageV1(raw).MustBody()
				} else if format == "insert-v2" {
					decodedSource = message.MustAsImmutableInsertMessageV2(raw).MustBody()
				} else {
					decodedSource = message.MustAsImmutableUpsertMessageV2(raw).MustBody()
				}
				decodedBefore := proto.Clone(decodedSource)
				reader, _, ok := AsInsertRowsReader(raw)
				require.True(t, ok)
				for _, selection := range [][]int64{nil, {2, 0}, {1}} {
					result, err := reader.ReadRows(t.Context(), schema, selection)
					require.NoError(t, err)
					got := result.GetFieldsData()[0].GetVectors()
					require.Equal(t, int64(32), got.GetDim())
					require.Equal(t, int64(16), got.GetSparseFloatVector().GetDim())
					logical := selection
					if logical == nil {
						logical = []int64{0, 1, 2}
					}
					var wantContents [][]byte
					var wantValidity []bool
					for _, row := range logical {
						valid := !nullable || row != 1
						if nullable {
							wantValidity = append(wantValidity, valid)
						}
						if valid {
							wantContents = append(wantContents, contents[row])
						}
					}
					require.Equal(t, wantValidity, got.GetValidData())
					require.Equal(t, len(wantContents), len(got.GetSparseFloatVector().GetContents()))
					for i := range wantContents {
						require.Equal(t, wantContents[i], got.GetSparseFloatVector().GetContents()[i])
					}
					require.Equal(t, sourcePayload, raw.Payload())
					require.True(t, proto.Equal(decodedBefore, decodedSource))
				}
				require.True(t, proto.Equal(sourceColumn, column))
			})
		}
	}
}

func TestInsertRowsReaderAllNullSparsePayload(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vector", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
	}}
	body := &message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}, RowIds: []int64{10, 20}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 32, ValidData: []bool{false, false}}}}}}}
	raw := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 2}}}).WithBody(body).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(42))
	source := message.MustAsImmutableUpsertMessageV2(raw).MustBody()
	require.Nil(t, source.Rows.Columns[0].GetVectors().Data)
	before := proto.Clone(source)
	payload := append([]byte(nil), raw.Payload()...)
	reader, _, ok := AsInsertRowsReader(raw)
	require.True(t, ok)
	for _, selection := range [][]int64{nil, {1}} {
		result, err := reader.ReadRows(t.Context(), schema, selection)
		require.NoError(t, err)
		vectors := result.FieldsData[0].GetVectors()
		require.NotNil(t, vectors.GetSparseFloatVector())
		require.Equal(t, int64(32), vectors.GetDim())
		require.Zero(t, vectors.GetSparseFloatVector().GetDim())
		require.Empty(t, vectors.GetSparseFloatVector().GetContents())
		require.Equal(t, make([]bool, result.GetNumRows()), vectors.GetValidData())
		require.True(t, proto.Equal(before, source))
		require.Equal(t, payload, raw.Payload())
	}
}

func TestInsertRowsReaderAllNullSparseEncodedSelection(t *testing.T) {
	row := typeutil.CreateSparseFloatRow([]uint32{0, 1, 2, 3, 4, 5, 6, 7}, []float32{1, 1, 1, 1, 1, 1, 1, 1})
	source := &msgpb.InsertRequest{NumRows: 4, RowIDs: []int64{10, 20, 30, 40}, Timestamps: []uint64{1, 1, 1, 1}, FieldsData: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 32, ValidData: []bool{true, false, false, true}, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 16, Contents: [][]byte{row, row}}}}}}}}
	before := proto.Clone(source)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vector", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
	}}
	var chunks []int
	var allValidity []bool
	nullChunks := 0
	for _, selected := range [][]int{{0}, {1, 2}, {3}} {
		count := len(selected)
		keys := make([]int64, count)
		rowIDs := make([]int64, count)
		for i, index := range selected {
			keys[i] = int64(index + 1)
			rowIDs[i] = source.RowIDs[index]
		}
		encoder, err := fastpb.NewUpsertViewEncoder(&message.UpsertMessageBody{Rows: &message.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}, RowIds: rowIDs}}, source, selected)
		require.NoError(t, err)
		raw := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: uint64(count)}}}).WithBodyEncoder(encoder).MustBuildMutable().WithTimeTick(42).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(len(chunks) + 1)))
		decoded := message.MustAsImmutableUpsertMessageV2(raw).MustBody()
		decodedBefore := proto.Clone(decoded)
		payload := append([]byte(nil), raw.Payload()...)
		inputVector := decoded.Rows.Columns[0].GetVectors()
		if inputVector.GetSparseFloatVector() == nil {
			require.Equal(t, []bool{false, false}, inputVector.GetValidData())
			require.Nil(t, inputVector.Data, "encoder must omit the sparse payload for an all-null chunk")
			nullChunks++
		}
		reader, _, ok := AsInsertRowsReader(raw)
		require.True(t, ok)
		selections, err := InsertSelections(t.Context(), raw)
		require.NoError(t, err)
		result, err := reader.ReadRows(t.Context(), schema, selections[0])
		require.NoError(t, err)
		outputVector := result.FieldsData[0].GetVectors()
		require.NotNil(t, outputVector.GetSparseFloatVector())
		require.Equal(t, inputVector.GetDim(), outputVector.GetDim())
		require.Equal(t, inputVector.GetSparseFloatVector().GetDim(), outputVector.GetSparseFloatVector().GetDim())
		require.Equal(t, inputVector.GetValidData(), outputVector.GetValidData())
		require.Equal(t, inputVector.GetSparseFloatVector().GetContents(), outputVector.GetSparseFloatVector().GetContents())
		require.Equal(t, rowIDs, result.GetRowIDs())
		allValidity = append(allValidity, outputVector.GetValidData()...)
		require.True(t, proto.Equal(decodedBefore, decoded))
		require.Equal(t, payload, raw.Payload())
		chunks = append(chunks, count)
	}
	require.Equal(t, []int{1, 2, 1}, chunks)
	require.Equal(t, 1, nullChunks)
	require.Equal(t, []bool{true, false, false, true}, allValidity)
	require.True(t, proto.Equal(before, source))
}
