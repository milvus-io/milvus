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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func testUpsertRows(t *testing.T, nullable bool) (message.ImmutableMessage, *schemapb.CollectionSchema) {
	t.Helper()
	version := int32(1)
	vector := &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}}
	if nullable {
		vector.ValidData = []bool{true, false, true}
		vector.GetFloatVector().Data = []float32{1, 2, 5, 6}
	}
	body := &message.UpsertMessageBody{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4, 5, 4}}}}, RowIds: []int64{10, 20, 30}, PartitionIndexes: []uint32{0, 1, 0}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: vector}}}, DeleteEffects: []*messagespb.UpsertDeleteEffect{{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{99}}}}, Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 30}}}}
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
	body.Columns = append(body.Columns, &schemapb.FieldData{FieldId: 102, Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{{FieldId: 103, Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: array}}}}}}, &schemapb.FieldData{FieldId: 104, Type: schemapb.DataType_ArrayOfVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_VectorArray{VectorArray: vectors}}}})
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
			body := &message.UpsertMessageBody{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, rows)}}}, RowIds: make([]int64, rows), Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, rows*128)}}}}}}}
			for i := 0; i < partitions; i++ {
				header.Partitions = append(header.Partitions, &message.PartitionSegmentAssignment{PartitionId: int64(i + 1), Rows: rows / uint64(partitions)})
			}
			if partitions > 1 {
				body.PartitionIndexes = make([]uint32, rows)
				for i := range body.PartitionIndexes {
					body.PartitionIndexes[i] = uint32(i % partitions)
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
