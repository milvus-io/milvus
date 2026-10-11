// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transformio

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func stringField(name string, values ...string) *schemapb.FieldData {
	return &schemapb.FieldData{FieldName: name, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}}}
}

func TestScalarNullAndBufferOwnership(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	field := stringField("text", "first", "third")
	typeutil.SetFieldDataValidData(field, []bool{true, false, true})
	table, err := FromFieldData([]*schemapb.FieldData{field}, 3, alloc)
	require.NoError(t, err)
	require.Equal(t, int64(3), table.NumRows())
	values := table.Column(0).Data().Chunk(0).(*array.String)
	require.True(t, values.IsNull(1))
	result, err := ToFieldData(table, nil)
	require.NoError(t, err)
	table.Release()
	alloc.AssertSize(t, 0)
	require.Equal(t, []string{"first", "", "third"}, result[0].GetScalars().GetStringData().GetData())
	require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(result[0]))
}

func TestArrayAndStructRemainNested(t *testing.T) {
	arrayField := &schemapb.FieldData{FieldName: "chunks", Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar, Data: []*schemapb.ScalarField{stringField("", "a", "b").GetScalars(), stringField("", "c").GetScalars()}}}}}}
	structField := &schemapb.FieldData{FieldName: "sections", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{arrayField}}}}
	table, err := FromFieldData([]*schemapb.FieldData{arrayField, structField}, 2, memory.DefaultAllocator)
	require.NoError(t, err)
	defer table.Release()
	require.Equal(t, arrow.LIST, table.Schema().Field(0).Type.ID())
	require.Equal(t, arrow.STRUCT, table.Schema().Field(1).Type.(*arrow.ListType).Elem().ID())
	result, err := ToFieldData(table, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, result[0].GetScalars().GetArrayData().Data[0].GetStringData().GetData())
	require.Equal(t, []string{"c"}, result[1].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data[1].GetStringData().GetData())
}

func TestNullableVectorRoundTrip(t *testing.T) {
	field := &schemapb.FieldData{FieldName: "embedding", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}}
	typeutil.SetFieldDataValidData(field, []bool{true, false, true})
	table, err := FromFieldData([]*schemapb.FieldData{field}, 3, memory.DefaultAllocator)
	require.NoError(t, err)
	defer table.Release()
	result, err := ToFieldData(table, []*schemapb.FieldSchema{{Name: "embedding", DataType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}})
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3, 4}, result[0].GetVectors().GetFloatVector().GetData())
	require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(result[0]))
}

func TestShapeAndOutputSchemaFailures(t *testing.T) {
	_, err := FromFieldData([]*schemapb.FieldData{stringField("text", "a")}, 2, memory.DefaultAllocator)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = FromFieldData([]*schemapb.FieldData{stringField("text", "a"), stringField("text", "b")}, 1, memory.DefaultAllocator)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	table, err := FromFieldData([]*schemapb.FieldData{stringField("temp", "a")}, 1, memory.DefaultAllocator)
	require.NoError(t, err)
	defer table.Release()
	_, err = ToFieldData(table, []*schemapb.FieldSchema{{Name: "text", DataType: schemapb.DataType_VarChar}})
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	_, err = ToFieldData(table, []*schemapb.FieldSchema{{Name: "temp", DataType: schemapb.DataType_Int64}})
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
}

func TestTypedEmptyTables(t *testing.T) {
	table, err := FromFieldData([]*schemapb.FieldData{stringField("text")}, 0, memory.DefaultAllocator)
	require.NoError(t, err)
	defer table.Release()
	result, err := ToFieldData(table, nil)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_VarChar, result[0].GetType())
	require.Empty(t, result[0].GetScalars().GetStringData().GetData())
	chunks := arrow.NewChunked(arrow.BinaryTypes.String, nil)
	column := arrow.NewColumn(table.Schema().Field(0), chunks)
	chunks.Release()
	zeroChunks := array.NewTable(table.Schema(), []arrow.Column{*column}, 0)
	column.Release()
	defer zeroChunks.Release()
	result, err = ToFieldData(zeroChunks, nil)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_VarChar, result[0].GetType())
	require.Empty(t, result[0].GetScalars().GetStringData().GetData())
}

func TestTimestampWireFormatIsNormalizedWithoutInputMutation(t *testing.T) {
	field := stringField("created_at", "2026-10-10T08:00:00+08:00")
	field.Type = schemapb.DataType_Timestamptz
	table, err := FromFieldDataWithTimezone([]*schemapb.FieldData{field}, 1, memory.DefaultAllocator, "Asia/Shanghai")
	require.NoError(t, err)
	defer table.Release()
	require.Equal(t, arrow.INT64, table.Schema().Field(0).Type.ID())
	require.NotNil(t, field.GetScalars().GetStringData(), "borrowed proto retains public string format")
	result, err := ToFieldData(table, nil)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_Timestamptz, result[0].GetType())
	require.NotEmpty(t, result[0].GetScalars().GetTimestamptzData().GetData())
}
