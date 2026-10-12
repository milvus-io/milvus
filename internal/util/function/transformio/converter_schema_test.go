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

package transformio

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestEveryVectorEncodingPreservesLogicalIdentityAndNulls(t *testing.T) {
	for _, dt := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector, schemapb.DataType_SparseFloatVector} {
		t.Run(dt.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			vector := &schemapb.VectorField{Dim: 4}
			switch dt {
			case schemapb.DataType_FloatVector:
				vector.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}}}
			case schemapb.DataType_BinaryVector:
				vector.Dim = 16
				vector.Data = &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2, 3, 4}}
			case schemapb.DataType_Float16Vector:
				vector.Data = &schemapb.VectorField_Float16Vector{Float16Vector: bytes.Repeat([]byte{1, 2}, 8)}
			case schemapb.DataType_BFloat16Vector:
				vector.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: bytes.Repeat([]byte{1, 2}, 8)}
			case schemapb.DataType_Int8Vector:
				vector.Data = &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}}
			case schemapb.DataType_SparseFloatVector:
				vector.Data = &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 4, Contents: [][]byte{typeutil.CreateSparseFloatRow([]uint32{1}, []float32{1}), typeutil.CreateSparseFloatRow([]uint32{3}, []float32{2})}}}
			}
			field := &schemapb.FieldData{FieldName: "v", FieldId: 100, Type: dt, Field: &schemapb.FieldData_Vectors{Vectors: vector}}
			typeutil.SetFieldDataValidData(field, []bool{true, false, true})
			table, err := FromFieldData([]*schemapb.FieldData{field}, 3, pool)
			require.NoError(t, err)
			defer table.Release()
			require.NoError(t, types.ValidateRuntimeArrowSchema(table.Schema()))
			require.NoError(t, types.ValidateArrowArrayAgainstField(table.Column(0).Data().Chunk(0), table.Schema().Field(0)))
			result, err := ToFieldData(table, nil)
			require.NoError(t, err)
			require.Equal(t, dt, result[0].GetType())
			require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(result[0]))
			require.True(t, proto.Equal(vector, result[0].GetVectors()), "vector bits or dimension changed")
		})
	}
}

func TestTaggedLogicalValuesRetainLegalDestinationMaterialization(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	for _, test := range []struct {
		name, value string
		target      schemapb.DataType
	}{
		{"json", "{\"a\":1}", schemapb.DataType_JSON},
		{"geometry", "POINT (1 2)", schemapb.DataType_Geometry},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := stringField("value", test.value)
			table, err := FromFieldData([]*schemapb.FieldData{source}, 1, pool)
			require.NoError(t, err)
			defer table.Release()
			output, err := ToFieldData(table, []*schemapb.FieldSchema{{Name: "value", DataType: test.target}})
			require.NoError(t, err)
			if test.target == schemapb.DataType_JSON {
				require.Equal(t, []byte(test.value), output[0].GetScalars().GetJsonData().GetData()[0])
			} else {
				value, err := common.ConvertWKBToWKT(output[0].GetScalars().GetGeometryData().GetData()[0])
				require.NoError(t, err)
				require.Equal(t, test.value, value)
			}
		})
	}
	child, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "a", DataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "value", DataType: schemapb.DataType_Struct}, child)
	require.NoError(t, err)
	b := array.NewStructBuilder(pool, field.Type.(*arrow.StructType))
	b.Append(true)
	b.FieldBuilder(0).(*array.Int64Builder).Append(1)
	values := b.NewArray()
	b.Release()
	defer values.Release()
	record := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 1)
	defer record.Release()
	table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
	defer table.Release()
	for _, destination := range [][]*schemapb.FieldSchema{nil, {{Name: "value", DataType: schemapb.DataType_JSON}}} {
		output, err := ToFieldData(table, destination)
		require.NoError(t, err)
		require.Equal(t, schemapb.DataType_JSON, output[0].GetType())
		require.JSONEq(t, "{\"a\":1}", string(output[0].GetScalars().GetJsonData().GetData()[0]))
	}
}

func TestGeometryWKTSourceNormalizesWithoutMutation(t *testing.T) {
	field := &schemapb.FieldData{FieldName: "geometry", Type: schemapb.DataType_Geometry, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"POINT (1 2)", ""}}}, ValidData: []bool{true, false}}}}
	table, err := FromFieldData([]*schemapb.FieldData{field}, 2, memory.DefaultAllocator)
	require.NoError(t, err)
	defer table.Release()
	require.NotNil(t, field.GetScalars().GetGeometryWktData())
	require.Equal(t, arrow.BINARY, table.Schema().Field(0).Type.ID())
	output, err := ToFieldData(table, nil)
	require.NoError(t, err)
	text, err := common.ConvertWKBToWKT(output[0].GetScalars().GetGeometryData().GetData()[0])
	require.NoError(t, err)
	require.Equal(t, "POINT (1 2)", text)
	require.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(output[0]))
}

func TestRecursiveArrayRoundTripAndTypedEmptySource(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}, Nullable: true}
	inner := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}, Nullable: true}
	root := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}, Nullable: true}
	schema := &schemapb.FieldSchema{Name: "nested", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true, TypeSchema: root}
	innerRow := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64, Data: []*schemapb.ScalarField{{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}}}}}
	field := &schemapb.FieldData{FieldName: "nested", Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Array, Data: []*schemapb.ScalarField{innerRow}}}}}}
	table, err := FromFieldData([]*schemapb.FieldData{field}, 1, pool, schema)
	require.NoError(t, err)
	defer table.Release()
	output, err := ToFieldData(table, []*schemapb.FieldSchema{schema})
	require.NoError(t, err)
	require.True(t, proto.Equal(field.GetScalars().GetArrayData(), output[0].GetScalars().GetArrayData()))
	empty := proto.Clone(field).(*schemapb.FieldData)
	empty.GetScalars().GetArrayData().Data = nil
	emptyTable, err := FromFieldData([]*schemapb.FieldData{empty}, 0, pool, schema)
	require.NoError(t, err)
	defer emptyTable.Release()
	require.NoError(t, types.ValidateRuntimeArrowSchema(emptyTable.Schema()))
}

func TestSourceNarrowingRejectsOutOfRangeValuesBeforeBuildingArrow(t *testing.T) {
	for _, test := range []struct {
		dt    schemapb.DataType
		value int32
	}{{schemapb.DataType_Int8, 1000}, {schemapb.DataType_Int16, 100000}} {
		t.Run(test.dt.String(), func(t *testing.T) {
			bad := &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{test.value}}}}
			scalar := &schemapb.FieldData{FieldName: "value", Type: test.dt, Field: &schemapb.FieldData_Scalars{Scalars: bad}}
			arrayField := &schemapb.FieldData{FieldName: "items", Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: test.dt, Data: []*schemapb.ScalarField{bad}}}}}}
			structure := &schemapb.FieldData{FieldName: "sections", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{arrayField}}}}
			nested := &schemapb.FieldData{FieldName: "nested", Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Array, Data: []*schemapb.ScalarField{{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: test.dt, Data: []*schemapb.ScalarField{bad}}}}}}}}}}
			for _, field := range []*schemapb.FieldData{scalar, arrayField, structure, nested} {
				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				table, err := FromFieldData([]*schemapb.FieldData{field}, 1, pool)
				require.ErrorIs(t, err, merr.ErrParameterInvalid, "source %s silently narrowed", field.GetFieldName())
				require.Nil(t, table)
				pool.AssertSize(t, 0)
			}
		})
	}
}

func TestVectorArrayAndStructVectorChildrenRoundTrip(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	row := &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}, ValidData: []bool{true, false, true}}
	field := &schemapb.FieldData{FieldName: "embedding", FieldId: 100, Type: schemapb.DataType_ArrayOfVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, ValidData: []bool{true, false}, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector, Data: []*schemapb.VectorField{row}}}}}}
	table, err := FromFieldData([]*schemapb.FieldData{field}, 2, pool)
	require.NoError(t, err)
	defer table.Release()
	result, err := ToFieldData(table, nil)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_ArrayOfVector, result[0].GetType())
	require.True(t, proto.Equal(row, result[0].GetVectors().GetVectorArray().GetData()[0]))
	require.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(result[0]))
	structure := &schemapb.FieldData{FieldName: "sections", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{field}}}}
	structTable, err := FromFieldData([]*schemapb.FieldData{structure}, 2, pool)
	require.NoError(t, err)
	defer structTable.Release()
	structResult, err := ToFieldData(structTable, nil)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_ArrayOfVector, structResult[0].GetStructArrays().GetFields()[0].GetType())
	require.True(t, proto.Equal(row, structResult[0].GetStructArrays().GetFields()[0].GetVectors().GetVectorArray().GetData()[0]))
}

func TestProtocolScalarWireEncodingsRoundTrip(t *testing.T) {
	for _, dt := range []schemapb.DataType{schemapb.DataType_Geometry, schemapb.DataType_Mol, schemapb.DataType_Date, schemapb.DataType_Time, schemapb.DataType_Decimal, schemapb.DataType_UUID} {
		t.Run(dt.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			scalar := &schemapb.ScalarField{}
			schema := &schemapb.FieldSchema{Name: "value", DataType: dt, Nullable: true}
			switch dt {
			case schemapb.DataType_Geometry:
				scalar.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{{1, 2}, nil, {3, 4}}}}
			case schemapb.DataType_Mol:
				scalar.Data = &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: [][]byte{{1, 2}, nil, {3, 4}}}}
			case schemapb.DataType_Date:
				scalar.Data = &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: []int32{1, 0, 3}}}
			case schemapb.DataType_Time:
				scalar.Data = &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{Data: []int64{1, 0, 3}}}
			case schemapb.DataType_Decimal, schemapb.DataType_UUID:
				width := 8
				if dt == schemapb.DataType_UUID {
					width = 16
				} else {
					schema.TypeParams = []*commonpb.KeyValuePair{{Key: "precision", Value: "18"}, {Key: "scale", Value: "2"}}
				}
				scalar.Data = &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{bytes.Repeat([]byte{1}, width), nil, bytes.Repeat([]byte{2}, width)}}}
			}
			field := &schemapb.FieldData{FieldName: "value", Type: dt, Field: &schemapb.FieldData_Scalars{Scalars: scalar}}
			typeutil.SetFieldDataValidData(field, []bool{true, false, true})
			table, err := FromFieldData([]*schemapb.FieldData{field}, 3, pool, schema)
			require.NoError(t, err)
			defer table.Release()
			output, err := ToFieldData(table, nil)
			require.NoError(t, err)
			require.Equal(t, dt, output[0].GetType())
			require.True(t, proto.Equal(scalar, output[0].GetScalars()))
		})
	}
}

func TestTargetBindingRejectsAmbiguousBinaryAndWrongEmptyTypes(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	fixed := array.NewFixedSizeBinaryBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: 4})
	defer fixed.Release()
	fixed.Append([]byte{1, 2, 3, 4})
	values := fixed.NewArray()
	defer values.Release()
	field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}})
	require.NoError(t, err)
	record := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 1)
	defer record.Release()
	table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
	defer table.Release()
	_, err = ToFieldData(table, []*schemapb.FieldSchema{{Name: "v", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}})
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	_, err = types.MilvusSchemaFromArrowField(arrow.Field{Name: "opaque", Type: arrow.BinaryTypes.Binary})
	require.Error(t, err)
	for _, rows := range []int{0, 2} {
		floats := array.MakeArrayOfNull(pool, arrow.PrimitiveTypes.Float32, rows)
		_, err := arrowToScalar(floats, schemapb.DataType_Int64, schemapb.DataType_None)
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
		floats.Release()
	}
	for _, dim := range []int64{0, -1, 7} {
		bytes := array.MakeArrayOfNull(pool, &arrow.FixedSizeBinaryType{ByteWidth: 1}, 0)
		_, err := arrowToVector(bytes, &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.FormatInt(dim, 10)}}})
		require.Error(t, err)
		bytes.Release()
	}
}
