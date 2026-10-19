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

package arrowconv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestArrowChunkedScalarLayouts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     arrow.DataType
		logical schemapb.DataType
		input   string
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, schemapb.DataType_Bool, `[false,true,null,false]`},
		{"int32", arrow.PrimitiveTypes.Int32, schemapb.DataType_Int32, `[0,12,null,34]`},
		{"int64", arrow.PrimitiveTypes.Int64, schemapb.DataType_Int64, `[0,12,null,34]`},
		{"float", arrow.PrimitiveTypes.Float32, schemapb.DataType_Float, `[0,1.25,null,2.5]`},
		{"double", arrow.PrimitiveTypes.Float64, schemapb.DataType_Double, `[0,1.25,null,2.5]`},
		{"varchar", arrow.BinaryTypes.String, schemapb.DataType_VarChar, `["skip","first",null,"last"]`},
		{"string", arrow.BinaryTypes.String, schemapb.DataType_String, `["skip","first",null,"last"]`},
		{"geometry binary", arrow.BinaryTypes.Binary, schemapb.DataType_Geometry, `["c2tpcA==","Zmlyc3Q=",null,"bGFzdA=="]`},
		{"json", arrow.BinaryTypes.Binary, schemapb.DataType_JSON, `["e30=","eyJhIjoxfQ==",null,"eyJiIjoyfQ=="]`},
		{"int8", arrow.PrimitiveTypes.Int8, schemapb.DataType_Int8, `[0,12,null,34]`},
		{"int16", arrow.PrimitiveTypes.Int16, schemapb.DataType_Int16, `[0,12,null,34]`},
		{"native int32", arrow.PrimitiveTypes.Int32, schemapb.DataType_Int8, `[0,12,null,34]`},
		{"text", arrow.BinaryTypes.String, schemapb.DataType_Text, `["skip","first",null,"last"]`},
		{"geometry string", arrow.BinaryTypes.String, schemapb.DataType_Geometry, `["skip","first",null,"last"]`},
		{"timestamp", arrow.PrimitiveTypes.Int64, schemapb.DataType_Timestamptz, `[0,12,null,34]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, tc.typ, strings.NewReader(tc.input))
			require.NoError(t, err)
			first := array.NewSlice(values, 1, 2) // All-valid chunk before a nullable chunk.
			last := array.NewSlice(values, 2, 4)
			column := arrow.NewChunked(tc.typ, []arrow.Array{first, last})
			first.Release()
			last.Release()
			values.Release()
			// Even with an absent nullable declaration, actual nulls need a full mask.
			schema := &schemapb.FieldSchema{Name: "field", FieldID: 123, DataType: tc.logical}
			output, err := ArrowChunkedColumnToFieldData(column, schema)
			column.Release()
			require.NoError(t, err)
			require.Equal(t, int64(123), output.FieldId)
			require.Equal(t, tc.logical, output.Type)
			require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(output))
			switch tc.logical {
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
				require.Equal(t, []int32{12, 0, 34}, output.GetScalars().GetIntData().Data)
			case schemapb.DataType_Text, schemapb.DataType_VarChar, schemapb.DataType_String:
				require.Equal(t, []string{"first", "", "last"}, output.GetScalars().GetStringData().Data)
			case schemapb.DataType_Geometry:
				require.Equal(t, []byte("first"), output.GetScalars().GetGeometryData().Data[0])
				require.Equal(t, []byte("last"), output.GetScalars().GetGeometryData().Data[2])
			case schemapb.DataType_Bool:
				require.Equal(t, []bool{true, false, false}, output.GetScalars().GetBoolData().Data)
			case schemapb.DataType_Int64:
				require.Equal(t, []int64{12, 0, 34}, output.GetScalars().GetLongData().Data)
			case schemapb.DataType_Float:
				require.Equal(t, []float32{1.25, 0, 2.5}, output.GetScalars().GetFloatData().Data)
			case schemapb.DataType_Double:
				require.Equal(t, []float64{1.25, 0, 2.5}, output.GetScalars().GetDoubleData().Data)
			case schemapb.DataType_JSON:
				require.Equal(t, [][]byte{[]byte(`{"a":1}`), {}, []byte(`{"b":2}`)}, output.GetScalars().GetJsonData().Data)
			case schemapb.DataType_Timestamptz:
				require.Equal(t, []int64{12, 0, 34}, output.GetScalars().GetTimestamptzData().Data)
			}
			empty := arrow.NewChunked(tc.typ, nil)
			field, err := ArrowChunkedColumnToFieldData(empty, schema)
			empty.Release()
			if tc.logical == schemapb.DataType_String {
				// GenEmptyFieldData does not support the legacy String enum.
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.logical, field.Type)
			require.NotNil(t, field.GetScalars())
		})
	}
}

func TestArrowChunkedNullableVector(t *testing.T) {
	for _, logical := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector} {
		t.Run(logical.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			dtype := &arrow.FixedSizeBinaryType{ByteWidth: 8}
			b := array.NewFixedSizeBinaryBuilder(pool, dtype)
			b.Append(make([]byte, 8)) // Excluded prefix exercises sliced buffers.
			b.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
			b.AppendNull()
			b.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
			values := b.NewArray()
			b.Release()
			parts := []arrow.Array{array.NewSlice(values, 0, 0), array.NewSlice(values, 1, 2), array.NewSlice(values, 2, 3), array.NewSlice(values, 3, 4), array.NewSlice(values, 4, 4)}
			column := arrow.NewChunked(dtype, parts)
			for _, part := range parts {
				part.Release()
			}
			values.Release()
			// Infer the dimension from the physical width, as on the native path.
			schema := &schemapb.FieldSchema{Name: "vector", FieldID: 105, DataType: logical, Nullable: true}
			output, err := ArrowChunkedColumnToFieldData(column, schema)
			column.Release()
			require.NoError(t, err)
			require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(output))
			expected := arrow.Float32Traits.CastToBytes([]float32{1, 2, 3, 4})
			switch logical {
			case schemapb.DataType_FloatVector:
				require.Equal(t, int64(2), output.GetVectors().Dim)
				require.Equal(t, []float32{1, 2, 3, 4}, output.GetVectors().GetFloatVector().Data)
			case schemapb.DataType_BinaryVector:
				require.Equal(t, int64(64), output.GetVectors().Dim)
				require.Equal(t, expected, output.GetVectors().GetBinaryVector())
			case schemapb.DataType_Float16Vector:
				require.Equal(t, int64(4), output.GetVectors().Dim)
				require.Equal(t, expected, output.GetVectors().GetFloat16Vector())
			case schemapb.DataType_BFloat16Vector:
				require.Equal(t, int64(4), output.GetVectors().Dim)
				require.Equal(t, expected, output.GetVectors().GetBfloat16Vector())
			case schemapb.DataType_Int8Vector:
				require.Equal(t, int64(8), output.GetVectors().Dim)
				require.Equal(t, expected, output.GetVectors().GetInt8Vector())
			}
		})
	}
}

func TestArrowChunkedMalformedLayout(t *testing.T) {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.Append("value")
	values := b.NewArray()
	b.Release()
	defer values.Release()
	for _, chunks := range [][]arrow.Array{nil, {values, values}} {
		column := arrow.NewChunked(arrow.BinaryTypes.String, chunks)
		output, err := ArrowChunkedColumnToFieldData(column, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64})
		column.Release()
		require.Nil(t, output)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
}

func TestNativeColumnTypeRejectsInvalidVectorDimensions(t *testing.T) {
	for _, tc := range []struct {
		typ  schemapb.DataType
		dims []string
	}{
		{schemapb.DataType_BinaryVector, []string{"63"}},
		{schemapb.DataType_FloatVector, []string{"-1"}},
		{schemapb.DataType_FloatVector, []string{"2", "2"}},
	} {
		for _, asArray := range []bool{false, true} {
			schema := &schemapb.FieldSchema{Name: "vector", DataType: tc.typ}
			for _, dim := range tc.dims {
				schema.TypeParams = append(schema.TypeParams, &commonpb.KeyValuePair{Key: "dim", Value: dim})
			}
			if asArray {
				schema.DataType = schemapb.DataType_ArrayOfVector
				schema.ElementType = tc.typ
			}
			dtype, err := NativeColumnType(schema)
			require.Nil(t, dtype)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		}
	}
}

func TestArrowChunkedSparseVectorDimension(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	b := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	row0 := typeutil.CreateSparseFloatRow([]uint32{1}, []float32{2})
	row1 := typeutil.CreateSparseFloatRow([]uint32{99}, []float32{3})
	b.Append(row0)
	first := b.NewArray()
	b.AppendNull()
	b.Append(row1)
	last := b.NewArray()
	b.Release()
	column := arrow.NewChunked(arrow.BinaryTypes.Binary, []arrow.Array{first, last})
	first.Release()
	last.Release()
	defer column.Release()
	output, err := ArrowChunkedColumnToFieldData(column, &schemapb.FieldSchema{Name: "sparse", FieldID: 107, DataType: schemapb.DataType_SparseFloatVector})
	require.NoError(t, err)
	require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(output))
	require.Equal(t, int64(100), output.GetVectors().Dim)
	require.Equal(t, int64(100), output.GetVectors().GetSparseFloatVector().Dim)
	require.Equal(t, [][]byte{row0, row1}, output.GetVectors().GetSparseFloatVector().Contents)
}

func TestVectorDimensionMismatchFailsEveryExportEntry(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	b := array.NewFixedSizeBinaryBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: 8})
	b.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
	values := b.NewArray()
	b.Release()
	defer values.Release()
	col := arrow.NewChunked(values.DataType(), []arrow.Array{values})
	defer col.Release()
	field := arrow.Field{Name: "v", Type: values.DataType(), Metadata: arrow.MetadataFrom(map[string]string{"milvus.field_id": "100"})}
	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 1)
	defer rec.Release()
	for _, dim := range []string{"1", "3", "0", "-1", "invalid"} {
		schema := &schemapb.FieldSchema{Name: "v", FieldID: 100, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: dim}}}
		schemas := map[int64]*schemapb.FieldSchema{100: schema}
		for _, run := range []func() error{
			func() error { _, err := arrowColumnToFieldData(values, schema); return err },
			func() error { _, err := ArrowChunkedColumnToFieldData(col, schema); return err },
			func() error { _, err := ArrowFieldsToProto(rec, schemas); return err },
			func() error { _, err := ArrowFieldsToProtoOrdered(rec, []int64{100}, schemas); return err },
		} {
			err := run()
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		}
	}
	out, err := arrowColumnToFieldData(values, &schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_FloatVector})
	require.NoError(t, err)
	require.Equal(t, int64(2), out.GetVectors().Dim)
	require.Equal(t, []float32{1, 2}, out.GetVectors().GetFloatVector().Data)
}

func TestVectorLayoutValidationIncludesEmptyAndArrayColumns(t *testing.T) {
	for _, tc := range []struct {
		typ   schemapb.DataType
		width int
		dim   string
	}{
		{schemapb.DataType_FloatVector, 8, "3"},
		{schemapb.DataType_FloatVector, 3, "1"},
		{schemapb.DataType_Float16Vector, 8, "3"},
		{schemapb.DataType_BFloat16Vector, 8, "3"},
		{schemapb.DataType_BinaryVector, 8, "63"},
		{schemapb.DataType_Int8Vector, 8, "7"},
	} {
		for _, asArray := range []bool{false, true} {
			var dtype arrow.DataType = &arrow.FixedSizeBinaryType{ByteWidth: tc.width}
			schema := &schemapb.FieldSchema{Name: "vector", DataType: tc.typ, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: tc.dim}}}
			if asArray {
				dtype = arrow.ListOf(dtype)
				schema.DataType = schemapb.DataType_ArrayOfVector
				schema.ElementType = tc.typ
			}
			col := arrow.NewChunked(dtype, nil)
			_, err := ArrowChunkedColumnToFieldData(col, schema)
			col.Release()
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		}
	}
}

func TestRecordExportConvertsColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	b := array.NewInt64Builder(pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	values := b.NewArray()
	b.Release()
	defer values.Release()
	field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{"milvus.field_id": "100"})}
	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, int64(values.Len()))
	defer rec.Release()
	schemas := map[int64]*schemapb.FieldSchema{100: {Name: "value", FieldID: 100, DataType: schemapb.DataType_Int64}}
	for _, run := range []func() ([]*schemapb.FieldData, error){
		func() ([]*schemapb.FieldData, error) { return ArrowFieldsToProto(rec, schemas) },
		func() ([]*schemapb.FieldData, error) { return ArrowFieldsToProtoOrdered(rec, []int64{100}, schemas) },
	} {
		fields, err := run()
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3}, fields[0].GetScalars().GetLongData().Data)
	}
}

func TestZeroChunkVectorExportInfersDimension(t *testing.T) {
	for _, vectorArray := range []bool{false, true} {
		var dtype arrow.DataType = &arrow.FixedSizeBinaryType{ByteWidth: 12}
		schema := &schemapb.FieldSchema{Name: "vector", FieldID: 100, DataType: schemapb.DataType_FloatVector}
		if vectorArray {
			dtype = arrow.ListOf(dtype)
			schema.DataType = schemapb.DataType_ArrayOfVector
			schema.ElementType = schemapb.DataType_FloatVector
		}
		col := arrow.NewChunked(dtype, nil)
		result, err := ArrowChunkedColumnToFieldData(col, schema)
		col.Release()
		require.NoError(t, err)
		require.Equal(t, int64(3), result.GetVectors().Dim)
		require.Empty(t, schema.TypeParams) // Inferred dimensions must not mutate the declaration.
		if vectorArray {
			require.Equal(t, int64(3), result.GetVectors().GetVectorArray().Dim)
			require.Equal(t, schemapb.DataType_FloatVector, result.GetVectors().GetVectorArray().ElementType)
		} else {
			require.NotNil(t, result.GetVectors().GetFloatVector())
		}
	}
}

func BenchmarkArrowChunkedColumnToFieldData(b *testing.B) {
	const rows = 10000
	for _, logical := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		for _, chunks := range []int{1, 100, 10000} {
			b.Run(fmt.Sprintf("%s/chunks=%d", logical, chunks), func(b *testing.B) {
				var values arrow.Array
				if logical == schemapb.DataType_Int64 {
					builder := array.NewInt64Builder(memory.DefaultAllocator)
					for i := 0; i < rows; i++ {
						builder.Append(int64(i))
					}
					values = builder.NewArray()
					builder.Release()
				} else {
					builder := array.NewStringBuilder(memory.DefaultAllocator)
					for i := 0; i < rows; i++ {
						builder.Append("group-by-value")
					}
					values = builder.NewArray()
					builder.Release()
				}
				parts := make([]arrow.Array, chunks)
				for i := range parts {
					parts[i] = array.NewSlice(values, int64(i*rows/chunks), int64((i+1)*rows/chunks))
				}
				column := arrow.NewChunked(values.DataType(), parts)
				for _, part := range parts {
					part.Release()
				}
				values.Release()
				defer column.Release()
				schema := &schemapb.FieldSchema{Name: "field", FieldID: 100, DataType: logical}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := ArrowChunkedColumnToFieldData(column, schema)
					if err != nil {
						b.Fatal(err)
					}
					if result == nil {
						b.Fatal("nil result")
					}
				}
			})
		}
	}
}

func TestArrowChunkedArrayPayload(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	first := &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}
	last := &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3}}}}
	builder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	for _, value := range []*schemapb.ScalarField{first, nil, last} {
		if value == nil {
			builder.AppendNull()
			continue
		}
		encoded, err := proto.Marshal(value)
		require.NoError(t, err)
		builder.Append(encoded)
	}
	values := builder.NewArray()
	builder.Release()
	parts := []arrow.Array{array.NewSlice(values, 0, 1), array.NewSlice(values, 1, 3)}
	column := arrow.NewChunked(values.DataType(), parts)
	for _, part := range parts {
		part.Release()
	}
	values.Release()
	result, err := ArrowChunkedColumnToFieldData(column, &schemapb.FieldSchema{Name: "array", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true})
	column.Release()
	require.NoError(t, err)
	require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(result))
	require.Equal(t, schemapb.DataType_Int32, result.GetScalars().GetArrayData().ElementType)
	rows := result.GetScalars().GetArrayData().Data
	require.Len(t, rows, 3)
	require.True(t, proto.Equal(first, rows[0]))
	require.True(t, proto.Equal(&schemapb.ScalarField{}, rows[1]))
	require.True(t, proto.Equal(last, rows[2]))
}

func TestArrowChunkedVectorArrayPayload(t *testing.T) {
	for _, logical := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector} {
		t.Run(logical.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			builder := array.NewListBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: 8})
			inner := builder.ValueBuilder().(*array.FixedSizeBinaryBuilder)
			builder.Append(true)
			inner.Append(make([]byte, 8)) // Excluded outer-list prefix.
			builder.Append(true)
			inner.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
			inner.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
			builder.AppendNull()
			builder.Append(true)
			inner.Append(arrow.Float32Traits.CastToBytes([]float32{5, 6}))
			values := builder.NewArray()
			builder.Release()
			parts := []arrow.Array{array.NewSlice(values, 1, 2), array.NewSlice(values, 2, 3), array.NewSlice(values, 3, 4)}
			column := arrow.NewChunked(values.DataType(), parts)
			for _, part := range parts {
				part.Release()
			}
			values.Release()
			result, err := ArrowChunkedColumnToFieldData(column, &schemapb.FieldSchema{Name: "vectors", DataType: schemapb.DataType_ArrayOfVector, ElementType: logical, Nullable: true})
			column.Release()
			require.NoError(t, err)
			require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(result))
			vectors := result.GetVectors().GetVectorArray()
			require.Equal(t, logical, vectors.ElementType)
			require.Len(t, vectors.Data, 3)
			require.Nil(t, vectors.Data[1].Data)
			for i, expected := range map[int][]float32{0: {1, 2, 3, 4}, 2: {5, 6}} {
				vector := vectors.Data[i]
				require.Equal(t, vectors.Dim, vector.Dim)
				raw := arrow.Float32Traits.CastToBytes(expected)
				switch logical {
				case schemapb.DataType_FloatVector:
					require.Equal(t, expected, vector.GetFloatVector().Data)
				case schemapb.DataType_BinaryVector:
					require.Equal(t, raw, vector.GetBinaryVector())
				case schemapb.DataType_Float16Vector:
					require.Equal(t, raw, vector.GetFloat16Vector())
				case schemapb.DataType_BFloat16Vector:
					require.Equal(t, raw, vector.GetBfloat16Vector())
				case schemapb.DataType_Int8Vector:
					require.Equal(t, raw, vector.GetInt8Vector())
				}
			}
		})
	}
}
