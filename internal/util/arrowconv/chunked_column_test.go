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
			case schemapb.DataType_Int8, schemapb.DataType_Int16:
				require.Equal(t, []int32{12, 0, 34}, output.GetScalars().GetIntData().Data)
			case schemapb.DataType_Text:
				require.Equal(t, []string{"first", "", "last"}, output.GetScalars().GetStringData().Data)
			case schemapb.DataType_Geometry:
				require.Equal(t, []byte("first"), output.GetScalars().GetGeometryData().Data[0])
				require.Equal(t, []byte("last"), output.GetScalars().GetGeometryData().Data[2])
			case schemapb.DataType_Timestamptz:
				require.Equal(t, []int64{12, 0, 34}, output.GetScalars().GetTimestamptzData().Data)
			}
			empty := arrow.NewChunked(tc.typ, nil)
			field, err := ArrowChunkedColumnToFieldData(empty, schema)
			empty.Release()
			require.NoError(t, err)
			require.Equal(t, tc.logical, field.Type)
			require.NotNil(t, field.GetScalars())
		})
	}
}

func TestArrowChunkedNullableVector(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	dtype := &arrow.FixedSizeBinaryType{ByteWidth: 8}
	b := array.NewFixedSizeBinaryBuilder(pool, dtype)
	b.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
	first := b.NewArray()
	b.AppendNull()
	b.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
	last := b.NewArray()
	b.Release()
	column := arrow.NewChunked(dtype, []arrow.Array{first, last})
	first.Release()
	last.Release()
	defer column.Release()
	schema := &schemapb.FieldSchema{Name: "vector", FieldID: 105, DataType: schemapb.DataType_FloatVector, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	output, err := ArrowChunkedColumnToFieldData(column, schema)
	require.NoError(t, err)
	require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(output))
	require.Equal(t, []float32{1, 2, 3, 4}, output.GetVectors().GetFloatVector().Data)
	require.Equal(t, int64(2), output.GetVectors().Dim)
	joined, err := array.Concatenate(column.Chunks(), pool)
	require.NoError(t, err)
	defer joined.Release()
	expected, err := ArrowColumnToFieldData(joined, schema)
	require.NoError(t, err)
	require.True(t, proto.Equal(expected, output))
}

func TestArrowChunkedMalformedLayout(t *testing.T) {
	column := arrow.NewChunked(arrow.BinaryTypes.String, nil)
	defer column.Release()
	output, err := ArrowChunkedColumnToFieldData(column, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64})
	require.Nil(t, output)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
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
			func() error { _, err := ArrowColumnToFieldData(values, schema); return err },
			func() error { _, err := ArrowChunkedColumnToFieldData(col, schema); return err },
			func() error { _, err := ArrowFieldsToProto(rec, schemas); return err },
			func() error { _, err := ArrowFieldsToProtoOrdered(rec, []int64{100}, schemas); return err },
		} {
			err := run()
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		}
	}
	out, err := ArrowColumnToFieldData(values, &schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_FloatVector})
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

func TestRecordExportUsesLogicalRowCount(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	b := array.NewInt64Builder(pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	values := b.NewArray()
	b.Release()
	defer values.Release()
	field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{"milvus.field_id": "100"})}
	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 2)
	defer rec.Release()
	schemas := map[int64]*schemapb.FieldSchema{100: {Name: "value", FieldID: 100, DataType: schemapb.DataType_Int64}}
	for _, run := range []func() ([]*schemapb.FieldData, error){
		func() ([]*schemapb.FieldData, error) { return ArrowFieldsToProto(rec, schemas) },
		func() ([]*schemapb.FieldData, error) { return ArrowFieldsToProtoOrdered(rec, []int64{100}, schemas) },
	} {
		fields, err := run()
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2}, fields[0].GetScalars().GetLongData().Data)
	}
}
