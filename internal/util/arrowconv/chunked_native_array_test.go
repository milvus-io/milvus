package arrowconv

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Include an empty chunk and nonzero slice offsets; omit the prefix row.
func slicedNativeColumn(values arrow.Array) *arrow.Chunked {
	parts := []arrow.Array{
		array.NewSlice(values, 1, 1),
		array.NewSlice(values, 1, 2),
		array.NewSlice(values, 2, int64(values.Len())),
	}
	column := arrow.NewChunked(values.DataType(), parts)
	for _, part := range parts {
		part.Release()
	}
	return column
}

func TestArrowChunkedNativeArrays(t *testing.T) {
	for _, nested := range []bool{false, true} {
		name := "scalar"
		if nested {
			name = "nested"
		}
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			field := &schemapb.FieldSchema{
				Name: name, FieldID: 101, DataType: schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int16, Nullable: true, ElementNullable: true,
			}
			row := &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{7, 0}},
			}}
			if nested {
				field.ElementType = schemapb.DataType_Array
				leaf := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int16}}
				inner := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}}
				field.TypeSchema = &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}
				row = &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int16, Data: []*schemapb.ScalarField{
						row, {Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}},
					}},
				}}
			}
			dtype, err := storage.ArrowTypeForField(field)
			require.NoError(t, err)
			native, err := NativeColumnType(field)
			require.NoError(t, err)
			require.True(t, arrow.TypeEqual(dtype, native))
			builder := array.NewBuilder(pool, dtype)
			for _, value := range []*schemapb.ScalarField{row, row, nil, row} {
				require.NoError(t, storage.SerializeNativeArrayRow(builder, value, field))
			}
			values := builder.NewArray()
			builder.Release()
			column := slicedNativeColumn(values)
			values.Release()
			result, err := ArrowChunkedColumnToFieldData(column, field)
			column.Release()
			require.NoError(t, err)
			require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(result))
			rows := result.GetScalars().GetArrayData().GetData()
			require.Len(t, rows, 3)
			require.True(t, proto.Equal(row, rows[0]))
			require.Nil(t, rows[1])
			require.True(t, proto.Equal(row, rows[2]))
			empty := arrow.NewChunked(dtype, nil)
			defer empty.Release()
			before := proto.Clone(field)
			result, err = ArrowChunkedColumnToFieldData(empty, field)
			require.NoError(t, err)
			require.Equal(t, field.ElementType, result.GetScalars().GetArrayData().GetElementType())
			require.Empty(t, result.GetScalars().GetArrayData().GetData())
			require.True(t, proto.Equal(before, field))
		})
	}
}

func nullableVectorSchema() *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name: "vectors", FieldID: 102, DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
	}
}

func TestArrowChunkedNullableVectorArray(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	field := nullableVectorSchema()
	dtype, err := NativeColumnType(field)
	require.NoError(t, err)
	builder := array.NewBuilder(pool, dtype).(*array.ListBuilder)
	child := builder.ValueBuilder().(*array.BinaryBuilder)
	builder.Append(true)
	child.Append(arrow.Float32Traits.CastToBytes([]float32{99, 99}))
	builder.Append(true)
	child.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
	child.AppendNull()
	child.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
	builder.AppendNull()
	builder.Append(true)
	child.AppendNulls(3)
	builder.Append(true) // Empty, non-null row.
	values := builder.NewArray()
	builder.Release()
	column := slicedNativeColumn(values)
	values.Release()
	result, err := ArrowChunkedColumnToFieldData(column, field)
	column.Release()
	require.NoError(t, err)
	require.Equal(t, []bool{true, false, true, true}, typeutil.GetFieldDataValidData(result))
	rows := result.GetVectors().GetVectorArray().GetData()
	require.Len(t, rows, 4)
	require.Equal(t, []float32{1, 2, 3, 4}, rows[0].GetFloatVector().GetData())
	require.Equal(t, []bool{true, false, true}, typeutil.GetVectorArrayElementValidData(rows[0]))
	require.NotNil(t, rows[1].GetFloatVector())
	require.Empty(t, rows[1].GetFloatVector().GetData())
	require.Equal(t, []bool{false, false, false}, typeutil.GetVectorArrayElementValidData(rows[2]))
	require.Zero(t, cap(rows[2].GetFloatVector().GetData()))
	require.NotNil(t, rows[3].GetFloatVector())
	require.Empty(t, rows[3].GetFloatVector().GetData())
	empty := arrow.NewChunked(dtype, nil)
	defer empty.Release()
	result, err = ArrowChunkedColumnToFieldData(empty, field)
	require.NoError(t, err)
	require.EqualValues(t, 2, result.GetVectors().GetVectorArray().GetDim())
	require.Empty(t, result.GetVectors().GetVectorArray().GetData())
}

func TestArrowChunkedNativeArrayFailures(t *testing.T) {
	for _, failure := range []string{"vector_width", "null_vector_row", "nonnullable_vector_child", "null_scalar_row"} {
		t.Run(failure, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			field := nullableVectorSchema()
			if failure == "null_scalar_row" {
				field.DataType, field.ElementType = schemapb.DataType_Array, schemapb.DataType_Int16
			}
			if failure == "nonnullable_vector_child" {
				field.ElementNullable = false
			}
			dtype, err := NativeColumnType(field)
			require.NoError(t, err)
			builder := array.NewBuilder(pool, dtype).(*array.ListBuilder)
			builder.Append(true) // Prefix omitted from the output.
			builder.Append(true) // Valid first chunk.
			switch failure {
			case "vector_width":
				builder.Append(true)
				builder.ValueBuilder().(*array.BinaryBuilder).Append([]byte{1})
			case "null_vector_row":
				builder.AppendNull()
				builder.ValueBuilder().(*array.BinaryBuilder).Append(make([]byte, 8))
			case "nonnullable_vector_child":
				builder.Append(true)
				builder.ValueBuilder().AppendNull()
			case "null_scalar_row":
				builder.AppendNull()
				builder.ValueBuilder().(*array.Int16Builder).Append(1)
			}
			values := builder.NewArray()
			builder.Release()
			record := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: field.Name, Type: dtype}}, nil), []arrow.Array{values}, int64(values.Len()))
			_, err = ArrowFieldsToProtoOrdered(record, []int64{field.FieldID}, map[int64]*schemapb.FieldSchema{field.FieldID: field})
			record.Release()
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			column := slicedNativeColumn(values)
			values.Release()
			_, err = ArrowChunkedColumnToFieldData(column, field)
			column.Release()
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
}

func TestArrowChunkedNativeArrayRejectsWrongEmptyLayout(t *testing.T) {
	field := &schemapb.FieldSchema{
		Name: "array", DataType: schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int16, ElementNullable: true,
	}
	for _, dtype := range []arrow.DataType{arrow.BinaryTypes.Binary, arrow.ListOf(arrow.PrimitiveTypes.Int32)} {
		column := arrow.NewChunked(dtype, nil)
		_, err := ArrowChunkedColumnToFieldData(column, field)
		column.Release()
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
}

func TestArrowNullableVectorArrayRequiresValidDimension(t *testing.T) {
	for _, dims := range [][]string{nil, {"0"}, {"-1"}, {"abc"}, {"2", "2"}} {
		field := nullableVectorSchema()
		field.TypeParams = nil
		for _, dim := range dims {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{Key: "dim", Value: dim})
		}
		column := arrow.NewChunked(arrow.ListOf(arrow.BinaryTypes.Binary), nil)
		_, err := ArrowChunkedColumnToFieldData(column, field)
		column.Release()
		require.Error(t, err)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
}
