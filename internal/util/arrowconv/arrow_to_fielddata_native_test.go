package arrowconv

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestArrowFieldsToProtoNativeNestedArray(t *testing.T) {
	leaf := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int16}}
	inner := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}}
	field := &schemapb.FieldSchema{
		FieldID: 101, Name: "nested", DataType: schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
		TypeSchema: &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}},
	}
	typ, err := storage.ArrowTypeForField(field)
	require.NoError(t, err)
	b := array.NewBuilder(memory.DefaultAllocator, typ)
	defer b.Release()
	row := &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
		ElementType: schemapb.DataType_Int16,
		Data: []*schemapb.ScalarField{
			{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{7, 0}}}},
			{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}},
		},
	}}}
	require.NoError(t, storage.SerializeNativeArrayRow(b, row, field))
	require.NoError(t, storage.SerializeNativeArrayRow(b, (*schemapb.ScalarField)(nil), field))
	col := b.NewArray()
	defer col.Release()
	md := arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"101"})
	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "nested", Type: typ, Nullable: true, Metadata: md}}, nil), []arrow.Array{col}, 2)
	defer rec.Release()
	result, err := ArrowFieldsToProto(rec, map[int64]*schemapb.FieldSchema{101: field})
	require.NoError(t, err)
	require.Len(t, result, 1)
	data := result[0].GetScalars().GetArrayData().GetData()
	require.Equal(t, row, data[0])
	require.Nil(t, data[1])
	require.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(result[0]))
}

func TestArrowFieldsToProtoNullableVectorArray(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID: 102, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, ElementNullable: true, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
	}
	typ, err := storage.ArrowTypeForField(field)
	require.NoError(t, err)
	b := array.NewBuilder(memory.DefaultAllocator, typ).(*array.ListBuilder)
	defer b.Release()
	b.Append(true)
	child := b.ValueBuilder().(*array.BinaryBuilder)
	child.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
	child.AppendNull()
	child.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
	b.AppendNull()
	col := b.NewArray()
	defer col.Release()
	md := arrow.NewMetadata([]string{"milvus.field_id"}, []string{"102"})
	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "vectors", Type: typ, Nullable: true, Metadata: md}}, nil), []arrow.Array{col}, 2)
	defer rec.Release()
	result, err := ArrowFieldsToProto(rec, map[int64]*schemapb.FieldSchema{102: field})
	require.NoError(t, err)
	row := result[0].GetVectors().GetVectorArray().GetData()[0]
	require.Equal(t, []bool{true, false, true}, typeutil.GetVectorArrayElementValidData(row))
	require.Equal(t, []float32{1, 2, 3, 4}, row.GetFloatVector().GetData())
	require.NotNil(t, result[0].GetVectors().GetVectorArray().GetData()[1].GetFloatVector())
	require.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(result[0]))
}

func TestArrowFieldsToProtoRejectsNonemptyNullVectorArray(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID: 103, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, ElementNullable: true, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
	}
	typ, err := storage.ArrowTypeForField(field)
	require.NoError(t, err)
	b := array.NewBuilder(memory.DefaultAllocator, typ).(*array.ListBuilder)
	defer b.Release()
	b.Append(true)
	b.ValueBuilder().(*array.BinaryBuilder).Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
	b.AppendNull()
	b.ValueBuilder().(*array.BinaryBuilder).Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
	b.Append(true)
	b.ValueBuilder().(*array.BinaryBuilder).Append(arrow.Float32Traits.CastToBytes([]float32{5, 6}))
	col := b.NewArray().(*array.List)
	defer col.Release()
	start, end := col.ValueOffsets(1)
	require.Greater(t, end, start)
	_, err = arrowListToVectorArray(col, field, col.Len())
	require.ErrorContains(t, err, "null ArrayOfVector row")
}

// The vector payload of an element-nullable VectorArray row is sized by its
// valid elements: an all-null row reserves nothing, a partly null row
// reserves exactly its valid vectors.
func TestArrowListToVectorArrayReservesValidElementsOnly(t *testing.T) {
	const dim = 16
	for _, tc := range []struct {
		elemType  schemapb.DataType
		byteWidth int
	}{
		{schemapb.DataType_FloatVector, dim * 4},
		{schemapb.DataType_Float16Vector, dim * 2},
		{schemapb.DataType_BFloat16Vector, dim * 2},
		{schemapb.DataType_Int8Vector, dim},
		{schemapb.DataType_BinaryVector, dim / 8},
	} {
		t.Run(tc.elemType.String(), func(t *testing.T) {
			field := &schemapb.FieldSchema{
				FieldID: 104, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
				ElementType: tc.elemType, ElementNullable: true, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "16"}},
			}
			typ, err := storage.ArrowTypeForField(field)
			require.NoError(t, err)
			b := array.NewBuilder(memory.DefaultAllocator, typ).(*array.ListBuilder)
			defer b.Release()
			child := b.ValueBuilder().(*array.BinaryBuilder)
			vector := make([]byte, tc.byteWidth)
			// Row 0: 64 null elements. Row 1: 64 elements, 2 of them valid.
			b.Append(true)
			child.AppendNulls(64)
			b.Append(true)
			for i := 0; i < 64; i++ {
				if i == 3 || i == 40 {
					child.Append(vector)
				} else {
					child.AppendNull()
				}
			}
			col := b.NewArray()
			defer col.Release()
			result, err := arrowListToVectorArray(col, field, col.Len())
			require.NoError(t, err)
			rows := result.Vectors.GetVectorArray().GetData()
			require.Len(t, rows, 2)
			payloadCap := func(row *schemapb.VectorField) int {
				switch data := row.GetData().(type) {
				case *schemapb.VectorField_FloatVector:
					return cap(data.FloatVector.GetData()) * 4
				case *schemapb.VectorField_Float16Vector:
					return cap(data.Float16Vector)
				case *schemapb.VectorField_Bfloat16Vector:
					return cap(data.Bfloat16Vector)
				case *schemapb.VectorField_Int8Vector:
					return cap(data.Int8Vector)
				case *schemapb.VectorField_BinaryVector:
					return cap(data.BinaryVector)
				}
				t.Fatalf("unexpected vector data %T", row.GetData())
				return 0
			}
			require.Len(t, typeutil.GetVectorArrayElementValidData(rows[0]), 64)
			require.Equal(t, 0, payloadCap(rows[0]))
			require.Len(t, typeutil.GetVectorArrayElementValidData(rows[1]), 64)
			require.Equal(t, 2*tc.byteWidth, payloadCap(rows[1]))
		})
	}
}
