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
	field := &schemapb.FieldSchema{FieldID: 101, Name: "nested", DataType: schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
		TypeSchema: &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}}
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
	field := &schemapb.FieldSchema{FieldID: 102, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, ElementNullable: true, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}}
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
	field := &schemapb.FieldSchema{FieldID: 103, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, ElementNullable: true, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}}
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
