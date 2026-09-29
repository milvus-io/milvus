package storage

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func testNativeLeaf(dt schemapb.DataType, nullable bool) *schemapb.ScalarField {
	row := emptyNativeScalar(dt)
	if nullable {
		row.ValidData = []bool{true, false, true}
	}
	switch dt {
	case schemapb.DataType_Bool:
		row.GetBoolData().Data = []bool{true, false, true}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		row.GetIntData().Data = []int32{1, 0, 3}
	case schemapb.DataType_Int64:
		row.GetLongData().Data = []int64{1, 0, 3}
	case schemapb.DataType_Float:
		row.GetFloatData().Data = []float32{1, 0, 3}
	case schemapb.DataType_Double:
		row.GetDoubleData().Data = []float64{1, 0, 3}
	case schemapb.DataType_VarChar:
		row.GetStringData().Data = []string{"one", "", "three"}
	}
	return row
}

func testNestedField(dt schemapb.DataType, innerNullable, leafNullable bool) *schemapb.FieldSchema {
	leaf := &schemapb.TypeSchema{Nullable: leafNullable, Kind: &schemapb.TypeSchema_LeafType{LeafType: dt}}
	inner := &schemapb.TypeSchema{Nullable: innerNullable, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}}
	root := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}
	return &schemapb.FieldSchema{FieldID: 101, Name: "nested", DataType: schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: innerNullable, TypeSchema: root}
}

func TestNativeArrayAllLeafTypesAndNullableLayers(t *testing.T) {
	leaves := []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float,
		schemapb.DataType_Double, schemapb.DataType_VarChar,
	}
	for _, dt := range leaves {
		for _, nested := range []bool{false, true} {
			for _, innerNullable := range []bool{false, true} {
				for _, leafNullable := range []bool{false, true} {
					if !nested && innerNullable || !nested && !leafNullable {
						continue
					}
					name := dt.String()
					if nested {
						name += "/nested"
					} else {
						name += "/single"
					}
					if innerNullable {
						name += "/inner-null"
					}
					if leafNullable {
						name += "/leaf-null"
					}
					t.Run(name, func(t *testing.T) {
						var field *schemapb.FieldSchema
						if nested {
							field = testNestedField(dt, innerNullable, leafNullable)
						} else {
							field = &schemapb.FieldSchema{FieldID: 101, Name: "single", DataType: schemapb.DataType_Array,
								ElementType: dt, Nullable: true, ElementNullable: true}
						}
						typ, err := ArrowTypeForField(field)
						require.NoError(t, err)
						listType := typ.(*arrow.ListType)
						if nested {
							require.Equal(t, innerNullable, listType.ElemField().Nullable)
							require.Equal(t, leafNullable, listType.Elem().(*arrow.ListType).ElemField().Nullable)
						} else {
							require.True(t, listType.ElemField().Nullable)
						}
						builder := array.NewBuilder(memory.DefaultAllocator, typ)
						defer builder.Release()
						var rows []*schemapb.ScalarField
						if nested {
							main := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
								ElementType: dt, Data: []*schemapb.ScalarField{testNativeLeaf(dt, leafNullable), emptyNativeScalar(dt)},
							}}}
							if innerNullable {
								main.GetArrayData().Data = append(main.GetArrayData().Data, emptyNativeScalar(dt))
								main.ValidData = []bool{true, true, false}
							}
							rows = []*schemapb.ScalarField{main,
								{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: dt}}}, nil}
						} else {
							rows = []*schemapb.ScalarField{testNativeLeaf(dt, true), emptyNativeScalar(dt), nil}
						}
						for _, row := range rows {
							require.NoError(t, SerializeNativeArrayRow(builder, row, field))
						}
						arr := builder.NewArray().(*array.List)
						defer arr.Release()
						rebuiltBuilder := array.NewBuilder(memory.DefaultAllocator, typ)
						defer rebuiltBuilder.Release()
						for i, want := range rows {
							got, err := DeserializeNativeArrayRow(arr, i, field)
							require.NoError(t, err)
							require.True(t, proto.Equal(want, got), "row %d: got %v, want %v", i, got, want)
							require.NoError(t, SerializeNativeArrayRow(rebuiltBuilder, got, field))
							if want != nil {
								wantBytes, err := proto.Marshal(want)
								require.NoError(t, err)
								gotBytes, err := proto.Marshal(got)
								require.NoError(t, err)
								require.Equal(t, wantBytes, gotBytes)
							}
						}
						rebuilt := rebuiltBuilder.NewArray()
						defer rebuilt.Release()
						require.True(t, array.Equal(arr, rebuilt))
						start, end := arr.ValueOffsets(2)
						require.Equal(t, start, end, "null row must own no children")
						if nested && innerNullable {
							inner := arr.ListValues().(*array.List)
							start, end = inner.ValueOffsets(2)
							require.Equal(t, start, end, "null inner list must own no leaves")
						}
					})
				}
			}
		}
	}
}

func TestNativeArrayRejectsInvalidDenseValidity(t *testing.T) {
	field := &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true}
	typ, err := ArrowTypeForField(field)
	require.NoError(t, err)
	b := array.NewBuilder(memory.DefaultAllocator, typ)
	defer b.Release()
	require.Error(t, SerializeNativeArrayRow(b, &schemapb.ScalarField{
		Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}},
		ValidData: []bool{true},
	}, field))
	for _, tc := range []struct {
		name          string
		innerNullable bool
		leafNullable  bool
		outerValid    []bool
		leafValid     []bool
	}{
		{"nonnullable inner carries bitmap", false, false, []bool{true}, nil},
		{"nonnullable leaf carries bitmap", true, false, []bool{true}, []bool{true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := testNestedField(schemapb.DataType_Int64, tc.innerNullable, tc.leafNullable)
			typ, err := ArrowTypeForField(field)
			require.NoError(t, err)
			builder := array.NewBuilder(memory.DefaultAllocator, typ)
			defer builder.Release()
			row := &schemapb.ScalarField{ValidData: tc.outerValid,
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64,
					Data: []*schemapb.ScalarField{{ValidData: tc.leafValid,
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}}}}
			require.Error(t, SerializeNativeArrayRow(builder, row, field))
		})
	}
}

func TestNativeArrayArrowSchemaNullableAndFieldID(t *testing.T) {
	field := testNestedField(schemapb.DataType_Int8, false, true)
	field.Nullable = false
	field.TypeSchema.Nullable = false
	schema, err := ConvertToArrowSchema(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}, true)
	require.NoError(t, err)
	got := schema.Field(0)
	require.False(t, got.Nullable)
	require.Equal(t, "101", got.Name)
	id, ok := got.Metadata.GetValue(packed.ArrowFieldIdMetadataKey)
	require.True(t, ok)
	require.Equal(t, "101", id)
	require.False(t, got.Type.(*arrow.ListType).ElemField().Nullable)
	require.True(t, got.Type.(*arrow.ListType).Elem().(*arrow.ListType).ElemField().Nullable)
}

func TestNativeArrayRecordBuilderCopyAndSlicedSize(t *testing.T) {
	field := testNestedField(schemapb.DataType_Int64, true, true)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	arrowSchema, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	row := &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
		ElementType: schemapb.DataType_Int64,
		Data: []*schemapb.ScalarField{
			{ValidData: []bool{true, false, true}, Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 0, 3}}}},
			emptyNativeScalar(schemapb.DataType_Int64),
		},
	}}}
	require.NoError(t, SerializeNativeArrayRow(b.Field(0), row, field))
	require.NoError(t, SerializeNativeArrayRow(b.Field(0), (*schemapb.ScalarField)(nil), field))
	require.NoError(t, SerializeNativeArrayRow(b.Field(0), &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64}}}, field))
	original := b.NewRecord()
	rec := NewSimpleArrowRecord(original, map[FieldID]int{field.GetFieldID(): 0})
	defer rec.Release()
	insert, err := RecordToInsertData(rec, schema, nil)
	require.NoError(t, err)
	arrayData := insert.Data[field.FieldID].(*ArrayFieldData)
	require.Equal(t, []bool{true, false, true}, arrayData.ValidData)
	require.True(t, proto.Equal(row, arrayData.GetRow(0).(*schemapb.ScalarField)))
	require.Nil(t, arrayData.GetRow(1))
	require.Nil(t, arrayData.Data[1])
	require.True(t, proto.Equal(
		&schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64}}},
		arrayData.GetRow(2).(*schemapb.ScalarField)))
	copyBuilder := NewRecordBuilder(schema)
	defer copyBuilder.Release()
	require.NoError(t, copyBuilder.Append(rec, 0, 3))
	copyRec := copyBuilder.Build()
	defer copyRec.Release()
	for i := 0; i < 3; i++ {
		got, err := DeserializeNativeArrayRow(copyRec.Column(field.GetFieldID()), i, field)
		require.NoError(t, err)
		if i == 0 {
			require.True(t, proto.Equal(row, got))
		} else if i == 1 {
			require.Nil(t, got)
		}
	}
	outer := copyRec.Column(field.GetFieldID()).(*array.List)
	start, end := outer.ValueOffsets(1)
	require.Equal(t, start, end)
	inner := outer.ListValues().(*array.List)
	start, end = inner.ValueOffsets(1)
	require.Equal(t, start, end)
	sliced := array.NewSlice(outer, 1, 3)
	defer sliced.Release()
	require.Less(t, ActualSizeInBytes(sliced.Data()), ActualSizeInBytes(outer.Data()))
	// Mix compaction appends only surviving row ranges when deletes are present.
	require.NoError(t, copyBuilder.Append(rec, 0, 1))
	require.NoError(t, copyBuilder.Append(rec, 2, 3))
	withoutDeleted := copyBuilder.Build()
	defer withoutDeleted.Release()
	require.Equal(t, 2, withoutDeleted.Len())
	got, err := DeserializeNativeArrayRow(withoutDeleted.Column(field.GetFieldID()), 0, field)
	require.NoError(t, err)
	require.True(t, proto.Equal(row, got))
}

func TestNativeArrayRecordBuilderCopiesNullableVectorElements(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 102, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	arrowSchema, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	list := b.Field(0).(*array.ListBuilder)
	list.Append(true)
	child := list.ValueBuilder().(*array.BinaryBuilder)
	child.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
	child.AppendNull()
	child.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
	list.AppendNull()
	original := b.NewRecord()
	rec := NewSimpleArrowRecord(original, map[FieldID]int{field.FieldID: 0})
	defer rec.Release()
	copyBuilder := NewRecordBuilder(schema)
	defer copyBuilder.Release()
	require.NoError(t, copyBuilder.Append(rec, 0, 2))
	copyRec := copyBuilder.Build()
	defer copyRec.Release()
	copyList := copyRec.Column(field.FieldID).(*array.List)
	require.True(t, array.Equal(original.Column(0), copyList))
	start, end := copyList.ValueOffsets(1)
	require.Equal(t, start, end)
}

func TestNativeArrayClusteringValueSerde(t *testing.T) {
	field := testNestedField(schemapb.DataType_Int64, true, true)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{Name: "struct", Fields: []*schemapb.FieldSchema{field}}},
	}
	row := &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
		ElementType: schemapb.DataType_Int64,
		Data: []*schemapb.ScalarField{
			{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{8, 0}}}},
			emptyNativeScalar(schemapb.DataType_Int64),
		},
	}}}
	values := []*Value{{Value: map[FieldID]any{
		common.RowIDField: int64(1), common.TimeStampField: int64(10), 100: int64(1000), field.FieldID: row,
	}}}
	rec, err := ValueSerializer(values, schema)
	require.NoError(t, err)
	defer rec.Release()
	got := make([]*Value, 1)
	require.NoError(t, ValueDeserializerWithSchema(rec, got, schema, true))
	require.True(t, proto.Equal(row, got[0].Value.(map[FieldID]interface{})[field.FieldID].(*schemapb.ScalarField)))
}
