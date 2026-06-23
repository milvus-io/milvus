package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func structElementCountTestInsertMsg(fieldData *schemapb.FieldData) *msgstream.InsertMsg {
	return &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			CollectionName: "test_collection",
			FieldsData:     []*schemapb.FieldData{fieldData},
		},
	}
}

func structElementCountTestStructData(subFields ...*schemapb.FieldData) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: "test_struct",
		Type:      schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{Fields: subFields},
		},
	}
}

func structElementCountTestScalarArray(fieldName string, rows ...[]int32) *schemapb.FieldData {
	data := make([]*schemapb.ScalarField, 0, len(rows))
	for _, row := range rows {
		data = append(data, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: row}},
		})
	}
	return &schemapb.FieldData{
		FieldName: fieldName,
		Type:      schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{Data: data},
				},
			},
		},
	}
}

func structElementCountTestVectorArray(fieldName string, rows ...[]float32) *schemapb.FieldData {
	data := make([]*schemapb.VectorField, 0, len(rows))
	for _, row := range rows {
		data = append(data, &schemapb.VectorField{
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: row}},
		})
	}
	return &schemapb.FieldData{
		FieldName: fieldName,
		Type:      schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{Data: data},
				},
			},
		},
	}
}

func TestCheckAndFlattenStructFieldDataRejectsMismatchedScalarElementCounts(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{Name: "field2", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestScalarArray("field1", []int32{1, 2}, []int32{3}),
		structElementCountTestScalarArray("field2", []int32{4}, []int32{5}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent struct element count")
	assert.Contains(t, err.Error(), "row 0")
	assert.Contains(t, err.Error(), "field2")
}

func TestCheckAndFlattenStructFieldDataRejectsMismatchedVectorElementCounts(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{
						Name:        "field2",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestScalarArray("field1", []int32{1, 2}),
		structElementCountTestVectorArray("field2", []float32{0.1, 0.2}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent struct element count")
	assert.Contains(t, err.Error(), "row 0")
	assert.Contains(t, err.Error(), "field2")
}

func TestCheckAndFlattenStructFieldDataAllowsMatchingScalarAndVectorElementCounts(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{
						Name:        "field2",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestScalarArray("field1", []int32{1, 2}, []int32{3}),
		structElementCountTestVectorArray("field2", []float32{0.1, 0.2, 0.3, 0.4}, []float32{0.5, 0.6}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	assert.Len(t, insertMsg.FieldsData, 2)
}

func TestCheckAndFlattenStructFieldDataAllowsRawPayloadNamesWithStoredStructSubFieldNames(t *testing.T) {
	const structName = "test_struct"
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: structName,
				Fields: []*schemapb.FieldSchema{
					{
						Name:        typeutil.ConcatStructFieldName(structName, "field1"),
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int32,
					},
					{
						Name:        typeutil.ConcatStructFieldName(structName, "field2"),
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestScalarArray("field1", []int32{1, 2}),
		structElementCountTestVectorArray("field2", []float32{0.1, 0.2, 0.3, 0.4}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	require.Len(t, insertMsg.FieldsData, 2)
	assert.Equal(t, typeutil.ConcatStructFieldName(structName, "field1"), insertMsg.FieldsData[0].GetFieldName())
	assert.Equal(t, typeutil.ConcatStructFieldName(structName, "field2"), insertMsg.FieldsData[1].GetFieldName())
}

func TestCheckAndFlattenStructFieldDataAllowsConsistentlyEmptyRows(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{
						Name:        "field2",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestScalarArray("field1", []int32{}, []int32{}),
		structElementCountTestVectorArray("field2", []float32{}, []float32{}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	assert.Len(t, insertMsg.FieldsData, 2)
}

func TestCheckAndFlattenStructFieldDataAllowsNullableNullRowAndPresentRow(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name:     "test_struct",
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true},
					{
						Name:        "field2",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						Nullable:    true,
						TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	field1 := structElementCountTestScalarArray("field1", []int32{1, 2})
	field1.ValidData = []bool{false, true}
	field2 := structElementCountTestVectorArray("field2", []float32{0.1, 0.2, 0.3, 0.4})
	field2.ValidData = []bool{false, true}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(field1, field2))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	assert.Len(t, insertMsg.FieldsData, 2)
}
