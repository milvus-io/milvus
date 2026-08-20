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

func structElementCountTestNestedIntArray(fieldName string, rows ...[][]int32) *schemapb.FieldData {
	data := make([]*schemapb.ScalarField, 0, len(rows))
	for _, row := range rows {
		elements := make([]*schemapb.ScalarField, 0, len(row))
		for _, element := range row {
			elements = append(elements, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: element}},
			})
		}
		data = append(data, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Int32,
					Data:        elements,
				},
			},
		})
	}
	return &schemapb.FieldData{
		FieldName: fieldName,
		Type:      schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_Array,
						Data:        data,
					},
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

type nullableIntArrayRow struct {
	values    []int32
	validData []bool
}

func structElementCountTestNullableScalarArray(fieldName string, rows ...nullableIntArrayRow) *schemapb.FieldData {
	data := make([]*schemapb.ScalarField, 0, len(rows))
	for _, row := range rows {
		data = append(data, &schemapb.ScalarField{
			Data:      &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: row.values}},
			ValidData: row.validData,
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

type nullableFloatVectorArrayRow struct {
	values    []float32
	validData []bool
}

func structElementCountTestNullableVectorArray(fieldName string, rows ...nullableFloatVectorArrayRow) *schemapb.FieldData {
	data := make([]*schemapb.VectorField, 0, len(rows))
	for _, row := range rows {
		data = append(data, &schemapb.VectorField{
			Data:      &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: row.values}},
			ValidData: row.validData,
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

func TestCheckAndFlattenStructFieldDataAllowsMatchingRecursiveArrayElementCounts(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{
						Name:        "nested",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Array,
						TypeSchema: &schemapb.TypeSchema{
							Kind: &schemapb.TypeSchema_ArrayElement{
								ArrayElement: &schemapb.TypeSchema{
									Kind: &schemapb.TypeSchema_ArrayElement{
										ArrayElement: &schemapb.TypeSchema{
											Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int32},
										},
									},
								},
							},
						},
					},
					{Name: "scalar", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestNestedIntArray("nested", [][]int32{{1, 2}, {3}}, [][]int32{{4, 5, 6}}),
		structElementCountTestScalarArray("scalar", []int32{10, 20}, []int32{30}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	assert.Len(t, insertMsg.FieldsData, 2)
}

func TestCheckAndFlattenStructFieldDataAllowsElementNullableSubFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, ElementNullable: true},
					{
						Name:            "field2",
						DataType:        schemapb.DataType_ArrayOfVector,
						ElementType:     schemapb.DataType_FloatVector,
						ElementNullable: true,
						TypeParams:      []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestNullableScalarArray("field1", nullableIntArrayRow{
			values:    []int32{1, 3},
			validData: []bool{true, false, true},
		}),
		structElementCountTestNullableVectorArray("field2", nullableFloatVectorArrayRow{
			values:    []float32{0.1, 0.2, 0.3, 0.4},
			validData: []bool{true, false, true},
		}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	require.Len(t, insertMsg.FieldsData, 2)
	assert.Len(t, insertMsg.FieldsData[0].GetScalars().GetArrayData().GetData(), 1)
	assert.Len(t, insertMsg.FieldsData[1].GetVectors().GetVectorArray().GetData(), 1)
}

func TestCheckAndFlattenStructFieldDataRejectsMismatchedElementNullableRowCounts(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, ElementNullable: true},
					{
						Name:            "field2",
						DataType:        schemapb.DataType_ArrayOfVector,
						ElementType:     schemapb.DataType_FloatVector,
						ElementNullable: true,
						TypeParams:      []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestNullableScalarArray("field1",
			nullableIntArrayRow{values: []int32{1}, validData: []bool{true}},
			nullableIntArrayRow{values: []int32{2}, validData: []bool{true}},
		),
		structElementCountTestNullableVectorArray("field2", nullableFloatVectorArrayRow{
			values:    []float32{0.1, 0.2},
			validData: []bool{true},
		}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent array length")
	assert.Contains(t, err.Error(), "field2")
}

func TestCheckAndFlattenStructFieldDataRejectsMismatchedElementNullableElementCounts(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, ElementNullable: true},
					{
						Name:            "field2",
						DataType:        schemapb.DataType_ArrayOfVector,
						ElementType:     schemapb.DataType_FloatVector,
						ElementNullable: true,
						TypeParams:      []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
		structElementCountTestNullableScalarArray("field1", nullableIntArrayRow{
			values:    []int32{1},
			validData: []bool{true, false},
		}),
		structElementCountTestNullableVectorArray("field2", nullableFloatVectorArrayRow{
			values:    []float32{0.1, 0.2, 0.3, 0.4},
			validData: []bool{true, false, true},
		}),
	))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent struct element count")
	assert.Contains(t, err.Error(), "row 0")
	assert.Contains(t, err.Error(), "field2")
}

func TestCheckAndFlattenStructFieldDataAllowsNullableStructWithElementNullableSubFields(t *testing.T) {
	validData := []bool{false, true}
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name:     "test_struct",
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, ElementNullable: true, Nullable: true},
					{
						Name:            "field2",
						DataType:        schemapb.DataType_ArrayOfVector,
						ElementType:     schemapb.DataType_FloatVector,
						ElementNullable: true,
						Nullable:        true,
						TypeParams:      []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	field1 := structElementCountTestNullableScalarArray("field1", nullableIntArrayRow{
		values:    []int32{1},
		validData: []bool{true, false},
	})
	typeutil.SetFieldDataValidData(field1, validData)
	field2 := structElementCountTestNullableVectorArray("field2", nullableFloatVectorArrayRow{
		values:    []float32{0.1, 0.2},
		validData: []bool{true, false},
	})
	typeutil.SetFieldDataValidData(field2, validData)
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(field1, field2))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	require.Len(t, insertMsg.FieldsData, 2)
	assert.Equal(t, validData, insertMsg.FieldsData[0].GetScalars().GetValidData())
	assert.Equal(t, validData, insertMsg.FieldsData[1].GetVectors().GetValidData())
	assert.Len(t, insertMsg.FieldsData[0].GetScalars().GetArrayData().GetData(), 1)
	assert.Len(t, insertMsg.FieldsData[1].GetVectors().GetVectorArray().GetData(), 1)
}

func TestStructElementNullableFlattenFillAndValidate(t *testing.T) {
	const structName = "test_struct"
	scalarFieldName := typeutil.ConcatStructFieldName(structName, "field1")
	vectorFieldName := typeutil.ConcatStructFieldName(structName, "field2")
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: structName,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:         101,
						Name:            scalarFieldName,
						DataType:        schemapb.DataType_Array,
						ElementType:     schemapb.DataType_Int32,
						ElementNullable: true,
					},
					{
						FieldID:         102,
						Name:            vectorFieldName,
						DataType:        schemapb.DataType_ArrayOfVector,
						ElementType:     schemapb.DataType_FloatVector,
						ElementNullable: true,
						TypeParams:      []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
					},
				},
			},
		},
	}
	scalarField := structElementCountTestNullableScalarArray("field1", nullableIntArrayRow{
		values:    []int32{1, 3},
		validData: []bool{true, false, true},
	})
	vectorField := structElementCountTestNullableVectorArray("field2", nullableFloatVectorArrayRow{
		values:    []float32{0.1, 0.2, 0.3, 0.4},
		validData: []bool{true, false, true},
	})
	vectorField.GetVectors().GetVectorArray().Dim = 2
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(scalarField, vectorField))

	require.NoError(t, checkAndFlattenStructFieldData(schema, insertMsg))
	require.Len(t, insertMsg.FieldsData, 2)
	assert.Equal(t, scalarFieldName, insertMsg.FieldsData[0].GetFieldName())
	assert.Equal(t, vectorFieldName, insertMsg.FieldsData[1].GetFieldName())

	schemaInfo := mustNewSchemaInfo(schema)
	require.NoError(t, fillFieldPropertiesOnly(insertMsg.FieldsData, schemaInfo))
	require.NoError(t, newValidateUtil().Validate(insertMsg.FieldsData, schemaInfo.SchemaHelper, 1))

	scalarArray := insertMsg.FieldsData[0].GetScalars().GetArrayData()
	assert.Equal(t, int64(101), insertMsg.FieldsData[0].GetFieldId())
	assert.Equal(t, schemapb.DataType_Int32, scalarArray.GetElementType())
	assert.Equal(t, []int32{1, 0, 3}, scalarArray.GetData()[0].GetIntData().GetData())
	assert.Equal(t, []bool{true, false, true}, scalarArray.GetData()[0].GetValidData())

	vectorArray := insertMsg.FieldsData[1].GetVectors().GetVectorArray()
	assert.Equal(t, int64(102), insertMsg.FieldsData[1].GetFieldId())
	assert.Equal(t, schemapb.DataType_FloatVector, vectorArray.GetElementType())
	assert.Equal(t, []float32{0.1, 0.2, 0.3, 0.4}, vectorArray.GetData()[0].GetFloatVector().GetData())
	assert.Equal(t, []bool{true, false, true}, vectorArray.GetData()[0].GetValidData())
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
	typeutil.SetFieldDataValidData(field1, []bool{false, true})
	field2 := structElementCountTestVectorArray("field2", []float32{0.1, 0.2, 0.3, 0.4})
	typeutil.SetFieldDataValidData(field2, []bool{false, true})
	insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(field1, field2))

	err := checkAndFlattenStructFieldData(schema, insertMsg)

	require.NoError(t, err)
	assert.Len(t, insertMsg.FieldsData, 2)
}
