package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestValidateStructArrayField_NullablePropagation(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	t.Run("nullable struct propagates to sub-fields", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:     "my_struct",
			Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
				},
				{
					Name:        "sub_b",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}, {Key: "max_length", Value: "256"}},
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		require.NoError(t, err)

		// Verify nullable was propagated
		for _, subField := range structField.Fields {
			assert.True(t, subField.GetNullable(), "sub-field %s should be nullable after propagation", subField.Name)
		}
	})

	t.Run("non-nullable struct rejects nullable sub-fields", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:     "my_struct",
			Nullable: false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					Nullable:    true, // This should be rejected
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nullable individually")
	})

	t.Run("non-nullable struct with non-nullable sub-fields passes", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:     "my_struct",
			Nullable: false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		assert.NoError(t, err)
	})
}

func TestCheckAndFlattenStructFieldData_ValidDataCopied(t *testing.T) {
	structName := "my_struct"
	subFieldName := "sub_a"
	transformedName := typeutil.ConcatStructFieldName(structName, subFieldName)

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID:  200,
				Name:     structName,
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     201,
						Name:        transformedName,
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int32,
						Nullable:    true,
					},
				},
			},
		},
	}

	validData := []bool{true, false, true}
	insertMsg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "pk",
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
							},
						},
					},
				},
				{
					FieldName: structName,
					Type:      schemapb.DataType_ArrayOfStruct,
					Field: &schemapb.FieldData_StructArrays{
						StructArrays: &schemapb.StructArrayField{
							Fields: []*schemapb.FieldData{
								{
									FieldName: subFieldName,
									FieldId:   201,
									Type:      schemapb.DataType_Array,
									ValidData: validData,
									Field: &schemapb.FieldData_Scalars{
										Scalars: &schemapb.ScalarField{
											Data: &schemapb.ScalarField_ArrayData{
												ArrayData: &schemapb.ArrayArray{
													Data: []*schemapb.ScalarField{
														{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}}},
														{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{}}}},
														{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3}}}},
													},
													ElementType: schemapb.DataType_Int32,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := checkAndFlattenStructFieldData(schema, insertMsg)
	require.NoError(t, err)

	// Find the flattened sub-field and verify ValidData is preserved
	found := false
	for _, fd := range insertMsg.GetFieldsData() {
		if fd.FieldName == transformedName {
			found = true
			assert.Equal(t, validData, fd.GetValidData(), "ValidData should be preserved in flattened sub-field")
			break
		}
	}
	assert.True(t, found, "flattened sub-field should exist")
}

func TestCheckAndFlattenStructFieldData_NullableStructCanBeOmitted(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID:  200,
				Name:     "nullable_struct",
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     201,
						Name:        "nullable_struct[sub_a]",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int32,
						Nullable:    true,
					},
				},
			},
		},
	}

	// Insert without providing the nullable struct
	insertMsg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "pk",
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2}},
							},
						},
					},
				},
			},
		},
	}

	err := checkAndFlattenStructFieldData(schema, insertMsg)
	assert.NoError(t, err)
}

func TestCheckAndFlattenStructFieldData_RequiredStructMustBePresent(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID:  200,
				Name:     "required_struct",
				Nullable: false,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     201,
						Name:        "required_struct[sub_a]",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int32,
					},
				},
			},
		},
	}

	// Insert without providing the required struct
	insertMsg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "pk",
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2}},
							},
						},
					},
				},
			},
		},
	}

	err := checkAndFlattenStructFieldData(schema, insertMsg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required struct array field")
}

func TestCheckAndFlattenStructFieldData_AllNullWithInitializedVectorsOneof(t *testing.T) {
	// Regression: pymilvus may pre-initialize the Vectors oneof (e.g. setting vectors.dim)
	// even when all rows are null, making Field non-nil with type *FieldData_Vectors.
	// The flatten logic must detect this as "no actual data" and skip.
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID:  200,
				Name:     "nullable_struct",
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     201,
						Name:        "nullable_struct[tag]",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_VarChar,
						Nullable:    true,
					},
					{
						FieldID:     202,
						Name:        "nullable_struct[vec]",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						Nullable:    true,
					},
				},
			},
		},
	}

	insertMsg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "pk",
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2}},
							},
						},
					},
				},
				{
					FieldName: "nullable_struct",
					Type:      schemapb.DataType_ArrayOfStruct,
					Field: &schemapb.FieldData_StructArrays{
						StructArrays: &schemapb.StructArrayField{
							Fields: []*schemapb.FieldData{
								{
									FieldName: "tag",
									Type:      schemapb.DataType_Array,
									ValidData: []bool{false, false},
									// Field is nil — truly no data
								},
								{
									FieldName: "vec",
									Type:      schemapb.DataType_ArrayOfVector,
									ValidData: []bool{false, false},
									// Vectors oneof initialized but no actual data inside
									Field: &schemapb.FieldData_Vectors{
										Vectors: &schemapb.VectorField{
											Dim: 4,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := checkAndFlattenStructFieldData(schema, insertMsg)
	assert.NoError(t, err)
}

func TestFillWithNullValue_ArrayOfVector(t *testing.T) {
	field := &schemapb.FieldData{
		FieldName: "vec_array",
		Type:      schemapb.DataType_ArrayOfVector,
		ValidData: []bool{true, false, true},
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 4,
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Data: []*schemapb.VectorField{
							{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}},
							{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{5, 6, 7, 8}}}},
						},
						Dim:         4,
						ElementType: schemapb.DataType_FloatVector,
					},
				},
			},
		},
	}

	fieldSchema := &schemapb.FieldSchema{
		Name:        "vec_array",
		DataType:    schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector,
		Nullable:    true,
	}
	err := FillWithNullValue(field, fieldSchema, 3)
	assert.NoError(t, err)

	vectorArray := field.GetVectors().GetVectorArray()
	require.NotNil(t, vectorArray)
	// ArrayOfVector stays in compact format (like other vector types).
	// Data length unchanged — only valid rows are stored.
	assert.Equal(t, 2, len(vectorArray.Data))
}

func TestSubFieldHasData(t *testing.T) {
	t.Run("scalars with data", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1}},
					},
				},
			},
		}
		assert.True(t, subFieldHasData(fd))
	})

	t.Run("scalars without data", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		assert.False(t, subFieldHasData(fd))
	})

	t.Run("vectors with data", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}},
					},
				},
			},
		}
		assert.True(t, subFieldHasData(fd))
	})

	t.Run("vectors initialized but no data (pymilvus dim-only case)", func(t *testing.T) {
		fd := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 4,
					// Data is nil - only dim was set
				},
			},
		}
		assert.False(t, subFieldHasData(fd))
	})

	t.Run("nil field", func(t *testing.T) {
		fd := &schemapb.FieldData{}
		assert.False(t, subFieldHasData(fd))
	})
}

func TestCheckAndFlattenStructFieldData_InconsistentSubFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID:  200,
				Name:     "my_struct",
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 201, Name: "my_struct[a]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true},
					{FieldID: 202, Name: "my_struct[b]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true},
				},
			},
		},
	}

	// Sub-field "a" has data, sub-field "b" does not
	insertMsg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "pk",
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
						},
					},
				},
				{
					FieldName: "my_struct",
					Type:      schemapb.DataType_ArrayOfStruct,
					Field: &schemapb.FieldData_StructArrays{
						StructArrays: &schemapb.StructArrayField{
							Fields: []*schemapb.FieldData{
								{
									FieldName: "a",
									Type:      schemapb.DataType_Array,
									Field: &schemapb.FieldData_Scalars{
										Scalars: &schemapb.ScalarField{
											Data: &schemapb.ScalarField_ArrayData{
												ArrayData: &schemapb.ArrayArray{
													Data:        []*schemapb.ScalarField{{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}}}},
													ElementType: schemapb.DataType_Int32,
												},
											},
										},
									},
								},
								{
									FieldName: "b",
									Type:      schemapb.DataType_Array,
									// No data
								},
							},
						},
					},
				},
			},
		},
	}

	err := checkAndFlattenStructFieldData(schema, insertMsg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent sub-field data")
}
