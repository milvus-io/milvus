package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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
									Field: &schemapb.FieldData_Scalars{
										Scalars: &schemapb.ScalarField{
											ValidData: validData,
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

	found := false
	for _, fd := range insertMsg.GetFieldsData() {
		if fd.FieldName == transformedName {
			found = true
			assert.Equal(t, validData, fd.GetValidData(), "flattened sub-field should carry legacy ValidData")
			assert.Equal(t, validData, fd.GetScalars().GetValidData(), "flattened sub-field should carry field-specific ValidData")
			break
		}
	}
	assert.True(t, found, "flattened sub-field should exist")
}
