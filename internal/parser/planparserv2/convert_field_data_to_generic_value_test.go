package planparserv2

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_ConvertFieldDataToGenericValue(t *testing.T) {
	toJSONBytes := func(v any) []byte {
		jsonBytes, err := json.Marshal(v)
		assert.Nil(t, err)
		return jsonBytes
	}
	scalarDatas := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int32,
			FieldName: "int",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{1, 2, 3, 4, 5, 6},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "int64",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3, 4, 5, 6},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Bool,
			FieldName: "bool",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{true, false, true, false},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Float,
			FieldName: "float",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: []float32{1.1, 2.2, 3.3, 4.4, 5.6},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Double,
			FieldName: "float",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{1.1, 2.2, 3.3, 4.4, 5.6},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_String,
			FieldName: "string",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"abc", "def", "ghi"},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_String,
			FieldName: "string",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"abc", "def", "ghi"},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Array,
			FieldName: "int64_array",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int64,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_LongData{
										LongData: &schemapb.LongArray{
											Data: []int64{1, 2, 3, 4, 5, 6},
										},
									},
								},
								{
									Data: &schemapb.ScalarField_LongData{
										LongData: &schemapb.LongArray{
											Data: []int64{2, 3, 4, 5, 6, 7},
										},
									},
								},
								{
									Data: &schemapb.ScalarField_LongData{
										LongData: &schemapb.LongArray{
											Data: []int64{3, 4, 5, 6, 7, 8},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Array,
			FieldName: "string_array",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_String,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_StringData{
										StringData: &schemapb.StringArray{
											Data: []string{"abc", "def", "ghi"},
										},
									},
								},
								{
									Data: &schemapb.ScalarField_StringData{
										StringData: &schemapb.StringArray{
											Data: []string{"jkl", "opq", "rst"},
										},
									},
								},
								{
									Data: &schemapb.ScalarField_StringData{
										StringData: &schemapb.StringArray{
											Data: []string{"uvw", "xyz"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Array,
			FieldName: "json_array",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_JSON,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_JsonData{
										JsonData: &schemapb.JSONArray{
											Data: [][]byte{toJSONBytes("abc"), toJSONBytes(1), toJSONBytes(2.2)},
										},
									},
								},
								{
									Data: &schemapb.ScalarField_JsonData{
										JsonData: &schemapb.JSONArray{
											Data: [][]byte{toJSONBytes("def"), toJSONBytes(100), toJSONBytes(22.2)},
										},
									},
								},
								{
									Data: &schemapb.ScalarField_JsonData{
										JsonData: &schemapb.JSONArray{
											Data: [][]byte{toJSONBytes("100"), toJSONBytes(100), toJSONBytes(99.99)},
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

	for _, data := range scalarDatas {
		values, err := ConvertFieldDataToGenericValue(data.GetScalars(), data.GetType())
		assert.Nil(t, err)
		fmt.Println(values)
	}
}

func generateExpressionFieldData(name string, dataType schemapb.DataType, data []interface{}) *schemapb.FieldData {
	switch dataType {
	case schemapb.DataType_Bool:
		boolData := make([]bool, len(data))
		for i, v := range data {
			boolData[i] = v.(bool)
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: boolData,
						},
					},
				},
			},
		}

	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		intData := make([]int32, len(data))
		for i, v := range data {
			intData[i] = v.(int32)
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: intData,
						},
					},
				},
			},
		}

	case schemapb.DataType_Int64:
		int64Data := make([]int64, len(data))
		for i, v := range data {
			int64Data[i] = v.(int64)
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: int64Data,
						},
					},
				},
			},
		}

	case schemapb.DataType_Float:
		floatData := make([]float32, len(data))
		for i, v := range data {
			floatData[i] = v.(float32)
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: floatData,
						},
					},
				},
			},
		}

	case schemapb.DataType_Double:
		doubleData := make([]float64, len(data))
		for i, v := range data {
			doubleData[i] = v.(float64)
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: doubleData,
						},
					},
				},
			},
		}

	case schemapb.DataType_String, schemapb.DataType_VarChar:
		stringData := make([]string, len(data))
		for i, v := range data {
			stringData[i] = v.(string)
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: stringData,
						},
					},
				},
			},
		}

	case schemapb.DataType_Array:
		// Handle array data here
		// Assume the inner data is already in an appropriate format.
		// Placeholder for array implementation.
		// You might want to define a recursive approach based on the data structure.
		arrayData := make([]*schemapb.ScalarField, len(data))
		elementType := schemapb.DataType_None
		for i, v := range data {
			array := v.(*schemapb.FieldData)
			arrayData[i] = array.GetScalars()
			elementType = array.GetType()
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data:        arrayData,
							ElementType: elementType,
						},
					},
				},
			},
		}

	case schemapb.DataType_JSON:
		// Handle JSON data here
		// Assuming the JSON data is encoded as strings or as map[string]interface{}
		jsonData := make([][]byte, len(data))
		for i, v := range data {
			jsonData[i] = v.([]byte)
		}
		return &schemapb.FieldData{
			FieldName: name,
			Type:      dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: jsonData,
						},
					},
				},
			},
		}

	default:
		return nil
	}
}
