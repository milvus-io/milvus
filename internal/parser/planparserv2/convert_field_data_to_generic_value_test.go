package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

type convertTestcase struct {
	input  map[string]*schemapb.TemplateValue
	expect map[string]*planpb.GenericValue
}

func generateJSONData(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}

func Test_ConvertToGenericValue(t *testing.T) {
	tests := []convertTestcase{
		{
			input: map[string]*schemapb.TemplateValue{
				"bool": {
					Val: &schemapb.TemplateValue_BoolVal{
						BoolVal: false,
					},
				},
			},
			expect: map[string]*planpb.GenericValue{
				"bool": {
					Val: &planpb.GenericValue_BoolVal{
						BoolVal: false,
					},
				},
			},
		},
		{
			input: map[string]*schemapb.TemplateValue{
				"int": {
					Val: &schemapb.TemplateValue_Int64Val{
						Int64Val: 999,
					},
				},
			},
			expect: map[string]*planpb.GenericValue{
				"int": {
					Val: &planpb.GenericValue_Int64Val{
						Int64Val: 999,
					},
				},
			},
		},
		{
			input: map[string]*schemapb.TemplateValue{
				"float": {
					Val: &schemapb.TemplateValue_FloatVal{
						FloatVal: 55.55,
					},
				},
			},
			expect: map[string]*planpb.GenericValue{
				"float": {
					Val: &planpb.GenericValue_FloatVal{
						FloatVal: 55.55,
					},
				},
			},
		},
		{
			input: map[string]*schemapb.TemplateValue{
				"string": {
					Val: &schemapb.TemplateValue_StringVal{
						StringVal: "abc",
					},
				},
			},
			expect: map[string]*planpb.GenericValue{
				"string": {
					Val: &planpb.GenericValue_StringVal{
						StringVal: "abc",
					},
				},
			},
		},
		{
			input: map[string]*schemapb.TemplateValue{
				"array": {
					Val: &schemapb.TemplateValue_ArrayVal{
						ArrayVal: &schemapb.TemplateArrayValue{
							Data: &schemapb.TemplateArrayValue_LongData{
								LongData: &schemapb.LongArray{
									Data: []int64{111, 222, 333},
								},
							},
						},
					},
				},
			},
			expect: map[string]*planpb.GenericValue{
				"array": {
					Val: &planpb.GenericValue_ArrayVal{
						ArrayVal: &planpb.Array{
							Array: []*planpb.GenericValue{
								{
									Val: &planpb.GenericValue_Int64Val{
										Int64Val: 111,
									},
								},
								{
									Val: &planpb.GenericValue_Int64Val{
										Int64Val: 222,
									},
								},
								{
									Val: &planpb.GenericValue_Int64Val{
										Int64Val: 333,
									},
								},
							},
							SameType:    true,
							ElementType: schemapb.DataType_Int64,
						},
					},
				},
			},
		},
		{
			input: map[string]*schemapb.TemplateValue{
				"not_same_array": {
					Val: &schemapb.TemplateValue_ArrayVal{
						ArrayVal: &schemapb.TemplateArrayValue{
							Data: &schemapb.TemplateArrayValue_JsonData{
								JsonData: &schemapb.JSONArray{
									Data: [][]byte{generateJSONData(111), generateJSONData(222.222), generateJSONData(true), generateJSONData("abc")},
								},
							},
						},
					},
				},
			},
			expect: map[string]*planpb.GenericValue{
				"not_same_array": {
					Val: &planpb.GenericValue_ArrayVal{
						ArrayVal: &planpb.Array{
							Array: []*planpb.GenericValue{
								{
									Val: &planpb.GenericValue_Int64Val{
										Int64Val: 111,
									},
								},
								{
									Val: &planpb.GenericValue_FloatVal{
										FloatVal: 222.222,
									},
								},
								{
									Val: &planpb.GenericValue_BoolVal{
										BoolVal: true,
									},
								},
								{
									Val: &planpb.GenericValue_StringVal{
										StringVal: "abc",
									},
								},
							},
							ElementType: schemapb.DataType_JSON,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		output, err := UnmarshalExpressionValues(tt.input)
		assert.Nil(t, err)
		assert.EqualValues(t, tt.expect, output)
	}
}

func generateTemplateValue(dataType schemapb.DataType, data interface{}) *schemapb.TemplateValue {
	switch dataType {
	case schemapb.DataType_Bool:
		return &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_BoolVal{
				BoolVal: data.(bool),
			},
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
		return &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_Int64Val{
				Int64Val: data.(int64),
			},
		}
	case schemapb.DataType_Float, schemapb.DataType_Double:
		return &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_FloatVal{
				FloatVal: data.(float64),
			},
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_StringVal{
				StringVal: data.(string),
			},
		}
	case schemapb.DataType_Array:
		// Handle array data here
		// Assume the inner data is already in an appropriate format.
		// Placeholder for array implementation.
		// You might want to define a recursive approach based on the data structure.
		return &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: data.(*schemapb.TemplateArrayValue),
			},
		}
	default:
		return nil
	}
}

func generateTemplateArrayValue(dataType schemapb.DataType, data interface{}) *schemapb.TemplateArrayValue {
	switch dataType {
	case schemapb.DataType_Bool:
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_BoolData{
				BoolData: &schemapb.BoolArray{
					Data: data.([]bool),
				},
			},
		}
	case schemapb.DataType_Int64:
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_LongData{
				LongData: &schemapb.LongArray{
					Data: data.([]int64),
				},
			},
		}
	case schemapb.DataType_Double:
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_DoubleData{
				DoubleData: &schemapb.DoubleArray{
					Data: data.([]float64),
				},
			},
		}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_StringData{
				StringData: &schemapb.StringArray{
					Data: data.([]string),
				},
			},
		}
	case schemapb.DataType_JSON:
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_JsonData{
				JsonData: &schemapb.JSONArray{
					Data: data.([][]byte),
				},
			},
		}
	case schemapb.DataType_Array:
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_ArrayData{
				ArrayData: &schemapb.TemplateArrayValueArray{
					Data: data.([]*schemapb.TemplateArrayValue),
				},
			},
		}
	default:
		return nil
	}
}
