package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
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

func Test_ConvertToGenericValue_NilReturnsError(t *testing.T) {
	_, err := ConvertToGenericValue("myVar", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expression template variable value is nil")
	assert.Contains(t, err.Error(), "myVar")
}

func Test_ConvertToGenericValue_UnknownTypeReturnsError(t *testing.T) {
	// A TemplateValue with no Val set (nil oneof) hits the default case.
	tv := &schemapb.TemplateValue{}
	_, err := ConvertToGenericValue("myVar", tv)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expression elements can only be scalars")
}

func Test_UnmarshalExpressionValues_NilTemplateValueReturnsError(t *testing.T) {
	input := map[string]*schemapb.TemplateValue{
		"key": nil,
	}
	_, err := UnmarshalExpressionValues(input)
	assert.Error(t, err)
}

func Test_ConvertArrayValue_BoolData(t *testing.T) {
	input := map[string]*schemapb.TemplateValue{
		"bools": generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_Bool, []bool{true, false, true})),
	}
	output, err := UnmarshalExpressionValues(input)
	assert.NoError(t, err)
	arr := output["bools"].GetArrayVal()
	assert.True(t, arr.GetSameType())
	assert.Equal(t, schemapb.DataType_Bool, arr.GetElementType())
	assert.Len(t, arr.GetArray(), 3)
	assert.True(t, arr.GetArray()[0].GetBoolVal())
	assert.False(t, arr.GetArray()[1].GetBoolVal())
	assert.True(t, arr.GetArray()[2].GetBoolVal())
}

func Test_ConvertArrayValue_DoubleData(t *testing.T) {
	input := map[string]*schemapb.TemplateValue{
		"doubles": generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_Double, []float64{1.1, 2.2, 3.3})),
	}
	output, err := UnmarshalExpressionValues(input)
	assert.NoError(t, err)
	arr := output["doubles"].GetArrayVal()
	assert.True(t, arr.GetSameType())
	assert.Equal(t, schemapb.DataType_Double, arr.GetElementType())
	assert.Len(t, arr.GetArray(), 3)
	assert.Equal(t, 1.1, arr.GetArray()[0].GetFloatVal())
	assert.Equal(t, 2.2, arr.GetArray()[1].GetFloatVal())
	assert.Equal(t, 3.3, arr.GetArray()[2].GetFloatVal())
}

func Test_ConvertArrayValue_StringData(t *testing.T) {
	input := map[string]*schemapb.TemplateValue{
		"strs": generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_VarChar, []string{"a", "b", "c"})),
	}
	output, err := UnmarshalExpressionValues(input)
	assert.NoError(t, err)
	arr := output["strs"].GetArrayVal()
	assert.True(t, arr.GetSameType())
	assert.Equal(t, schemapb.DataType_VarChar, arr.GetElementType())
	assert.Len(t, arr.GetArray(), 3)
	assert.Equal(t, "a", arr.GetArray()[0].GetStringVal())
	assert.Equal(t, "b", arr.GetArray()[1].GetStringVal())
	assert.Equal(t, "c", arr.GetArray()[2].GetStringVal())
}

func Test_ConvertArrayValue_NestedArrayData(t *testing.T) {
	inner := generateTemplateArrayValue(schemapb.DataType_Int64, []int64{10, 20})
	input := map[string]*schemapb.TemplateValue{
		"nested": generateTemplateValue(schemapb.DataType_Array, generateTemplateArrayValue(schemapb.DataType_Array, []*schemapb.TemplateArrayValue{inner})),
	}
	output, err := UnmarshalExpressionValues(input)
	assert.NoError(t, err)
	outer := output["nested"].GetArrayVal()
	assert.Equal(t, schemapb.DataType_Array, outer.GetElementType())
	assert.Len(t, outer.GetArray(), 1)
	innerArr := outer.GetArray()[0].GetArrayVal()
	assert.Len(t, innerArr.GetArray(), 2)
	assert.Equal(t, int64(10), innerArr.GetArray()[0].GetInt64Val())
	assert.Equal(t, int64(20), innerArr.GetArray()[1].GetInt64Val())
}

func Test_ConvertArrayValue_UnknownTypeReturnsError(t *testing.T) {
	// Build a TemplateArrayValue with no Data set (nil oneof) to hit the default error branch.
	unknownArr := &schemapb.TemplateArrayValue{}
	tv := &schemapb.TemplateValue{
		Val: &schemapb.TemplateValue_ArrayVal{
			ArrayVal: unknownArr,
		},
	}
	_, err := ConvertToGenericValue("myVar", tv)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown template variable value type")
}

func Test_ConvertArrayValue_InvalidJSONReturnsError(t *testing.T) {
	badJSON := &schemapb.TemplateArrayValue{
		Data: &schemapb.TemplateArrayValue_JsonData{
			JsonData: &schemapb.JSONArray{
				Data: [][]byte{[]byte("not valid json {{")},
			},
		},
	}
	tv := &schemapb.TemplateValue{
		Val: &schemapb.TemplateValue_ArrayVal{
			ArrayVal: badJSON,
		},
	}
	_, err := ConvertToGenericValue("myVar", tv)
	assert.Error(t, err)
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
