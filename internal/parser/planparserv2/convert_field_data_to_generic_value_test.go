package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

type convertTestcase struct {
	input  map[string]*schemapb.TemplateValue
	expect map[string]*planpb.GenericValue
}

func Test_ConvertToGenericValue(t *testing.T) {
	tests := []convertTestcase{
		{
			input: map[string]*schemapb.TemplateValue{
				"bool": {
					Type: schemapb.DataType_Bool,
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
					Type: schemapb.DataType_Int64,
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
					Type: schemapb.DataType_Float,
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
					Type: schemapb.DataType_VarChar,
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
					Type: schemapb.DataType_Array,
					Val: &schemapb.TemplateValue_ArrayVal{
						ArrayVal: &schemapb.TemplateArrayValue{
							Array: []*schemapb.TemplateValue{
								{
									Type: schemapb.DataType_Int64,
									Val: &schemapb.TemplateValue_Int64Val{
										Int64Val: 111,
									},
								},
								{
									Type: schemapb.DataType_Int64,
									Val: &schemapb.TemplateValue_Int64Val{
										Int64Val: 222,
									},
								},
								{
									Type: schemapb.DataType_Int64,
									Val: &schemapb.TemplateValue_Int64Val{
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
					Type: schemapb.DataType_Array,
					Val: &schemapb.TemplateValue_ArrayVal{
						ArrayVal: &schemapb.TemplateArrayValue{
							Array: []*schemapb.TemplateValue{
								{
									Type: schemapb.DataType_Int64,
									Val: &schemapb.TemplateValue_Int64Val{
										Int64Val: 111,
									},
								},
								{
									Type: schemapb.DataType_Float,
									Val: &schemapb.TemplateValue_FloatVal{
										FloatVal: 222.222,
									},
								},
								{
									Type: schemapb.DataType_Bool,
									Val: &schemapb.TemplateValue_BoolVal{
										BoolVal: true,
									},
								},
								{
									Type: schemapb.DataType_VarChar,
									Val: &schemapb.TemplateValue_StringVal{
										StringVal: "abc",
									},
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

func generateExpressionFieldData(dataType schemapb.DataType, data interface{}) *schemapb.TemplateValue {
	switch dataType {
	case schemapb.DataType_Bool:
		return &schemapb.TemplateValue{
			Type: dataType,
			Val: &schemapb.TemplateValue_BoolVal{
				BoolVal: data.(bool),
			},
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
		return &schemapb.TemplateValue{
			Type: dataType,
			Val: &schemapb.TemplateValue_Int64Val{
				Int64Val: data.(int64),
			},
		}
	case schemapb.DataType_Float, schemapb.DataType_Double:
		return &schemapb.TemplateValue{
			Type: dataType,
			Val: &schemapb.TemplateValue_FloatVal{
				FloatVal: data.(float64),
			},
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return &schemapb.TemplateValue{
			Type: dataType,
			Val: &schemapb.TemplateValue_StringVal{
				StringVal: data.(string),
			},
		}
	case schemapb.DataType_Array:
		// Handle array data here
		// Assume the inner data is already in an appropriate format.
		// Placeholder for array implementation.
		// You might want to define a recursive approach based on the data structure.
		value := data.([]interface{})
		arrayData := make([]*schemapb.TemplateValue, len(value))
		elementType := schemapb.DataType_None
		sameType := true
		for i, v := range value {
			element := v.(*schemapb.TemplateValue)
			arrayData[i] = element
			if elementType == schemapb.DataType_None {
				elementType = element.GetType()
			} else if elementType != element.GetType() {
				sameType = false
				elementType = schemapb.DataType_JSON
			}
		}
		return &schemapb.TemplateValue{
			Type: dataType,
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Array:       arrayData,
					ElementType: elementType,
					SameType:    sameType,
				},
			},
		}

	default:
		return nil
	}
}
