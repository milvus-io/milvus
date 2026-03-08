package planparserv2

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func convertArrayValue(templateName string, templateValue *schemapb.TemplateArrayValue) (*planpb.GenericValue, error) {
	var arrayValues []*planpb.GenericValue
	var elementType schemapb.DataType
	switch templateValue.GetData().(type) {
	case *schemapb.TemplateArrayValue_BoolData:
		elements := templateValue.GetBoolData().GetData()
		arrayValues = make([]*planpb.GenericValue, len(elements))
		for i, element := range elements {
			arrayValues[i] = &planpb.GenericValue{
				Val: &planpb.GenericValue_BoolVal{
					BoolVal: element,
				},
			}
		}
		elementType = schemapb.DataType_Bool
	case *schemapb.TemplateArrayValue_LongData:
		elements := templateValue.GetLongData().GetData()
		arrayValues = make([]*planpb.GenericValue, len(elements))
		for i, element := range elements {
			arrayValues[i] = &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: element,
				},
			}
		}
		elementType = schemapb.DataType_Int64
	case *schemapb.TemplateArrayValue_DoubleData:
		elements := templateValue.GetDoubleData().GetData()
		arrayValues = make([]*planpb.GenericValue, len(elements))
		for i, element := range elements {
			arrayValues[i] = &planpb.GenericValue{
				Val: &planpb.GenericValue_FloatVal{
					FloatVal: element,
				},
			}
		}
		elementType = schemapb.DataType_Double
	case *schemapb.TemplateArrayValue_StringData:
		elements := templateValue.GetStringData().GetData()
		arrayValues = make([]*planpb.GenericValue, len(elements))
		for i, element := range elements {
			arrayValues[i] = &planpb.GenericValue{
				Val: &planpb.GenericValue_StringVal{
					StringVal: element,
				},
			}
		}
		elementType = schemapb.DataType_VarChar
	case *schemapb.TemplateArrayValue_ArrayData:
		elements := templateValue.GetArrayData().GetData()
		arrayValues = make([]*planpb.GenericValue, len(elements))
		for i, element := range elements {
			targetValue, err := convertArrayValue(templateName, element)
			if err != nil {
				return nil, err
			}
			arrayValues[i] = targetValue
		}
		elementType = schemapb.DataType_Array
	case *schemapb.TemplateArrayValue_JsonData:
		elements := templateValue.GetJsonData().GetData()
		arrayValues = make([]*planpb.GenericValue, len(elements))
		for i, element := range elements {
			var jsonElement interface{}
			err := json.Unmarshal(element, &jsonElement)
			if err != nil {
				return nil, err
			}
			decoder := json.NewDecoder(bytes.NewBuffer(element))
			decoder.UseNumber()
			var value interface{}
			if err = decoder.Decode(&value); err != nil {
				return nil, err
			}
			parsedValue, _, err := parseJSONValue(value)
			if err != nil {
				return nil, err
			}
			arrayValues[i] = parsedValue
		}
		elementType = schemapb.DataType_JSON
	default:
		return nil, fmt.Errorf("unknown template variable value type: %v", templateValue.GetData())
	}
	return &planpb.GenericValue{
		Val: &planpb.GenericValue_ArrayVal{
			ArrayVal: &planpb.Array{
				Array:       arrayValues,
				SameType:    elementType != schemapb.DataType_JSON,
				ElementType: elementType,
			},
		},
	}, nil
}

func ConvertToGenericValue(templateName string, templateValue *schemapb.TemplateValue) (*planpb.GenericValue, error) {
	if templateValue == nil {
		return nil, fmt.Errorf("expression template variable value is nil, template name: {%s}", templateName)
	}
	switch templateValue.GetVal().(type) {
	case *schemapb.TemplateValue_BoolVal:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_BoolVal{
				BoolVal: templateValue.GetBoolVal(),
			},
		}, nil
	case *schemapb.TemplateValue_Int64Val:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_Int64Val{
				Int64Val: templateValue.GetInt64Val(),
			},
		}, nil
	case *schemapb.TemplateValue_FloatVal:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_FloatVal{
				FloatVal: templateValue.GetFloatVal(),
			},
		}, nil
	case *schemapb.TemplateValue_StringVal:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_StringVal{
				StringVal: templateValue.GetStringVal(),
			},
		}, nil
	case *schemapb.TemplateValue_ArrayVal:
		return convertArrayValue(templateName, templateValue.GetArrayVal())
	default:
		return nil, errors.New("expression elements can only be scalars")
	}
}

func UnmarshalExpressionValues(input map[string]*schemapb.TemplateValue) (map[string]*planpb.GenericValue, error) {
	result := make(map[string]*planpb.GenericValue, len(input))
	for name, value := range input {
		rv, err := ConvertToGenericValue(name, value)
		if err != nil {
			return nil, err
		}
		result[name] = rv
	}
	return result, nil
}
