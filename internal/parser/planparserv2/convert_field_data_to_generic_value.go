package planparserv2

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

func ConvertToGenericValue(templateName string, templateValue *schemapb.TemplateValue) (*planpb.GenericValue, error) {
	if templateValue == nil {
		return nil, fmt.Errorf("expression template variable value is nil, template name: {%s}", templateName)
	}
	switch templateValue.GetType() {
	case schemapb.DataType_Bool:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_BoolVal{
				BoolVal: templateValue.GetBoolVal(),
			},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_Int64Val{
				Int64Val: templateValue.GetInt64Val(),
			},
		}, nil
	case schemapb.DataType_Float, schemapb.DataType_Double:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_FloatVal{
				FloatVal: templateValue.GetFloatVal(),
			},
		}, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_StringVal{
				StringVal: templateValue.GetStringVal(),
			},
		}, nil
	case schemapb.DataType_Array:
		elements := templateValue.GetArrayVal().GetArray()
		arrayValues := make([]*planpb.GenericValue, len(elements))
		for i, element := range elements {
			arrayElement, err := ConvertToGenericValue(templateName, element)
			if err != nil {
				return nil, err
			}
			arrayValues[i] = arrayElement
		}
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_ArrayVal{
				ArrayVal: &planpb.Array{
					Array:       arrayValues,
					SameType:    templateValue.GetArrayVal().GetSameType(),
					ElementType: templateValue.GetArrayVal().GetElementType(),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("expression elements can only be scalars")
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
