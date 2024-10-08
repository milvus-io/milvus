package planparserv2

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

func ConvertFieldDataToGenericValue(templateName string, templateValue *schemapb.ScalarField, dataType schemapb.DataType) ([]*planpb.GenericValue, error) {
	if templateValue == nil {
		return nil, fmt.Errorf("expression template variable values is nil, template name: {%s}", templateName)
	}
	values := make([]*planpb.GenericValue, 0)
	switch dataType {
	case schemapb.DataType_Bool:
		elements := templateValue.GetBoolData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_BoolVal{
					BoolVal: element,
				},
			})
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		elements := templateValue.GetIntData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: int64(element),
				},
			})
		}
	case schemapb.DataType_Int64:
		elements := templateValue.GetLongData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: element,
				},
			})
		}
	case schemapb.DataType_Float:
		elements := templateValue.GetFloatData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_FloatVal{
					FloatVal: float64(element),
				},
			})
		}
	case schemapb.DataType_Double:
		elements := templateValue.GetDoubleData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_FloatVal{
					FloatVal: element,
				},
			})
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		elements := templateValue.GetStringData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_StringVal{
					StringVal: element,
				},
			})
		}
	case schemapb.DataType_Array:
		elements := templateValue.GetArrayData().GetData()
		for _, element := range elements {
			arrayElements, err := ConvertFieldDataToGenericValue("", element, templateValue.GetArrayData().GetElementType())
			if err != nil {
				return nil, err
			}
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_ArrayVal{
					ArrayVal: &planpb.Array{
						Array:       arrayElements,
						SameType:    templateValue.GetArrayData().GetElementType() != schemapb.DataType_JSON,
						ElementType: templateValue.GetArrayData().GetElementType(),
					},
				},
			})
		}
	case schemapb.DataType_JSON:
		elements := templateValue.GetJsonData().GetData()
		for _, element := range elements {
			var j interface{}
			desc := json.NewDecoder(strings.NewReader(string(element)))
			desc.UseNumber()
			err := desc.Decode(&j)
			if err != nil {
				return nil, err
			}
			value, _, err := parseJSONValue(j)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	default:
		return nil, fmt.Errorf("expression elements can only be scalars")

	}

	return values, nil
}

func UnmarshalExpressionValues(input []*schemapb.FieldData) (map[string]*planpb.GenericValue, error) {
	result := make(map[string]*planpb.GenericValue, len(input))
	for _, data := range input {
		if _, ok := result[data.GetFieldName()]; ok {
			return nil, fmt.Errorf("the number of elements in the template variable name is incorrect: {%s}", data.GetFieldName())
		}
		rv, err := ConvertFieldDataToGenericValue(data.GetFieldName(), data.GetScalars(), data.GetType())
		if err != nil {
			return nil, err
		}
		if len(rv) != 1 {
			return nil, fmt.Errorf("the number of elements in the template variable name is incorrect: {%s}", data.GetFieldName())
		}
		result[data.GetFieldName()] = rv[0]
	}
	return result, nil
}
