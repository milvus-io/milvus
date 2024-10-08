package planparserv2

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

func ConvertFieldDataToGenericValue(data *schemapb.ScalarField, dataType schemapb.DataType) ([]*planpb.GenericValue, error) {
	if data == nil {
		return nil, fmt.Errorf("convert field data is nil")
	}
	values := make([]*planpb.GenericValue, 0)
	switch dataType {
	case schemapb.DataType_Bool:
		elements := data.GetBoolData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_BoolVal{
					BoolVal: element,
				},
			})
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		elements := data.GetIntData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: int64(element),
				},
			})
		}
	case schemapb.DataType_Int64:
		elements := data.GetLongData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: element,
				},
			})
		}
	case schemapb.DataType_Float:
		elements := data.GetFloatData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_FloatVal{
					FloatVal: float64(element),
				},
			})
		}
	case schemapb.DataType_Double:
		elements := data.GetDoubleData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_FloatVal{
					FloatVal: element,
				},
			})
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		elements := data.GetStringData().GetData()
		for _, element := range elements {
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_StringVal{
					StringVal: element,
				},
			})
		}
	case schemapb.DataType_Array:
		elements := data.GetArrayData().GetData()
		for _, element := range elements {
			arrayElements, err := ConvertFieldDataToGenericValue(element, data.GetArrayData().GetElementType())
			if err != nil {
				return nil, err
			}
			values = append(values, &planpb.GenericValue{
				Val: &planpb.GenericValue_ArrayVal{
					ArrayVal: &planpb.Array{
						Array:       arrayElements,
						SameType:    data.GetArrayData().GetElementType() != schemapb.DataType_JSON,
						ElementType: data.GetArrayData().GetElementType(),
					},
				},
			})
		}
	case schemapb.DataType_JSON:
		elements := data.GetJsonData().GetData()
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

func UnmarshalExpressionValues(input ...*schemapb.FieldData) (map[string]*planpb.GenericValue, error) {
	result := make(map[string]*planpb.GenericValue, len(input))
	for _, data := range input {
		rv, err := ConvertFieldDataToGenericValue(data.GetScalars(), data.GetType())
		if err != nil {
			return nil, err
		}
		if len(rv) != 1 {
			return nil, fmt.Errorf("invalue elements num for %s", data.GetFieldName())
		}
		result[data.GetFieldName()] = rv[0]
	}
	return result, nil
}
