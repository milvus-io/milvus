package httpserver

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/tidwall/gjson"
)

func assembleStructArrayField(data gjson.Result, structArrayField *schemapb.StructArrayFieldSchema) (map[string]any, error) {
	if data.Type == gjson.Null {
		return nil, merr.WrapErrParameterInvalidMsg("nil input json data for struct array field")
	}
	structArray := data.Array()
	if len(structArray) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("empty array input json data for struct array field")
	}

	structFieldDatas := make(map[string]any)
	for _, structSubField := range structArrayField.Fields {

		subFieldName, err := typeutil.ExtractStructFieldName(structSubField.Name)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg(err.Error())
		}
		switch structSubField.ElementType {
		case schemapb.DataType_Bool:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: []bool{},
					},
				},
			}
		case schemapb.DataType_Int8:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{},
					},
				},
			}
		case schemapb.DataType_Int16:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{},
					},
				},
			}
		case schemapb.DataType_Int32:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{},
					},
				},
			}
		case schemapb.DataType_Int64:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{},
					},
				},
			}
		case schemapb.DataType_Float:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{},
					},
				},
			}
		case schemapb.DataType_Double:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: []float64{},
					},
				},
			}
		case schemapb.DataType_VarChar:
			structFieldDatas[subFieldName] = &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{},
					},
				},
			}
		case schemapb.DataType_FloatVector:
			dim, _ := typeutil.GetDim(structSubField)
			structFieldDatas[subFieldName] = &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: make([]float32, 0),
					},
				},
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("unsupported element type: %s", structSubField.ElementType))
		}
	}

	for _, structItem := range structArray {
		for _, structSubField := range structArrayField.Fields {
			subFieldName, err := typeutil.ExtractStructFieldName(structSubField.Name)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(err.Error())
			}
			dataString := structItem.Get(subFieldName).String()
			switch structSubField.ElementType {
			case schemapb.DataType_Bool:
				val, err := cast.ToBoolE(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid bool value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_BoolData).BoolData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_BoolData).BoolData.Data, val)
			case schemapb.DataType_Int8:
				val, err := cast.ToInt8E(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid int8 value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_IntData).IntData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_IntData).IntData.Data, int32(val))
			case schemapb.DataType_Int16:
				val, err := cast.ToInt16E(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid int16 value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_IntData).IntData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_IntData).IntData.Data, int32(val))
			case schemapb.DataType_Int32:
				val, err := cast.ToInt32E(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid int32 value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_IntData).IntData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_IntData).IntData.Data, val)
			case schemapb.DataType_Int64:
				val, err := cast.ToInt64E(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid int64 value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_LongData).LongData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_LongData).LongData.Data, val)
			case schemapb.DataType_Float:
				val, err := cast.ToFloat32E(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid float value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_FloatData).FloatData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_FloatData).FloatData.Data, val)
			case schemapb.DataType_Double:
				val, err := cast.ToFloat64E(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid double value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_DoubleData).DoubleData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_DoubleData).DoubleData.Data, val)
			case schemapb.DataType_VarChar:
				val, err := cast.ToStringE(dataString)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid varchar value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_StringData).StringData.Data =
					append(structFieldDatas[subFieldName].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_StringData).StringData.Data, val)
			case schemapb.DataType_FloatVector:
				var vectorArray []float32
				err := json.Unmarshal([]byte(dataString), &vectorArray)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid float vector value: %s", dataString))
				}
				structFieldDatas[subFieldName].(*schemapb.VectorField).Data.(*schemapb.VectorField_FloatVector).FloatVector.Data =
					append(structFieldDatas[subFieldName].(*schemapb.VectorField).Data.(*schemapb.VectorField_FloatVector).FloatVector.Data, vectorArray...)
			default:
				return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("unsupported element type: %s", structSubField.ElementType))
			}
		}
	}
	return structFieldDatas, nil
}
