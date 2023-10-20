package httpserver

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/parameterutil.go"
)

func ParseUsernamePassword(c *gin.Context) (string, string, bool) {
	username, password, ok := c.Request.BasicAuth()
	if !ok {
		token := GetAuthorization(c)
		i := strings.IndexAny(token, util.CredentialSeperator)
		if i != -1 {
			username = token[:i]
			password = token[i+1:]
		}
	} else {
		c.Header("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
	}
	return username, password, username != "" && password != ""
}

func GetAuthorization(c *gin.Context) string {
	auth := c.Request.Header.Get("Authorization")
	return strings.TrimPrefix(auth, "Bearer ")
}

// find the primary field of collection
func getPrimaryField(schema *schemapb.CollectionSchema) (*schemapb.FieldSchema, bool) {
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			return field, true
		}
	}
	return nil, false
}

func joinArray(data interface{}) string {
	var buffer bytes.Buffer
	arr := reflect.ValueOf(data)

	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			buffer.WriteString(",")
		}

		buffer.WriteString(fmt.Sprintf("%v", arr.Index(i)))
	}

	return buffer.String()
}

func convertRange(field *schemapb.FieldSchema, result gjson.Result) (string, error) {
	var resultStr string
	fieldType := field.DataType

	if fieldType == schemapb.DataType_Int64 {
		var dataArray []int64
		for _, data := range result.Array() {
			if data.Type == gjson.String {
				value, err := cast.ToInt64E(data.Str)
				if err != nil {
					return "", err
				}
				dataArray = append(dataArray, value)
			} else {
				value, err := cast.ToInt64E(data.Raw)
				if err != nil {
					return "", err
				}
				dataArray = append(dataArray, value)
			}
		}
		resultStr = joinArray(dataArray)
	} else if fieldType == schemapb.DataType_VarChar {
		var dataArray []string
		for _, data := range result.Array() {
			value, err := cast.ToStringE(data.Str)
			if err != nil {
				return "", err
			}
			dataArray = append(dataArray, value)
		}
		resultStr = joinArray(dataArray)
	}
	return resultStr, nil
}

// generate the expression: $primaryFieldName in [1,2,3]
func checkGetPrimaryKey(coll *schemapb.CollectionSchema, idResult gjson.Result) (string, error) {
	primaryField, ok := getPrimaryField(coll)
	if !ok {
		return "", errors.New("fail to find primary key from collection description")
	}
	resultStr, err := convertRange(primaryField, idResult)
	if err != nil {
		return "", err
	}
	filter := primaryField.Name + " in [" + resultStr + "]"
	return filter, nil
}

// --------------------- collection details --------------------- //
func printFields(fields []*schemapb.FieldSchema) []gin.H {
	var res []gin.H
	for _, field := range fields {
		fieldDetail := gin.H{
			HTTPReturnFieldName:       field.Name,
			HTTPReturnFieldPrimaryKey: field.IsPrimaryKey,
			HTTPReturnFieldAutoID:     field.AutoID,
			HTTPReturnDescription:     field.Description,
		}
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			dim, _ := getDim(field)
			fieldDetail[HTTPReturnFieldType] = field.DataType.String() + "(" + strconv.FormatInt(dim, 10) + ")"
		} else if field.DataType == schemapb.DataType_VarChar {
			maxLength, _ := parameterutil.GetMaxLength(field)
			fieldDetail[HTTPReturnFieldType] = field.DataType.String() + "(" + strconv.FormatInt(maxLength, 10) + ")"
		} else {
			fieldDetail[HTTPReturnFieldType] = field.DataType.String()
		}
		res = append(res, fieldDetail)
	}
	return res
}

func getMetricType(pairs []*commonpb.KeyValuePair) string {
	metricType := DefaultMetricType
	for _, pair := range pairs {
		if pair.Key == common.MetricTypeKey {
			metricType = pair.Value
			break
		}
	}
	return metricType
}

func printIndexes(indexes []*milvuspb.IndexDescription) []gin.H {
	var res []gin.H
	for _, index := range indexes {
		res = append(res, gin.H{
			HTTPReturnIndexName:        index.IndexName,
			HTTPReturnIndexField:       index.FieldName,
			HTTPReturnIndexMetricsType: getMetricType(index.Params),
		})
	}
	return res
}

// --------------------- insert param --------------------- //

func checkAndSetData(body string, collDescResp *milvuspb.DescribeCollectionResponse) (error, []map[string]interface{}) {
	var reallyDataArray []map[string]interface{}
	dataResult := gjson.Get(body, "data")
	dataResultArray := dataResult.Array()
	if len(dataResultArray) == 0 {
		return merr.ErrMissingRequiredParameters, reallyDataArray
	}

	var fieldNames []string
	for _, field := range collDescResp.Schema.Fields {
		fieldNames = append(fieldNames, field.Name)
	}

	for _, data := range dataResultArray {
		reallyData := map[string]interface{}{}
		var vectorArray []float32
		var binaryArray []byte
		if data.Type == gjson.JSON {
			for _, field := range collDescResp.Schema.Fields {
				fieldType := field.DataType
				fieldName := field.Name

				dataString := gjson.Get(data.Raw, fieldName).String()

				if field.IsPrimaryKey && collDescResp.Schema.AutoID {
					if dataString != "" {
						return merr.WrapErrParameterInvalid("", "set primary key but autoID == true"), reallyDataArray
					}
					continue
				}

				switch fieldType {
				case schemapb.DataType_FloatVector:
					for _, vector := range gjson.Get(data.Raw, fieldName).Array() {
						vectorArray = append(vectorArray, cast.ToFloat32(vector.Num))
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_BinaryVector:
					for _, vector := range gjson.Get(data.Raw, fieldName).Array() {
						binaryArray = append(binaryArray, cast.ToUint8(vector.Num))
					}
					reallyData[fieldName] = binaryArray
				case schemapb.DataType_Bool:
					result, err := cast.ToBoolE(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int8:
					result, err := cast.ToInt8E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int16:
					result, err := cast.ToInt16E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int32:
					result, err := cast.ToInt32E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int64:
					result, err := cast.ToInt64E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray
					}
					reallyData[fieldName] = result
				case schemapb.DataType_JSON:
					reallyData[fieldName] = []byte(dataString)
				case schemapb.DataType_Float:
					result, err := cast.ToFloat32E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Double:
					result, err := cast.ToFloat64E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray
					}
					reallyData[fieldName] = result
				case schemapb.DataType_VarChar:
					reallyData[fieldName] = dataString
				case schemapb.DataType_String:
					reallyData[fieldName] = dataString
				default:
					return merr.WrapErrParameterInvalid("", schemapb.DataType_name[int32(fieldType)], "fieldName: "+fieldName), reallyDataArray
				}
			}

			// fill dynamic schema
			if collDescResp.Schema.EnableDynamicField {
				for mapKey, mapValue := range data.Map() {
					if !containsString(fieldNames, mapKey) {
						mapValueStr := mapValue.String()
						switch mapValue.Type {
						case gjson.True, gjson.False:
							reallyData[mapKey] = cast.ToBool(mapValueStr)
						case gjson.String:
							reallyData[mapKey] = mapValueStr
						case gjson.Number:
							if strings.Contains(mapValue.Raw, ".") {
								reallyData[mapKey] = cast.ToFloat64(mapValue.Raw)
							} else {
								reallyData[mapKey] = cast.ToInt64(mapValueStr)
							}
						case gjson.JSON:
							reallyData[mapKey] = mapValue.Value()
						case gjson.Null:
							// skip null
						default:
							log.Warn("unknown json type found", zap.Int("mapValue.Type", int(mapValue.Type)))
						}
					}
				}
			}

			reallyDataArray = append(reallyDataArray, reallyData)
		} else {
			return merr.WrapErrParameterInvalid(gjson.JSON, data.Type, "NULL:0, FALSE:1, NUMBER:2, STRING:3, TRUE:4, JSON:5"), reallyDataArray
		}
	}
	return nil, reallyDataArray
}

func containsString(arr []string, s string) bool {
	for _, str := range arr {
		if str == s {
			return true
		}
	}
	return false
}

func getDim(field *schemapb.FieldSchema) (int64, error) {
	dimensionInSchema, err := funcutil.GetAttrByKeyFromRepeatedKV(common.DimKey, field.TypeParams)
	if err != nil {
		return 0, err
	}
	dim, err := strconv.Atoi(dimensionInSchema)
	if err != nil {
		return 0, err
	}
	return int64(dim), nil
}

func convertFloatVectorToArray(vector [][]float32, dim int64) ([]float32, error) {
	floatArray := make([]float32, 0)
	for _, arr := range vector {
		if int64(len(arr)) != dim {
			return nil, errors.New("vector length diff from dimension")
		}
		for i := int64(0); i < dim; i++ {
			floatArray = append(floatArray, arr[i])
		}
	}
	return floatArray, nil
}

func convertBinaryVectorToArray(vector [][]byte, dim int64) ([]byte, error) {
	binaryArray := make([]byte, 0)
	bytesLen := dim / 8
	for _, arr := range vector {
		if int64(len(arr)) != bytesLen {
			return nil, errors.New("vector length diff from dimension")
		}
		for i := int64(0); i < bytesLen; i++ {
			binaryArray = append(binaryArray, arr[i])
		}
	}
	return binaryArray, nil
}

type fieldCandi struct {
	name    string
	v       reflect.Value
	options map[string]string
}

func reflectValueCandi(v reflect.Value) (map[string]fieldCandi, error) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	result := make(map[string]fieldCandi)
	switch v.Kind() {
	case reflect.Map: // map[string]interface{}
		iter := v.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			result[key] = fieldCandi{
				name: key,
				v:    iter.Value(),
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupport row type: %s", v.Kind().String())
	}
}

func convertToIntArray(dataType schemapb.DataType, arr interface{}) []int32 {
	var res []int32
	switch dataType {
	case schemapb.DataType_Int8:
		for _, num := range arr.([]int8) {
			res = append(res, int32(num))
		}
	case schemapb.DataType_Int16:
		for _, num := range arr.([]int16) {
			res = append(res, int32(num))
		}
	}
	return res
}

func anyToColumns(rows []map[string]interface{}, sch *schemapb.CollectionSchema) ([]*schemapb.FieldData, error) {
	rowsLen := len(rows)
	if rowsLen == 0 {
		return []*schemapb.FieldData{}, errors.New("0 length column")
	}

	isDynamic := sch.EnableDynamicField
	var dim int64

	nameColumns := make(map[string]interface{})
	fieldData := make(map[string]*schemapb.FieldData)
	for _, field := range sch.Fields {
		// skip auto id pk field
		if field.IsPrimaryKey && field.AutoID {
			continue
		}
		var data interface{}
		switch field.DataType {
		case schemapb.DataType_Bool:
			data = make([]bool, 0, rowsLen)
		case schemapb.DataType_Int8:
			data = make([]int8, 0, rowsLen)
		case schemapb.DataType_Int16:
			data = make([]int16, 0, rowsLen)
		case schemapb.DataType_Int32:
			data = make([]int32, 0, rowsLen)
		case schemapb.DataType_Int64:
			data = make([]int64, 0, rowsLen)
		case schemapb.DataType_Float:
			data = make([]float32, 0, rowsLen)
		case schemapb.DataType_Double:
			data = make([]float64, 0, rowsLen)
		case schemapb.DataType_String:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_VarChar:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_JSON:
			data = make([][]byte, 0, rowsLen)
		case schemapb.DataType_FloatVector:
			data = make([][]float32, 0, rowsLen)
			dim, _ = getDim(field)
		case schemapb.DataType_BinaryVector:
			data = make([][]byte, 0, rowsLen)
			dim, _ = getDim(field)
		default:
			return nil, fmt.Errorf("the type(%v) of field(%v) is not supported, use other sdk please", field.DataType, field.Name)
		}
		nameColumns[field.Name] = data
		fieldData[field.Name] = &schemapb.FieldData{
			Type:      field.DataType,
			FieldName: field.Name,
			FieldId:   field.FieldID,
			IsDynamic: field.IsDynamic,
		}
	}
	if dim == 0 {
		return nil, errors.New("cannot find dimension")
	}

	dynamicCol := make([][]byte, 0, rowsLen)

	for _, row := range rows {
		// collection schema name need not be same, since receiver could have other names
		v := reflect.ValueOf(row)
		set, err := reflectValueCandi(v)
		if err != nil {
			return nil, err
		}

		for idx, field := range sch.Fields {
			// skip auto id pk field
			if field.IsPrimaryKey && field.AutoID {
				// remove pk field from candidates set, avoid adding it into dynamic column
				delete(set, field.Name)
				continue
			}

			candi, ok := set[field.Name]
			if !ok {
				return nil, fmt.Errorf("row %d does not has field %s", idx, field.Name)
			}
			switch field.DataType {
			case schemapb.DataType_Bool:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]bool), candi.v.Interface().(bool))
			case schemapb.DataType_Int8:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int8), candi.v.Interface().(int8))
			case schemapb.DataType_Int16:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int16), candi.v.Interface().(int16))
			case schemapb.DataType_Int32:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int32), candi.v.Interface().(int32))
			case schemapb.DataType_Int64:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int64), candi.v.Interface().(int64))
			case schemapb.DataType_Float:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]float32), candi.v.Interface().(float32))
			case schemapb.DataType_Double:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]float64), candi.v.Interface().(float64))
			case schemapb.DataType_String:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]string), candi.v.Interface().(string))
			case schemapb.DataType_VarChar:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]string), candi.v.Interface().(string))
			case schemapb.DataType_JSON:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
			case schemapb.DataType_FloatVector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]float32), candi.v.Interface().([]float32))
			case schemapb.DataType_BinaryVector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
			default:
				return nil, fmt.Errorf("the type(%v) of field(%v) is not supported, use other sdk please", field.DataType, field.Name)
			}

			delete(set, field.Name)
		}

		if isDynamic {
			m := make(map[string]interface{})
			for name, candi := range set {
				m[name] = candi.v.Interface()
			}
			bs, err := json.Marshal(m)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal dynamic field %w", err)
			}
			dynamicCol = append(dynamicCol, bs)
		}
	}
	columns := make([]*schemapb.FieldData, 0, len(nameColumns))
	for name, column := range nameColumns {
		colData := fieldData[name]
		switch colData.Type {
		case schemapb.DataType_Bool:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: column.([]bool),
						},
					},
				},
			}
		case schemapb.DataType_Int8:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: convertToIntArray(colData.Type, column),
						},
					},
				},
			}
		case schemapb.DataType_Int16:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: convertToIntArray(colData.Type, column),
						},
					},
				},
			}
		case schemapb.DataType_Int32:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: column.([]int32),
						},
					},
				},
			}
		case schemapb.DataType_Int64:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: column.([]int64),
						},
					},
				},
			}
		case schemapb.DataType_Float:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: column.([]float32),
						},
					},
				},
			}
		case schemapb.DataType_Double:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: column.([]float64),
						},
					},
				},
			}
		case schemapb.DataType_String:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: column.([]string),
						},
					},
				},
			}
		case schemapb.DataType_VarChar:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: column.([]string),
						},
					},
				},
			}
		case schemapb.DataType_JSON:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BytesData{
						BytesData: &schemapb.BytesArray{
							Data: column.([][]byte),
						},
					},
				},
			}
		case schemapb.DataType_FloatVector:
			arr, err := convertFloatVectorToArray(column.([][]float32), dim)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: arr,
						},
					},
				},
			}
		case schemapb.DataType_BinaryVector:
			arr, err := convertBinaryVectorToArray(column.([][]byte), dim)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: arr,
					},
				},
			}
		default:
			return nil, fmt.Errorf("the type(%v) of field(%v) is not supported, use other sdk please", colData.Type, name)
		}
		columns = append(columns, colData)
	}
	if isDynamic {
		columns = append(columns, &schemapb.FieldData{
			Type:      schemapb.DataType_JSON,
			FieldName: "",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: dynamicCol,
						},
					},
				},
			},
			IsDynamic: true,
		})
	}
	return columns, nil
}

// --------------------- search param --------------------- //
func serialize(fv []float32) []byte {
	data := make([]byte, 0, 4*len(fv)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range fv {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}

func vector2PlaceholderGroupBytes(vectors []float32) []byte {
	var placeHolderType commonpb.PlaceholderType
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}
	if len(vectors) != 0 {
		placeHolderType = commonpb.PlaceholderType_FloatVector

		ph.Type = placeHolderType
		ph.Values = append(ph.Values, serialize(vectors))
	}
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			ph,
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

// --------------------- get/query/search response --------------------- //
func genDynamicFields(fields []string, list []*schemapb.FieldData) []string {
	nonDynamicFieldNames := make(map[string]struct{})
	for _, field := range list {
		if !field.IsDynamic {
			nonDynamicFieldNames[field.FieldName] = struct{}{}
		}
	}
	dynamicFields := []string{}
	for _, fieldName := range fields {
		if _, exist := nonDynamicFieldNames[fieldName]; !exist {
			dynamicFields = append(dynamicFields, fieldName)
		}
	}
	return dynamicFields
}

func buildQueryResp(rowsNum int64, needFields []string, fieldDataList []*schemapb.FieldData, ids *schemapb.IDs, scores []float32) ([]map[string]interface{}, error) {
	var queryResp []map[string]interface{}

	columnNum := len(fieldDataList)
	if rowsNum == int64(0) {
		if columnNum > 0 {
			switch fieldDataList[0].Type {
			case schemapb.DataType_Bool:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetBoolData().Data))
			case schemapb.DataType_Int8:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetIntData().Data))
			case schemapb.DataType_Int16:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetIntData().Data))
			case schemapb.DataType_Int32:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetIntData().Data))
			case schemapb.DataType_Int64:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetLongData().Data))
			case schemapb.DataType_Float:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetFloatData().Data))
			case schemapb.DataType_Double:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetDoubleData().Data))
			case schemapb.DataType_String:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetStringData().Data))
			case schemapb.DataType_VarChar:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetStringData().Data))
			case schemapb.DataType_JSON:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetJsonData().Data))
			case schemapb.DataType_Array:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetArrayData().Data))
			case schemapb.DataType_BinaryVector:
				rowsNum = int64(len(fieldDataList[0].GetVectors().GetBinaryVector())*8) / fieldDataList[0].GetVectors().GetDim()
			case schemapb.DataType_FloatVector:
				rowsNum = int64(len(fieldDataList[0].GetVectors().GetFloatVector().Data)) / fieldDataList[0].GetVectors().GetDim()
			default:
				return nil, fmt.Errorf("the type(%v) of field(%v) is not supported, use other sdk please", fieldDataList[0].Type, fieldDataList[0].FieldName)
			}
		} else if ids != nil {
			switch ids.IdField.(type) {
			case *schemapb.IDs_IntId:
				int64Pks := ids.GetIntId().GetData()
				rowsNum = int64(len(int64Pks))
			case *schemapb.IDs_StrId:
				stringPks := ids.GetStrId().GetData()
				rowsNum = int64(len(stringPks))
			default:
				return nil, fmt.Errorf("the type of primary key(id) is not supported, use other sdk please")
			}
		}
	}
	if rowsNum == int64(0) {
		return []map[string]interface{}{}, nil
	}
	dynamicOutputFields := genDynamicFields(needFields, fieldDataList)
	for i := int64(0); i < rowsNum; i++ {
		row := map[string]interface{}{}
		if columnNum > 0 {
			for j := 0; j < columnNum; j++ {
				switch fieldDataList[j].Type {
				case schemapb.DataType_Bool:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetBoolData().Data[i]
				case schemapb.DataType_Int8:
					row[fieldDataList[j].FieldName] = int8(fieldDataList[j].GetScalars().GetIntData().Data[i])
				case schemapb.DataType_Int16:
					row[fieldDataList[j].FieldName] = int16(fieldDataList[j].GetScalars().GetIntData().Data[i])
				case schemapb.DataType_Int32:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetIntData().Data[i]
				case schemapb.DataType_Int64:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetLongData().Data[i]
				case schemapb.DataType_Float:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetFloatData().Data[i]
				case schemapb.DataType_Double:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetDoubleData().Data[i]
				case schemapb.DataType_String:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetStringData().Data[i]
				case schemapb.DataType_VarChar:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetStringData().Data[i]
				case schemapb.DataType_BinaryVector:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetVectors().GetBinaryVector()[i*(fieldDataList[j].GetVectors().GetDim()/8) : (i+1)*(fieldDataList[j].GetVectors().GetDim()/8)]
				case schemapb.DataType_FloatVector:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetVectors().GetFloatVector().Data[i*fieldDataList[j].GetVectors().GetDim() : (i+1)*fieldDataList[j].GetVectors().GetDim()]
				case schemapb.DataType_Array:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetArrayData().Data[i]
				case schemapb.DataType_JSON:
					data, ok := fieldDataList[j].GetScalars().Data.(*schemapb.ScalarField_JsonData)
					if ok && !fieldDataList[j].IsDynamic {
						row[fieldDataList[j].FieldName] = data.JsonData.Data[i]
					} else {
						var dataMap map[string]interface{}

						err := json.Unmarshal(fieldDataList[j].GetScalars().GetJsonData().Data[i], &dataMap)
						if err != nil {
							log.Error(fmt.Sprintf("[BuildQueryResp] Unmarshal error %s", err.Error()))
							return nil, err
						}

						if containsString(dynamicOutputFields, fieldDataList[j].FieldName) {
							for key, value := range dataMap {
								row[key] = value
							}
						} else {
							for _, dynamicField := range dynamicOutputFields {
								if _, ok := dataMap[dynamicField]; ok {
									row[dynamicField] = dataMap[dynamicField]
								}
							}
						}
					}
				default:
					row[fieldDataList[j].FieldName] = ""
				}
			}
		}
		if ids != nil {
			switch ids.IdField.(type) {
			case *schemapb.IDs_IntId:
				int64Pks := ids.GetIntId().GetData()
				row[DefaultPrimaryFieldName] = int64Pks[i]
			case *schemapb.IDs_StrId:
				stringPks := ids.GetStrId().GetData()
				row[DefaultPrimaryFieldName] = stringPks[i]
			default:
				return nil, fmt.Errorf("the type of primary key(id) is not supported, use other sdk please")
			}
		}
		if scores != nil && int64(len(scores)) > i {
			row[HTTPReturnDistance] = scores[i]
		}
		queryResp = append(queryResp, row)
	}

	return queryResp, nil
}
