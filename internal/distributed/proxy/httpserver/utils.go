// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func HTTPReturn(c *gin.Context, code int, result gin.H) {
	c.Set(HTTPReturnCode, result[HTTPReturnCode])
	if errorMsg, ok := result[HTTPReturnMessage]; ok {
		c.Set(HTTPReturnMessage, errorMsg)
	}
	c.JSON(code, result)
}

// HTTPReturnStream uses custom jsonRender that encodes JSON data directly into the response stream,
// it uses less memory since it does not buffer the entire JSON structure before sending it,
// unlike c.JSON in HTTPReturn, which serializes the JSON fully in memory before writing it to the response.
func HTTPReturnStream(c *gin.Context, code int, result gin.H) {
	c.Set(HTTPReturnCode, result[HTTPReturnCode])
	if errorMsg, ok := result[HTTPReturnMessage]; ok {
		c.Set(HTTPReturnMessage, errorMsg)
	}
	c.Render(code, jsonRender{Data: result})
}

func HTTPAbortReturn(c *gin.Context, code int, result gin.H) {
	c.Set(HTTPReturnCode, result[HTTPReturnCode])
	if errorMsg, ok := result[HTTPReturnMessage]; ok {
		c.Set(HTTPReturnMessage, errorMsg)
	}
	c.AbortWithStatusJSON(code, result)
}

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
		dataArray := make([]int64, 0, len(result.Array()))
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
		dataArray := make([]string, 0, len(result.Array()))
		for _, data := range result.Array() {
			value, err := cast.ToStringE(data.Str)
			if err != nil {
				return "", err
			}
			dataArray = append(dataArray, fmt.Sprintf(`"%s"`, value))
		}
		resultStr = joinArray(dataArray)
	}
	return resultStr, nil
}

// generate the expression: $primaryFieldName in [1,2,3]
func checkGetPrimaryKey(coll *schemapb.CollectionSchema, idResult gjson.Result) (string, error) {
	primaryField, ok := getPrimaryField(coll)
	if !ok {
		return "", fmt.Errorf("collection: %s has no primary field", coll.Name)
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
	res := make([]gin.H, 0, len(fields))
	for _, field := range fields {
		if field.Name == common.MetaFieldName {
			continue
		}
		fieldDetail := printFieldDetail(field, true)
		res = append(res, fieldDetail)
	}
	return res
}

func printFieldsV2(fields []*schemapb.FieldSchema) []gin.H {
	res := make([]gin.H, 0, len(fields))
	for _, field := range fields {
		if field.Name == common.MetaFieldName {
			continue
		}
		fieldDetail := printFieldDetail(field, false)
		res = append(res, fieldDetail)
	}
	return res
}

func printFieldDetail(field *schemapb.FieldSchema, oldVersion bool) gin.H {
	fieldDetail := gin.H{
		HTTPReturnFieldName:          field.Name,
		HTTPReturnFieldPrimaryKey:    field.IsPrimaryKey,
		HTTPReturnFieldPartitionKey:  field.IsPartitionKey,
		HTTPReturnFieldClusteringKey: field.IsClusteringKey,
		HTTPReturnFieldAutoID:        field.AutoID,
		HTTPReturnDescription:        field.Description,
		HTTPReturnFieldNullable:      field.Nullable,
	}
	if field.DefaultValue != nil {
		fieldDetail[HTTPRequestDefaultValue] = field.DefaultValue
	}
	if field.GetIsFunctionOutput() {
		fieldDetail[HTTPReturnFieldIsFunctionOutput] = true
	}
	if typeutil.IsVectorType(field.DataType) {
		fieldDetail[HTTPReturnFieldType] = field.DataType.String()
		if oldVersion {
			dim, _ := getDim(field)
			fieldDetail[HTTPReturnFieldType] = field.DataType.String() + "(" + strconv.FormatInt(dim, 10) + ")"
		}
	} else if field.DataType == schemapb.DataType_VarChar {
		fieldDetail[HTTPReturnFieldType] = field.DataType.String()
		if oldVersion {
			maxLength, _ := parameterutil.GetMaxLength(field)
			fieldDetail[HTTPReturnFieldType] = field.DataType.String() + "(" + strconv.FormatInt(maxLength, 10) + ")"
		}
	} else {
		fieldDetail[HTTPReturnFieldType] = field.DataType.String()
	}
	if !oldVersion {
		fieldDetail[HTTPReturnFieldID] = field.FieldID
		if field.TypeParams != nil {
			fieldDetail[Params] = field.TypeParams
		}
		if field.DataType == schemapb.DataType_Array {
			fieldDetail[HTTPReturnFieldElementType] = field.GetElementType().String()
		}
	}
	return fieldDetail
}

func printFunctionDetails(functions []*schemapb.FunctionSchema) []gin.H {
	res := make([]gin.H, 0, len(functions))
	for _, function := range functions {
		res = append(res, gin.H{
			HTTPReturnFunctionName:             function.Name,
			HTTPReturnDescription:              function.Description,
			HTTPReturnFunctionType:             function.Type,
			HTTPReturnFunctionID:               function.Id,
			HTTPReturnFunctionInputFieldNames:  function.InputFieldNames,
			HTTPReturnFunctionOutputFieldNames: function.OutputFieldNames,
			HTTPReturnFunctionParams:           function.Params,
		})
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
	res := make([]gin.H, 0, len(indexes))
	for _, index := range indexes {
		res = append(res, gin.H{
			HTTPIndexName:             index.IndexName,
			HTTPIndexField:            index.FieldName,
			HTTPReturnIndexMetricType: getMetricType(index.Params),
		})
	}
	return res
}

// --------------------- insert param --------------------- //

func checkAndSetData(body string, collSchema *schemapb.CollectionSchema) (error, []map[string]interface{}, map[string][]bool) {
	var reallyDataArray []map[string]interface{}
	validDataMap := make(map[string][]bool)
	dataResult := gjson.Get(body, HTTPRequestData)
	dataResultArray := dataResult.Array()
	if len(dataResultArray) == 0 {
		return merr.ErrMissingRequiredParameters, reallyDataArray, validDataMap
	}

	fieldNames := make([]string, 0, len(collSchema.Fields))
	for _, field := range collSchema.Fields {
		if field.IsDynamic {
			continue
		}
		fieldNames = append(fieldNames, field.Name)
	}

	for _, data := range dataResultArray {
		reallyData := map[string]interface{}{}
		if data.Type == gjson.JSON {
			for _, field := range collSchema.Fields {
				if field.IsDynamic {
					continue
				}
				fieldType := field.DataType
				fieldName := field.Name

				if field.Nullable || field.DefaultValue != nil {
					value := gjson.Get(data.Raw, fieldName)
					if value.Type == gjson.Null {
						validDataMap[fieldName] = append(validDataMap[fieldName], false)
						continue
					} else {
						validDataMap[fieldName] = append(validDataMap[fieldName], true)
					}
				}

				dataString := gjson.Get(data.Raw, fieldName).String()
				// if has pass pk than just to try to set it
				if field.IsPrimaryKey && field.AutoID && len(dataString) == 0 {
					continue
				}

				// if field is a function output field, user must not provide data for it
				if field.GetIsFunctionOutput() {
					if dataString != "" {
						return merr.WrapErrParameterInvalid("", "not allowed to provide input data for function output field: "+fieldName), reallyDataArray, validDataMap
					}
					continue
				}

				switch fieldType {
				case schemapb.DataType_FloatVector:
					if dataString == "" {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName), reallyDataArray, validDataMap
					}
					var vectorArray []float32
					err := json.Unmarshal([]byte(dataString), &vectorArray)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_BinaryVector:
					if dataString == "" {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName), reallyDataArray, validDataMap
					}
					vectorStr := gjson.Get(data.Raw, fieldName).Raw
					var vectorArray []byte
					err := json.Unmarshal([]byte(vectorStr), &vectorArray)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_SparseFloatVector:
					if dataString == "" {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName), reallyDataArray, validDataMap
					}
					sparseVec, err := typeutil.CreateSparseFloatRowFromJSON([]byte(dataString))
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = sparseVec
				case schemapb.DataType_Float16Vector:
					fallthrough
				case schemapb.DataType_BFloat16Vector:
					if dataString == "" {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName), reallyDataArray, validDataMap
					}
					vectorJSON := gjson.Get(data.Raw, fieldName)
					// Clients may send float32 vector because they are inconvenient of processing float16 or bfloat16.
					// Float32 vector is an array in JSON format, like `[1.0, 2.0, 3.0]`, `[1, 2, 3]`, etc,
					// while float16 or bfloat16 vector is a string in JSON format, like `"4z1jPgAAgL8="`, `"gD+AP4A/gD8="`, etc.
					if vectorJSON.IsArray() {
						// `data` is a float32 vector
						// same as `case schemapb.DataType_FloatVector`
						var vectorArray []float32
						err := json.Unmarshal([]byte(dataString), &vectorArray)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = vectorArray
					} else if vectorJSON.Type == gjson.String {
						// `data` is a float16 or bfloat16 vector
						// same as `case schemapb.DataType_BinaryVector`
						vectorStr := gjson.Get(data.Raw, fieldName).Raw
						var vectorArray []byte
						err := json.Unmarshal([]byte(vectorStr), &vectorArray)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = vectorArray
					} else {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, "invalid vector field: "+fieldName), reallyDataArray, validDataMap
					}
				case schemapb.DataType_Bool:
					result, err := cast.ToBoolE(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int8:
					result, err := cast.ToInt8E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int16:
					result, err := cast.ToInt16E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int32:
					result, err := cast.ToInt32E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int64:
					result, err := json.Number(dataString).Int64()
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Array:
					switch field.ElementType {
					case schemapb.DataType_Bool:
						arr := make([]bool, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_BoolData{
								BoolData: &schemapb.BoolArray{
									Data: arr,
								},
							},
						}
					case schemapb.DataType_Int8:
						arr := make([]int32, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{
									Data: arr,
								},
							},
						}
					case schemapb.DataType_Int16:
						arr := make([]int32, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{
									Data: arr,
								},
							},
						}
					case schemapb.DataType_Int32:
						arr := make([]int32, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{
									Data: arr,
								},
							},
						}
					case schemapb.DataType_Int64:
						arr := make([]int64, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: arr,
								},
							},
						}
					case schemapb.DataType_Float:
						arr := make([]float32, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_FloatData{
								FloatData: &schemapb.FloatArray{
									Data: arr,
								},
							},
						}
					case schemapb.DataType_Double:
						arr := make([]float64, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_DoubleData{
								DoubleData: &schemapb.DoubleArray{
									Data: arr,
								},
							},
						}
					case schemapb.DataType_VarChar:
						arr := make([]string, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error()), reallyDataArray, validDataMap
						}
						reallyData[fieldName] = &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: arr,
								},
							},
						}
					}
				case schemapb.DataType_JSON:
					reallyData[fieldName] = []byte(dataString)
				case schemapb.DataType_Float:
					result, err := cast.ToFloat32E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Double:
					result, err := cast.ToFloat64E(dataString)
					if err != nil {
						return merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error()), reallyDataArray, validDataMap
					}
					reallyData[fieldName] = result
				case schemapb.DataType_VarChar:
					reallyData[fieldName] = dataString
				case schemapb.DataType_String:
					reallyData[fieldName] = dataString
				default:
					return merr.WrapErrParameterInvalid("", schemapb.DataType_name[int32(fieldType)], "fieldName: "+fieldName), reallyDataArray, validDataMap
				}
			}

			// fill dynamic schema

			for mapKey, mapValue := range data.Map() {
				if !containsString(fieldNames, mapKey) {
					if collSchema.EnableDynamicField {
						if mapKey == common.MetaFieldName {
							return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("use the invalid field name(%s) when enable dynamicField", mapKey)), nil, nil
						}
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
					} else {
						return merr.WrapErrParameterInvalidMsg("has pass more field without dynamic schema, please check it"), nil, nil
					}
				}
			}

			reallyDataArray = append(reallyDataArray, reallyData)
		} else {
			return merr.WrapErrParameterInvalid(gjson.JSON, data.Type, "NULL:0, FALSE:1, NUMBER:2, STRING:3, TRUE:4, JSON:5"), reallyDataArray, validDataMap
		}
	}
	return nil, reallyDataArray, validDataMap
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
			return nil, fmt.Errorf("[]float32 size %d doesn't equal to vector dimension %d of %s",
				len(arr), dim, schemapb.DataType_name[int32(schemapb.DataType_FloatVector)])
		}
		for i := int64(0); i < dim; i++ {
			floatArray = append(floatArray, arr[i])
		}
	}
	return floatArray, nil
}

func convertBinaryVectorToArray(vector [][]byte, dim int64, dataType schemapb.DataType) ([]byte, error) {
	var bytesLen int64
	switch dataType {
	case schemapb.DataType_BinaryVector:
		bytesLen = dim / 8
	case schemapb.DataType_Float16Vector:
		bytesLen = dim * 2
	case schemapb.DataType_BFloat16Vector:
		bytesLen = dim * 2
	}
	binaryArray := make([]byte, 0, len(vector)*int(bytesLen))
	for _, arr := range vector {
		if int64(len(arr)) != bytesLen {
			return nil, fmt.Errorf("[]byte size %d doesn't equal to vector dimension %d of %s",
				len(arr), dim, schemapb.DataType_name[int32(dataType)])
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

func anyToColumns(rows []map[string]interface{}, validDataMap map[string][]bool, sch *schemapb.CollectionSchema, inInsert bool) ([]*schemapb.FieldData, error) {
	rowsLen := len(rows)
	if rowsLen == 0 {
		return []*schemapb.FieldData{}, fmt.Errorf("no row need to be convert to columns")
	}

	isDynamic := sch.EnableDynamicField

	nameColumns := make(map[string]interface{})
	nameDims := make(map[string]int64)
	fieldData := make(map[string]*schemapb.FieldData)

	for _, field := range sch.Fields {
		if (field.IsPrimaryKey && field.AutoID && inInsert) || field.IsDynamic {
			continue
		}
		// skip function output field
		if field.GetIsFunctionOutput() {
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
		case schemapb.DataType_Array:
			data = make([]*schemapb.ScalarField, 0, rowsLen)
		case schemapb.DataType_JSON:
			data = make([][]byte, 0, rowsLen)
		case schemapb.DataType_FloatVector:
			data = make([][]float32, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_BinaryVector:
			data = make([][]byte, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_Float16Vector:
			data = make([][]byte, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_BFloat16Vector:
			data = make([][]byte, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_SparseFloatVector:
			data = make([][]byte, 0, rowsLen)
			nameDims[field.Name] = int64(0)
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
	if len(nameDims) == 0 && len(sch.Functions) == 0 {
		return nil, fmt.Errorf("collection: %s has no vector field or functions", sch.Name)
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
			if field.IsDynamic {
				continue
			}
			candi, ok := set[field.Name]
			if field.IsPrimaryKey && field.AutoID && inInsert {
				if ok {
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("no need to pass pk field(%s) when autoid==true in insert", field.Name))
				}
				continue
			}
			if (field.Nullable || field.DefaultValue != nil) && !ok {
				continue
			}
			if field.GetIsFunctionOutput() {
				if ok {
					return nil, fmt.Errorf("row %d has data provided for function output field %s", idx, field.Name)
				}
				continue
			}
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
			case schemapb.DataType_Array:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]*schemapb.ScalarField), candi.v.Interface().(*schemapb.ScalarField))
			case schemapb.DataType_JSON:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
			case schemapb.DataType_FloatVector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]float32), candi.v.Interface().([]float32))
			case schemapb.DataType_BinaryVector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
			case schemapb.DataType_Float16Vector:
				switch candi.v.Interface().(type) {
				case []byte:
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
				case []float32:
					vec := typeutil.Float32ArrayToFloat16Bytes(candi.v.Interface().([]float32))
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), vec)
				default:
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid type(%v) of field(%v) ", field.DataType, field.Name))
				}
			case schemapb.DataType_BFloat16Vector:
				switch candi.v.Interface().(type) {
				case []byte:
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
				case []float32:
					vec := typeutil.Float32ArrayToBFloat16Bytes(candi.v.Interface().([]float32))
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), vec)
				default:
					return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid type(%v) of field(%v) ", field.DataType, field.Name))
				}
			case schemapb.DataType_SparseFloatVector:
				content := candi.v.Interface().([]byte)
				rowSparseDim := typeutil.SparseFloatRowDim(content)
				if rowSparseDim > nameDims[field.Name] {
					nameDims[field.Name] = rowSparseDim
				}
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), content)
			default:
				return nil, fmt.Errorf("the type(%v) of field(%v) is not supported, use other sdk please", field.DataType, field.Name)
			}

			delete(set, field.Name)
		}
		// if is not dynamic, but pass more field, will throw err in /internal/distributed/proxy/httpserver/utils.go@checkAndSetData
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
		case schemapb.DataType_Array:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data: column.([]*schemapb.ScalarField),
						},
					},
				},
			}
		case schemapb.DataType_JSON:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: column.([][]byte),
						},
					},
				},
			}
		case schemapb.DataType_FloatVector:
			dim := nameDims[name]
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
			dim := nameDims[name]
			arr, err := convertBinaryVectorToArray(column.([][]byte), dim, colData.Type)
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
		case schemapb.DataType_Float16Vector:
			dim := nameDims[name]
			arr, err := convertBinaryVectorToArray(column.([][]byte), dim, colData.Type)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: arr,
					},
				},
			}
		case schemapb.DataType_BFloat16Vector:
			dim := nameDims[name]
			arr, err := convertBinaryVectorToArray(column.([][]byte), dim, colData.Type)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: arr,
					},
				},
			}
		case schemapb.DataType_SparseFloatVector:
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: nameDims[name],
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{
							Dim:      nameDims[name],
							Contents: column.([][]byte),
						},
					},
				},
			}
		default:
			return nil, fmt.Errorf("the type(%v) of field(%v) is not supported, use other sdk please", colData.Type, name)
		}
		colData.ValidData = validDataMap[name]
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

func serializeFloatVectors(vectorStr string, dataType schemapb.DataType, dimension, bytesLen int64, fpArrayToBytesFunc func([]float32) []byte) ([][]byte, error) {
	var fp32Values [][]float32
	err := json.Unmarshal([]byte(vectorStr), &fp32Values)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr, err.Error())
	}
	values := make([][]byte, 0, len(fp32Values))
	for _, vectorArray := range fp32Values {
		if int64(len(vectorArray)) != dimension {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr,
				fmt.Sprintf("dimension: %d, but length of []float: %d", dimension, len(vectorArray)))
		}
		vectorBytes := fpArrayToBytesFunc(vectorArray)
		values = append(values, vectorBytes)
	}
	return values, nil
}

func serializeByteVectors(vectorStr string, dataType schemapb.DataType, dimension, bytesLen int64) ([][]byte, error) {
	values := make([][]byte, 0)
	err := json.Unmarshal([]byte(vectorStr), &values)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr, err.Error())
	}
	for _, vectorArray := range values {
		if int64(len(vectorArray)) != bytesLen {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], string(vectorArray),
				fmt.Sprintf("dimension: %d, bytesLen: %d, but length of []byte: %d", dimension, bytesLen, len(vectorArray)))
		}
	}
	return values, nil
}

// serializeFloatOrByteVectors serializes float32/float16/bfloat16 vectors.
// `[[1, 2, 3], [4.0, 5.0, 6.0]] is float32 vector,
// `["4z1jPgAAgL8=", "gD+AP4A/gD8="]` is float16/bfloat16 vector.
func serializeFloatOrByteVectors(jsonResult gjson.Result, dataType schemapb.DataType, dimension int64, fpArrayToBytesFunc func([]float32) []byte) ([][]byte, error) {
	firstElement := jsonResult.Get("0")

	// Clients may send float32 vector because they are inconvenient of processing float16 or bfloat16.
	// Float32 vector is an array in JSON format, like `[1.0, 2.0, 3.0]`, `[1, 2, 3]`, etc,
	// while float16 or bfloat16 vector is a string in JSON format, like `"4z1jPgAAgL8="`, `"gD+AP4A/gD8="`, etc.
	if firstElement.IsArray() {
		return serializeFloatVectors(jsonResult.Raw, dataType, dimension, dimension*2, fpArrayToBytesFunc)
	} else if firstElement.Type == gjson.String || !firstElement.Exists() {
		// consider corner case: `[]`
		return serializeByteVectors(jsonResult.Raw, dataType, dimension, dimension*2)
	}
	return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], jsonResult.Raw, "invalid type")
}

func serializeSparseFloatVectors(vectors []gjson.Result, dataType schemapb.DataType) ([][]byte, error) {
	values := make([][]byte, 0, len(vectors))
	for _, vector := range vectors {
		vectorBytes := []byte(vector.String())
		sparseVector, err := typeutil.CreateSparseFloatRowFromJSON(vectorBytes)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(), err.Error())
		}
		values = append(values, sparseVector)
	}
	return values, nil
}

func convertQueries2Placeholder(body string, dataType schemapb.DataType, dimension int64) (*commonpb.PlaceholderValue, error) {
	var valueType commonpb.PlaceholderType
	var values [][]byte
	var err error
	switch dataType {
	case schemapb.DataType_FloatVector:
		valueType = commonpb.PlaceholderType_FloatVector
		values, err = serializeFloatVectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, dimension*4, typeutil.Float32ArrayToBytes)
	case schemapb.DataType_BinaryVector:
		valueType = commonpb.PlaceholderType_BinaryVector
		values, err = serializeByteVectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, dimension/8)
	case schemapb.DataType_Float16Vector:
		valueType = commonpb.PlaceholderType_Float16Vector
		values, err = serializeFloatOrByteVectors(gjson.Get(body, HTTPRequestData), dataType, dimension, typeutil.Float32ArrayToFloat16Bytes)
	case schemapb.DataType_BFloat16Vector:
		valueType = commonpb.PlaceholderType_BFloat16Vector
		values, err = serializeFloatOrByteVectors(gjson.Get(body, HTTPRequestData), dataType, dimension, typeutil.Float32ArrayToBFloat16Bytes)
	case schemapb.DataType_SparseFloatVector:
		valueType = commonpb.PlaceholderType_SparseFloatVector
		values, err = serializeSparseFloatVectors(gjson.Get(body, HTTPRequestData).Array(), dataType)
	case schemapb.DataType_VarChar:
		valueType = commonpb.PlaceholderType_VarChar
		res := gjson.Get(body, HTTPRequestData).Array()
		values = make([][]byte, 0, len(res))
		for _, v := range res {
			values = append(values, []byte(v.String()))
		}
	}
	if err != nil {
		return nil, err
	}
	return &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   valueType,
		Values: values,
	}, nil
}

// todo: support [][]byte for BinaryVector
func vectors2PlaceholderGroupBytes(vectors [][]float32) []byte {
	var placeHolderType commonpb.PlaceholderType
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}
	if len(vectors) != 0 {
		placeHolderType = commonpb.PlaceholderType_FloatVector

		ph.Type = placeHolderType
		for _, vector := range vectors {
			ph.Values = append(ph.Values, typeutil.Float32ArrayToBytes(vector))
		}
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

func buildQueryResp(rowsNum int64, needFields []string, fieldDataList []*schemapb.FieldData, ids *schemapb.IDs, scores []float32, enableInt64 bool) ([]map[string]interface{}, error) {
	columnNum := len(fieldDataList)
	if rowsNum == int64(0) { // always
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
			case schemapb.DataType_Array:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetArrayData().Data))
			case schemapb.DataType_JSON:
				rowsNum = int64(len(fieldDataList[0].GetScalars().GetJsonData().Data))
			case schemapb.DataType_BinaryVector:
				rowsNum = int64(len(fieldDataList[0].GetVectors().GetBinaryVector())*8) / fieldDataList[0].GetVectors().GetDim()
			case schemapb.DataType_FloatVector:
				rowsNum = int64(len(fieldDataList[0].GetVectors().GetFloatVector().Data)) / fieldDataList[0].GetVectors().GetDim()
			case schemapb.DataType_Float16Vector:
				rowsNum = int64(len(fieldDataList[0].GetVectors().GetFloat16Vector())/2) / fieldDataList[0].GetVectors().GetDim()
			case schemapb.DataType_BFloat16Vector:
				rowsNum = int64(len(fieldDataList[0].GetVectors().GetBfloat16Vector())/2) / fieldDataList[0].GetVectors().GetDim()
			case schemapb.DataType_SparseFloatVector:
				rowsNum = int64(len(fieldDataList[0].GetVectors().GetSparseFloatVector().Contents))
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
	queryResp := make([]map[string]interface{}, 0, rowsNum)
	dynamicOutputFields := genDynamicFields(needFields, fieldDataList)
	for i := int64(0); i < rowsNum; i++ {
		row := map[string]interface{}{}
		if columnNum > 0 {
			for j := 0; j < columnNum; j++ {
				switch fieldDataList[j].Type {
				case schemapb.DataType_Bool:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetBoolData().Data[i]
				case schemapb.DataType_Int8:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = int8(fieldDataList[j].GetScalars().GetIntData().Data[i])
				case schemapb.DataType_Int16:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = int16(fieldDataList[j].GetScalars().GetIntData().Data[i])
				case schemapb.DataType_Int32:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetIntData().Data[i]
				case schemapb.DataType_Int64:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					if enableInt64 {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetLongData().Data[i]
					} else {
						row[fieldDataList[j].FieldName] = strconv.FormatInt(fieldDataList[j].GetScalars().GetLongData().Data[i], 10)
					}
				case schemapb.DataType_Float:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetFloatData().Data[i]
				case schemapb.DataType_Double:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetDoubleData().Data[i]
				case schemapb.DataType_String:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetStringData().Data[i]
				case schemapb.DataType_VarChar:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetStringData().Data[i]
				case schemapb.DataType_BinaryVector:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetVectors().GetBinaryVector()[i*(fieldDataList[j].GetVectors().GetDim()/8) : (i+1)*(fieldDataList[j].GetVectors().GetDim()/8)]
				case schemapb.DataType_FloatVector:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetVectors().GetFloatVector().Data[i*fieldDataList[j].GetVectors().GetDim() : (i+1)*fieldDataList[j].GetVectors().GetDim()]
				case schemapb.DataType_Float16Vector:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetVectors().GetFloat16Vector()[i*(fieldDataList[j].GetVectors().GetDim()*2) : (i+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
				case schemapb.DataType_BFloat16Vector:
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetVectors().GetBfloat16Vector()[i*(fieldDataList[j].GetVectors().GetDim()*2) : (i+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
				case schemapb.DataType_SparseFloatVector:
					row[fieldDataList[j].FieldName] = typeutil.SparseFloatBytesToMap(fieldDataList[j].GetVectors().GetSparseFloatVector().Contents[i])
				case schemapb.DataType_Array:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetArrayData().Data[i]
				case schemapb.DataType_JSON:
					if len(fieldDataList[j].ValidData) != 0 && !fieldDataList[j].ValidData[i] {
						row[fieldDataList[j].FieldName] = nil
						continue
					}
					data, ok := fieldDataList[j].GetScalars().Data.(*schemapb.ScalarField_JsonData)
					if ok && !fieldDataList[j].IsDynamic {
						row[fieldDataList[j].FieldName] = string(data.JsonData.Data[i])
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
				if enableInt64 {
					row[DefaultPrimaryFieldName] = int64Pks[i]
				} else {
					row[DefaultPrimaryFieldName] = strconv.FormatInt(int64Pks[i], 10)
				}
			case *schemapb.IDs_StrId:
				stringPks := ids.GetStrId().GetData()
				row[DefaultPrimaryFieldName] = stringPks[i]
			default:
				return nil, fmt.Errorf("the type of primary key(id) is not supported, use other sdk please")
			}
		}
		if scores != nil && int64(len(scores)) > i {
			row[HTTPReturnDistance] = scores[i] // only 8 decimal places
		}
		queryResp = append(queryResp, row)
	}

	return queryResp, nil
}

func formatInt64(intArray []int64) []string {
	stringArray := make([]string, 0, len(intArray))
	for _, i := range intArray {
		stringArray = append(stringArray, strconv.FormatInt(i, 10))
	}
	return stringArray
}

func CheckLimiter(ctx context.Context, req interface{}, pxy types.ProxyComponent) (any, error) {
	if !paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		return nil, nil
	}
	// apply limiter for http/http2 server
	limiter, err := pxy.GetRateLimiter()
	if err != nil {
		log.Error("Get proxy rate limiter for httpV1/V2 server failed", zap.Error(err))
		return nil, err
	}

	dbID, collectionIDToPartIDs, rt, n, err := proxy.GetRequestInfo(ctx, req)
	if err != nil {
		return nil, err
	}
	err = limiter.Check(dbID, collectionIDToPartIDs, rt, n)
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.TotalLabel).Inc()
	if err != nil {
		metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.FailLabel).Inc()
		return proxy.GetFailedResponse(req, err), err
	}
	metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.SuccessLabel).Inc()
	return nil, nil
}

func convertConsistencyLevel(reqConsistencyLevel string) (commonpb.ConsistencyLevel, bool, error) {
	if reqConsistencyLevel != "" {
		level, ok := commonpb.ConsistencyLevel_value[reqConsistencyLevel]
		if !ok {
			return 0, false, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("parameter:'%s' is incorrect, please check it", reqConsistencyLevel))
		}
		return commonpb.ConsistencyLevel(level), false, nil
	}
	// ConsistencyLevel_Bounded default in PyMilvus
	return commonpb.ConsistencyLevel_Bounded, true, nil
}

func convertDefaultValue(value interface{}, dataType schemapb.DataType) (*schemapb.ValueField, error) {
	if value == nil {
		return nil, nil
	}
	switch dataType {
	case schemapb.DataType_Bool:
		v, ok := value.(bool)
		if !ok {
			return nil, merr.WrapErrParameterInvalid("bool", value, "Wrong defaultValue type")
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_BoolData{
				BoolData: v,
			},
		}
		return data, nil

	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		// all passed number is float64 type
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalid("number", value, "Wrong defaultValue type")
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_IntData{
				IntData: int32(v),
			},
		}
		return data, nil

	case schemapb.DataType_Int64:
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalid("number", value, "Wrong defaultValue type")
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_LongData{
				LongData: int64(v),
			},
		}
		return data, nil

	case schemapb.DataType_Float:
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalid("number", value, "Wrong defaultValue type")
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_FloatData{
				FloatData: float32(v),
			},
		}
		return data, nil

	case schemapb.DataType_Double:
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalid("number", value, "Wrong defaultValue type")
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_DoubleData{
				DoubleData: v,
			},
		}
		return data, nil

	case schemapb.DataType_String, schemapb.DataType_VarChar:
		v, ok := value.(string)
		if !ok {
			return nil, merr.WrapErrParameterInvalid("string", value, "Wrong defaultValue type")
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{
				StringData: v,
			},
		}
		return data, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("Unexpected default value type: %s", dataType.String()))
	}
}

func convertToExtraParams(indexParam IndexParam) ([]*commonpb.KeyValuePair, error) {
	var params []*commonpb.KeyValuePair
	if indexParam.IndexType != "" {
		params = append(params, &commonpb.KeyValuePair{Key: common.IndexTypeKey, Value: indexParam.IndexType})
	}
	if indexParam.IndexType == "" {
		for key, value := range indexParam.Params {
			if key == common.IndexTypeKey {
				params = append(params, &commonpb.KeyValuePair{Key: common.IndexTypeKey, Value: fmt.Sprintf("%v", value)})
				break
			}
		}
	}
	if indexParam.MetricType != "" {
		params = append(params, &commonpb.KeyValuePair{Key: common.MetricTypeKey, Value: indexParam.MetricType})
	}
	if len(indexParam.Params) != 0 {
		v, err := json.Marshal(indexParam.Params)
		if err != nil {
			return nil, err
		}
		params = append(params, &commonpb.KeyValuePair{Key: common.IndexParamsKey, Value: string(v)})
	}
	return params, nil
}

func getElementTypeParams(param interface{}) (string, error) {
	if str, ok := param.(string); ok {
		return str, nil
	}

	jsonBytes, err := json.Marshal(param)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func MetricsHandlerFunc(c *gin.Context) {
	path := c.Request.URL.Path
	metrics.RestfulFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10), path,
	).Inc()
	if c.Request.ContentLength >= 0 {
		metrics.RestfulReceiveBytes.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10), path,
		).Add(float64(c.Request.ContentLength))
	}
	start := time.Now()

	// Process request
	c.Next()

	latency := time.Since(start)
	metrics.RestfulReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10), path,
	).Observe(float64(latency.Milliseconds()))

	// see https://github.com/milvus-io/milvus/issues/35767, counter cannot add negative value
	// when response is not written(say timeout/network broken), panicking may happen if not check
	if size := c.Writer.Size(); size > 0 {
		metrics.RestfulSendBytes.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10), path,
		).Add(float64(c.Writer.Size()))
	}
}

func LoggerHandlerFunc() gin.HandlerFunc {
	return gin.LoggerWithConfig(gin.LoggerConfig{
		SkipPaths: proxy.Params.ProxyCfg.GinLogSkipPaths.GetAsStrings(),
		Formatter: func(param gin.LogFormatterParams) string {
			if param.Latency > time.Minute {
				param.Latency = param.Latency.Truncate(time.Second)
			}
			traceID, ok := param.Keys["traceID"]
			if !ok {
				traceID = ""
			}

			accesslog.SetHTTPParams(&param)
			return fmt.Sprintf("[%v] [GIN] [%s] [traceID=%s] [code=%3d] [latency=%v] [client=%s] [method=%s] [error=%s]\n",
				param.TimeStamp.Format("2006/01/02 15:04:05.000 Z07:00"),
				param.Path,
				traceID,
				param.StatusCode,
				param.Latency,
				param.ClientIP,
				param.Method,
				param.ErrorMessage,
			)
		},
	})
}

func RequestHandlerFunc(c *gin.Context) {
	_, err := strconv.ParseBool(c.Request.Header.Get(mhttp.HTTPHeaderAllowInt64))
	if err != nil {
		if paramtable.Get().HTTPCfg.AcceptTypeAllowInt64.GetAsBool() {
			c.Request.Header.Set(mhttp.HTTPHeaderAllowInt64, "true")
		} else {
			c.Request.Header.Set(mhttp.HTTPHeaderAllowInt64, "false")
		}
	}
	c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
	c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
	c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
	c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, OPTIONS, PATCH")
	if c.Request.Method == "OPTIONS" {
		c.AbortWithStatus(204)
		return
	}
	c.Next()
}

func generateTemplateArrayData(list []interface{}) *schemapb.TemplateArrayValue {
	dtype := getTemplateArrayType(list)
	var data *schemapb.TemplateArrayValue
	switch dtype {
	case schemapb.DataType_Bool:
		result := make([]bool, len(list))
		for i, item := range list {
			result[i] = item.(bool)
		}
		data = &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_BoolData{
				BoolData: &schemapb.BoolArray{
					Data: result,
				},
			},
		}
	case schemapb.DataType_String:
		result := make([]string, len(list))
		for i, item := range list {
			result[i] = item.(string)
		}
		data = &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_StringData{
				StringData: &schemapb.StringArray{
					Data: result,
				},
			},
		}
	case schemapb.DataType_Int64:
		result := make([]int64, len(list))
		for i, item := range list {
			result[i] = int64(item.(float64))
		}
		data = &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_LongData{
				LongData: &schemapb.LongArray{
					Data: result,
				},
			},
		}
	case schemapb.DataType_Float:
		result := make([]float64, len(list))
		for i, item := range list {
			result[i] = item.(float64)
		}
		data = &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_DoubleData{
				DoubleData: &schemapb.DoubleArray{
					Data: result,
				},
			},
		}
	case schemapb.DataType_Array:
		result := make([]*schemapb.TemplateArrayValue, len(list))
		for i, item := range list {
			result[i] = generateTemplateArrayData(item.([]interface{}))
		}
		data = &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_ArrayData{
				ArrayData: &schemapb.TemplateArrayValueArray{
					Data: result,
				},
			},
		}
	case schemapb.DataType_JSON:
		result := make([][]byte, len(list))
		for i, item := range list {
			bytes, err := json.Marshal(item)
			// won't happen
			if err != nil {
				panic(fmt.Sprintf("marshal data(%v) fail, please check it!", item))
			}
			result[i] = bytes
		}
		data = &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_JsonData{
				JsonData: &schemapb.JSONArray{
					Data: result,
				},
			},
		}
	// won't happen
	default:
		panic(fmt.Sprintf("Unexpected data(%v) type when generateTemplateArrayData, please check it!", list))
	}
	return data
}

func getTemplateArrayType(value []interface{}) schemapb.DataType {
	dtype := getTemplateType(value[0])

	for _, v := range value {
		if getTemplateType(v) != dtype {
			return schemapb.DataType_JSON
		}
	}
	return dtype
}

func getTemplateType(value interface{}) schemapb.DataType {
	switch v := value.(type) {
	case bool:
		return schemapb.DataType_Bool
	case string:
		return schemapb.DataType_String
	case float64:
		// note: all passed number is float64 type
		// if field type is float64, but value in ExpressionTemplate is int64, it's ok to use TemplateValue_Int64Val to store it
		// it will convert to float64 in ./internal/parser/planparserv2/utils.go, Line 233
		if v == math.Trunc(v) && v >= math.MinInt64 && v <= math.MaxInt64 {
			return schemapb.DataType_Int64
		}
		return schemapb.DataType_Float
	// it won't happen
	// case int64:
	case []interface{}:
		return schemapb.DataType_Array
	default:
		panic(fmt.Sprintf("Unexpected data(%v) when getTemplateType, please check it!", value))
	}
}

func generateExpressionTemplate(params map[string]interface{}) map[string]*schemapb.TemplateValue {
	expressionTemplate := make(map[string]*schemapb.TemplateValue, len(params))

	for name, value := range params {
		dtype := getTemplateType(value)
		var data *schemapb.TemplateValue
		switch dtype {
		case schemapb.DataType_Bool:
			data = &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_BoolVal{
					BoolVal: value.(bool),
				},
			}
		case schemapb.DataType_String:
			data = &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_StringVal{
					StringVal: value.(string),
				},
			}
		case schemapb.DataType_Int64:
			data = &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_Int64Val{
					Int64Val: int64(value.(float64)),
				},
			}
		case schemapb.DataType_Float:
			data = &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_FloatVal{
					FloatVal: value.(float64),
				},
			}
		case schemapb.DataType_Array:
			data = &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_ArrayVal{
					ArrayVal: generateTemplateArrayData(value.([]interface{})),
				},
			}
		default:
			panic(fmt.Sprintf("Unexpected data(%v) when generateExpressionTemplate, please check it!", data))
		}
		expressionTemplate[name] = data
	}
	return expressionTemplate
}

func WrapErrorToResponse(err error) *milvuspb.BoolResponse {
	return &milvuspb.BoolResponse{
		Status: merr.Status(err),
	}
}

// after 2.5.2, all parameters of search_params can be written into one layer
// no more parameters will be written searchParams.params
// to ensure compatibility and milvus can still get a json format parameter
// try to write all the parameters under searchParams into searchParams.Params
func generateSearchParams(reqSearchParams map[string]interface{}) ([]*commonpb.KeyValuePair, error) {
	var searchParams []*commonpb.KeyValuePair
	var params interface{}
	if val, ok := reqSearchParams[Params]; ok {
		params = val
	}

	paramsMap := make(map[string]interface{})
	if params != nil {
		var ok bool
		if paramsMap, ok = params.(map[string]interface{}); !ok {
			return nil, merr.WrapErrParameterInvalidMsg("searchParams.params must be a dict")
		}
	}

	deepEqual := func(value1, value2 interface{}) bool {
		// try to handle 10.0==10
		switch v1 := value1.(type) {
		case float64:
			if v2, ok := value2.(int); ok {
				return v1 == float64(v2)
			}
		case int:
			if v2, ok := value2.(float64); ok {
				return float64(v1) == v2
			}
		}
		return reflect.DeepEqual(value1, value2)
	}

	for key, value := range reqSearchParams {
		if val, ok := paramsMap[key]; ok {
			if !deepEqual(val, value) {
				return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("ambiguous parameter: %s, in search_param: %v, in search_param.params: %v", key, value, val))
			}
		} else if key != Params {
			paramsMap[key] = value
		}
	}

	bs, _ := json.Marshal(paramsMap)
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: Params, Value: string(bs)})

	for key, value := range reqSearchParams {
		if key != Params {
			// for compatibility
			if key == "ignoreGrowing" {
				key = common.IgnoreGrowing
			}
			searchParams = append(searchParams, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
		}
	}
	// need to exposure ParamRoundDecimal in req?
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamRoundDecimal, Value: "-1"})
	return searchParams, nil
}
