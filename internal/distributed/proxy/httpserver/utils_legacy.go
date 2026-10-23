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

// Legacy value handling from 2.6 before the REST value fidelity backport.
// Keep this path unchanged while strictValidation is disabled.
package httpserver

import (
	"bytes"

	"fmt"
	"math"

	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cast"
	"github.com/tidwall/gjson"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus/internal/json"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func legacyJoinArray(data interface{}) string {
	var buffer bytes.Buffer
	arr := reflect.ValueOf(data)

	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			buffer.WriteString(",")
		}

		fmt.Fprintf(&buffer, "%v", arr.Index(i))
	}

	return buffer.String()
}

func legacyConvertRange(field *schemapb.FieldSchema, result gjson.Result) (string, error) {
	var resultStr string
	fieldType := field.DataType

	switch fieldType {
	case schemapb.DataType_Int64:
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
		resultStr = legacyJoinArray(dataArray)
	case schemapb.DataType_VarChar:
		dataArray := make([]string, 0, len(result.Array()))
		for _, data := range result.Array() {
			value, err := cast.ToStringE(data.Str)
			if err != nil {
				return "", err
			}
			dataArray = append(dataArray, fmt.Sprintf(`"%s"`, value))
		}
		resultStr = legacyJoinArray(dataArray)
	}
	return resultStr, nil
}

func legacyCheckGetPrimaryKey(coll *schemapb.CollectionSchema, idResult gjson.Result) (string, error) {
	primaryField, ok := getPrimaryField(coll)
	if !ok {
		return "", merr.WrapErrParameterInvalidMsg("collection: %s has no primary field", coll.Name)
	}
	resultStr, err := legacyConvertRange(primaryField, idResult)
	if err != nil {
		return "", err
	}
	filter := primaryField.Name + " in [" + resultStr + "]"
	return filter, nil
}

func legacyConvertIDsToSchemapbIDs(ids []interface{}, pkField *schemapb.FieldSchema) (*schemapb.IDs, error) {
	if len(ids) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("ids array cannot be empty")
	}

	switch pkField.DataType {
	case schemapb.DataType_Int64:
		int64IDs := make([]int64, 0, len(ids))
		for i, id := range ids {
			var int64ID int64
			switch v := id.(type) {
			case int64:
				int64ID = v
			case int:
				int64ID = int64(v)
			case float64:

				if v != math.Trunc(v) {
					return nil, merr.WrapErrParameterInvalidMsg("invalid int64 id at index %d: %v has fractional part", i, v)
				}
				int64ID = int64(v)
			case string:

				parsed, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidErr(err, "invalid int64 id at index %d: %v", i, id)
				}
				int64ID = parsed
			default:
				return nil, merr.WrapErrParameterInvalidMsg("invalid id type at index %d: expected int64, got %T", i, id)
			}
			int64IDs = append(int64IDs, int64ID)
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: int64IDs,
				},
			},
		}, nil

	case schemapb.DataType_VarChar:
		stringIDs := make([]string, 0, len(ids))
		for i, id := range ids {
			var stringID string
			switch v := id.(type) {
			case string:
				stringID = v
			case int64, int, float64:

				stringID = fmt.Sprintf("%v", v)
			default:
				return nil, merr.WrapErrParameterInvalidMsg("invalid id type at index %d: expected string, got %T", i, id)
			}
			if stringID == "" {
				return nil, merr.WrapErrParameterInvalidMsg("empty string id at index %d", i)
			}
			stringIDs = append(stringIDs, stringID)
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: stringIDs,
				},
			},
		}, nil

	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported primary key type: %s", pkField.DataType.String())
	}
}

func legacyCheckAndSetData(body []byte, collSchema *schemapb.CollectionSchema, partialUpdate bool) ([]map[string]interface{}, map[string][]bool, error) {
	var reallyDataArray []map[string]interface{}
	validDataMap := make(map[string][]bool)
	dataResult := gjson.GetBytes(body, HTTPRequestData)
	dataResultArray := dataResult.Array()
	if len(dataResultArray) == 0 {
		return reallyDataArray, validDataMap, merr.ErrMissingRequiredParameters
	}

	fieldNames := make([]string, 0, len(collSchema.Fields)+len(collSchema.StructArrayFields))
	for _, field := range collSchema.Fields {
		if field.IsDynamic {
			continue
		}
		fieldNames = append(fieldNames, field.Name)
	}
	for _, structField := range collSchema.StructArrayFields {
		fieldNames = append(fieldNames, structField.GetName())
	}

	for _, data := range dataResultArray {
		reallyData := map[string]interface{}{}
		if data.Type == gjson.JSON {
			for _, structField := range collSchema.StructArrayFields {
				rawValue := gjson.Get(data.Raw, structField.GetName())
				if partialUpdate && !rawValue.Exists() {
					continue
				}
				structRow, err := legacyParseStructArrayRow(rawValue.Raw, structField)
				if err != nil {
					return reallyDataArray, validDataMap, err
				}
				reallyData[structField.GetName()] = structRow
			}
			for _, field := range collSchema.Fields {
				if field.IsDynamic {
					continue
				}
				fieldType := field.DataType
				fieldName := field.Name
				fieldValue := data.Get(fieldName)

				if partialUpdate && !fieldValue.Exists() {
					continue
				}

				if field.Nullable || field.DefaultValue != nil {
					value := gjson.Get(data.Raw, fieldName)
					if value.Type == gjson.Null {
						validDataMap[fieldName] = append(validDataMap[fieldName], false)
						continue
					} else {
						validDataMap[fieldName] = append(validDataMap[fieldName], true)
					}
				}

				dataString := fieldValue.String()

				if field.IsPrimaryKey && field.AutoID && len(dataString) == 0 {
					continue
				}

				if field.GetIsFunctionOutput() && dataString == "" {
					continue
				}

				switch fieldType {
				case schemapb.DataType_FloatVector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					var vectorArray []float32
					err := json.Unmarshal([]byte(dataString), &vectorArray)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_BinaryVector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					vectorStr := gjson.Get(data.Raw, fieldName).Raw
					var vectorArray []byte
					err := json.Unmarshal([]byte(vectorStr), &vectorArray)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_SparseFloatVector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					sparseVec, err := typeutil.CreateSparseFloatRowFromJSON([]byte(dataString))
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = sparseVec
				case schemapb.DataType_Float16Vector:
					fallthrough
				case schemapb.DataType_BFloat16Vector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					vectorJSON := gjson.Get(data.Raw, fieldName)

					if vectorJSON.IsArray() {

						var vectorArray []float32
						err := json.Unmarshal([]byte(dataString), &vectorArray)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						reallyData[fieldName] = vectorArray
					} else if vectorJSON.Type == gjson.String {

						vectorStr := gjson.Get(data.Raw, fieldName).Raw
						var vectorArray []byte
						err := json.Unmarshal([]byte(vectorStr), &vectorArray)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						reallyData[fieldName] = vectorArray
					} else {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, "invalid vector field: "+fieldName)
					}
				case schemapb.DataType_Int8Vector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					var vectorArray []int8
					err := json.Unmarshal([]byte(dataString), &vectorArray)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_Bool:
					result, err := cast.ToBoolE(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int8:
					result, err := cast.ToInt8E(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int16:
					result, err := cast.ToInt16E(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int32:
					result, err := cast.ToInt32E(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int64:
					result, err := json.Number(dataString).Int64()
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Array:
					switch field.ElementType {
					case schemapb.DataType_Bool:
						arr := make([]bool, 0)
						err := json.Unmarshal([]byte(dataString), &arr)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
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
				case schemapb.DataType_Geometry:
					reallyData[fieldName] = dataString
				case schemapb.DataType_Float:
					result, err := cast.ToFloat32E(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Double:
					result, err := cast.ToFloat64E(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Timestamptz:
					reallyData[fieldName] = dataString
				case schemapb.DataType_VarChar:
					reallyData[fieldName] = dataString
				case schemapb.DataType_String:
					reallyData[fieldName] = dataString
				default:
					return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid("", schemapb.DataType_name[int32(fieldType)], "fieldName: "+fieldName)
				}
			}

			for mapKey, mapValue := range data.Map() {
				if !containsString(fieldNames, mapKey) {
					if collSchema.EnableDynamicField {
						if mapKey == common.MetaFieldName {
							return nil, nil, merr.WrapErrParameterInvalidMsg("use the invalid field name(%s) when enable dynamicField", mapKey)
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

						default:
							log.Warn("unknown json type found", zap.Int("mapValue.Type", int(mapValue.Type)))
						}
					} else {
						return nil, nil, merr.WrapErrParameterInvalidMsg("has pass more field without dynamic schema, please check it")
					}
				}
			}

			reallyDataArray = append(reallyDataArray, reallyData)
		} else {
			return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(gjson.JSON, data.Type, "NULL:0, FALSE:1, NUMBER:2, STRING:3, TRUE:4, JSON:5")
		}
	}
	return reallyDataArray, validDataMap, nil
}

func legacyParseStructArrayRow(rawJSON string, structSchema *schemapb.StructArrayFieldSchema) (structArrayRow, error) {
	if rawJSON == "" {
		return nil, merr.WrapErrParameterInvalidMsg("missing struct array field: %s", structSchema.GetName())
	}
	parsed := gjson.Parse(rawJSON)
	if !parsed.IsArray() {
		return nil, merr.WrapErrParameterInvalidMsg(
			"struct array field %s expects a JSON array of objects", structSchema.GetName())
	}
	elems := parsed.Array()
	collected := make(map[string][]gjson.Result, len(structSchema.GetFields()))
	expected := make(map[string]struct{}, len(structSchema.GetFields()))
	for _, sub := range structSchema.GetFields() {
		key := subShortName(sub)
		collected[key] = make([]gjson.Result, 0, len(elems))
		expected[key] = struct{}{}
	}
	for idx, elem := range elems {
		if elem.Type != gjson.JSON || !elem.IsObject() {
			return nil, merr.WrapErrParameterInvalidMsg(
				"struct array field %s element #%d must be a JSON object", structSchema.GetName(), idx)
		}
		seen := make(map[string]struct{}, len(structSchema.GetFields()))
		elem.ForEach(func(key, value gjson.Result) bool {
			name := key.String()
			if _, ok := expected[name]; !ok {
				return true
			}
			collected[name] = append(collected[name], value)
			seen[name] = struct{}{}
			return true
		})
		if len(seen) != len(expected) {
			for name := range expected {
				if _, ok := seen[name]; !ok {
					return nil, merr.WrapErrParameterInvalidMsg(
						"struct array field %s element #%d missing sub-field %s",
						structSchema.GetName(), idx, name)
				}
			}
		}
	}

	row := structArrayRow{}
	for _, sub := range structSchema.GetFields() {
		key := subShortName(sub)
		vals := collected[key]
		switch sub.GetDataType() {
		case schemapb.DataType_Array:
			scalar, err := legacyBuildStructSubArrayScalar(sub, vals)
			if err != nil {
				return nil, err
			}
			row[key] = scalar
		case schemapb.DataType_ArrayOfVector:
			vec, err := legacyBuildStructSubVectorField(sub, vals)
			if err != nil {
				return nil, err
			}
			row[key] = vec
		default:
			return nil, merr.WrapErrParameterInvalidMsg(
				"sub-field %s of struct %s has unsupported data type %s",
				key, structSchema.GetName(), sub.GetDataType())
		}
	}
	return row, nil
}

func legacyBuildStructSubArrayScalar(sub *schemapb.FieldSchema, vals []gjson.Result) (*schemapb.ScalarField, error) {
	switch sub.GetElementType() {
	case schemapb.DataType_Bool:
		arr := make([]bool, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.True && v.Type != gjson.False {
				return nil, wrapStructSubParseError(sub, v, "expect bool")
			}
			arr = append(arr, v.Bool())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		arr := make([]int32, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect integer")
			}
			arr = append(arr, int32(v.Int()))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int64:
		arr := make([]int64, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect integer")
			}
			arr = append(arr, v.Int())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: arr}},
		}, nil
	case schemapb.DataType_Float:
		arr := make([]float32, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect float")
			}
			arr = append(arr, float32(v.Float()))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: arr}},
		}, nil
	case schemapb.DataType_Double:
		arr := make([]float64, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect double")
			}
			arr = append(arr, v.Float())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: arr}},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		arr := make([]string, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.String {
				return nil, wrapStructSubParseError(sub, v, "expect string")
			}
			arr = append(arr, v.String())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: arr}},
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s has unsupported array element type %s",
			sub.GetName(), sub.GetElementType())
	}
}

func legacyBuildStructSubVectorField(sub *schemapb.FieldSchema, vals []gjson.Result) (*schemapb.VectorField, error) {
	dim, err := getDim(sub)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s: %s", sub.GetName(), err.Error())
	}
	switch sub.GetElementType() {
	case schemapb.DataType_FloatVector:
		packed := &schemapb.FloatArray{}
		for _, v := range vals {
			if !v.IsArray() {
				return nil, wrapStructSubParseError(sub, v, "expect float vector array")
			}
			var row []float32
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s vector dim mismatch: expect %d, got %d",
					sub.GetName(), dim, len(row))
			}
			packed.Data = append(packed.Data, row...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_FloatVector{FloatVector: packed},
		}, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		isFloat16 := sub.GetElementType() == schemapb.DataType_Float16Vector
		bytesPerVec := dim * 2
		buf := make([]byte, 0, int(bytesPerVec)*len(vals))
		for _, v := range vals {
			b, err := decodeByteVectorElement(v, dim, bytesPerVec, isFloat16)
			if err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			buf = append(buf, b...)
		}
		if isFloat16 {
			return &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: buf},
			}, nil
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: buf},
		}, nil
	case schemapb.DataType_BinaryVector:
		bytesPerVec := dim / 8
		buf := make([]byte, 0, int(bytesPerVec)*len(vals))
		for _, v := range vals {
			if v.Type != gjson.String {
				return nil, wrapStructSubParseError(sub, v, "binary vector must be base64-encoded string")
			}
			var row []byte
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != bytesPerVec {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s binary vector byte-length mismatch: expect %d, got %d",
					sub.GetName(), bytesPerVec, len(row))
			}
			buf = append(buf, row...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: buf},
		}, nil
	case schemapb.DataType_Int8Vector:
		buf := make([]byte, 0, int(dim)*len(vals))
		for _, v := range vals {
			if !v.IsArray() {
				return nil, wrapStructSubParseError(sub, v, "expect int8 vector array")
			}
			var row []int8
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s int8 vector dim mismatch: expect %d, got %d",
					sub.GetName(), dim, len(row))
			}
			for _, x := range row {
				buf = append(buf, byte(x))
			}
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Int8Vector{Int8Vector: buf},
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s has unsupported vector element type %s",
			sub.GetName(), sub.GetElementType())
	}
}

func legacyEncodeEmbListQuery(vecs []gjson.Result, elemType schemapb.DataType, dim int64, qIdx int) ([]byte, error) {
	switch elemType {
	case schemapb.DataType_FloatVector:
		buf := make([]byte, 0, int(dim*4)*len(vecs))
		for vIdx, v := range vecs {
			if !v.IsArray() {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] must be a float vector array", qIdx, vIdx)
			}
			var row []float32
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] dim mismatch: expect %d got %d", qIdx, vIdx, dim, len(row))
			}
			buf = append(buf, typeutil.Float32ArrayToBytes(row)...)
		}
		return buf, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		isFloat16 := elemType == schemapb.DataType_Float16Vector
		bytesPerVec := dim * 2
		buf := make([]byte, 0, int(bytesPerVec)*len(vecs))
		for vIdx, v := range vecs {
			b, err := decodeByteVectorElement(v, dim, bytesPerVec, isFloat16)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d]: %s", qIdx, vIdx, err.Error())
			}
			buf = append(buf, b...)
		}
		return buf, nil
	case schemapb.DataType_BinaryVector:
		bytesPerVec := dim / 8
		buf := make([]byte, 0, int(bytesPerVec)*len(vecs))
		for vIdx, v := range vecs {
			if v.Type != gjson.String {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] binary vector must be base64-encoded string", qIdx, vIdx)
			}
			var row []byte
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != bytesPerVec {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] byte-length mismatch: expect %d got %d", qIdx, vIdx, bytesPerVec, len(row))
			}
			buf = append(buf, row...)
		}
		return buf, nil
	case schemapb.DataType_Int8Vector:
		buf := make([]byte, 0, int(dim)*len(vecs))
		for vIdx, v := range vecs {
			if !v.IsArray() {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] must be an int8 vector array", qIdx, vIdx)
			}
			var row []int8
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] dim mismatch: expect %d got %d", qIdx, vIdx, dim, len(row))
			}
			buf = append(buf, typeutil.Int8ArrayToBytes(row)...)
		}
		return buf, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"unsupported embedding list element type %s", elemType)
	}
}

func legacyAnyToColumns(rows []map[string]interface{}, validDataMap map[string][]bool, sch *schemapb.CollectionSchema, inInsert bool, partialUpdate bool) ([]*schemapb.FieldData, error) {
	rowsLen := len(rows)
	if rowsLen == 0 {
		return []*schemapb.FieldData{}, merr.WrapErrParameterInvalidMsg("no row need to be convert to columns")
	}

	isDynamic := sch.EnableDynamicField
	allowInsertAutoID, _ := common.IsAllowInsertAutoID(sch.GetProperties()...)
	isAutoIDPK := false
	pkFieldName := ""

	nameColumns := make(map[string]interface{})
	nameDims := make(map[string]int64)
	fieldData := make(map[string]*schemapb.FieldData)

	presentFieldNames := make(map[string]struct{})
	for _, row := range rows {
		for name := range row {
			presentFieldNames[name] = struct{}{}
		}
	}

	for _, field := range sch.Fields {
		if field.IsPrimaryKey {
			pkFieldName = field.Name
			if field.AutoID {
				isAutoIDPK = true
			}
		}
		if (field.IsPrimaryKey && field.AutoID && inInsert && !allowInsertAutoID) || field.IsDynamic {
			continue
		}

		if field.GetIsFunctionOutput() {
			if _, ok := presentFieldNames[field.Name]; !ok {
				continue
			}
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
		case schemapb.DataType_Timestamptz:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_String:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_VarChar:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_Array:
			data = make([]*schemapb.ScalarField, 0, rowsLen)
		case schemapb.DataType_JSON:
			data = make([][]byte, 0, rowsLen)
		case schemapb.DataType_Geometry:
			data = make([]string, 0, rowsLen)
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
		case schemapb.DataType_Int8Vector:
			data = make([][]int8, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		default:
			return nil, merr.WrapErrParameterInvalidMsg("the type(%v) of field(%v) is not supported, use other sdk please", field.DataType, field.Name)
		}
		nameColumns[field.Name] = data
		fieldData[field.Name] = &schemapb.FieldData{
			Type:      field.DataType,
			FieldName: field.Name,
			FieldId:   field.FieldID,
			IsDynamic: field.IsDynamic,
		}
	}
	if len(nameDims) == 0 && len(sch.Functions) == 0 && !partialUpdate {
		return nil, merr.WrapErrParameterInvalidMsg("collection: %s has no vector field or functions", sch.Name)
	}

	dynamicCol := make([][]byte, 0, rowsLen)
	fieldLen := make(map[string]int)

	for _, row := range rows {

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
				if !ok {
					continue
				}
				if !allowInsertAutoID {
					return nil, merr.WrapErrParameterInvalidMsg("no need to pass pk field(%s) when autoid==true in insert", field.Name)
				}
			}
			if (field.Nullable || field.DefaultValue != nil) && !ok {
				continue
			}
			if field.GetIsFunctionOutput() {
				if _, allocated := nameColumns[field.Name]; !allocated {
					continue
				}
			}
			if !ok {
				if partialUpdate {
					continue
				}
				return nil, merr.WrapErrParameterInvalidMsg("row %d does not has field %s", idx, field.Name)
			}
			fieldLen[field.Name] += 1
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
			case schemapb.DataType_Timestamptz:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]string), candi.v.Interface().(string))
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
			case schemapb.DataType_Geometry:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]string), candi.v.Interface().(string))
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
					return nil, merr.WrapErrParameterInvalidMsg("invalid type(%v) of field(%v) ", field.DataType, field.Name)
				}
			case schemapb.DataType_BFloat16Vector:
				switch candi.v.Interface().(type) {
				case []byte:
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
				case []float32:
					vec := typeutil.Float32ArrayToBFloat16Bytes(candi.v.Interface().([]float32))
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), vec)
				default:
					return nil, merr.WrapErrParameterInvalidMsg("invalid type(%v) of field(%v) ", field.DataType, field.Name)
				}
			case schemapb.DataType_SparseFloatVector:
				content := candi.v.Interface().([]byte)
				rowSparseDim := typeutil.SparseFloatRowDim(content)
				if rowSparseDim > nameDims[field.Name] {
					nameDims[field.Name] = rowSparseDim
				}
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), content)
			case schemapb.DataType_Int8Vector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]int8), candi.v.Interface().([]int8))
			default:
				return nil, merr.WrapErrParameterInvalidMsg("the type(%v) of field(%v) is not supported, use other sdk please", field.DataType, field.Name)
			}

			delete(set, field.Name)
		}
		for _, structField := range sch.GetStructArrayFields() {
			delete(set, structField.GetName())
		}

		if isDynamic {
			m := make(map[string]interface{})
			for name, candi := range set {
				m[name] = candi.v.Interface()
			}
			bs, err := json.Marshal(m)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidErr(err, "failed to marshal dynamic field")
			}
			dynamicCol = append(dynamicCol, bs)
		}
	}
	columns := make([]*schemapb.FieldData, 0, len(nameColumns))
	for name, column := range nameColumns {
		validData, hasValidData := validDataMap[name]
		if fieldLen[name] == 0 && name == pkFieldName && isAutoIDPK {
			continue
		}
		if fieldLen[name] == 0 && partialUpdate {
			if hasValidData {
				if len(validData) != rowsLen {
					log.Info("field len is not equal to rows len",
						zap.String("fieldName", name),
						zap.Int("fieldLen", len(validData)),
						zap.Int("rowsLen", rowsLen))
					return nil, merr.WrapErrParameterInvalidMsg("column %s has length %d, expected %d", name, len(validData), rowsLen)
				}
			} else {
				log.Info("skip empty field for partial update",
					zap.String("fieldName", name))
				continue
			}
		}
		if fieldLen[name] != rowsLen && partialUpdate && (!hasValidData || len(validData) != rowsLen) {

			log.Info("field len is not equal to rows len",
				zap.String("fieldName", name),
				zap.Int("fieldLen", fieldLen[name]),
				zap.Int("rowsLen", rowsLen))
			return nil, merr.WrapErrParameterInvalidMsg("column %s has length %d, expected %d", name, fieldLen[name], rowsLen)
		}

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
		case schemapb.DataType_Timestamptz:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: column.([]string),
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
		case schemapb.DataType_Geometry:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{
						GeometryWktData: &schemapb.GeometryWktArray{
							Data: column.([]string),
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
		case schemapb.DataType_Int8Vector:
			dim := nameDims[name]
			arr, err := convertInt8VectorToArray(column.([][]int8), dim)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: arr,
					},
				},
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("the type(%v) of field(%v) is not supported, use other sdk please", colData.Type, name)
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
	for _, structField := range sch.GetStructArrayFields() {
		perRow := make([]structArrayRow, 0, rowsLen)
		for rowIdx, row := range rows {
			val, ok := row[structField.GetName()]
			if !ok {
				if partialUpdate {
					continue
				}
				return nil, merr.WrapErrParameterInvalidMsg("row %d does not has struct field %s", rowIdx, structField.GetName())
			}
			sr, ok := val.(structArrayRow)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("row %d struct field %s has unexpected payload type %T",
					rowIdx, structField.GetName(), val)
			}
			perRow = append(perRow, sr)
		}
		if len(perRow) == 0 {
			continue
		}
		structFieldData, err := buildStructArrayFieldData(structField, perRow)
		if err != nil {
			return nil, err
		}
		columns = append(columns, structFieldData)
	}
	return columns, nil
}

func legacySerializeFloatVectors(vectorStr string, dataType schemapb.DataType, dimension, bytesLen int64, fpArrayToBytesFunc func([]float32) []byte) ([][]byte, error) {
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

func legacySerializeFloatOrByteVectors(jsonResult gjson.Result, dataType schemapb.DataType, dimension int64, fpArrayToBytesFunc func([]float32) []byte) ([][]byte, error) {
	firstElement := jsonResult.Get("0")

	if firstElement.IsArray() {
		return legacySerializeFloatVectors(jsonResult.Raw, dataType, dimension, dimension*2, fpArrayToBytesFunc)
	} else if firstElement.Type == gjson.String || !firstElement.Exists() {

		return serializeByteVectors(jsonResult.Raw, dataType, dimension, dimension*2)
	}
	return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], jsonResult.Raw, "invalid type")
}

func legacySerializeSparseFloatVectors(vectors []gjson.Result, dataType schemapb.DataType) ([][]byte, error) {
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

func legacySerializeInt8Vectors(vectorStr string, dataType schemapb.DataType, dimension int64, int8ArrayToBytesFunc func([]int8) []byte) ([][]byte, error) {
	var int8Values [][]int8
	err := json.Unmarshal([]byte(vectorStr), &int8Values)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr, err.Error())
	}
	values := make([][]byte, 0, len(int8Values))
	for _, vectorArray := range int8Values {
		if int64(len(vectorArray)) != dimension {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr,
				fmt.Sprintf("dimension: %d, but length of []int8: %d", dimension, len(vectorArray)))
		}
		vectorBytes := int8ArrayToBytesFunc(vectorArray)
		values = append(values, vectorBytes)
	}
	return values, nil
}

func legacyConvertQueries2Placeholder(body string, dataType schemapb.DataType, dimension int64) (*commonpb.PlaceholderValue, error) {
	var valueType commonpb.PlaceholderType
	var values [][]byte
	var err error
	switch dataType {
	case schemapb.DataType_FloatVector:
		valueType = commonpb.PlaceholderType_FloatVector
		values, err = legacySerializeFloatVectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, dimension*4, typeutil.Float32ArrayToBytes)
	case schemapb.DataType_BinaryVector:
		valueType = commonpb.PlaceholderType_BinaryVector
		values, err = serializeByteVectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, dimension/8)
	case schemapb.DataType_Float16Vector:
		valueType = commonpb.PlaceholderType_Float16Vector
		values, err = legacySerializeFloatOrByteVectors(gjson.Get(body, HTTPRequestData), dataType, dimension, typeutil.Float32ArrayToFloat16Bytes)
	case schemapb.DataType_BFloat16Vector:
		valueType = commonpb.PlaceholderType_BFloat16Vector
		values, err = legacySerializeFloatOrByteVectors(gjson.Get(body, HTTPRequestData), dataType, dimension, typeutil.Float32ArrayToBFloat16Bytes)
	case schemapb.DataType_SparseFloatVector:
		valueType = commonpb.PlaceholderType_SparseFloatVector
		values, err = legacySerializeSparseFloatVectors(gjson.Get(body, HTTPRequestData).Array(), dataType)
	case schemapb.DataType_Int8Vector:
		valueType = commonpb.PlaceholderType_Int8Vector
		values, err = legacySerializeInt8Vectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, typeutil.Int8ArrayToBytes)
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

func legacyBuildQueryResp(rowsNum int64, needFields []string, fieldDataList []*schemapb.FieldData, ids *schemapb.IDs,
	scores []float32, enableInt64 bool, collectionSchema *schemapb.CollectionSchema,
) ([]map[string]interface{}, error) {
	columnNum := len(fieldDataList)
	if rowsNum == int64(0) {
		if columnNum > 0 {
			var err error
			rowsNum, err = fieldDataRowCount(fieldDataList[0])
			if err != nil {
				return nil, err
			}
		} else if ids != nil {
			switch ids.GetIdField().(type) {
			case *schemapb.IDs_IntId:
				int64Pks := ids.GetIntId().GetData()
				rowsNum = int64(len(int64Pks))
			case *schemapb.IDs_StrId:
				stringPks := ids.GetStrId().GetData()
				rowsNum = int64(len(stringPks))
			default:
				return nil, merr.WrapErrParameterInvalidMsg("the type of primary key(id) is not supported, use other sdk please")
			}
		}
	}
	if rowsNum == int64(0) {
		return []map[string]interface{}{}, nil
	}
	fieldDataAccessors := make([]*fieldDataRowAccessor, 0, columnNum)
	for _, fieldData := range fieldDataList {
		accessor, err := newFieldDataRowAccessor(fieldData)
		if err != nil {
			return nil, err
		}
		fieldDataAccessors = append(fieldDataAccessors, accessor)
	}
	queryResp := make([]map[string]interface{}, 0, rowsNum)
	dynamicOutputFields := genDynamicFields(needFields, fieldDataList)

	pkFieldName := DefaultPrimaryFieldName
	if collectionSchema != nil {
		fieldsSchema := collectionSchema.GetFields()
		for _, field := range fieldsSchema {
			if field.GetIsPrimaryKey() {
				pkFieldName = field.GetName()
				break
			}
		}
	}
	for i := int64(0); i < rowsNum; i++ {
		row := map[string]interface{}{}
		if columnNum > 0 {
			for j := 0; j < columnNum; j++ {
				fieldData := fieldDataList[j]
				dataIdx, valid, err := fieldDataAccessors[j].rowIndex(i)
				if err != nil {
					return nil, err
				}
				if !valid {
					if !fieldData.GetIsDynamic() {
						row[fieldData.GetFieldName()] = nil
					}
					continue
				}
				switch fieldDataList[j].GetType() {
				case schemapb.DataType_Bool:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetBoolData().GetData()[dataIdx]
				case schemapb.DataType_Int8:
					row[fieldDataList[j].GetFieldName()] = int8(fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx])
				case schemapb.DataType_Int16:
					row[fieldDataList[j].GetFieldName()] = int16(fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx])
				case schemapb.DataType_Int32:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx]
				case schemapb.DataType_Int64:
					if enableInt64 {
						row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetLongData().GetData()[dataIdx]
					} else {
						row[fieldDataList[j].GetFieldName()] = strconv.FormatInt(fieldDataList[j].GetScalars().GetLongData().GetData()[dataIdx], 10)
					}
				case schemapb.DataType_Float:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetFloatData().GetData()[dataIdx]
				case schemapb.DataType_Double:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetDoubleData().GetData()[dataIdx]
				case schemapb.DataType_Timestamptz:
					if fieldDataList[j].GetScalars().GetTimestamptzData() != nil {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetTimestamptzData().GetData()[dataIdx]
					} else {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetStringData().GetData()[dataIdx]
					}
				case schemapb.DataType_String:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetStringData().GetData()[dataIdx]
				case schemapb.DataType_VarChar:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetStringData().GetData()[dataIdx]
				case schemapb.DataType_BinaryVector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetBinaryVector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()/8) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()/8)]
				case schemapb.DataType_FloatVector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetFloatVector().GetData()[dataIdx*fieldDataList[j].GetVectors().GetDim() : (dataIdx+1)*fieldDataList[j].GetVectors().GetDim()]
				case schemapb.DataType_Float16Vector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetFloat16Vector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()*2) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
				case schemapb.DataType_BFloat16Vector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetBfloat16Vector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()*2) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
				case schemapb.DataType_SparseFloatVector:
					row[fieldDataList[j].GetFieldName()] = typeutil.SparseFloatBytesToMap(fieldDataList[j].GetVectors().GetSparseFloatVector().Contents[dataIdx])
				case schemapb.DataType_Int8Vector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetInt8Vector()[dataIdx*fieldDataList[j].GetVectors().GetDim() : (dataIdx+1)*fieldDataList[j].GetVectors().GetDim()]
				case schemapb.DataType_Array:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetArrayData().GetData()[dataIdx]
				case schemapb.DataType_JSON:
					data, ok := fieldDataList[j].GetScalars().GetData().(*schemapb.ScalarField_JsonData)
					if ok && !fieldDataList[j].GetIsDynamic() {
						row[fieldDataList[j].GetFieldName()] = string(data.JsonData.GetData()[dataIdx])
					} else {
						var dataMap map[string]interface{}

						err := json.Unmarshal(fieldDataList[j].GetScalars().GetJsonData().Data[dataIdx], &dataMap)
						if err != nil {
							log.Error(fmt.Sprintf("[BuildQueryResp] Unmarshal error %s", err.Error()))
							return nil, err
						}

						if containsString(dynamicOutputFields, fieldDataList[j].GetFieldName()) {
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
				case schemapb.DataType_Geometry:
					if fieldDataList[j].GetScalars().GetGeometryData() != nil {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetGeometryData().GetData()[dataIdx]
					} else {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetGeometryWktData().Data[dataIdx]
					}
				case schemapb.DataType_ArrayOfStruct:
					structRow, err := extractStructArrayRow(fieldDataList[j], int(dataIdx), collectionSchema)
					if err != nil {
						return nil, err
					}
					row[fieldDataList[j].GetFieldName()] = structRow
				default:
					row[fieldDataList[j].GetFieldName()] = ""
				}
			}
		}
		if ids != nil {
			switch ids.GetIdField().(type) {
			case *schemapb.IDs_IntId:
				int64Pks := ids.GetIntId().GetData()
				if enableInt64 {
					row[pkFieldName] = int64Pks[i]
				} else {
					row[pkFieldName] = strconv.FormatInt(int64Pks[i], 10)
				}
			case *schemapb.IDs_StrId:
				stringPks := ids.GetStrId().GetData()
				row[pkFieldName] = stringPks[i]
			default:
				return nil, merr.WrapErrParameterInvalidMsg("the type of primary key(id) is not supported, use other sdk please")
			}
		}
		if scores != nil && int64(len(scores)) > i {
			row[HTTPReturnDistance] = scores[i]
		}
		queryResp = append(queryResp, row)
	}

	return queryResp, nil
}

func legacyGenerateTemplateArrayData(list []interface{}) *schemapb.TemplateArrayValue {
	dtype := legacyGetTemplateArrayType(list)
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
			result[i] = legacyGenerateTemplateArrayData(item.([]interface{}))
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

	default:
		panic(fmt.Sprintf("Unexpected data(%v) type when generateTemplateArrayData, please check it!", list))
	}
	return data
}

func legacyGetTemplateArrayType(value []interface{}) schemapb.DataType {
	dtype := legacyGetTemplateType(value[0])

	for _, v := range value {
		if legacyGetTemplateType(v) != dtype {
			return schemapb.DataType_JSON
		}
	}
	return dtype
}

func legacyGetTemplateType(value interface{}) schemapb.DataType {
	switch v := value.(type) {
	case bool:
		return schemapb.DataType_Bool
	case string:
		return schemapb.DataType_String
	case float64:

		if v == math.Trunc(v) && v >= math.MinInt64 && v <= math.MaxInt64 {
			return schemapb.DataType_Int64
		}
		return schemapb.DataType_Float

	case []interface{}:
		return schemapb.DataType_Array
	default:
		panic(fmt.Sprintf("Unexpected data(%v) when getTemplateType, please check it!", value))
	}
}

func legacyGenerateExpressionTemplate(params map[string]interface{}) map[string]*schemapb.TemplateValue {
	expressionTemplate := make(map[string]*schemapb.TemplateValue, len(params))

	for name, value := range params {
		dtype := legacyGetTemplateType(value)
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
					ArrayVal: legacyGenerateTemplateArrayData(value.([]interface{})),
				},
			}
		default:
			panic(fmt.Sprintf("Unexpected data(%v) when generateExpressionTemplate, please check it!", data))
		}
		expressionTemplate[name] = data
	}
	return expressionTemplate
}
