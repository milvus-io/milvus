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

package csv

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/nullutil"
	pkgcommon "github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type RowParser interface {
	Parse(raw []string) (Row, error)
}
type rowParser struct {
	nullkey              string
	header               []string
	name2Dim             map[string]int
	name2Field           map[string]*schemapb.FieldSchema
	name2StructField     map[string]*schemapb.StructArrayFieldSchema
	structArrays         map[string]map[string]*schemapb.FieldSchema
	structArraySubFields map[string]interface{}
	pkField              *schemapb.FieldSchema
	dynamicField         *schemapb.FieldSchema
	allowInsertAutoID    bool

	timezone string
}

func NewRowParser(schema *schemapb.CollectionSchema, header []string, nullkey string) (RowParser, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	dynamicField := typeutil.GetDynamicField(schema)

	functionOutputFields := make(map[string]int64)
	for _, field := range schema.GetFields() {
		if field.GetIsFunctionOutput() {
			functionOutputFields[field.GetName()] = field.GetFieldID()
		}
	}

	allFields := typeutil.GetAllFieldSchemas(schema)

	name2Field := lo.SliceToMap(
		lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
			return !field.GetIsFunctionOutput() && !typeutil.IsAutoPKField(field) && field.GetName() != dynamicField.GetName()
		}),
		func(field *schemapb.FieldSchema) (string, *schemapb.FieldSchema) {
			return field.GetName(), field
		},
	)

	structArrays := make(map[string]map[string]*schemapb.FieldSchema)
	name2StructField := make(map[string]*schemapb.StructArrayFieldSchema)

	structArraySubFields := make(map[string]interface{})
	for _, sa := range schema.GetStructArrayFields() {
		name2StructField[sa.GetName()] = sa
		structArrays[sa.GetName()] = make(map[string]*schemapb.FieldSchema)
		for _, subField := range sa.GetFields() {
			structArraySubFields[subField.GetName()] = nil
			structArrays[sa.GetName()][subField.GetName()] = subField
		}
	}

	name2Dim := make(map[string]int)
	for name, field := range name2Field {
		if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			name2Dim[name] = int(dim)
		}
	}

	// check whether csv header contains all fields in schema
	// except auto generated primary key and dynamic field
	headerMap := make(map[string]bool)
	for _, name := range header {
		headerMap[name] = true
	}
	allowInsertAutoID, _ := pkgcommon.IsAllowInsertAutoID(schema.GetProperties()...)

	// check if csv header provides the primary key while it should be auto-generated
	_, pkInHeader := headerMap[pkField.GetName()]
	if pkInHeader && pkField.GetAutoID() && !allowInsertAutoID {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", pkField.GetName()))
	}

	// function output field, don't provide
	for fieldName := range functionOutputFields {
		_, existInHeader := headerMap[fieldName]
		if existInHeader {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("the field '%s' is output by function, no need to provide", fieldName))
		}
	}

	for fieldName, field := range name2Field {
		_, existInHeader := headerMap[fieldName]
		_, subField := structArraySubFields[fieldName]
		if field.GetNullable() || field.GetDefaultValue() != nil || subField {
			// nullable/defaultValue fields, provide or not provide both ok
		} else if !existInHeader {
			// not nullable/defaultValue/autoPK/functionOutput fields, must provide
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("value of field is missed: '%s'", field.GetName()))
		}
	}
	return &rowParser{
		nullkey:              nullkey,
		name2Dim:             name2Dim,
		header:               header,
		name2Field:           name2Field,
		name2StructField:     name2StructField,
		structArrays:         structArrays,
		structArraySubFields: structArraySubFields,
		pkField:              pkField,
		dynamicField:         dynamicField,
		allowInsertAutoID:    allowInsertAutoID,
		timezone:             common.GetSchemaTimezone(schema),
	}, nil
}

// reconstructArrayForStructArray reconstructs the StructArray data format from CSV.
// StructArray are passed in with format for one row: StructArray: [element 1, element 2, ...]
// where each element contains one value of all sub-fields in StructArrayField.
// So we need to reconstruct it to be handled by handleField.
//
// For example, let StructArrayFieldSchema { sub-field1: array of int32, sub-field2: array of float vector }
// When we have one row in CSV (as JSON string):
//
//	"[{\"sub-field1\": 1, \"sub-field2\": [1.0, 2.0]}, {\"sub-field1\": 2, \"sub-field2\": [3.0, 4.0]}]"
//
// we reconstruct it to be handled by handleField as:
//
//	{"struct[sub-field1]": "[1, 2]", "struct[sub-field2]": "[[1.0, 2.0], [3.0, 4.0]]"}
func (r *rowParser) reconstructArrayForStructArray(structName string, subFieldsMap map[string]*schemapb.FieldSchema, raw string) (map[string][]any, error) {
	// Parse the JSON array string
	var structs []any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&structs); err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("invalid StructArray format in CSV, failed to parse JSON: %v", err))
	}

	flatStructs := make(map[string][]any)
	for _, elem := range structs {
		dict, ok := elem.(map[string]any)
		if !ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("invalid element in StructArray, expect map[string]any but got type %T", elem))
		}
		for key, value := range dict {
			fieldName := typeutil.ConcatStructFieldName(structName, key)
			_, ok := subFieldsMap[fieldName]
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("field %s not found", fieldName))
			}

			flatStructs[fieldName] = append(flatStructs[fieldName], value)
		}
	}
	return flatStructs, nil
}

func (r *rowParser) Parse(strArr []string) (Row, error) {
	if len(strArr) != len(r.header) {
		return nil, merr.WrapErrImportFailed("the number of fields in the row is not equal to the header")
	}

	row := make(Row)
	dynamicValues := make(map[string]string)
	// read values from csv file
	for index, value := range strArr {
		csvFieldName := r.header[index]
		if subFieldsMap, ok := r.structArrays[csvFieldName]; ok {
			_, ok := r.name2StructField[csvFieldName]
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("struct field %s is not found in schema", csvFieldName))
			}
			flatStructs, err := r.reconstructArrayForStructArray(r.header[index], subFieldsMap, value)
			if err != nil {
				return nil, err
			}
			for subKey, subValues := range flatStructs {
				field, ok := r.name2Field[subKey]
				if !ok {
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("sub field %s of struct field %s is not found in schema", subKey, csvFieldName))
				}
				// TODO: how to get max capacity from a StructFieldSchema?
				data, err := r.parseStructEntity(field, subValues)
				if err != nil {
					return nil, err
				}
				row[field.GetFieldID()] = data
			}
		} else if field, ok := r.name2Field[csvFieldName]; ok {
			data, err := r.parseEntity(field, value, false)
			if err != nil {
				return nil, err
			}
			row[field.GetFieldID()] = data
		} else if r.dynamicField != nil {
			dynamicValues[r.header[index]] = value
		} else if r.pkField.GetName() == r.header[index] && r.pkField.GetAutoID() && r.allowInsertAutoID {
			data, err := r.parseEntity(r.pkField, value, false)
			if err != nil {
				return nil, err
			}
			row[r.pkField.GetFieldID()] = data
		} else {
			// from v2.6, we don't intend to return error for redundant fields, just skip it
			continue
		}
	}

	// if nullable/defaultValue fields have no values, fill with nil or default value
	for fieldName, field := range r.name2Field {
		fieldID := field.GetFieldID()
		if _, ok := row[fieldID]; !ok {
			if field.GetNullable() {
				row[fieldID] = nil
			}
			if field.GetDefaultValue() != nil {
				data, err := nullutil.GetDefaultValue(field)
				if err != nil {
					return nil, err
				}
				row[fieldID] = data
			}
		}
		_, subField := r.structArraySubFields[fieldName]
		if _, ok := row[fieldID]; !ok && !subField {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("value of field '%s' is missed", fieldName))
		}
	}

	// combine the redundant pairs into dynamic field
	// for csv which is directly uploaded to minio, it's necessary to check and put the fields not in schema into dynamic field
	err := r.combineDynamicRow(dynamicValues, row)
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *rowParser) combineDynamicRow(dynamicValues map[string]string, row Row) error {
	if r.dynamicField == nil {
		return nil
	}

	dynamicFieldID := r.dynamicField.GetFieldID()
	MetaName := r.dynamicField.GetName()
	if len(dynamicValues) == 0 {
		row[dynamicFieldID] = []byte("{}")
		return nil
	}

	newDynamicValues := make(map[string]any)
	if str, ok := dynamicValues[MetaName]; ok {
		// parse $meta field to json object
		var mp map[string]interface{}
		err := json.Unmarshal([]byte(str), &mp)
		if err != nil {
			return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON format string")
		}
		// put the all dynamic fields into newDynamicValues
		for k, v := range mp {
			if _, ok = dynamicValues[k]; ok {
				return merr.WrapErrImportFailed(fmt.Sprintf("duplicated key in dynamic field, key=%s", k))
			}
			newDynamicValues[k] = v
		}
		// remove $meta field from dynamicValues
		delete(dynamicValues, MetaName)
	}
	// put dynamic fields (except $meta) into newDynamicValues
	// due to the limit of csv, the number value is stored as string
	for k, v := range dynamicValues {
		newDynamicValues[k] = v
	}

	// check if stasify the json format
	dynamicBytes, err := json.Marshal(newDynamicValues)
	if err != nil {
		return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON object")
	}
	row[dynamicFieldID] = dynamicBytes
	return nil
}

func (r *rowParser) parseStructEntity(field *schemapb.FieldSchema, values []any) (any, error) {
	dataType := field.GetDataType()
	switch dataType {
	case schemapb.DataType_ArrayOfVector:
		maxCapacity, err := parameterutil.GetMaxCapacity(field)
		if err != nil {
			return nil, err
		}
		if err := common.CheckArrayCapacity(len(values), maxCapacity, field); err != nil {
			return nil, err
		}
		vectorFieldData, err := r.arrayOfVectorToFieldData(values, field)
		if err != nil {
			return nil, err
		}
		return vectorFieldData, nil
	case schemapb.DataType_Array:
		maxCapacity, err := parameterutil.GetMaxCapacity(field)
		if err != nil {
			return nil, err
		}
		if err := common.CheckArrayCapacity(len(values), maxCapacity, field); err != nil {
			return nil, err
		}

		// elements in array not support null value
		scalarFieldData, err := r.arrayToFieldData(values, field)
		if err != nil {
			return nil, err
		}
		return scalarFieldData, nil
	default:
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("parse csv failed, unsupport data type: %s for struct field: %s", dataType.String(), field.GetName()))
	}
}

func (r *rowParser) parseEntity(field *schemapb.FieldSchema, obj string, useElementType bool) (any, error) {
	if field.GetDefaultValue() != nil && obj == r.nullkey {
		return nullutil.GetDefaultValue(field)
	}

	nullable := field.GetNullable()
	if nullable && obj == r.nullkey {
		return nil, nil
	}

	dataType := field.GetDataType()
	if useElementType {
		dataType = field.GetElementType()
	}

	switch dataType {
	case schemapb.DataType_Bool:
		b, err := strconv.ParseBool(obj)
		if err != nil {
			return false, r.wrapTypeError(obj, field)
		}
		return b, nil
	case schemapb.DataType_Int8:
		num, err := strconv.ParseInt(obj, 10, 8)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return int8(num), nil
	case schemapb.DataType_Int16:
		num, err := strconv.ParseInt(obj, 10, 16)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return int16(num), nil
	case schemapb.DataType_Int32:
		num, err := strconv.ParseInt(obj, 10, 32)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return int32(num), nil
	case schemapb.DataType_Int64:
		num, err := strconv.ParseInt(obj, 10, 64)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return num, nil
	case schemapb.DataType_Float:
		num, err := strconv.ParseFloat(obj, 32)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return float32(num), typeutil.VerifyFloats32([]float32{float32(num)})
	case schemapb.DataType_Double:
		num, err := strconv.ParseFloat(obj, 64)
		if err != nil {
			return 0, r.wrapTypeError(obj, field)
		}
		return num, typeutil.VerifyFloats64([]float64{num})
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		maxLength, err := parameterutil.GetMaxLength(field)
		if err != nil {
			return nil, err
		}
		if err := common.CheckValidString(obj, maxLength, field); err != nil {
			return nil, err
		}
		return obj, nil
	case schemapb.DataType_BinaryVector:
		var vec []byte
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()]/8 {
			return nil, r.wrapDimError(len(vec)*8, field)
		}
		return vec, nil
	case schemapb.DataType_JSON:
		var data interface{}
		err := json.Unmarshal([]byte(obj), &data)
		if err != nil {
			return nil, err
		}
		return []byte(obj), nil
	case schemapb.DataType_Geometry:
		wkbValue, err := pkgcommon.ConvertWKTToWKB(obj)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		return wkbValue, nil
	case schemapb.DataType_Timestamptz:
		tz, err := funcutil.ValidateAndReturnUnixMicroTz(obj, r.timezone)
		if err != nil {
			return nil, err
		}
		return tz, nil
	case schemapb.DataType_FloatVector:
		var vec []float32
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()] {
			return nil, r.wrapDimError(len(vec), field)
		}
		return vec, typeutil.VerifyFloats32(vec)
	case schemapb.DataType_Float16Vector:
		var vec []float32
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()] {
			return nil, r.wrapDimError(len(vec), field)
		}
		vec2 := make([]byte, len(vec)*2)
		for i := 0; i < len(vec); i++ {
			copy(vec2[i*2:], typeutil.Float32ToFloat16Bytes(vec[i]))
		}
		return vec2, typeutil.VerifyFloats16(vec2)
	case schemapb.DataType_BFloat16Vector:
		var vec []float32
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()] {
			return nil, r.wrapDimError(len(vec), field)
		}
		vec2 := make([]byte, len(vec)*2)
		for i := 0; i < len(vec); i++ {
			copy(vec2[i*2:], typeutil.Float32ToBFloat16Bytes(vec[i]))
		}
		return vec2, typeutil.VerifyBFloats16(vec2)
	case schemapb.DataType_SparseFloatVector:
		// use dec.UseNumber() to avoid float64 precision loss
		var vec map[string]interface{}
		dec := json.NewDecoder(strings.NewReader(obj))
		dec.UseNumber()
		err := dec.Decode(&vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		vec2, err := typeutil.CreateSparseFloatRowFromMap(vec)
		if err != nil {
			return nil, err
		}
		return vec2, nil
	case schemapb.DataType_Int8Vector:
		var vec []int8
		err := json.Unmarshal([]byte(obj), &vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		if len(vec) != r.name2Dim[field.GetName()] {
			return nil, r.wrapDimError(len(vec), field)
		}
		return vec, nil
	case schemapb.DataType_Array:
		var vec []interface{}
		desc := json.NewDecoder(strings.NewReader(obj))
		desc.UseNumber()
		err := desc.Decode(&vec)
		if err != nil {
			return nil, r.wrapTypeError(obj, field)
		}
		maxCapacity, err := parameterutil.GetMaxCapacity(field)
		if err != nil {
			return nil, err
		}
		if err = common.CheckArrayCapacity(len(vec), maxCapacity, field); err != nil {
			return nil, err
		}
		// elements in array not support null value
		scalarFieldData, err := r.arrayToFieldData(vec, field)
		if err != nil {
			return nil, err
		}
		return scalarFieldData, nil
	default:
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("parse csv failed, unsupport data type: %s", field.GetDataType().String()))
	}
}

func (r *rowParser) arrayToFieldData(arr []interface{}, field *schemapb.FieldSchema) (*schemapb.ScalarField, error) {
	eleType := field.GetElementType()
	switch eleType {
	case schemapb.DataType_Bool:
		values := make([]bool, len(arr))
		for i, v := range arr {
			value, ok := v.(bool)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			values[i] = value
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		values := make([]int32, len(arr))
		for i, v := range arr {
			value, ok := v.(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseInt(value.String(), 10, 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse int32: %w", err)
			}
			values[i] = int32(num)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: values,
				},
			},
		}, nil

	case schemapb.DataType_Int64:
		values := make([]int64, len(arr))
		for i, v := range arr {
			value, ok := v.(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseInt(value.String(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse int64: %w", err)
			}
			values[i] = num
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Float:
		values := make([]float32, len(arr))
		for i, v := range arr {
			value, ok := v.(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseFloat(value.String(), 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse float32: %w", err)
			}
			values[i] = float32(num)
		}
		if err := typeutil.VerifyFloats32(values); err != nil {
			return nil, fmt.Errorf("float32 verification failed: %w", err)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Double:
		values := make([]float64, len(arr))
		for i, v := range arr {
			value, ok := v.(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseFloat(value.String(), 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse float64: %w", err)
			}
			values[i] = num
		}
		if err := typeutil.VerifyFloats64(values); err != nil {
			return nil, fmt.Errorf("float64 verification failed: %w", err)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Timestamptz:
		values := make([]int64, len(arr))
		for i, v := range arr {
			value, ok := v.(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseInt(value.String(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse timesamptz: %w", err)
			}
			values[i] = num
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_TimestamptzData{
				TimestamptzData: &schemapb.TimestamptzArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		values := make([]string, len(arr))
		for i, v := range arr {
			value, ok := v.(string)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			maxLength, err := parameterutil.GetMaxLength(field)
			if err != nil {
				return nil, err
			}
			if err := common.CheckValidString(value, maxLength, field); err != nil {
				return nil, err
			}
			values[i] = value
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{
					Data: values,
				},
			},
		}, nil
	default:
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("parse csv failed, unsupported array data type: %s", eleType.String()))
	}
}

func (r *rowParser) arrayOfVectorToFieldData(vectors []any, field *schemapb.FieldSchema) (*schemapb.VectorField, error) {
	elementType := field.GetElementType()
	switch elementType {
	case schemapb.DataType_FloatVector:
		dim, err := typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
		values := make([]float32, 0, len(vectors)*int(dim))

		for _, vectorAny := range vectors {
			var vector []float32
			v, ok := vectorAny.([]interface{})
			if !ok {
				return nil, r.wrapTypeError(vectorAny, field)
			}
			vector = make([]float32, len(v))
			for i, elem := range v {
				value, ok := elem.(json.Number)
				if !ok {
					return nil, r.wrapArrayValueTypeError(elem, elementType)
				}
				num, err := strconv.ParseFloat(value.String(), 32)
				if err != nil {
					return nil, fmt.Errorf("failed to parse float: %w", err)
				}
				vector[i] = float32(num)
			}

			if len(vector) != int(dim) {
				return nil, r.wrapDimError(len(vector), field)
			}
			values = append(values, vector...)
		}

		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: values,
				},
			},
		}, nil

	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_BinaryVector,
		schemapb.DataType_Int8Vector, schemapb.DataType_SparseFloatVector:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("not implemented element type for CSV: %s", elementType.String()))
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported element type: %s", elementType.String()))
	}
}

func (r *rowParser) wrapTypeError(v any, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expected type '%s' for field '%s', got type '%T' with value '%v'",
			field.GetDataType().String(), field.GetName(), v, v))
}

func (r *rowParser) wrapDimError(actualDim int, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expected dim '%d' for field '%s' with type '%s', got dim '%d'",
			r.name2Dim[field.GetName()], field.GetName(), field.GetDataType().String(), actualDim))
}

func (r *rowParser) wrapArrayValueTypeError(v any, eleType schemapb.DataType) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expected element type '%s' in array field, got type '%T' with value '%v'",
			eleType.String(), v, v))
}
