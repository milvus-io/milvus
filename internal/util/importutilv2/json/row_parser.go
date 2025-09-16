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

package json

import (
	"fmt"
	"strconv"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/nullutil"
	pkgcommon "github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type RowParser interface {
	Parse(raw any) (Row, error)
}

type rowParser struct {
	id2Dim               map[int64]int
	id2Field             map[int64]*schemapb.FieldSchema
	name2FieldID         map[string]int64
	pkField              *schemapb.FieldSchema
	dynamicField         *schemapb.FieldSchema
	functionOutputFields map[string]int64

	structArrays      map[string]interface{}
	allowInsertAutoID bool
}

func NewRowParser(schema *schemapb.CollectionSchema) (RowParser, error) {
	allFields := typeutil.GetAllFieldSchemas(schema)

	id2Field := lo.KeyBy(allFields, func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})

	functionOutputFields := make(map[string]int64)
	id2Dim := make(map[int64]int)
	for id, field := range id2Field {
		if field.GetIsFunctionOutput() {
			functionOutputFields[field.GetName()] = field.GetFieldID()
		}
		if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
			dim, err := typeutil.GetDim(field)
			if err != nil {
				return nil, err
			}
			id2Dim[id] = int(dim)
		}
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	dynamicField := typeutil.GetDynamicField(schema)

	name2FieldID := lo.SliceToMap(
		lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
			return !field.GetIsFunctionOutput() && !typeutil.IsAutoPKField(field) && field.GetName() != dynamicField.GetName()
		}),
		func(field *schemapb.FieldSchema) (string, int64) {
			return field.GetName(), field.GetFieldID()
		},
	)
	allowInsertAutoID, _ := pkgcommon.IsAllowInsertAutoID(schema.GetProperties()...)

	sturctArrays := lo.SliceToMap(
		schema.GetStructArrayFields(),
		func(sa *schemapb.StructArrayFieldSchema) (string, interface{}) {
			return sa.GetName(), nil
		},
	)

	return &rowParser{
		id2Dim:               id2Dim,
		id2Field:             id2Field,
		name2FieldID:         name2FieldID,
		pkField:              pkField,
		dynamicField:         dynamicField,
		functionOutputFields: functionOutputFields,
		structArrays:         sturctArrays,
		allowInsertAutoID:    allowInsertAutoID,
	}, nil
}

func (r *rowParser) wrapTypeError(v any, fieldID int64) error {
	field := r.id2Field[fieldID]
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expected type '%s' for field '%s', got type '%T' with value '%v'",
			field.GetDataType().String(), field.GetName(), v, v))
}

func (r *rowParser) wrapDimError(actualDim int, fieldID int64) error {
	field := r.id2Field[fieldID]
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expected dim '%d' for field '%s' with type '%s', got dim '%d'",
			r.id2Dim[fieldID], field.GetName(), field.GetDataType().String(), actualDim))
}

func (r *rowParser) wrapArrayValueTypeError(v any, eleType schemapb.DataType) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expected element type '%s' in array field, got type '%T' with value '%v'",
			eleType.String(), v, v))
}

// StructArray are passed in with format for one row: StructArray: [element 1, element 2, ...]
// where each element contains one value of all sub-fields in StructArrayField.
// So we need to reconstruct it to be handled by handleField.
//
// For example, let StructArrayFieldSchema { sub-field1: array of int32, sub-field2: array of float vector }
// When we have one row:
//
//	[{"sub-field1": 1, "sub-field2": [1.0, 2.0]}, {"sub-field1": 2, "sub-field2": [3.0, 4.0]}],
//
// we reconstruct it to be handled by handleField as:
//
//	{"sub-field1": [1, 2], "sub-field2": [[1.0, 2.0], [3.0, 4.0]]}
func reconstructArrayForStructArray(raw any) (map[string]any, error) {
	rows, ok := raw.([]any)
	if !ok {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("invalid StructArray format in JSON, each row should be a key-value map, but got type %T", raw))
	}

	buf := make(map[string][]any)
	for _, elem := range rows {
		row, ok := elem.(map[string]any)
		if !ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("invalid element in StructArray, expect map[string]any but got type %T", elem))
		}
		for key, value := range row {
			buf[key] = append(buf[key], value)
		}
	}

	out := make(map[string]any, len(buf))
	for k, v := range buf {
		out[k] = v
	}
	return out, nil
}

func (r *rowParser) Parse(raw any) (Row, error) {
	stringMap, ok := raw.(map[string]any)
	if !ok {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("invalid JSON format, each row should be a key-value map, but got type %T", raw))
	}
	if _, ok = stringMap[r.pkField.GetName()]; ok && r.pkField.GetAutoID() && !r.allowInsertAutoID {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", r.pkField.GetName()))
	}

	row := make(Row)
	dynamicValues := make(map[string]any)

	handleField := func(key string, value any) error {
		if fieldID, ok := r.name2FieldID[key]; ok {
			data, err := r.parseEntity(fieldID, value)
			if err != nil {
				return err
			}
			row[fieldID] = data
		} else if r.dynamicField != nil {
			// has dynamic field, put redundant pair to dynamicValues
			dynamicValues[key] = value
		} else {
			// from v2.6, we don't intend to return error for redundant fields, just skip it
			return nil
		}

		return nil
	}

	// read values from json file
	for key, value := range stringMap {
		if _, ok := r.functionOutputFields[key]; ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field '%s' is output by function, no need to provide", key))
		}

		if _, ok := r.structArrays[key]; ok {
			values, err := reconstructArrayForStructArray(value)
			if err != nil {
				return nil, err
			}

			for subKey, subValue := range values {
				if err := handleField(subKey, subValue); err != nil {
					return nil, err
				}
			}
		} else {
			if err := handleField(key, value); err != nil {
				return nil, err
			}
		}
	}

	// if nullable/defaultValue fields have no values, fill with nil or default value
	for fieldName, fieldID := range r.name2FieldID {
		if _, ok = row[fieldID]; !ok {
			if r.id2Field[fieldID].GetNullable() {
				row[fieldID] = nil
			}
			if r.id2Field[fieldID].GetDefaultValue() != nil {
				data, err := nullutil.GetDefaultValue(r.id2Field[fieldID])
				if err != nil {
					return nil, err
				}
				row[fieldID] = data
			}
		}
		if _, ok = row[fieldID]; !ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("value of field '%s' is missed", fieldName))
		}
	}

	// combine the redundant pairs into dynamic field(if it has)
	err := r.combineDynamicRow(dynamicValues, row)
	if err != nil {
		return nil, err
	}
	return row, err
}

func (r *rowParser) combineDynamicRow(dynamicValues map[string]any, row Row) error {
	if r.dynamicField == nil {
		return nil
	}

	// Combine the dynamic field value
	// valid inputs:
	// case 1: {"id": 1, "vector": [], "x": 8, "$meta": "{\"y\": 8}"} ==>> {"id": 1, "vector": [], "$meta": "{\"y\": 8, \"x\": 8}"}
	// case 2: {"id": 1, "vector": [], "x": 8, "$meta": {}} ==>> {"id": 1, "vector": [], "$meta": {\"x\": 8}}
	// case 3: {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 4: {"id": 1, "vector": [], "$meta": {"x": 8}}
	// case 5: {"id": 1, "vector": [], "$meta": {}}
	// case 6: {"id": 1, "vector": [], "x": 8} ==>> {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 7: {"id": 1, "vector": []}
	// invalid inputs:
	// case 8: {"id": 1, "vector": [], "x": 6, "$meta": {"x": 8}} ==>> duplicated key is not allowed
	// case 9: {"id": 1, "vector": [], "x": 6, "$meta": "{\"x\": 8}"} ==>> duplicated key is not allowed
	dynamicFieldID := r.dynamicField.GetFieldID()
	if len(dynamicValues) == 0 {
		// case 7
		row[dynamicFieldID] = []byte("{}")
		return nil
	}

	if obj, ok := dynamicValues[r.dynamicField.GetName()]; ok {
		var mp map[string]interface{}
		switch value := obj.(type) {
		case string:
			// case 1, 3
			err := json.Unmarshal([]byte(value), &mp)
			if err != nil {
				return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON format string")
			}
		case map[string]interface{}:
			// case 2, 4, 5
			mp = value
		default:
			// invalid input
			return merr.WrapErrImportFailed("illegal value for dynamic field, not a JSON object")
		}
		// put the all dynamic fields into newDynamicValues
		for k, v := range mp {
			if _, ok = dynamicValues[k]; ok {
				// case 8, 9
				return merr.WrapErrImportFailed(fmt.Sprintf("duplicated key is not allowed, key=%s", k))
			}
			dynamicValues[k] = v
		}
		// remove $meta field from dynamicValues
		delete(dynamicValues, r.dynamicField.GetName())
	}

	data, err := r.parseEntity(dynamicFieldID, dynamicValues)
	if err != nil {
		return err
	}
	row[dynamicFieldID] = data
	return nil
}

func (r *rowParser) parseEntity(fieldID int64, obj any) (any, error) {
	field := r.id2Field[fieldID]
	if field.GetDefaultValue() != nil && obj == nil {
		return nullutil.GetDefaultValue(field)
	}

	if field.GetNullable() && obj == nil {
		return nil, nil
	}

	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		b, ok := obj.(bool)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		return b, nil
	case schemapb.DataType_Int8:
		value, ok := obj.(json.Number)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		num, err := strconv.ParseInt(value.String(), 0, 8)
		if err != nil {
			return nil, err
		}
		return int8(num), nil
	case schemapb.DataType_Int16:
		value, ok := obj.(json.Number)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		num, err := strconv.ParseInt(value.String(), 0, 16)
		if err != nil {
			return nil, err
		}
		return int16(num), nil
	case schemapb.DataType_Int32:
		value, ok := obj.(json.Number)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		num, err := strconv.ParseInt(value.String(), 0, 32)
		if err != nil {
			return nil, err
		}
		return int32(num), nil
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		value, ok := obj.(json.Number)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		num, err := strconv.ParseInt(value.String(), 0, 64)
		if err != nil {
			return nil, err
		}
		return num, nil
	case schemapb.DataType_Float:
		value, ok := obj.(json.Number)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		num, err := strconv.ParseFloat(value.String(), 32)
		if err != nil {
			return nil, err
		}
		return float32(num), typeutil.VerifyFloats32([]float32{float32(num)})
	case schemapb.DataType_Double:
		value, ok := obj.(json.Number)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		num, err := strconv.ParseFloat(value.String(), 64)
		if err != nil {
			return nil, err
		}
		return num, typeutil.VerifyFloats64([]float64{num})
	case schemapb.DataType_BinaryVector:
		arr, ok := obj.([]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		if len(arr) != r.id2Dim[fieldID]/8 {
			return nil, r.wrapDimError(len(arr)*8, fieldID)
		}
		vec := make([]byte, len(arr))
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapTypeError(arr[i], fieldID)
			}
			num, err := strconv.ParseUint(value.String(), 0, 8)
			if err != nil {
				return nil, err
			}
			vec[i] = byte(num)
		}
		return vec, nil
	case schemapb.DataType_FloatVector:
		arr, ok := obj.([]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		if len(arr) != r.id2Dim[fieldID] {
			return nil, r.wrapDimError(len(arr), fieldID)
		}
		vec := make([]float32, len(arr))

		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapTypeError(arr[i], fieldID)
			}
			num, err := strconv.ParseFloat(value.String(), 32)
			if err != nil {
				return nil, err
			}
			vec[i] = float32(num)
		}
		return vec, typeutil.VerifyFloats32(vec)
	case schemapb.DataType_Float16Vector:
		// parse float string to Float16 bytes
		arr, ok := obj.([]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		if len(arr) != r.id2Dim[fieldID] {
			return nil, r.wrapDimError(len(arr), fieldID)
		}
		vec := make([]byte, len(arr)*2)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapTypeError(arr[i], fieldID)
			}
			num, err := strconv.ParseFloat(value.String(), 32)
			if err != nil {
				return nil, err
			}
			copy(vec[i*2:], typeutil.Float32ToFloat16Bytes(float32(num)))
		}
		return vec, typeutil.VerifyFloats16(vec)
	case schemapb.DataType_BFloat16Vector:
		// parse float string to BFloat16 bytes
		arr, ok := obj.([]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		if len(arr) != r.id2Dim[fieldID] {
			return nil, r.wrapDimError(len(arr), fieldID)
		}
		vec := make([]byte, len(arr)*2)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapTypeError(arr[i], fieldID)
			}
			num, err := strconv.ParseFloat(value.String(), 32)
			if err != nil {
				return nil, err
			}
			copy(vec[i*2:], typeutil.Float32ToBFloat16Bytes(float32(num)))
		}
		return vec, typeutil.VerifyBFloats16(vec)
	case schemapb.DataType_SparseFloatVector:
		arr, ok := obj.(map[string]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		vec, err := typeutil.CreateSparseFloatRowFromMap(arr)
		if err != nil {
			return nil, err
		}
		return vec, nil
	case schemapb.DataType_Int8Vector:
		arr, ok := obj.([]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		if len(arr) != r.id2Dim[fieldID] {
			return nil, r.wrapDimError(len(arr), fieldID)
		}
		vec := make([]int8, len(arr))
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapTypeError(arr[i], fieldID)
			}
			num, err := strconv.ParseInt(value.String(), 10, 8)
			if err != nil {
				return nil, err
			}
			vec[i] = int8(num)
		}
		return vec, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		value, ok := obj.(string)
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		maxLength, err := parameterutil.GetMaxLength(field)
		if err != nil {
			return nil, err
		}
		if err = common.CheckVarcharLength(value, maxLength, field); err != nil {
			return nil, err
		}
		return value, nil
	case schemapb.DataType_JSON:
		// for JSON data, we accept two kinds input: string and map[string]interface
		// user can write JSON content as {"FieldJSON": "{\"x\": 8}"} or {"FieldJSON": {"x": 8}}
		if value, ok := obj.(string); ok {
			var dummy interface{}
			err := json.Unmarshal([]byte(value), &dummy)
			if err != nil {
				return nil, err
			}
			return []byte(value), nil
		} else if mp, ok := obj.(map[string]interface{}); ok {
			bs, err := json.Marshal(mp)
			if err != nil {
				return nil, err
			}
			return bs, nil
		} else {
			return nil, r.wrapTypeError(obj, fieldID)
		}
	case schemapb.DataType_Array:
		arr, ok := obj.([]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		maxCapacity, err := parameterutil.GetMaxCapacity(field)
		if err != nil {
			return nil, err
		}
		if err = common.CheckArrayCapacity(len(arr), maxCapacity, field); err != nil {
			return nil, err
		}
		scalarFieldData, err := r.arrayToFieldData(arr, field)
		if err != nil {
			return nil, err
		}
		return scalarFieldData, nil
	case schemapb.DataType_ArrayOfVector:
		arr, ok := obj.([]interface{})
		if !ok {
			return nil, r.wrapTypeError(obj, fieldID)
		}
		maxCapacity, err := parameterutil.GetMaxCapacity(field)
		if err != nil {
			return nil, err
		}
		if err = common.CheckArrayCapacity(len(arr), maxCapacity, field); err != nil {
			return nil, err
		}
		vectorFieldData, err := r.arrayOfVectorToFieldData(arr, field)
		if err != nil {
			return nil, err
		}
		return vectorFieldData, nil

	default:
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("parse json failed, unsupport data type: %s",
				field.GetDataType().String()))
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
			num, err := strconv.ParseInt(value.String(), 0, 32)
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
			num, err := strconv.ParseInt(value.String(), 0, 64)
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
			num, err := strconv.ParseInt(value.String(), 0, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse int64: %w", err)
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
			if err := common.CheckVarcharLength(value, maxLength, field); err != nil {
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
			fmt.Sprintf("parse json failed, unsupported array data type '%s'", eleType.String()))
	}
}

func (r *rowParser) arrayOfVectorToFieldData(vectors []any, field *schemapb.FieldSchema) (*schemapb.VectorField, error) {
	elementType := field.GetElementType()
	fieldID := field.GetFieldID()
	switch elementType {
	case schemapb.DataType_FloatVector:
		values := make([]float32, 0, len(vectors)*10)
		dim := r.id2Dim[fieldID]
		for _, vectorAny := range vectors {
			vector, ok := vectorAny.([]any)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected slice as vector, but got %T", vectorAny))
			}
			for _, v := range vector {
				value, ok := v.(json.Number)
				if !ok {
					return nil, r.wrapTypeError(value, fieldID)
				}
				num, err := strconv.ParseFloat(value.String(), 32)
				if err != nil {
					return nil, err
				}
				values = append(values, float32(num))
			}
		}
		return &schemapb.VectorField{
			Dim: int64(dim),
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: values,
				},
			},
		}, nil

	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_BinaryVector,
		schemapb.DataType_Int8Vector, schemapb.DataType_SparseFloatVector:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("not implemented element type: %s", elementType.String()))
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported element type: %s", elementType.String()))
	}
}
