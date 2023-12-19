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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type RowParser interface {
	Parse(row Row) (Row, error)
}

type rowParser struct {
	dim      int
	id2Field map[int64]*schemapb.FieldSchema
}

func NewRowParser(schema *schemapb.CollectionSchema) (RowParser, error) {
	id2Field := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	vecField, err := typeutil.GetVectorFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	dim, err := typeutil.GetDim(vecField)
	if err != nil {
		return nil, err
	}
	return &rowParser{
		dim:      int(dim),
		id2Field: id2Field,
	}, nil
}

func (r *rowParser) wrapTypeError(v any, fieldID int64) error {
	field := r.id2Field[fieldID]
	return merr.WrapErrImportFailed(fmt.Sprintf("expected type '%s' for field '%s', got type '%T' with value '%v'",
		field.GetDataType().String(), field.GetName(), v, v))
}

func (r *rowParser) wrapDimError(actualDim int, fieldID int64) error {
	field := r.id2Field[fieldID]
	return merr.WrapErrImportFailed(fmt.Sprintf("expected dim '%d' for field '%s' with type '%s', got dim '%d'",
		r.dim, field.GetName(), field.GetDataType().String(), actualDim))
}

func (r *rowParser) wrapArrayValueTypeError(v any, eleType schemapb.DataType) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("expected element type '%s' in array field, got type '%T' with value '%v'",
		eleType.String(), v, v))
}

func (r *rowParser) Parse(row Row) (Row, error) {
	result := make(Row)
	for fieldID, obj := range row {
		switch r.id2Field[fieldID].GetDataType() {
		case schemapb.DataType_Bool:
			b, ok := obj.(bool)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			result[fieldID] = b
		case schemapb.DataType_Int8:
			value, ok := obj.(json.Number)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			num, err := strconv.ParseInt(value.String(), 0, 8)
			if err != nil {
				return nil, err
			}
			result[fieldID] = int8(num)
		case schemapb.DataType_Int16:
			value, ok := obj.(json.Number)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			num, err := strconv.ParseInt(value.String(), 0, 16)
			if err != nil {
				return nil, err
			}
			result[fieldID] = int16(num)
		case schemapb.DataType_Int32:
			value, ok := obj.(json.Number)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			num, err := strconv.ParseInt(value.String(), 0, 32)
			if err != nil {
				return nil, err
			}
			result[fieldID] = int32(num)
		case schemapb.DataType_Int64:
			value, ok := obj.(json.Number)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			num, err := strconv.ParseInt(value.String(), 0, 64)
			if err != nil {
				return nil, err
			}
			result[fieldID] = num
		case schemapb.DataType_Float:
			value, ok := obj.(json.Number)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			num, err := strconv.ParseFloat(value.String(), 32)
			if err != nil {
				return nil, err
			}
			result[fieldID] = float32(num)
		case schemapb.DataType_Double:
			value, ok := obj.(json.Number)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			num, err := strconv.ParseFloat(value.String(), 64)
			if err != nil {
				return nil, err
			}
			result[fieldID] = num
		case schemapb.DataType_BinaryVector:
			arr, ok := obj.([]interface{})
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			if len(arr)*8 != r.dim {
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
			result[fieldID] = vec
		case schemapb.DataType_FloatVector:
			arr, ok := obj.([]interface{})
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			if len(arr) != r.dim {
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
			result[fieldID] = vec
		case schemapb.DataType_Float16Vector:
			arr, ok := obj.([]interface{})
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			if len(arr)/2 != r.dim {
				return nil, r.wrapDimError(len(arr)/2, fieldID)
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
			result[fieldID] = vec
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			value, ok := obj.(string)
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			result[fieldID] = value
		case schemapb.DataType_JSON:
			// for JSON data, we accept two kinds input: string and map[string]interface
			// user can write JSON content as {"FieldJSON": "{\"x\": 8}"} or {"FieldJSON": {"x": 8}}
			if value, ok := obj.(string); ok {
				var dummy interface{}
				err := json.Unmarshal([]byte(value), &dummy)
				if err != nil {
					return nil, err
				}
				result[fieldID] = []byte(value)
			} else if mp, ok := obj.(map[string]interface{}); ok {
				bs, err := json.Marshal(mp)
				if err != nil {
					return nil, err
				}
				result[fieldID] = bs
			} else {
				return nil, r.wrapTypeError(obj, fieldID)
			}
		case schemapb.DataType_Array:
			arr, ok := obj.([]interface{})
			if !ok {
				return nil, r.wrapTypeError(obj, fieldID)
			}
			scalarFieldData, err := r.arrayToFieldData(arr, r.id2Field[fieldID].GetElementType())
			if err != nil {
				return nil, err
			}
			result[fieldID] = scalarFieldData
		default:
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("parse json failed, unsupport data type: %s",
				r.id2Field[fieldID].GetDataType().String()))
		}
	}
	return result, nil
}

func (r *rowParser) arrayToFieldData(arr []interface{}, eleType schemapb.DataType) (*schemapb.ScalarField, error) {
	switch eleType {
	case schemapb.DataType_Bool:
		values := make([]bool, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(bool)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			values = append(values, value)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		values := make([]int32, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseInt(value.String(), 0, 32)
			if err != nil {
				return nil, err
			}
			values = append(values, int32(num))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Int64:
		values := make([]int64, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseInt(value.String(), 0, 64)
			if err != nil {
				return nil, err
			}
			values = append(values, num)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Float:
		values := make([]float32, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseFloat(value.String(), 32)
			if err != nil {
				return nil, err
			}
			values = append(values, float32(num))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_Double:
		values := make([]float64, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(json.Number)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			num, err := strconv.ParseFloat(value.String(), 64)
			if err != nil {
				return nil, err
			}
			values = append(values, num)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{
					Data: values,
				},
			},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		values := make([]string, 0)
		for i := 0; i < len(arr); i++ {
			value, ok := arr[i].(string)
			if !ok {
				return nil, r.wrapArrayValueTypeError(arr, eleType)
			}
			values = append(values, value)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{
					Data: values,
				},
			},
		}, nil
	default:
		return nil, errors.New(fmt.Sprintf("unsupported array data type '%s'", eleType.String()))
	}
}
