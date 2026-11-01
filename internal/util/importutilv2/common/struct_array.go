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

package common

import (
	"fmt"
	"math"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ParseStructArrayRow pivots one struct row, given as the list of its element
// objects decoded from JSON (numbers as json.Number), into one row value per
// sub-field, keyed by sub-field ID. The values use the layout the insert path
// stores: *schemapb.ScalarField for Array sub-fields and *schemapb.VectorField
// for ArrayOfVector sub-fields (see StructSubFieldRow).
//
// The schema sub-fields drive the pivot, so every sub-field gets exactly one
// entry per element. A key missing from an element is a null element when the
// sub-field is element-nullable and an error otherwise. Keys that are not
// sub-fields of the schema (unknown or dropped sub-fields) are ignored. A null
// element is rejected: a struct element itself cannot be null.
func ParseStructArrayRow(structSchema *schemapb.StructArrayFieldSchema, elements []any) (map[int64]any, error) {
	objects := make([]map[string]any, len(elements))
	for i, element := range elements {
		if element == nil {
			return nil, merr.WrapErrImportFailedMsg(
				"element %d of struct field '%s' is null; a struct element cannot be null, set its sub-field values to null instead",
				i, structSchema.GetName())
		}
		object, ok := element.(map[string]any)
		if !ok {
			return nil, merr.WrapErrImportFailedMsg(
				"element %d of struct field '%s' should be a key-value map, but got type %T",
				i, structSchema.GetName(), element)
		}
		objects[i] = object
	}

	row := make(map[int64]any, len(structSchema.GetFields()))
	for _, field := range structSchema.GetFields() {
		subName, err := typeutil.ExtractStructFieldName(field.GetName())
		if err != nil {
			return nil, merr.Wrapf(err, "struct field '%s'", structSchema.GetName())
		}
		values := make([]any, len(objects))
		for i, object := range objects {
			value, ok := object[subName]
			if !ok && !field.GetElementNullable() {
				return nil, merr.WrapErrImportFailedMsg(
					"sub-field '%s' of struct field '%s' is missing from element %d and is not element-nullable",
					subName, structSchema.GetName(), i)
			}
			values[i] = value
		}
		dim := 0
		if field.GetDataType() == schemapb.DataType_ArrayOfVector {
			value, err := typeutil.GetDim(field)
			if err != nil {
				return nil, merr.Wrapf(err, "struct sub-field '%s'", field.GetName())
			}
			dim = int(value)
		}
		data, err := StructSubFieldRow(field, values, dim)
		if err != nil {
			return nil, err
		}
		row[field.GetFieldID()] = data
	}
	return row, nil
}

// StructSubFieldRow converts the logical element values of one struct
// sub-field row (one entry per struct element, nil for a null element) into
// the stored row value. dim is used only for ArrayOfVector sub-fields.
//
//   - Array<T>: *schemapb.ScalarField with one dense payload slot per element.
//     When the sub-field is element-nullable, null slots hold the zero value
//     and valid_data has one entry per element.
//   - Array<Array<T>>: *schemapb.ScalarField with ArrayData{ElementType: T}
//     and one child per element. A null inner array is a typed empty leaf with
//     no valid_data; valid_data on the row exists iff the sub-field is
//     element-nullable. A non-null inner array is a dense leaf payload with
//     valid_data iff the leaf is nullable.
//   - ArrayOfVector: *schemapb.VectorField holding only the non-null vectors;
//     valid_data has one entry per element iff the sub-field is
//     element-nullable.
//
// Capacity limits count logical elements, null ones included.
func StructSubFieldRow(field *schemapb.FieldSchema, values []any, dim int) (any, error) {
	switch field.GetDataType() {
	case schemapb.DataType_Array:
		maxCapacity, err := parameterutil.GetMaxCapacity(field)
		if err != nil {
			return nil, err
		}
		if err := CheckArrayCapacity(len(values), maxCapacity, field); err != nil {
			return nil, err
		}
		if typeutil.IsNestedArrayTypeSchema(field.GetTypeSchema()) {
			row, err := nestedArrayRow(field, values)
			if err != nil {
				return nil, err
			}
			return row, nil
		}
		leaf, err := newArrayLeaf(field, field.GetElementType(), field.GetElementNullable(), field, -1)
		if err != nil {
			return nil, err
		}
		row, err := leaf.row(values)
		if err != nil {
			return nil, err
		}
		return row, nil
	case schemapb.DataType_ArrayOfVector:
		maxCapacity, err := parameterutil.GetMaxCapacity(field)
		if err != nil {
			return nil, err
		}
		if err := CheckArrayCapacity(len(values), maxCapacity, field); err != nil {
			return nil, err
		}
		row, err := ArrayOfVectorToFieldData(values, field, dim)
		if err != nil {
			return nil, err
		}
		return row, nil
	default:
		return nil, merr.WrapErrImportSysFailedMsg(
			"unsupported data type %s for struct sub-field '%s'", field.GetDataType().String(), field.GetName())
	}
}

// nestedArrayRow builds an Array<Array<T>> row. The TypeSchema root carries
// the outer capacity (checked by the caller), the inner node the inner
// capacity and element nullability, and the leaf node the leaf nullability and
// the VarChar max_length.
func nestedArrayRow(field *schemapb.FieldSchema, values []any) (*schemapb.ScalarField, error) {
	leafType, leafNullable, depth, err := typeutil.GetFieldArrayLeaf(field)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "resolve Array leaf of struct sub-field '%s'", field.GetName())
	}
	if depth != 2 {
		return nil, merr.WrapErrImportSysFailedMsg(
			"nested Array struct sub-field '%s' supports exactly two levels, got %d", field.GetName(), depth)
	}
	inner := field.GetTypeSchema().GetArrayElement()
	innerCapacity, err := parameterutil.GetMaxCapacityFromTypeSchema(inner)
	if err != nil {
		return nil, err
	}
	leafNode := inner.GetArrayElement()
	leafField := &schemapb.FieldSchema{
		Name:            field.GetName(),
		DataType:        schemapb.DataType_Array,
		ElementType:     leafType,
		ElementNullable: leafNullable,
		TypeParams:      leafNode.GetTypeParams(),
	}

	leaf, err := newArrayLeaf(field, leafType, leafNullable, leafField, 0)
	if err != nil {
		return nil, err
	}

	elementNullable := field.GetElementNullable()
	children := make([]*schemapb.ScalarField, len(values))
	var validData []bool
	if elementNullable {
		validData = make([]bool, len(values))
	}
	for i, value := range values {
		if value == nil {
			if !elementNullable {
				return nil, merr.WrapErrImportFailedMsg(
					"field '%s' element %d is null, but the sub-field is not element-nullable", field.GetName(), i)
			}
			children[i] = emptyLeafRow(leafType)
			continue
		}
		innerValues, ok := value.([]any)
		if !ok {
			return nil, merr.WrapErrImportFailedMsg(
				"field '%s' element %d should be an array, but got type %T with value %v", field.GetName(), i, value, value)
		}
		if int64(len(innerValues)) > innerCapacity {
			return nil, merr.WrapErrImportFailedMsg(
				"inner array capacity(%d) of field '%s' element %d exceeds max_capacity(%d)",
				len(innerValues), field.GetName(), i, innerCapacity)
		}
		leaf.outer = i
		child, err := leaf.row(innerValues)
		if err != nil {
			return nil, err
		}
		children[i] = child
		if elementNullable {
			validData[i] = true
		}
	}

	row := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: leafType, Data: children},
		},
	}
	if elementNullable {
		typeutil.SetArrayElementValidData(row, validData)
	}
	return row, nil
}

// emptyLeafRow is the typed empty payload storage expects for a null inner
// array.
func emptyLeafRow(leafType schemapb.DataType) *schemapb.ScalarField {
	switch leafType {
	case schemapb.DataType_Bool:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{}}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}}
	case schemapb.DataType_Int64:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}}
	case schemapb.DataType_Float:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{}}}
	case schemapb.DataType_Double:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{}}}
	default:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}}
	}
}

// arrayLeaf converts one list of scalar values: a single-level Array row or
// one inner array of a nested Array row.
type arrayLeaf struct {
	field       *schemapb.FieldSchema // the sub-field, named in errors
	elementType schemapb.DataType
	nullable    bool
	maxLength   int64
	stringField *schemapb.FieldSchema // field passed to the string checks
	outer       int                   // enclosing element of a nested leaf, -1 for a single-level Array
}

// newArrayLeaf resolves the leaf settings. lengthField carries the VarChar
// max_length: the sub-field itself for a single-level Array, a field built from
// the leaf TypeSchema node for a nested one.
func newArrayLeaf(field *schemapb.FieldSchema, elementType schemapb.DataType, nullable bool, lengthField *schemapb.FieldSchema, outer int) (*arrayLeaf, error) {
	leaf := &arrayLeaf{
		field:       field,
		elementType: elementType,
		nullable:    nullable,
		stringField: lengthField,
		outer:       outer,
	}
	if typeutil.IsStringType(elementType) {
		maxLength, err := parameterutil.GetMaxLength(lengthField)
		if err != nil {
			return nil, err
		}
		leaf.maxLength = maxLength
	}
	return leaf, nil
}

func (l *arrayLeaf) position(i int) string {
	if l.outer < 0 {
		return fmt.Sprintf("element %d", i)
	}
	return fmt.Sprintf("element %d inner element %d", l.outer, i)
}

func (l *arrayLeaf) nullErr(i int) error {
	if l.outer < 0 {
		return merr.WrapErrImportFailedMsg(
			"field '%s' %s is null, but the sub-field is not element-nullable", l.field.GetName(), l.position(i))
	}
	return merr.WrapErrImportFailedMsg(
		"field '%s' %s is null, but the array leaf is not nullable", l.field.GetName(), l.position(i))
}

func (l *arrayLeaf) typeErr(i int, value any) error {
	return merr.WrapErrImportFailedMsg(
		"expected element type '%s' for field '%s' at %s, got type '%T' with value '%v'",
		l.elementType.String(), l.field.GetName(), l.position(i), value, value)
}

func (l *arrayLeaf) row(values []any) (*schemapb.ScalarField, error) {
	var row *schemapb.ScalarField
	var validData []bool
	var err error
	switch l.elementType {
	case schemapb.DataType_Bool:
		var data []bool
		data, validData, err = convertLeaf(l, values, l.parseBool)
		row = &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		var data []int32
		data, validData, err = convertLeaf(l, values, l.parseInt32)
		row = &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}}
	case schemapb.DataType_Int64:
		var data []int64
		data, validData, err = convertLeaf(l, values, l.parseInt64)
		row = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}}
	case schemapb.DataType_Float:
		var data []float32
		data, validData, err = convertLeaf(l, values, l.parseFloat32)
		row = &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}}
	case schemapb.DataType_Double:
		var data []float64
		data, validData, err = convertLeaf(l, values, l.parseFloat64)
		row = &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		var data []string
		data, validData, err = convertLeaf(l, values, l.parseString)
		row = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}}
	default:
		return nil, merr.WrapErrImportSysFailedMsg(
			"unsupported array element type '%s' for struct sub-field '%s'", l.elementType.String(), l.field.GetName())
	}
	if err != nil {
		return nil, err
	}
	if l.nullable {
		typeutil.SetArrayElementValidData(row, validData)
	}
	return row, nil
}

// convertLeaf parses values into a dense payload. A null value leaves the zero
// value in its slot and a false validity entry; validity is returned only for
// a nullable leaf.
func convertLeaf[T any](l *arrayLeaf, values []any, parse func(int, any) (T, error)) ([]T, []bool, error) {
	data := make([]T, len(values))
	var validData []bool
	if l.nullable {
		validData = make([]bool, len(values))
	}
	for i, value := range values {
		if value == nil {
			if !l.nullable {
				return nil, nil, l.nullErr(i)
			}
			continue
		}
		parsed, err := parse(i, value)
		if err != nil {
			return nil, nil, err
		}
		data[i] = parsed
		if l.nullable {
			validData[i] = true
		}
	}
	return data, validData, nil
}

func (l *arrayLeaf) parseBool(i int, value any) (bool, error) {
	b, ok := value.(bool)
	if !ok {
		return false, l.typeErr(i, value)
	}
	return b, nil
}

func (l *arrayLeaf) parseInteger(i int, value any) (int64, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, l.typeErr(i, value)
	}
	num, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil {
		return 0, merr.WrapErrImportFailedMsg(
			"failed to parse '%s' as %s for field '%s' at %s: %v",
			number.String(), l.elementType.String(), l.field.GetName(), l.position(i), err)
	}
	return num, nil
}

func (l *arrayLeaf) parseInt32(i int, value any) (int32, error) {
	num, err := l.parseInteger(i, value)
	if err != nil {
		return 0, err
	}
	var lower, upper int64
	switch l.elementType {
	case schemapb.DataType_Int8:
		lower, upper = math.MinInt8, math.MaxInt8
	case schemapb.DataType_Int16:
		lower, upper = math.MinInt16, math.MaxInt16
	default:
		lower, upper = math.MinInt32, math.MaxInt32
	}
	if num < lower || num > upper {
		return 0, merr.WrapErrImportFailedMsg(
			"value %d for field '%s' at %s is out of range for %s [%d, %d]",
			num, l.field.GetName(), l.position(i), l.elementType.String(), lower, upper)
	}
	return int32(num), nil
}

func (l *arrayLeaf) parseInt64(i int, value any) (int64, error) {
	return l.parseInteger(i, value)
}

func (l *arrayLeaf) parseFloat(i int, value any, bitSize int) (float64, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, l.typeErr(i, value)
	}
	num, err := strconv.ParseFloat(number.String(), bitSize)
	if err != nil {
		return 0, merr.WrapErrImportFailedMsg(
			"failed to parse '%s' as %s for field '%s' at %s: %v",
			number.String(), l.elementType.String(), l.field.GetName(), l.position(i), err)
	}
	if err := typeutil.VerifyFloat(num); err != nil {
		return 0, merr.Wrapf(err, "field '%s' at %s", l.field.GetName(), l.position(i))
	}
	return num, nil
}

func (l *arrayLeaf) parseFloat32(i int, value any) (float32, error) {
	num, err := l.parseFloat(i, value, 32)
	return float32(num), err
}

func (l *arrayLeaf) parseFloat64(i int, value any) (float64, error) {
	return l.parseFloat(i, value, 64)
}

func (l *arrayLeaf) parseString(i int, value any) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", l.typeErr(i, value)
	}
	if err := CheckValidString(s, l.maxLength, l.stringField); err != nil {
		return "", err
	}
	return s, nil
}
