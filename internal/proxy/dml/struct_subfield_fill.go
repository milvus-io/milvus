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

package dml

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func structSubFieldSchemasByName(parent *schemapb.StructArrayFieldSchema) (map[string]*schemapb.FieldSchema, error) {
	byName := make(map[string]*schemapb.FieldSchema, len(parent.GetFields())*3)
	for _, child := range parent.GetFields() {
		byName[child.GetName()] = child
		byName[storedStructSubFieldName(parent.GetName(), child.GetName())] = child
		if typeutil.IsStructSubField(child.GetName()) {
			rawName, err := typeutil.ExtractStructFieldName(child.GetName())
			if err != nil {
				return nil, err
			}
			byName[rawName] = child
		}
	}
	return byName, nil
}

func validateStructSubFieldNames(parent *schemapb.StructArrayFieldSchema, data *schemapb.StructArrayField, byName map[string]*schemapb.FieldSchema) (map[*schemapb.FieldSchema]struct{}, error) {
	seen := make(map[*schemapb.FieldSchema]struct{}, len(data.GetFields()))
	for _, child := range data.GetFields() {
		schema := byName[child.GetFieldName()]
		if schema == nil {
			return nil, merr.WrapErrParameterInvalidMsg("sub-field %q does not exist in struct field %q", child.GetFieldName(), parent.GetName())
		}
		if _, exists := seen[schema]; exists {
			return nil, merr.WrapErrParameterInvalidMsg("duplicated sub-field %q in struct field %q", child.GetFieldName(), parent.GetName())
		}
		seen[schema] = struct{}{}
	}
	return seen, nil
}

// fillOmittedStructSubFields appends compact N-null-element rows for omitted
// element-nullable children. An all-null struct has no reference payload and is
// left to the existing row-null handling.
func fillOmittedStructSubFields(parent *schemapb.StructArrayFieldSchema, data *schemapb.StructArrayField, numRows int) error {
	if data == nil {
		return merr.WrapErrParameterInvalidMsg("struct field %q has nil data", parent.GetName())
	}
	byName, err := structSubFieldSchemasByName(parent)
	if err != nil {
		return err
	}
	seen, err := validateStructSubFieldNames(parent, data, byName)
	if err != nil {
		return err
	}
	var reference *schemapb.FieldData
	for _, child := range data.GetFields() {
		if subFieldHasData(child) {
			reference = child
			break
		}
	}
	if reference == nil {
		return nil
	}
	missing := make([]*schemapb.FieldSchema, 0, len(parent.GetFields())-len(seen))
	for _, child := range parent.GetFields() {
		if _, present := seen[child]; present {
			continue
		}
		if !child.GetElementNullable() {
			return merr.WrapErrParameterInvalidMsg("sub-field %q of struct field %q is required", child.GetName(), parent.GetName())
		}
		missing = append(missing, child)
	}
	if len(missing) == 0 {
		return nil
	}

	counter, err := newStructSubFieldRowCounter(reference, byName[reference.GetFieldName()], parent.GetName())
	if err != nil {
		return err
	}
	rowValidity := typeutil.GetFieldDataValidData(reference)
	if len(rowValidity) > 0 {
		if len(rowValidity) != numRows || countValidStructRows(rowValidity) != counter.rows {
			return merr.WrapErrParameterInvalidMsg("sub-field %q of struct field %q has invalid row valid_data", reference.GetFieldName(), parent.GetName())
		}
	} else if counter.rows != numRows {
		return merr.WrapErrParameterInvalidMsg("sub-field %q of struct field %q has %d payload rows, expected %d", reference.GetFieldName(), parent.GetName(), counter.rows, numRows)
	}
	counts := make([]int, counter.rows)
	for row := range counts {
		counts[row], err = counter.count(row)
		if err != nil {
			return merr.Wrapf(err, "struct %q sub-field %q payload row %d", parent.GetName(), reference.GetFieldName(), row)
		}
	}
	for _, child := range missing {
		filled, err := newNullStructSubFieldData(child, counts, rowValidity)
		if err != nil {
			return err
		}
		data.Fields = append(data.Fields, filled)
	}
	return nil
}

func countValidStructRows(validity []bool) int {
	count := 0
	for _, valid := range validity {
		if valid {
			count++
		}
	}
	return count
}

func structFieldHasPayload(data *schemapb.StructArrayField) bool {
	for _, child := range data.GetFields() {
		if subFieldHasData(child) {
			return true
		}
	}
	return false
}

func newNullStructSubFieldData(child *schemapb.FieldSchema, counts []int, rowValidity []bool) (*schemapb.FieldData, error) {
	filled, err := typeutil.GenEmptyFieldData(child)
	if err != nil {
		return nil, err
	}
	filled.FieldName = structChildRawName(child)
	filled.FieldId = child.GetFieldID()
	filled.Type = child.GetDataType()
	if len(rowValidity) > 0 {
		typeutil.SetFieldDataValidData(filled, slices.Clone(rowValidity))
	}
	nested := typeutil.IsNestedArrayTypeSchema(child.GetTypeSchema())
	var nestedLeaf schemapb.DataType
	if nested {
		// The collection schema was validated at DDL time; an unresolvable
		// chain here is a Milvus bug, not a request error.
		nestedLeaf, _, _, err = typeutil.GetArrayLeaf(child.GetTypeSchema())
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "resolve Array leaf of struct sub-field %s", child.GetName())
		}
	}
	for _, count := range counts {
		switch child.GetDataType() {
		case schemapb.DataType_Array:
			var row *schemapb.ScalarField
			if nested {
				row = &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
						ElementType: nestedLeaf,
					}},
				}
			} else if child.GetElementType() == schemapb.DataType_String {
				row = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}}
			} else {
				empty, err := typeutil.GenEmptyFieldData(&schemapb.FieldSchema{DataType: child.GetElementType()})
				if err != nil {
					return nil, err
				}
				row = empty.GetScalars()
			}
			typeutil.SetArrayElementValidData(row, make([]bool, count))
			filled.GetScalars().GetArrayData().Data = append(filled.GetScalars().GetArrayData().Data, row)
		case schemapb.DataType_ArrayOfVector:
			row, err := typeutil.NewEmptyArrayOfVectorRow(filled.GetVectors().GetDim(), child.GetElementType())
			if err != nil {
				return nil, err
			}
			typeutil.SetVectorArrayElementValidData(row, make([]bool, count))
			filled.GetVectors().GetVectorArray().Data = append(filled.GetVectors().GetVectorArray().Data, row)
		default:
			return nil, merr.WrapErrParameterInvalidMsg("struct sub-field %q has unsupported type %s", child.GetName(), child.GetDataType())
		}
	}
	return filled, nil
}

type structSubFieldRowCounter struct {
	name  string
	rows  int
	count func(int) (int, error)
}

func newStructSubFieldRowCounter(data *schemapb.FieldData, schema *schemapb.FieldSchema, structName string) (structSubFieldRowCounter, error) {
	if schema == nil || data.GetType() != schema.GetDataType() {
		return structSubFieldRowCounter{}, merr.WrapErrParameterInvalidMsg("sub-field %q of struct field %q has an incompatible type", data.GetFieldName(), structName)
	}
	switch schema.GetDataType() {
	case schemapb.DataType_Array:
		array := data.GetScalars().GetArrayData()
		if array == nil {
			return structSubFieldRowCounter{}, merr.WrapErrParameterInvalidMsg("scalar array data is nil in struct field '%s', sub-field '%s'", structName, data.GetFieldName())
		}
		return structSubFieldRowCounter{name: data.GetFieldName(), rows: len(array.GetData()), count: func(rowIndex int) (int, error) {
			if rowIndex < 0 || rowIndex >= len(array.GetData()) || array.GetData()[rowIndex].GetData() == nil {
				return 0, merr.WrapErrParameterInvalidMsg("nil array data")
			}
			row := array.GetData()[rowIndex]
			if typeutil.IsNestedArrayTypeSchema(schema.GetTypeSchema()) {
				if row.GetArrayData() == nil {
					return 0, merr.WrapErrParameterInvalidMsg("nested array data is nil")
				}
				if schema.GetElementNullable() {
					return len(typeutil.GetArrayElementValidData(row)), nil
				}
				return len(row.GetArrayData().GetData()), nil
			}
			if schema.GetElementNullable() {
				return len(typeutil.GetArrayElementValidData(row)), nil
			}
			switch schema.GetElementType() {
			case schemapb.DataType_Bool:
				return len(row.GetBoolData().GetData()), nil
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
				return len(row.GetIntData().GetData()), nil
			case schemapb.DataType_Int64:
				return len(row.GetLongData().GetData()), nil
			case schemapb.DataType_Float:
				return len(row.GetFloatData().GetData()), nil
			case schemapb.DataType_Double:
				return len(row.GetDoubleData().GetData()), nil
			case schemapb.DataType_VarChar, schemapb.DataType_String:
				return len(row.GetStringData().GetData()), nil
			default:
				return 0, merr.WrapErrParameterInvalidMsg("unsupported array element type %s", schema.GetElementType().String())
			}
		}}, nil
	case schemapb.DataType_ArrayOfVector:
		array := data.GetVectors().GetVectorArray()
		if array == nil {
			return structSubFieldRowCounter{}, merr.WrapErrParameterInvalidMsg("vector array data is nil in struct field '%s', sub-field '%s'", structName, data.GetFieldName())
		}
		dim, err := typeutil.GetDim(schema)
		if err != nil {
			return structSubFieldRowCounter{}, merr.Wrapf(err, "sub-field %q in struct %q", data.GetFieldName(), structName)
		}
		width, err := vectorArrayElementWidth(schema.GetElementType(), dim)
		if err != nil {
			return structSubFieldRowCounter{}, merr.Wrapf(err, "sub-field %q in struct %q", data.GetFieldName(), structName)
		}
		return structSubFieldRowCounter{name: data.GetFieldName(), rows: len(array.GetData()), count: func(rowIndex int) (int, error) {
			if rowIndex < 0 || rowIndex >= len(array.GetData()) || array.GetData()[rowIndex].GetData() == nil {
				return 0, merr.WrapErrParameterInvalidMsg("nil vector array data")
			}
			row := array.GetData()[rowIndex]
			var payloadLen int
			switch schema.GetElementType() {
			case schemapb.DataType_FloatVector:
				payloadLen = len(row.GetFloatVector().GetData())
			case schemapb.DataType_BinaryVector:
				payloadLen = len(row.GetBinaryVector())
			case schemapb.DataType_Float16Vector:
				payloadLen = len(row.GetFloat16Vector())
			case schemapb.DataType_BFloat16Vector:
				payloadLen = len(row.GetBfloat16Vector())
			case schemapb.DataType_Int8Vector:
				payloadLen = len(row.GetInt8Vector())
			}
			if payloadLen%width != 0 {
				return 0, merr.WrapErrParameterInvalidMsg("payload length %d is not divisible by vector width %d", payloadLen, width)
			}
			if schema.GetElementNullable() {
				return len(typeutil.GetVectorArrayElementValidData(row)), nil
			}
			return payloadLen / width, nil
		}}, nil
	default:
		return structSubFieldRowCounter{}, merr.WrapErrParameterInvalidMsg("unexpected field data type in struct array field, fieldName: %s", structName)
	}
}
