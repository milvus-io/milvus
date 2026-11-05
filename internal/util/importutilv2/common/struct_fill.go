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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// FillMissingStructSubFields backfills the element-nullable sub-fields a batch
// does not carry: a sub-field added after the file was written, or one the
// file omits. Per struct, the first sub-field with rows (in schema order) is
// the reference: a null reference row stays a null row, a valid row with N
// elements gets N null elements (N=0 gives a valid empty array). A struct with
// no sub-field data at all is left to AppendNullableDefaultFieldsData when it
// is nullable. A missing sub-field that is not element-nullable is rejected;
// the readers reject it first, this is the backstop. A sub-field that already
// has rows is not touched, so a batch the Parquet reader filled is left as is.
func FillMissingStructSubFields(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	if len(schema.GetStructArrayFields()) == 0 {
		return nil
	}
	if !lo.SomeBy(lo.Values(data.Data), func(fd storage.FieldData) bool { return fd != nil && fd.RowNum() > 0 }) {
		return nil
	}
	for _, structField := range schema.GetStructArrayFields() {
		var refField *schemapb.FieldSchema
		var refData storage.FieldData
		missing := make([]*schemapb.FieldSchema, 0)
		for _, subField := range structField.GetFields() {
			fieldData, ok := data.Data[subField.GetFieldID()]
			if ok && fieldData != nil && fieldData.RowNum() > 0 {
				if refField == nil {
					refField, refData = subField, fieldData
				}
				continue
			}
			missing = append(missing, subField)
		}
		if len(missing) == 0 {
			continue
		}
		if refField == nil {
			if structField.GetNullable() {
				continue
			}
			return merr.WrapErrImportFailedMsg("struct field '%s' is not nullable but has no sub-field data", structField.GetName())
		}
		for _, subField := range missing {
			if !subField.GetElementNullable() {
				return merr.WrapErrImportFailedMsg("struct field '%s' is missing sub-field '%s', which is not element-nullable",
					structField.GetName(), subField.GetName())
			}
		}
		for _, subField := range missing {
			filled, err := storage.NewFieldData(subField.GetDataType(), subField, refData.RowNum())
			if err != nil {
				return err
			}
			if err := AppendNullElementRows(structField.GetName(), subField, refField, refData, filled); err != nil {
				return err
			}
			data.Data[subField.GetFieldID()] = filled
		}
	}
	return nil
}

// AppendNullElementRows extends dst, a column of the element-nullable
// sub-field subField, to the rows of its sibling refData: for each reference
// row from dst.RowNum() on, a null row stays null and a valid row gets as many
// null elements as the reference row has elements. Appending only the rows dst
// lacks lets a reader fill each chunk as it reads it.
func AppendNullElementRows(structName string, subField, refField *schemapb.FieldSchema, refData, dst storage.FieldData) error {
	rows := refData.RowNum()
	from := dst.RowNum()
	if from > rows {
		return merr.WrapErrImportSysFailedMsg(
			"sub-field '%s' of struct field '%s' has %d rows, more than the %d rows of sub-field '%s'",
			subField.GetName(), structName, from, rows, refField.GetName())
	}
	refValidData := refData.GetValidData()
	if refData.GetNullable() && len(refValidData) != rows {
		return merr.WrapErrImportSysFailedMsg(
			"nullable sub-field '%s' of struct field '%s' has invalid ValidData length %d, expected %d",
			refField.GetName(), structName, len(refValidData), rows)
	}
	for i := from; i < rows; i++ {
		if refData.GetNullable() && !refValidData[i] {
			if !dst.GetNullable() {
				return merr.WrapErrImportSysFailedMsg(
					"struct field '%s' row %d is null in sub-field '%s', but sub-field '%s' is not nullable",
					structName, i, refField.GetName(), subField.GetName())
			}
			if err := dst.AppendRow(nil); err != nil {
				return err
			}
			continue
		}
		count, err := StructSubFieldRowElementCount(refData, refField, i, structName)
		if err != nil {
			return err
		}
		var row any
		switch fd := dst.(type) {
		case *storage.ArrayFieldData:
			row, err = storage.NullElementsArrayRow(subField, count)
		case *storage.VectorArrayFieldData:
			row, err = storage.NullElementsVectorArrayRow(subField, fd.Dim, count)
		default:
			return merr.WrapErrImportSysFailedMsg(
				"unexpected column type '%s' for sub-field '%s' of struct field '%s'",
				dst.GetDataType().String(), subField.GetName(), structName)
		}
		if err != nil {
			return err
		}
		if err := dst.AppendRow(row); err != nil {
			return err
		}
	}
	return nil
}

// StructSubFieldRowElementCount returns the number of struct elements in row i
// of a struct sub-field column: the array length for scalar Array sub-fields,
// or the number of vectors for ArrayOfVector sub-fields. Element-nullable rows
// are counted by their element valid_data, so null elements count too; their
// payload must match the dense (scalar) or compact (vector) layout the writer
// expects, and a mismatch there is a reader bug.
func StructSubFieldRowElementCount(fieldData storage.FieldData, subField *schemapb.FieldSchema, i int, structName string) (int, error) {
	subFieldName := subField.GetName()
	switch fd := fieldData.(type) {
	case *storage.ArrayFieldData:
		row := fd.Data[i]
		count, ok := scalarFieldElementCount(row)
		if !ok {
			return 0, merr.WrapErrImportFailedMsg(
				"invalid scalar array data at row %d of sub-field '%s' of struct field '%s': missing or unsupported scalar payload",
				i, subFieldName, structName)
		}
		if !subField.GetElementNullable() {
			return count, nil
		}
		// Dense layout: one payload slot (one inner array when nested) per
		// logical element, null elements included.
		valid := typeutil.GetArrayElementValidData(row)
		if len(valid) != count {
			return 0, merr.WrapErrImportSysFailedMsg(
				"element-nullable sub-field '%s' of struct field '%s' at row %d has %d payload elements but %d element validity entries",
				subFieldName, structName, i, count, len(valid))
		}
		return len(valid), nil
	case *storage.VectorArrayFieldData:
		row := fd.Data[i]
		var payloadLen, width int
		dim := int(fd.Dim)
		switch fd.ElementType {
		case schemapb.DataType_FloatVector:
			payloadLen = len(row.GetFloatVector().GetData())
			width = dim
		case schemapb.DataType_BinaryVector:
			payloadLen = len(row.GetBinaryVector())
			width = (dim + 7) / 8
		case schemapb.DataType_Float16Vector:
			payloadLen = len(row.GetFloat16Vector())
			width = dim * 2
		case schemapb.DataType_BFloat16Vector:
			payloadLen = len(row.GetBfloat16Vector())
			width = dim * 2
		case schemapb.DataType_Int8Vector:
			payloadLen = len(row.GetInt8Vector())
			width = dim
		default:
			return 0, merr.WrapErrImportSysFailedMsg(
				"unsupported vector element type '%s' for sub-field '%s' of struct field '%s'",
				fd.ElementType.String(), subFieldName, structName)
		}
		if subField.GetElementNullable() {
			// Compact layout: the payload holds only the valid vectors.
			valid := typeutil.GetVectorArrayElementValidData(row)
			validCount := lo.Count(valid, true)
			if width <= 0 || validCount*width != payloadLen {
				return 0, merr.WrapErrImportSysFailedMsg(
					"element-nullable sub-field '%s' of struct field '%s' at row %d has payload length %d, expected %d valid vectors of width %d",
					subFieldName, structName, i, payloadLen, validCount, width)
			}
			return len(valid), nil
		}
		if width <= 0 || payloadLen%width != 0 {
			return 0, merr.WrapErrImportFailedMsg(
				"corrupted vector array data at row %d of sub-field '%s' of struct field '%s', payload length %d is not a multiple of vector width %d",
				i, subFieldName, structName, payloadLen, width)
		}
		return payloadLen / width, nil
	default:
		return 0, merr.WrapErrImportSysFailedMsg(
			"unexpected column type '%s' for sub-field '%s' of struct field '%s'",
			fieldData.GetDataType().String(), subFieldName, structName)
	}
}

// scalarFieldElementCount returns the number of elements held by one Array row
// and whether its scalar payload type is recognized. A typed empty payload is
// valid and returns (0, true); a nil or unset payload returns (0, false).
func scalarFieldElementCount(sf *schemapb.ScalarField) (int, bool) {
	switch d := sf.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		return len(d.BoolData.GetData()), true
	case *schemapb.ScalarField_IntData:
		return len(d.IntData.GetData()), true
	case *schemapb.ScalarField_LongData:
		return len(d.LongData.GetData()), true
	case *schemapb.ScalarField_FloatData:
		return len(d.FloatData.GetData()), true
	case *schemapb.ScalarField_DoubleData:
		return len(d.DoubleData.GetData()), true
	case *schemapb.ScalarField_StringData:
		return len(d.StringData.GetData()), true
	case *schemapb.ScalarField_BytesData:
		return len(d.BytesData.GetData()), true
	case *schemapb.ScalarField_ArrayData:
		return len(d.ArrayData.GetData()), true
	case *schemapb.ScalarField_JsonData:
		return len(d.JsonData.GetData()), true
	case *schemapb.ScalarField_TimestamptzData:
		return len(d.TimestamptzData.GetData()), true
	default:
		return 0, false
	}
}
