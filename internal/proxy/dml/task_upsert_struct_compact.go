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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/fieldvalidator"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// compactRetrievedStructField returns a private copy of a struct field read
// back by Query, converted to the layout an insert request must use.
//
// Query returns every nullable level of a scalar or nested Array sub-field
// dense: one payload entry (a zero value or an empty inner Array) per logical
// slot, next to the validity bitmap of that level. Insert input is compact at
// each nullable level: the payload holds only the slots whose bit is true.
// Partial upsert writes kept old struct values back through the insert
// validation, so it converts them here, level by level:
//   - a single-level Array sub-field with element_nullable keeps the payload
//     of its valid elements;
//   - a nested Array sub-field keeps the inner Arrays that are valid when the
//     inner level is nullable, and then the valid leaves of every kept inner
//     Array when the leaf is nullable.
//
// Bitmaps, element types and empty Arrays are kept as they are. Vector
// sub-fields are left alone, since Query already returns their elements
// compact. Entity rows are not removed: null struct rows keep their
// placeholders, and callers still index rows by their logical position.
// The copy shares nothing with field, so neither the Query response nor a
// later retry sees the conversion.
func compactRetrievedStructField(structSchema *schemapb.StructArrayFieldSchema, field *schemapb.FieldData) (*schemapb.FieldData, error) {
	compacted := proto.Clone(field).(*schemapb.FieldData)
	for _, child := range compacted.GetStructArrays().GetFields() {
		childSchema := findStructChildSchema(structSchema, child.GetFieldName())
		if childSchema == nil {
			return nil, merr.WrapErrServiceInternalMsg("retrieved struct field %q contains unknown sub-field %q",
				field.GetFieldName(), child.GetFieldName())
		}
		if err := compactRetrievedStructChild(child, childSchema); err != nil {
			return nil, merr.Wrapf(err, "retrieved struct field %q sub-field %q", field.GetFieldName(), child.GetFieldName())
		}
	}
	return compacted, nil
}

func compactRetrievedStructChild(child *schemapb.FieldData, childSchema *schemapb.FieldSchema) error {
	if childSchema.GetDataType() != schemapb.DataType_Array {
		return nil
	}
	nested := typeutil.IsNestedArrayTypeSchema(childSchema.GetTypeSchema())
	if !nested && !childSchema.GetElementNullable() {
		return nil
	}
	arrayData := child.GetScalars().GetArrayData()
	if child.GetType() != schemapb.DataType_Array || arrayData == nil {
		return merr.WrapErrServiceInternalMsg("expected Array data, got %s", child.GetType().String())
	}
	rows := arrayData.GetData()
	rowValidData := typeutil.GetFieldDataValidData(child)
	if len(rowValidData) > 0 && len(rowValidData) != len(rows) {
		return merr.WrapErrServiceInternalMsg("has %d rows but %d row validity bits", len(rows), len(rowValidData))
	}
	for rowIndex, row := range rows {
		if len(rowValidData) > 0 && !rowValidData[rowIndex] {
			continue
		}
		var err error
		if nested {
			err = compactRetrievedNestedArray(row, childSchema.GetTypeSchema(), 0)
		} else {
			err = compactRetrievedArrayLeaf(row)
		}
		if err != nil {
			return merr.Wrapf(err, "row %d", rowIndex)
		}
	}
	return nil
}

// compactRetrievedNestedArray walks one Array value with the same type chain
// that fieldvalidator uses to check and fill nested Arrays.
func compactRetrievedNestedArray(row *schemapb.ScalarField, arrayType *schemapb.TypeSchema, level int) error {
	elementSchema := arrayType.GetArrayElement()
	if elementSchema.GetArrayElement() == nil {
		if !elementSchema.GetNullable() {
			return nil
		}
		if err := compactRetrievedArrayLeaf(row); err != nil {
			return merr.Wrapf(err, "level %d", level)
		}
		return nil
	}
	arrayData := row.GetArrayData()
	if arrayData == nil {
		return merr.WrapErrServiceInternalMsg("level %d expects Array data", level)
	}
	if elementSchema.GetNullable() {
		children, err := keepValidElements(arrayData.GetData(), typeutil.GetArrayElementValidData(row))
		if err != nil {
			return merr.Wrapf(err, "level %d", level)
		}
		arrayData.Data = children
	}
	for index, child := range arrayData.GetData() {
		if err := compactRetrievedNestedArray(child, elementSchema, level+1); err != nil {
			return merr.Wrapf(err, "element %d", index)
		}
	}
	return nil
}

// compactRetrievedArrayLeaf drops the placeholders of null scalar elements
// from one Array value.
func compactRetrievedArrayLeaf(row *schemapb.ScalarField) error {
	validData := typeutil.GetArrayElementValidData(row)
	var err error
	switch data := row.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		data.BoolData.Data, err = keepValidElements(data.BoolData.GetData(), validData)
	case *schemapb.ScalarField_IntData:
		data.IntData.Data, err = keepValidElements(data.IntData.GetData(), validData)
	case *schemapb.ScalarField_LongData:
		data.LongData.Data, err = keepValidElements(data.LongData.GetData(), validData)
	case *schemapb.ScalarField_FloatData:
		data.FloatData.Data, err = keepValidElements(data.FloatData.GetData(), validData)
	case *schemapb.ScalarField_DoubleData:
		data.DoubleData.Data, err = keepValidElements(data.DoubleData.GetData(), validData)
	case *schemapb.ScalarField_StringData:
		data.StringData.Data, err = keepValidElements(data.StringData.GetData(), validData)
	default:
		if len(validData) > 0 && fieldvalidator.GetValidNumber(validData) > 0 {
			return merr.WrapErrServiceInternalMsg("unsupported Array element payload %T", row.GetData())
		}
	}
	return err
}

// keepValidElements returns the payload entries whose validity bit is true.
// A payload that is already compact, or has no null slot, is returned as is.
func keepValidElements[T any](values []T, validData []bool) ([]T, error) {
	if len(validData) == 0 {
		return values, nil
	}
	validCount := fieldvalidator.GetValidNumber(validData)
	if len(values) == validCount {
		return values, nil
	}
	if len(values) != len(validData) {
		return nil, merr.WrapErrServiceInternalMsg("payload has %d entries for %d slots with %d valid",
			len(values), len(validData), validCount)
	}
	kept := make([]T, 0, validCount)
	for i, valid := range validData {
		if valid {
			kept = append(kept, values[i])
		}
	}
	return kept, nil
}
