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

package transformio

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func nestedArrayToArrow(field *schemapb.FieldData, rows int64, alloc memory.Allocator, schema *schemapb.FieldSchema) (arrow.Array, error) {
	var dt arrow.DataType
	var err error
	if schema != nil && schema.GetTypeSchema() != nil {
		declared, e := types.ArrowFieldFromMilvusSchema(schema)
		dt, err = declared.Type, e
	} else {
		dt, err = inferNestedArrayType(field.GetScalars().GetArrayData(), 0)
	}
	if err != nil {
		return nil, err
	}
	list, ok := dt.(*arrow.ListType)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("nested array schema requires a list")
	}
	indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(field.GetScalars().GetArrayData().GetData()))
	if err != nil {
		return nil, err
	}
	builder := array.NewListBuilderWithField(alloc, list.ElemField())
	defer builder.Release()
	for _, index := range indexes {
		if index < 0 {
			builder.AppendNull()
			continue
		}
		if err := appendNestedArrayRow(builder, field.GetScalars().GetArrayData().GetData()[index], list.ElemField(), 0); err != nil {
			return nil, err
		}
	}
	return builder.NewArray(), nil
}

func inferNestedArrayType(data *schemapb.ArrayArray, depth int) (arrow.DataType, error) {
	if data == nil || depth > 64 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid nested array payload")
	}
	var child arrow.DataType
	var err error
	if data.GetElementType() == schemapb.DataType_Array {
		for _, row := range data.GetData() {
			if row.GetArrayData() != nil {
				child, err = inferNestedArrayType(row.GetArrayData(), depth+1)
				break
			}
		}
		if child == nil && err == nil {
			return nil, merr.WrapErrParameterInvalidMsg("empty nested array input requires its collection type schema")
		}
	} else {
		child, err = scalarType(data.GetElementType())
	}
	if err != nil {
		return nil, err
	}
	return arrow.ListOfField(arrow.Field{Name: "item", Type: child, Nullable: true, Metadata: arrow.NewMetadata([]string{types.MetadataKeyMilvusDataType}, []string{strconv.Itoa(int(data.GetElementType()))})}), nil
}

func appendNestedArrayRow(builder *array.ListBuilder, row *schemapb.ScalarField, element arrow.Field, depth int) error {
	if row == nil || depth > 64 {
		return merr.WrapErrParameterInvalidMsg("invalid nested array row")
	}
	builder.Append(true)
	if child, ok := element.Type.(*arrow.ListType); ok {
		data := row.GetArrayData()
		if data == nil {
			return merr.WrapErrParameterInvalidMsg("nested array payload is missing")
		}
		indexes, err := logicalIndexes(row.GetValidData(), int64(len(data.GetData())), len(data.GetData()))
		if len(row.GetValidData()) > 0 {
			indexes, err = logicalIndexes(row.GetValidData(), int64(len(row.GetValidData())), len(data.GetData()))
		}
		if err != nil {
			return err
		}
		for _, index := range indexes {
			if index < 0 {
				builder.ValueBuilder().AppendNull()
				continue
			}
			if err := appendNestedArrayRow(builder.ValueBuilder().(*array.ListBuilder), data.GetData()[index], child.ElemField(), depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	dataType := schemapb.DataType_None
	if raw, ok := element.Metadata.GetValue(types.MetadataKeyMilvusDataType); ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid nested array logical type")
		}
		dataType = schemapb.DataType(parsed)
	}
	if dataType == schemapb.DataType_None {
		schema, err := types.MilvusSchemaFromArrowField(element)
		if err != nil {
			return err
		}
		dataType = schema.GetDataType()
	}
	if err := validateScalarInputType(row, dataType); err != nil {
		return err
	}
	count := scalarLen(row, dataType)
	logicalCount := count
	if len(row.GetValidData()) > 0 {
		logicalCount = len(row.GetValidData())
	}
	indexes, err := logicalIndexes(row.GetValidData(), int64(logicalCount), count)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if index < 0 {
			builder.ValueBuilder().AppendNull()
			continue
		}
		if err := appendScalar(builder.ValueBuilder(), row, dataType, index); err != nil {
			return err
		}
	}
	return nil
}

func validateStorageNullability(values arrow.Array, schema *schemapb.FieldSchema) error {
	if values.NullN() > 0 && !schema.GetNullable() {
		return merr.WrapErrFunctionFailedMsg("nonnullable output field %q contains null", schema.GetName())
	}
	if recursive := schema.GetTypeSchema(); recursive != nil {
		return validateRecursiveStorageNulls(values, recursive, 0)
	}
	if schema.GetDataType() != schemapb.DataType_Array && schema.GetDataType() != schemapb.DataType_ArrayOfVector {
		return nil
	}
	for row := 0; row < values.Len(); row++ {
		if values.IsNull(row) {
			continue
		}
		start, end, child, ok := listValues(values, row)
		if !ok {
			return typeMismatch(schema.GetDataType(), values)
		}
		if !schema.GetElementNullable() {
			for index := start; index < end; index++ {
				if child.IsNull(int(index)) {
					return merr.WrapErrFunctionFailedMsg("array output field %q has undeclared null elements", schema.GetName())
				}
			}
		}
	}
	return nil
}

func validateRecursiveStorageNulls(values arrow.Array, schema *schemapb.TypeSchema, depth int) error {
	if schema == nil || depth > 64 {
		return merr.WrapErrFunctionFailedMsg("invalid recursive output schema")
	}
	if values.NullN() > 0 && !schema.GetNullable() {
		return merr.WrapErrFunctionFailedMsg("recursive output contains undeclared null values")
	}
	if schema.GetArrayElement() == nil {
		return nil
	}
	for row := 0; row < values.Len(); row++ {
		if values.IsNull(row) {
			continue
		}
		start, end, child, ok := listValues(values, row)
		if !ok {
			return merr.WrapErrFunctionFailedMsg("recursive output schema requires a list")
		}
		sliced := array.NewSlice(child, start, end)
		err := validateRecursiveStorageNulls(sliced, schema.GetArrayElement(), depth+1)
		sliced.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

func arrowToScalarForSchema(values arrow.Array, schema *schemapb.FieldSchema) (*schemapb.ScalarField, error) {
	if schema.GetDataType() == schemapb.DataType_Array && schema.GetTypeSchema() != nil {
		return arrowToRecursiveScalar(values, schema.GetTypeSchema(), 0)
	}
	return arrowToScalar(values, schema.GetDataType(), schema.GetElementType())
}

func arrowToRecursiveScalar(values arrow.Array, schema *schemapb.TypeSchema, depth int) (*schemapb.ScalarField, error) {
	if schema == nil || depth > 64 {
		return nil, merr.WrapErrFunctionFailedMsg("invalid recursive output schema")
	}
	if schema.GetArrayElement() == nil {
		return arrowToScalar(values, schema.GetLeafType(), schemapb.DataType_None)
	}
	childSchema := schema.GetArrayElement()
	elementType := childSchema.GetLeafType()
	if childSchema.GetArrayElement() != nil {
		elementType = schemapb.DataType_Array
	}
	rows := make([]*schemapb.ScalarField, values.Len())
	for row := range rows {
		start, end, children, ok := listValues(values, row)
		if !ok {
			return nil, merr.WrapErrFunctionFailedMsg("recursive output requires a list")
		}
		sliced := array.NewSlice(children, start, end)
		child, err := arrowToRecursiveScalar(sliced, childSchema, depth+1)
		if err != nil {
			sliced.Release()
			return nil, err
		}
		if sliced.NullN() > 0 {
			child.ValidData = make([]bool, sliced.Len())
			for index := range child.ValidData {
				child.ValidData[index] = sliced.IsValid(index)
			}
		}
		sliced.Release()
		rows[row] = child
	}
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: elementType, Data: rows}}}, nil
}
