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

package parquet

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	common2 "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	sparseVectorIndice = "indices"
	sparseVectorValues = "values"
)

func WrapTypeErr(expect *schemapb.FieldSchema, actual string) error {
	nullable := ""
	if expect.GetNullable() {
		nullable = "nullable"
	}
	elementType := ""
	if expect.GetDataType() == schemapb.DataType_Array {
		elementType = expect.GetElementType().String()
	}
	// error message examples:
	// "expect 'Int32' type for field 'xxx', but got 'bool' type"
	// "expect nullable 'Int32 Array' type for field 'xxx', but got 'bool' type"
	// "expect 'FloatVector' type for field 'xxx', but got 'bool' type"
	return merr.WrapErrImportFailed(
		fmt.Sprintf("expect %s '%s %s' type for field '%s', but got '%s' type",
			nullable, elementType, expect.GetDataType().String(), expect.GetName(), actual))
}

func WrapNullRowErr(field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("the field '%s' is not nullable but the file contains null value", field.GetName()))
}

func WrapNullElementErr(field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("array element is not allowed to be null value for field '%s'", field.GetName()))
}

func WrapNullStructElementErr(structName string, element int) error {
	return merr.WrapErrImportFailedMsg(
		"element %d of struct field '%s' is null; a struct element cannot be null, set its sub-field values to null instead",
		element, structName)
}

func CreateFieldReaders(ctx context.Context, fileReader *pqarrow.FileReader, schema *schemapb.CollectionSchema) (map[int64]*FieldReader, error) {
	// Create map for all fields including sub-fields from StructArrayFields
	allFields := typeutil.GetAllFieldSchemas(schema)
	nameToField := lo.KeyBy(allFields, func(field *schemapb.FieldSchema) string {
		return field.GetName()
	})

	pqSchema, err := fileReader.Schema()
	if err != nil {
		return nil, merr.WrapErrImportFailedMsg("get parquet schema failed, err=%v", err)
	}

	if err := rejectFlatStructSubFieldColumns(schema, pqSchema); err != nil {
		return nil, err
	}

	// Check if we have nested struct format
	nestedStructs := make(map[string]int) // struct name -> column index
	for _, structField := range schema.StructArrayFields {
		for i, pqField := range pqSchema.Fields() {
			if pqField.Name != structField.Name {
				continue
			}
			listType, ok := pqField.Type.(*arrow.ListType)
			if !ok {
				return nil, merr.WrapErrImportFailedMsg("struct field is not a list of structs: %s", structField.Name)
			}
			structType, ok := listType.Elem().(*arrow.StructType)
			if !ok {
				return nil, merr.WrapErrImportFailedMsg("struct field is not a list of structs: %s", structField.Name)
			}
			nestedStructs[structField.Name] = i
			// Verify struct fields match. An element-nullable sub-field may be
			// absent: it is filled with null elements (see below).
			for _, subField := range structField.Fields {
				fieldName, err := typeutil.ExtractStructFieldName(subField.Name)
				if err != nil {
					return nil, merr.WrapErrImportFailed(err.Error())
				}
				found := false
				for _, f := range structType.Fields() {
					if f.Name == fieldName {
						found = true
						break
					}
				}
				if !found && !subField.GetElementNullable() {
					return nil, merr.WrapErrImportFailedMsg("field not found in struct: %s", fieldName)
				}
			}
		}
	}

	// Original flat format handling
	err = isSchemaEqual(schema, pqSchema)
	if err != nil {
		return nil, merr.WrapErrImportFailedMsg("schema not equal, err=%v", err)
	}

	// this loop is for "how many fields are provided by this parquet file?"
	readFields := make(map[string]int64)
	crs := make(map[int64]*FieldReader)
	allowInsertAutoID, _ := common.IsAllowInsertAutoID(schema.GetProperties()...)
	for i, pqField := range pqSchema.Fields() {
		// Skip if it's a struct column
		if _, isStruct := nestedStructs[pqField.Name]; isStruct {
			continue
		}

		field, ok := nameToField[pqField.Name]
		if !ok {
			// redundant fields, ignore. only accepts a special field "$meta" to store dynamic data
			continue
		}

		// auto-id field must not be provided
		if typeutil.IsAutoPKField(field) && !allowInsertAutoID {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", field.GetName()))
		}
		// validate function output field
		if field.GetIsFunctionOutput() {
			if typeutil.IsBM25FunctionOutputField(field, schema) {
				return nil, merr.WrapErrImportFailed(
					fmt.Sprintf("not allowed to provide data for BM25 function output field '%s'", field.GetName()))
			}
			if !common.GetCollectionAllowInsertNonBM25FunctionOutputs(schema.GetProperties()) {
				return nil, merr.WrapErrImportFailed(
					fmt.Sprintf("not allowed to provide data for function output field '%s', "+
						"set collection property '%s' to enable", field.GetName(), common.CollectionAllowInsertNonBM25FunctionOutputs))
			}
		}
		cr, err := NewFieldReader(ctx, fileReader, i, field, common2.GetSchemaTimezone(schema))
		if err != nil {
			return nil, err
		}
		if _, ok = crs[field.GetFieldID()]; ok {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("there is multi field with name: %s", field.GetName()))
		}
		crs[field.GetFieldID()] = cr
		readFields[field.GetName()] = field.GetFieldID()
	}

	for _, structField := range schema.StructArrayFields {
		columnIndex, ok := nestedStructs[structField.Name]
		if !ok {
			if structField.GetNullable() {
				continue
			}
			return nil, merr.WrapErrImportFailedMsg("struct field not found in parquet schema: %s", structField.Name)
		}

		listType := pqSchema.Field(columnIndex).Type.(*arrow.ListType)
		structType := listType.Elem().(*arrow.StructType)

		// Find the index of each sub-field in the arrow struct, -1 if absent.
		fieldIndices := make([]int, len(structField.Fields))
		presentSubFields := 0
		for j, subField := range structField.Fields {
			fieldName, err := typeutil.ExtractStructFieldName(subField.Name)
			if err != nil {
				return nil, merr.WrapErrImportFailed(err.Error())
			}

			fieldIndices[j] = -1
			for i, f := range structType.Fields() {
				if f.Name == fieldName {
					fieldIndices[j] = i
					break
				}
			}

			if fieldIndices[j] == -1 {
				if subField.GetElementNullable() {
					continue
				}
				return nil, merr.WrapErrImportFailedMsg("field not found in struct: %s", fieldName)
			}
			presentSubFields++
		}

		// Create reader for each sub-field. An absent element-nullable sub-field
		// is filled with null elements by the DataNode from a present sibling.
		// When the column carries none of the sub-fields (all its children were
		// dropped or are unknown), no sibling gives the element counts, so the
		// absent sub-fields are read from the shape of the column itself.
		for j, subField := range structField.Fields {
			var reader *FieldReader
			var err error
			switch {
			case fieldIndices[j] != -1:
				reader, err = NewStructFieldReader(ctx, fileReader, columnIndex, fieldIndices[j], structField.GetName(), subField)
			case presentSubFields == 0:
				reader, err = NewStructNullElementsReader(ctx, fileReader, columnIndex, structField.GetName(), subField)
			default:
				continue
			}
			if err != nil {
				return nil, err
			}

			crs[subField.FieldID] = reader
			readFields[subField.Name] = subField.FieldID
		}
	}

	// this loop is for "are there any fields not provided in the parquet file?"
	for _, field := range nameToField {
		// auto-id field, function output field already checked
		// dynamic field, nullable field, default value field, not provided or provided both ok
		// element-nullable struct sub-field, filled with null elements
		if typeutil.IsAutoPKField(field) || field.GetIsDynamic() || field.GetIsFunctionOutput() ||
			field.GetNullable() || field.GetDefaultValue() != nil || field.GetElementNullable() {
			continue
		}
		// the other field must be provided
		if _, ok := crs[field.GetFieldID()]; !ok {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("no parquet field for milvus field '%s'", field.GetName()))
		}
	}

	mlog.Info(ctx, "create parquet column readers", mlog.Any("readFields", readFields))
	return crs, nil
}

func rejectFlatStructSubFieldColumns(schema *schemapb.CollectionSchema, arrSchema *arrow.Schema) error {
	arrNameToField := lo.KeyBy(arrSchema.Fields(), func(field arrow.Field) string {
		return field.Name
	})
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			if _, ok := arrNameToField[subField.GetName()]; ok {
				return merr.WrapErrImportFailedMsg(
					"struct field '%s' must be provided as list<struct>; flat sub-field column '%s' is not supported",
					structField.GetName(), subField.GetName())
			}
		}
	}
	return nil
}

func isArrowIntegerType(dataType arrow.Type) bool {
	switch dataType {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return true
	default:
		return false
	}
}

func isArrowFloatingType(dataType arrow.Type) bool {
	switch dataType {
	case arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

func isArrowArithmeticType(dataType arrow.Type) bool {
	return isArrowIntegerType(dataType) || isArrowFloatingType(dataType)
}

func isArrowDataTypeConvertible(src arrow.DataType, dst arrow.DataType, field *schemapb.FieldSchema, allowFixedSizeList bool) bool {
	srcType := src.ID()
	dstType := dst.ID()
	// arrow.UINT8 is the byte-backed Arrow representation for fp16/bf16 vectors, not INT8_VECTOR.
	if isFP16BF16VectorField(field) && dstType == arrow.UINT8 && isArrowFloatingType(srcType) {
		return true
	}
	switch srcType {
	case arrow.BOOL:
		return dstType == arrow.BOOL
	case arrow.UINT8:
		return dstType == arrow.UINT8
	case arrow.INT8:
		return isArrowArithmeticType(dstType)
	case arrow.INT16:
		return isArrowArithmeticType(dstType) && dstType != arrow.INT8
	case arrow.INT32:
		return isArrowArithmeticType(dstType) && dstType != arrow.INT8 && dstType != arrow.INT16
	case arrow.INT64:
		return isArrowFloatingType(dstType) || dstType == arrow.INT64
	case arrow.FLOAT32:
		return isArrowFloatingType(dstType)
	case arrow.FLOAT64:
		// TODO caiyd: need do strict type check
		// return dstType == arrow.FLOAT64
		return isArrowFloatingType(dstType)
	case arrow.STRING:
		return dstType == arrow.STRING
	case arrow.BINARY:
		return dstType == arrow.LIST && dst.(*arrow.ListType).Elem().ID() == arrow.UINT8
	case arrow.LIST:
		return dstType == arrow.LIST && isArrowDataTypeConvertible(src.(*arrow.ListType).Elem(), dst.(*arrow.ListType).Elem(), field, false)
	case arrow.FIXED_SIZE_LIST:
		return allowFixedSizeList && isFixedSizeListImportTarget(field) &&
			dstType == arrow.LIST &&
			isArrowDataTypeConvertible(src.(*arrow.FixedSizeListType).Elem(), dst.(*arrow.ListType).Elem(), field, false)
	case arrow.NULL:
		// if nullable==true or has set default_value, can use null type
		return field.GetNullable() || field.GetDefaultValue() != nil
	case arrow.STRUCT:
		if field.GetDataType() == schemapb.DataType_SparseFloatVector {
			valid, _ := IsValidSparseVectorSchema(src)
			return valid
		}
		return false
	case arrow.FIXED_SIZE_BINARY:
		return dstType == arrow.FIXED_SIZE_BINARY
	default:
		return false
	}
}

func isFP16BF16VectorField(field *schemapb.FieldSchema) bool {
	return field.GetDataType() == schemapb.DataType_Float16Vector || field.GetDataType() == schemapb.DataType_BFloat16Vector
}

func isFixedSizeListImportTarget(field *schemapb.FieldSchema) bool {
	switch field.GetDataType() {
	case schemapb.DataType_Array:
		switch field.GetElementType() {
		case schemapb.DataType_Bool,
			schemapb.DataType_Int8,
			schemapb.DataType_Int16,
			schemapb.DataType_Int32,
			schemapb.DataType_Int64,
			schemapb.DataType_Float,
			schemapb.DataType_Double,
			schemapb.DataType_VarChar,
			schemapb.DataType_String:
			return true
		default:
			return false
		}
	case schemapb.DataType_BinaryVector,
		schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector:
		return true
	default:
		return false
	}
}

// This method returns two booleans
// The first boolean value means the arrowType is a valid sparse vector schema
// The second boolean value: true means the sparse vector is stored as JSON-format string,
// false means the sparse vector is stored as parquet struct
func IsValidSparseVectorSchema(arrowType arrow.DataType) (bool, bool) {
	arrowID := arrowType.ID()
	if arrowID == arrow.STRUCT {
		arrType := arrowType.(*arrow.StructType)
		indicesType, ok1 := arrType.FieldByName(sparseVectorIndice)
		valuesType, ok2 := arrType.FieldByName(sparseVectorValues)
		if !ok1 || !ok2 {
			return false, false
		}

		// indices can be uint32 list or int64 list
		// values can be float32 list or float64 list
		isValidType := func(finger string, expectedType arrow.DataType) bool {
			return finger == arrow.ListOf(expectedType).Fingerprint()
		}
		indicesFinger := indicesType.Type.Fingerprint()
		valuesFinger := valuesType.Type.Fingerprint()
		indicesTypeIsOK := (isValidType(indicesFinger, arrow.PrimitiveTypes.Int32) ||
			isValidType(indicesFinger, arrow.PrimitiveTypes.Uint32) ||
			isValidType(indicesFinger, arrow.PrimitiveTypes.Int64) ||
			isValidType(indicesFinger, arrow.PrimitiveTypes.Uint64))
		valuesTypeIsOK := (isValidType(valuesFinger, arrow.PrimitiveTypes.Float32) ||
			isValidType(valuesFinger, arrow.PrimitiveTypes.Float64))
		return indicesTypeIsOK && valuesTypeIsOK, false
	}
	return arrowID == arrow.STRING, true
}

// For ArrayOfVector, use natural user format (list of list of primitives)
// instead of internal fixed_size_binary format
func convertElementTypeOfVectorArrayToArrowType(field *schemapb.FieldSchema) (arrow.DataType, error) {
	if field.GetDataType() != schemapb.DataType_ArrayOfVector {
		return nil, merr.WrapErrParameterInvalidMsg("field is not a vector array: %v", field.GetDataType().String())
	}

	var elemType arrow.DataType
	switch field.GetElementType() {
	case schemapb.DataType_FloatVector:
		elemType = arrow.ListOf(arrow.PrimitiveTypes.Float32)
	case schemapb.DataType_BinaryVector:
		elemType = arrow.ListOf(arrow.PrimitiveTypes.Uint8)
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		elemType = arrow.ListOf(arrow.PrimitiveTypes.Uint8)
	case schemapb.DataType_Int8Vector:
		elemType = arrow.ListOf(arrow.PrimitiveTypes.Int8)
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported element type for ArrayOfVector: %v", field.GetElementType().String())
	}
	return elemType, nil
}

func convertToArrowDataType(field *schemapb.FieldSchema, isArray bool) (arrow.DataType, error) {
	dataType := field.GetDataType()
	if isArray {
		dataType = field.GetElementType()
	}
	switch dataType {
	case schemapb.DataType_Bool:
		return &arrow.BooleanType{}, nil
	case schemapb.DataType_Int8:
		return &arrow.Int8Type{}, nil
	case schemapb.DataType_Int16:
		return &arrow.Int16Type{}, nil
	case schemapb.DataType_Int32:
		return &arrow.Int32Type{}, nil
	case schemapb.DataType_Int64:
		return &arrow.Int64Type{}, nil
	case schemapb.DataType_Float:
		return &arrow.Float32Type{}, nil
	case schemapb.DataType_Double:
		return &arrow.Float64Type{}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text, schemapb.DataType_Timestamptz:
		return &arrow.StringType{}, nil
	case schemapb.DataType_JSON:
		return &arrow.StringType{}, nil
	case schemapb.DataType_Geometry:
		return &arrow.StringType{}, nil
	case schemapb.DataType_Array:
		if typeutil.IsNestedArrayTypeSchema(field.GetTypeSchema()) {
			return convertNestedArrayToArrowType(field, isArray)
		}
		if isArray {
			// Only a nested Array has an Array element, and it carries a nested
			// type_schema; the schema was validated at DDL time.
			return nil, merr.WrapErrImportSysFailedMsg(
				"field '%s' has element type Array but no nested type_schema", field.GetName())
		}
		elemType, err := convertToArrowDataType(field, true)
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     elemType,
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Uint8Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	case schemapb.DataType_FloatVector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Float32Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	case schemapb.DataType_SparseFloatVector:
		return &arrow.StringType{}, nil
	case schemapb.DataType_Int8Vector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Int8Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	case schemapb.DataType_ArrayOfVector:
		elemType, err := convertElementTypeOfVectorArrayToArrowType(field)
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     elemType,
			Nullable: true,
			Metadata: arrow.Metadata{},
		}), nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported data type %v", dataType.String())
	}
}

// nestedArrayLeaf resolves the leaf of a nested Array<Array<T>> field. The
// schema was validated at DDL time, so a chain that does not resolve to exactly
// two list levels is a Milvus bug.
func nestedArrayLeaf(field *schemapb.FieldSchema) (schemapb.DataType, bool, error) {
	leaf, leafNullable, depth, err := typeutil.GetFieldArrayLeaf(field)
	if err != nil {
		return schemapb.DataType_None, false, merr.WrapErrServiceInternalErr(err, "resolve Array leaf of field %s", field.GetName())
	}
	if depth != 2 {
		return schemapb.DataType_None, false, merr.WrapErrImportSysFailedMsg(
			"nested Array field '%s' supports exactly two list levels, got %d", field.GetName(), depth)
	}
	return leaf, leafNullable, nil
}

// convertNestedArrayToArrowType returns the arrow type of a nested
// Array<Array<T>> field: list<T> for one element (a struct child, isArray=true)
// and list<list<T>> for the whole field. Item nullability follows the
// type_schema: the inner array is nullable iff element_nullable, the leaf iff
// the leaf node is nullable.
func convertNestedArrayToArrowType(field *schemapb.FieldSchema, isArray bool) (arrow.DataType, error) {
	leaf, leafNullable, err := nestedArrayLeaf(field)
	if err != nil {
		return nil, err
	}
	leafType, err := convertToArrowDataType(&schemapb.FieldSchema{Name: field.GetName(), DataType: leaf}, false)
	if err != nil {
		return nil, err
	}
	inner := arrow.ListOfField(arrow.Field{
		Name:     "item",
		Type:     leafType,
		Nullable: leafNullable,
		Metadata: arrow.Metadata{},
	})
	if isArray {
		return inner, nil
	}
	return arrow.ListOfField(arrow.Field{
		Name:     "item",
		Type:     inner,
		Nullable: field.GetElementNullable(),
		Metadata: arrow.Metadata{},
	}), nil
}

// structChildArrowType returns the arrow type of one element of a struct
// sub-field, i.e. the type of its child in the list<struct> column.
func structChildArrowType(subField *schemapb.FieldSchema) (arrow.DataType, error) {
	switch subField.GetDataType() {
	case schemapb.DataType_Array:
		return convertToArrowDataType(subField, true)
	case schemapb.DataType_ArrayOfVector:
		return convertElementTypeOfVectorArrayToArrowType(subField)
	default:
		return nil, merr.WrapErrImportFailedMsg("unsupported data type in struct field: %v", subField.GetDataType())
	}
}

// isStructChildConvertible reports whether a struct child of arrow type src can
// be read into subField. Unlike isArrowDataTypeConvertible, the arrow null type
// is checked against the nullability of the level it appears at: a null child
// iff element_nullable, a list<null> nested child iff the leaf is nullable.
// The row-level nullable flag never applies to a child.
func isStructChildConvertible(src arrow.DataType, subField *schemapb.FieldSchema) (bool, error) {
	if src.ID() == arrow.NULL {
		return subField.GetElementNullable(), nil
	}
	expected, err := structChildArrowType(subField)
	if err != nil {
		return false, err
	}
	switch subField.GetDataType() {
	case schemapb.DataType_Array:
		if !typeutil.IsNestedArrayTypeSchema(subField.GetTypeSchema()) {
			return isArrowDataTypeConvertible(src, expected, subField, false), nil
		}
		_, leafNullable, err := nestedArrayLeaf(subField)
		if err != nil {
			return false, err
		}
		srcList, ok := src.(*arrow.ListType)
		if !ok {
			return false, nil
		}
		if srcList.Elem().ID() == arrow.NULL {
			return leafNullable, nil
		}
		return isArrowDataTypeConvertible(srcList.Elem(), expected.(*arrow.ListType).Elem(), subField, false), nil
	case schemapb.DataType_ArrayOfVector:
		// A vector cannot hold null components, whatever the row nullability.
		if srcList, ok := src.(*arrow.ListType); ok && srcList.Elem().ID() == arrow.NULL {
			return false, nil
		}
		return isArrowDataTypeConvertible(src, expected, subField, false), nil
	default:
		return false, nil
	}
}

// This method is used only by import util and related tests. Returned arrow.Schema
// doesn't include function output fields.
func ConvertToArrowSchemaForUT(schema *schemapb.CollectionSchema, useNullType bool) (*arrow.Schema, error) {
	arrFields := make([]arrow.Field, 0, 10)

	for _, field := range schema.Fields {
		if typeutil.IsAutoPKField(field) || field.GetIsFunctionOutput() {
			continue
		}
		arrDataType, err := convertToArrowDataType(field, false)
		if err != nil {
			return nil, err
		}
		nullable := field.GetNullable()
		if field.GetNullable() && useNullType {
			arrDataType = arrow.Null
		}
		if field.GetDefaultValue() != nil && useNullType {
			arrDataType = arrow.Null
			nullable = true
		}
		arrFields = append(arrFields, arrow.Field{
			Name:     field.GetName(),
			Type:     arrDataType,
			Nullable: nullable,
			Metadata: arrow.Metadata{},
		})
	}

	for _, structField := range schema.StructArrayFields {
		// Build struct fields for row-wise format
		structFields := make([]arrow.Field, 0, len(structField.Fields))
		for _, subField := range structField.Fields {
			fieldName, err := typeutil.ExtractStructFieldName(subField.Name)
			if err != nil {
				return nil, merr.WrapErrImportFailed(err.Error())
			}

			arrDataType, err := structChildArrowType(subField)
			if err != nil {
				return nil, err
			}

			// A child value is one struct element, so its nullability is the
			// element-level one; row nulls are null list slots.
			structFields = append(structFields, arrow.Field{
				Name:     fieldName,
				Type:     arrDataType,
				Nullable: subField.GetElementNullable(),
			})
		}

		// Create list<struct> type
		structType := arrow.StructOf(structFields...)
		listType := arrow.ListOf(structType)

		arrFields = append(arrFields, arrow.Field{
			Name:     structField.Name,
			Type:     listType,
			Nullable: structField.GetNullable(),
		})
	}

	return arrow.NewSchema(arrFields, nil), nil
}

func isSchemaEqual(schema *schemapb.CollectionSchema, arrSchema *arrow.Schema) error {
	arrNameToField := lo.KeyBy(arrSchema.Fields(), func(field arrow.Field) string {
		return field.Name
	})

	// Check all fields (including struct sub-fields which are stored as separate columns)
	for _, field := range schema.Fields {
		// ignore autoPKField and functionOutputField
		if typeutil.IsAutoPKField(field) || field.GetIsFunctionOutput() {
			continue
		}
		arrField, ok := arrNameToField[field.GetName()]
		if !ok {
			// Special fields no need to provide in data files, the parquet file doesn't contain this field, no need to compare
			// 1. dynamic field(name is "$meta"), ignore
			// 2. nullable field, filled with null values
			// 3. default value field, filled with default value
			if field.GetIsDynamic() || field.GetNullable() || field.GetDefaultValue() != nil {
				continue
			}
			return merr.WrapErrImportFailedMsg("field '%s' not in arrow schema", field.GetName())
		}
		toArrDataType, err := convertToArrowDataType(field, false)
		if err != nil {
			return err
		}
		if !isArrowDataTypeConvertible(arrField.Type, toArrDataType, field, true) {
			return merr.WrapErrImportFailedMsg("field '%s' type mis-match, expect arrow type '%s', get arrow data type '%s'",
				field.Name, toArrDataType.String(), arrField.Type.String())
		}
	}

	for _, structField := range schema.StructArrayFields {
		arrStructField, ok := arrNameToField[structField.Name]
		if !ok {
			if structField.GetNullable() {
				continue
			}
			return merr.WrapErrImportFailedMsg("struct field not found in arrow schema: %s", structField.Name)
		}

		// Verify the arrow field is list<struct> type
		listType, ok := arrStructField.Type.(*arrow.ListType)
		if !ok {
			return merr.WrapErrImportFailedMsg("struct field '%s' should be list type in arrow schema, but got '%s'",
				structField.Name, arrStructField.Type.String())
		}

		structType, ok := listType.Elem().(*arrow.StructType)
		if !ok {
			return merr.WrapErrImportFailedMsg("struct field '%s' should contain struct elements in arrow schema, but got '%s'",
				structField.Name, listType.Elem().String())
		}

		// Create a map of struct field names to arrow.Field for quick lookup
		structFieldMap := make(map[string]arrow.Field)
		for _, arrowField := range structType.Fields() {
			structFieldMap[arrowField.Name] = arrowField
		}

		// Verify each sub-field in the struct. Children that match no sub-field
		// (unknown or dropped sub-fields) are ignored and never read.
		for _, subField := range structField.Fields {
			// Extract actual field name (remove structName[] prefix if present)
			fieldName, err := typeutil.ExtractStructFieldName(subField.Name)
			if err != nil {
				return merr.WrapErrImportFailed(err.Error())
			}

			arrowSubField, ok := structFieldMap[fieldName]
			if !ok {
				// filled with null elements, see CreateFieldReaders
				if subField.GetElementNullable() {
					continue
				}
				return merr.WrapErrImportFailedMsg("sub-field '%s' not found in struct '%s' of arrow schema",
					fieldName, structField.Name)
			}

			expectedArrowType, err := structChildArrowType(subField)
			if err != nil {
				return err
			}

			// Check if the arrow type is convertible to the expected type
			convertible, err := isStructChildConvertible(arrowSubField.Type, subField)
			if err != nil {
				return err
			}
			if !convertible {
				return merr.WrapErrImportFailedMsg("sub-field '%s' in struct '%s' type mis-match, expect arrow type '%s', got '%s'",
					fieldName, structField.Name, expectedArrowType.String(), arrowSubField.Type.String())
			}
		}
	}

	return nil
}

// todo(smellthemoon): use byte to store valid_data
func bytesToValidData(length int, bytes []byte) []bool {
	bools := make([]bool, 0, length)
	if len(bytes) == 0 {
		// parquet field is "optional" or "required"
		// for "required" field, the arrow.array.NullBitmapBytes() returns an empty byte list
		// which means all the elements are valid. In this case, we simply construct an all-true bool array
		for i := 0; i < length; i++ {
			bools = append(bools, true)
		}
		return bools
	}

	// for "optional" field, the arrow.array.NullBitmapBytes() returns a non-empty byte list
	// with each bit representing the existence of an element
	for i := 0; i < length; i++ {
		bit := (bytes[uint(i)/8] & BitMask[byte(i)%8]) != 0
		bools = append(bools, bit)
	}

	return bools
}

var (
	BitMask        = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	FlippedBitMask = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}
)
