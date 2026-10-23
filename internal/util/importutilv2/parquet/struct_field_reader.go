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
	"math"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// StructFieldReader reads a specific field from a list<struct> column
type StructFieldReader struct {
	columnReader *pqarrow.ColumnReader
	structName   string
	field        *schemapb.FieldSchema
	fieldIndex   int
	dim          int
	// nullElementsOnly marks a reader for a sub-field the file does not carry
	// in a struct column none of whose children is a current sub-field. It
	// reads one leaf of the column only for the list shape and emits a null
	// element per struct element.
	nullElementsOnly bool
}

// collectSubFieldLeaves returns the parquet leaf column indices belonging to the
// fieldIndex-th sub-field of the list<struct> column at columnIndex.
func collectSubFieldLeaves(manifest *pqarrow.SchemaManifest, columnIndex, fieldIndex int) (map[int]bool, error) {
	if manifest == nil || columnIndex < 0 || columnIndex >= len(manifest.Fields) {
		return nil, merr.WrapErrImportSysFailedMsg("struct column index %d out of range", columnIndex)
	}
	listField := &manifest.Fields[columnIndex]
	if len(listField.Children) != 1 {
		return nil, merr.WrapErrImportSysFailedMsg("struct column %d is not a list of structs", columnIndex)
	}
	structField := &listField.Children[0]
	if fieldIndex < 0 || fieldIndex >= len(structField.Children) {
		return nil, merr.WrapErrImportSysFailedMsg("struct sub-field index %d out of range", fieldIndex)
	}

	leaves := make(map[int]bool)
	collectLeafColumnIndices(&structField.Children[fieldIndex], leaves)
	// Unreachable with the current recursion, which always records at least one
	// index for any node it is handed. Kept as a guard so a future change to
	// collectLeafColumnIndices cannot silently hand arrow an empty leaf set,
	// which GetFieldReader answers with a nil reader rather than an error.
	if len(leaves) == 0 {
		return nil, merr.WrapErrImportSysFailedMsg("no leaf column found for struct sub-field index %d", fieldIndex)
	}
	return leaves, nil
}

// collectLeafColumnIndices walks a schema subtree and records every leaf column index.
//
// Recursion terminates on len(Children) == 0 rather than SchemaField.IsLeaf():
// arrow assigns ColIndex only in populateLeaf and zero-initializes every other
// SchemaField, so IsLeaf() (ColIndex != -1) reports true for group nodes and
// yields a bogus index of 0. arrow's own getReader guards the same way.
func collectLeafColumnIndices(field *pqarrow.SchemaField, leaves map[int]bool) {
	if len(field.Children) == 0 {
		leaves[field.ColIndex] = true
		return
	}
	for i := range field.Children {
		collectLeafColumnIndices(&field.Children[i], leaves)
	}
}

// shapeLeaf returns one leaf column index of the list<struct> column at
// columnIndex. Every leaf carries the repetition and definition levels of the
// outer list and of the struct, so any one of them yields the row validity,
// the element count of each row and the struct slot validity. A direct
// primitive child is preferred because it decodes the fewest values.
func shapeLeaf(manifest *pqarrow.SchemaManifest, columnIndex int) (map[int]bool, error) {
	if manifest == nil || columnIndex < 0 || columnIndex >= len(manifest.Fields) {
		return nil, merr.WrapErrImportSysFailedMsg("struct column index %d out of range", columnIndex)
	}
	listField := &manifest.Fields[columnIndex]
	if len(listField.Children) != 1 || len(listField.Children[0].Children) == 0 {
		return nil, merr.WrapErrImportSysFailedMsg("struct column %d is not a list of non-empty structs", columnIndex)
	}
	children := listField.Children[0].Children
	for i := range children {
		if len(children[i].Children) == 0 {
			return map[int]bool{children[i].ColIndex: true}, nil
		}
	}
	node := &children[0]
	for len(node.Children) > 0 {
		node = &node.Children[0]
	}
	return map[int]bool{node.ColIndex: true}, nil
}

// NewStructFieldReader creates a reader for extracting a field from nested struct
func NewStructFieldReader(ctx context.Context, fileReader *pqarrow.FileReader, columnIndex int,
	fieldIndex int, structName string, field *schemapb.FieldSchema,
) (*FieldReader, error) {
	// Only pull the leaf columns of this sub-field. Using GetColumn here would
	// decode every leaf of the list<struct> column for every sub-field reader,
	// reading the whole column N times for N sub-fields.
	includedLeaves, err := collectSubFieldLeaves(fileReader.Manifest, columnIndex, fieldIndex)
	if err != nil {
		return nil, merr.Wrapf(err, "failed to resolve leaf columns for struct sub-field '%s'", field.GetName())
	}
	return newStructFieldReader(ctx, fileReader, columnIndex, includedLeaves, structName, field, false)
}

// NewStructNullElementsReader creates a reader for an element-nullable
// sub-field of a struct column that carries none of the current sub-fields,
// e.g. a file whose children were all dropped from the schema since. There is
// no sibling column for FillMissingStructSubFields to take the element counts
// from, so this reader takes them from the outer list of the column and emits
// the rows that fill would build: a null struct row stays a null row, a valid
// row of N elements gets N null elements.
func NewStructNullElementsReader(ctx context.Context, fileReader *pqarrow.FileReader, columnIndex int,
	structName string, field *schemapb.FieldSchema,
) (*FieldReader, error) {
	if !field.GetElementNullable() {
		return nil, merr.WrapErrImportSysFailedMsg("sub-field '%s' of struct field '%s' is not element-nullable",
			field.GetName(), structName)
	}
	includedLeaves, err := shapeLeaf(fileReader.Manifest, columnIndex)
	if err != nil {
		return nil, merr.Wrapf(err, "failed to resolve the shape leaf of struct field '%s'", structName)
	}
	return newStructFieldReader(ctx, fileReader, columnIndex, includedLeaves, structName, field, true)
}

func newStructFieldReader(ctx context.Context, fileReader *pqarrow.FileReader, columnIndex int,
	includedLeaves map[int]bool, structName string, field *schemapb.FieldSchema, nullElementsOnly bool,
) (*FieldReader, error) {
	rowGroups := make([]int, fileReader.ParquetReader().NumRowGroups())
	for i := range rowGroups {
		rowGroups[i] = i
	}

	columnReader, err := fileReader.GetFieldReader(ctx, columnIndex, includedLeaves, rowGroups)
	if err != nil {
		return nil, err
	}
	if columnReader == nil {
		return nil, merr.WrapErrImportSysFailedMsg("no column reader for struct sub-field '%s'", field.GetName())
	}

	dim := 0
	if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
		d, err := typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
		dim = int(d)
	} else if field.GetDataType() == schemapb.DataType_ArrayOfVector {
		// For ArrayOfVector, get the dimension from the element type
		d, err := typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
		dim = int(d)
	}

	sfr := &StructFieldReader{
		columnReader: columnReader,
		structName:   structName,
		field:        field,
		// Leaf pruning leaves exactly one surviving child under the struct, so
		// the sub-field always sits at position 0 in the arrow struct this
		// reader produces.
		fieldIndex:       0,
		dim:              dim,
		nullElementsOnly: nullElementsOnly,
	}

	fr := &FieldReader{
		columnIndex:  columnIndex,
		columnReader: columnReader,
		field:        field,
		dim:          dim,
		structReader: sfr,
	}

	return fr, nil
}

// Next extracts the specific field from struct array
func (r *StructFieldReader) Next(count int64) (any, any, error) {
	chunked, err := r.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}

	// If no more data, return nil to signal EOF
	if chunked.Len() == 0 {
		return nil, nil, nil
	}

	if r.nullElementsOnly {
		return r.readNullElementsField(chunked)
	}

	switch r.field.GetDataType() {
	case schemapb.DataType_Array:
		if typeutil.IsNestedArrayTypeSchema(r.field.GetTypeSchema()) {
			return r.readNestedArrayField(chunked)
		}
		return r.readArrayField(chunked)
	case schemapb.DataType_ArrayOfVector:
		return r.readArrayOfVectorField(chunked)
	default:
		return nil, nil, merr.WrapErrImportFailedMsg("unsupported data type for struct field: %v", r.field.GetDataType())
	}
}

// isArrowNull reports whether value i of arr is null. An arrow null-typed array
// has no validity bitmap, so its IsNull answers false although every value is null.
func isArrowNull(arr arrow.Array, i int) bool {
	return arr.DataType().ID() == arrow.NULL || arr.IsNull(i)
}

// emptyScalarField returns an empty ScalarField typed for an Array element type.
// Int8 and Int16 elements live in IntData, String and VarChar in StringData.
func emptyScalarField(elementType schemapb.DataType) (*schemapb.ScalarField, error) {
	switch elementType {
	case schemapb.DataType_Bool:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{}}}}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{}}}}, nil
	case schemapb.DataType_Int64:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{}}}}, nil
	case schemapb.DataType_Float:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{}}}}, nil
	case schemapb.DataType_Double:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{}}}}, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{}}}}, nil
	default:
		return nil, merr.WrapErrImportFailedMsg("unsupported element type for struct field: %v", elementType)
	}
}

// appendZeroScalar appends the placeholder of a null element to a row built by
// emptyScalarField.
func appendZeroScalar(row *schemapb.ScalarField) {
	switch data := row.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		data.BoolData.Data = append(data.BoolData.Data, false)
	case *schemapb.ScalarField_IntData:
		data.IntData.Data = append(data.IntData.Data, 0)
	case *schemapb.ScalarField_LongData:
		data.LongData.Data = append(data.LongData.Data, 0)
	case *schemapb.ScalarField_FloatData:
		data.FloatData.Data = append(data.FloatData.Data, 0)
	case *schemapb.ScalarField_DoubleData:
		data.DoubleData.Data = append(data.DoubleData.Data, 0)
	case *schemapb.ScalarField_StringData:
		data.StringData.Data = append(data.StringData.Data, "")
	}
}

// arrowIntegerValue reads value i of an integer arrow array.
func arrowIntegerValue(values arrow.Array, i int) (int64, bool) {
	switch v := values.(type) {
	case *array.Int8:
		return int64(v.Value(i)), true
	case *array.Int16:
		return int64(v.Value(i)), true
	case *array.Int32:
		return int64(v.Value(i)), true
	case *array.Int64:
		return v.Value(i), true
	default:
		return 0, false
	}
}

// arrowFloatingValue reads value i of an integer or floating arrow array.
func arrowFloatingValue(values arrow.Array, i int) (float64, bool) {
	switch v := values.(type) {
	case *array.Float32:
		return float64(v.Value(i)), true
	case *array.Float64:
		return v.Value(i), true
	default:
		n, ok := arrowIntegerValue(values, i)
		return float64(n), ok
	}
}

// appendArrowScalar converts value i of a scalar arrow array to elementType and
// appends it to a row built by emptyScalarField. It accepts the integer and
// floating widening that isArrowDataTypeConvertible lets through the schema
// check, so the reader never rejects a type the schema check accepted. Int8
// and Int16 values are range checked because the storage writer casts them
// without one.
func appendArrowScalar(row *schemapb.ScalarField, field *schemapb.FieldSchema, elementType schemapb.DataType,
	maxLength int64, values arrow.Array, i int,
) error {
	switch elementType {
	case schemapb.DataType_Bool:
		v, ok := values.(*array.Boolean)
		if !ok {
			return WrapTypeErr(field, values.DataType().Name())
		}
		row.GetBoolData().Data = append(row.GetBoolData().Data, v.Value(i))
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		v, ok := arrowIntegerValue(values, i)
		if !ok {
			return WrapTypeErr(field, values.DataType().Name())
		}
		lower, upper := int64(math.MinInt32), int64(math.MaxInt32)
		switch elementType {
		case schemapb.DataType_Int8:
			lower, upper = math.MinInt8, math.MaxInt8
		case schemapb.DataType_Int16:
			lower, upper = math.MinInt16, math.MaxInt16
		}
		if v < lower || v > upper {
			return merr.WrapErrImportFailedMsg("value %d is out of range of %s for field '%s'", v, elementType.String(), field.GetName())
		}
		row.GetIntData().Data = append(row.GetIntData().Data, int32(v))
	case schemapb.DataType_Int64:
		v, ok := arrowIntegerValue(values, i)
		if !ok {
			return WrapTypeErr(field, values.DataType().Name())
		}
		row.GetLongData().Data = append(row.GetLongData().Data, v)
	case schemapb.DataType_Float:
		v, ok := arrowFloatingValue(values, i)
		if !ok {
			return WrapTypeErr(field, values.DataType().Name())
		}
		value := float32(v)
		if err := typeutil.VerifyFloat(float64(value)); err != nil {
			return merr.Wrap(err, "float32 verification failed")
		}
		row.GetFloatData().Data = append(row.GetFloatData().Data, value)
	case schemapb.DataType_Double:
		v, ok := arrowFloatingValue(values, i)
		if !ok {
			return WrapTypeErr(field, values.DataType().Name())
		}
		if err := typeutil.VerifyFloat(v); err != nil {
			return merr.Wrap(err, "float64 verification failed")
		}
		row.GetDoubleData().Data = append(row.GetDoubleData().Data, v)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		v, ok := values.(*array.String)
		if !ok {
			return WrapTypeErr(field, values.DataType().Name())
		}
		value := strings.Clone(v.Value(i))
		if err := common.CheckValidString(value, maxLength, field); err != nil {
			return err
		}
		row.GetStringData().Data = append(row.GetStringData().Data, value)
	default:
		return merr.WrapErrImportFailedMsg("unsupported element type for struct field: %v", elementType)
	}
	return nil
}

// checkStructSlots rejects a null struct element in the slots [start, end) of
// one row.
func (r *StructFieldReader) checkStructSlots(structArray *array.Struct, start, end int64) error {
	for structIdx := start; structIdx < end; structIdx++ {
		if structArray.IsNull(int(structIdx)) {
			return WrapNullStructElementErr(r.structName, int(structIdx-start))
		}
	}
	return nil
}

// readArrayField reads a single-level Array sub-field. Each row is a dense
// typed payload with one value per struct element. An element-nullable
// sub-field writes a zero placeholder at a null element and carries element
// valid_data of the full row length; a legacy sub-field rejects null elements
// and carries no valid_data.
func (r *StructFieldReader) readArrayField(chunked *arrow.Chunked) (any, any, error) {
	result := make([]*schemapb.ScalarField, 0)
	var validData []bool
	if r.field.GetNullable() {
		validData = make([]bool, 0)
	}
	elementType := r.field.GetElementType()
	elementNullable := r.field.GetElementNullable()
	maxCapacity, err := parameterutil.GetMaxCapacity(r.field)
	if err != nil {
		return nil, nil, err
	}
	var maxLength int64
	if typeutil.IsStringType(elementType) {
		maxLength, err = parameterutil.GetMaxLength(r.field)
		if err != nil {
			return nil, nil, err
		}
	}

	appendNullRow := func() error {
		scalarField, err := emptyScalarField(elementType)
		if err != nil {
			return err
		}
		result = append(result, scalarField)
		validData = append(validData, false)
		return nil
	}

	for _, chunk := range chunked.Chunks() {
		switch listArray := chunk.(type) {
		case *array.Null:
			if !r.field.GetNullable() {
				return nil, nil, WrapNullRowErr(r.field)
			}
			for i := 0; i < listArray.Len(); i++ {
				if err := appendNullRow(); err != nil {
					return nil, nil, err
				}
			}
		case *array.List:
			if !r.field.GetNullable() && listArray.NullN() > 0 {
				return nil, nil, WrapNullRowErr(r.field)
			}

			structArray, ok := listArray.ListValues().(*array.Struct)
			if !ok {
				return nil, nil, merr.WrapErrImportFailed("expected struct in list")
			}

			fieldArray := structArray.Field(r.fieldIndex)

			for i := 0; i < listArray.Len(); i++ {
				if listArray.IsNull(i) {
					if err := appendNullRow(); err != nil {
						return nil, nil, err
					}
					continue
				}

				startIdx, endIdx := listArray.ValueOffsets(i)
				if err := r.checkStructSlots(structArray, startIdx, endIdx); err != nil {
					return nil, nil, err
				}
				if err := common.CheckArrayCapacity(int(endIdx-startIdx), maxCapacity, r.field); err != nil {
					return nil, nil, err
				}

				row, err := emptyScalarField(elementType)
				if err != nil {
					return nil, nil, err
				}
				var elementValid []bool
				if elementNullable {
					elementValid = make([]bool, 0, endIdx-startIdx)
				}
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					if isArrowNull(fieldArray, int(structIdx)) {
						if !elementNullable {
							return nil, nil, WrapNullElementErr(r.field)
						}
						appendZeroScalar(row)
						elementValid = append(elementValid, false)
						continue
					}
					if err := appendArrowScalar(row, r.field, elementType, maxLength, fieldArray, int(structIdx)); err != nil {
						return nil, nil, err
					}
					if elementNullable {
						elementValid = append(elementValid, true)
					}
				}
				if elementNullable {
					typeutil.SetArrayElementValidData(row, elementValid)
				}

				result = append(result, row)
				if r.field.GetNullable() {
					validData = append(validData, true)
				}
			}
		default:
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}
	}

	return result, validData, nil
}

// readNestedArrayField reads a nested Array<Array<T>> sub-field, whose struct
// child is list<T> (one inner array per struct element). Each row is
// ArrayData{ElementType: T} with one child per struct element. A null inner
// array (only when element_nullable) is a typed empty leaf with no valid_data;
// element valid_data of the full row length is set iff element_nullable. A
// non-null inner array is a dense typed leaf payload, with leaf valid_data of
// its full length iff the leaf is nullable.
func (r *StructFieldReader) readNestedArrayField(chunked *arrow.Chunked) (any, any, error) {
	result := make([]*schemapb.ScalarField, 0)
	var validData []bool
	if r.field.GetNullable() {
		validData = make([]bool, 0)
	}
	leaf, leafNullable, err := nestedArrayLeaf(r.field)
	if err != nil {
		return nil, nil, err
	}
	elementNullable := r.field.GetElementNullable()
	innerSchema := r.field.GetTypeSchema().GetArrayElement()
	maxCapacity, err := parameterutil.GetMaxCapacity(r.field)
	if err != nil {
		return nil, nil, err
	}
	innerCapacity, err := parameterutil.GetMaxCapacityFromTypeSchema(innerSchema)
	if err != nil {
		return nil, nil, err
	}
	var maxLength int64
	if typeutil.IsStringType(leaf) {
		// max_length of a nested VarChar sits on the leaf node, as in the proxy.
		maxLength, err = parameterutil.GetMaxLength(&schemapb.FieldSchema{
			Name:        r.field.GetName(),
			DataType:    schemapb.DataType_Array,
			ElementType: leaf,
			TypeParams:  innerSchema.GetArrayElement().GetTypeParams(),
		})
		if err != nil {
			return nil, nil, err
		}
	}

	newRow := func(children []*schemapb.ScalarField) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: leaf, Data: children},
		}}
	}
	appendNullRow := func() {
		result = append(result, newRow(nil))
		validData = append(validData, false)
	}

	// readInner reads the inner array at slot of a non-null list child.
	readInner := func(child *array.List, slot int) (*schemapb.ScalarField, error) {
		start, end := child.ValueOffsets(slot)
		if err := common.CheckArrayCapacity(int(end-start), innerCapacity, r.field); err != nil {
			return nil, err
		}
		inner, err := emptyScalarField(leaf)
		if err != nil {
			return nil, err
		}
		var leafValid []bool
		if leafNullable {
			leafValid = make([]bool, 0, end-start)
		}
		values := child.ListValues()
		for pos := start; pos < end; pos++ {
			if isArrowNull(values, int(pos)) {
				if !leafNullable {
					return nil, merr.WrapErrImportFailedMsg(
						"nested array leaf is not allowed to be null value for field '%s'", r.field.GetName())
				}
				appendZeroScalar(inner)
				leafValid = append(leafValid, false)
				continue
			}
			if err := appendArrowScalar(inner, r.field, leaf, maxLength, values, int(pos)); err != nil {
				return nil, err
			}
			if leafNullable {
				leafValid = append(leafValid, true)
			}
		}
		if leafNullable {
			typeutil.SetArrayElementValidData(inner, leafValid)
		}
		return inner, nil
	}

	for _, chunk := range chunked.Chunks() {
		switch listArray := chunk.(type) {
		case *array.Null:
			if !r.field.GetNullable() {
				return nil, nil, WrapNullRowErr(r.field)
			}
			for i := 0; i < listArray.Len(); i++ {
				appendNullRow()
			}
		case *array.List:
			if !r.field.GetNullable() && listArray.NullN() > 0 {
				return nil, nil, WrapNullRowErr(r.field)
			}

			structArray, ok := listArray.ListValues().(*array.Struct)
			if !ok {
				return nil, nil, merr.WrapErrImportFailed("expected struct in list")
			}

			fieldArray := structArray.Field(r.fieldIndex)
			listChild, isList := fieldArray.(*array.List)
			if !isList && fieldArray.DataType().ID() != arrow.NULL {
				return nil, nil, WrapTypeErr(r.field, fieldArray.DataType().Name())
			}

			for i := 0; i < listArray.Len(); i++ {
				if listArray.IsNull(i) {
					appendNullRow()
					continue
				}

				startIdx, endIdx := listArray.ValueOffsets(i)
				if err := r.checkStructSlots(structArray, startIdx, endIdx); err != nil {
					return nil, nil, err
				}
				if err := common.CheckArrayCapacity(int(endIdx-startIdx), maxCapacity, r.field); err != nil {
					return nil, nil, err
				}

				children := make([]*schemapb.ScalarField, 0, endIdx-startIdx)
				var elementValid []bool
				if elementNullable {
					elementValid = make([]bool, 0, endIdx-startIdx)
				}
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					if isArrowNull(fieldArray, int(structIdx)) {
						if !elementNullable {
							return nil, nil, WrapNullElementErr(r.field)
						}
						empty, err := emptyScalarField(leaf)
						if err != nil {
							return nil, nil, err
						}
						children = append(children, empty)
						elementValid = append(elementValid, false)
						continue
					}
					inner, err := readInner(listChild, int(structIdx))
					if err != nil {
						return nil, nil, err
					}
					children = append(children, inner)
					if elementNullable {
						elementValid = append(elementValid, true)
					}
				}

				row := newRow(children)
				if elementNullable {
					typeutil.SetArrayElementValidData(row, elementValid)
				}
				result = append(result, row)
				if r.field.GetNullable() {
					validData = append(validData, true)
				}
			}
		default:
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}
	}

	return result, validData, nil
}

// readArrayOfVectorField reads an ArrayOfVector sub-field. The payload of a row
// holds only its non-null vectors; an element-nullable sub-field carries
// element valid_data of the full row length, a legacy one rejects null vectors.
func (r *StructFieldReader) readArrayOfVectorField(chunked *arrow.Chunked) (any, any, error) {
	result := make([]*schemapb.VectorField, 0)
	var validData []bool
	if r.field.GetNullable() {
		validData = make([]bool, 0)
	}
	if _, err := vectorArrayBytesPerVector(r.field.GetElementType(), int64(r.dim)); err != nil {
		return nil, nil, err
	}
	maxCapacity, err := parameterutil.GetMaxCapacity(r.field)
	if err != nil {
		return nil, nil, err
	}

	for _, chunk := range chunked.Chunks() {
		switch listArray := chunk.(type) {
		case *array.Null:
			if !r.field.GetNullable() {
				return nil, nil, WrapNullRowErr(r.field)
			}
			for i := 0; i < listArray.Len(); i++ {
				result = append(result, emptyVectorArrayRow(int64(r.dim), r.field.GetElementType()))
				validData = append(validData, false)
			}
		case *array.List:
			if !r.field.GetNullable() && listArray.NullN() > 0 {
				return nil, nil, WrapNullRowErr(r.field)
			}

			structArray, ok := listArray.ListValues().(*array.Struct)
			if !ok {
				return nil, nil, merr.WrapErrImportFailed("expected struct in list")
			}

			fieldArray := structArray.Field(r.fieldIndex)
			// A null-typed child means every vector is null, allowed only for
			// an element-nullable sub-field.
			allNull := fieldArray.DataType().ID() == arrow.NULL
			if allNull && !r.field.GetElementNullable() {
				return nil, nil, WrapNullElementErr(r.field)
			}
			vectorList, isList := fieldArray.(*array.List)
			if !isList && !allNull {
				return nil, nil, merr.WrapErrImportFailed("expected list array for vector field")
			}

			for i := 0; i < listArray.Len(); i++ {
				if listArray.IsNull(i) {
					result = append(result, emptyVectorArrayRow(int64(r.dim), r.field.GetElementType()))
					validData = append(validData, false)
					continue
				}

				startIdx, endIdx := listArray.ValueOffsets(i)
				if err := r.checkStructSlots(structArray, startIdx, endIdx); err != nil {
					return nil, nil, err
				}
				if err = common.CheckArrayCapacity(int(endIdx-startIdx), maxCapacity, r.field); err != nil {
					return nil, nil, err
				}
				var rowData *schemapb.VectorField
				if allNull {
					rowData = emptyVectorArrayRow(int64(r.dim), r.field.GetElementType())
					typeutil.SetVectorArrayElementValidData(rowData, make([]bool, endIdx-startIdx))
				} else {
					rowData, err = buildVectorArrayFieldFromList(r.field, int64(r.dim), vectorList, startIdx, endIdx)
					if err != nil {
						return nil, nil, err
					}
				}
				result = append(result, rowData)
				if r.field.GetNullable() {
					validData = append(validData, true)
				}
			}
		default:
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}
	}

	return result, validData, nil
}

// readNullElementsField emits the rows of a sub-field the file does not carry
// from the shape of the list<struct> column: a null struct row is a null row,
// a valid row of N struct elements is a valid row of N null elements (N=0 is
// a valid empty row). The rows are built by the storage helpers
// FillMissingStructSubFields uses, so the layout is the same as that fill.
func (r *StructFieldReader) readNullElementsField(chunked *arrow.Chunked) (any, any, error) {
	isVector := r.field.GetDataType() == schemapb.DataType_ArrayOfVector
	if !isVector && r.field.GetDataType() != schemapb.DataType_Array {
		return nil, nil, merr.WrapErrImportSysFailedMsg("unexpected data type %s for struct sub-field '%s'",
			r.field.GetDataType().String(), r.field.GetName())
	}
	maxCapacity, err := parameterutil.GetMaxCapacity(r.field)
	if err != nil {
		return nil, nil, err
	}
	var validData []bool
	if r.field.GetNullable() {
		validData = make([]bool, 0)
	}
	scalarRows := make([]*schemapb.ScalarField, 0)
	vectorRows := make([]*schemapb.VectorField, 0)
	appendNullRow := func() {
		// the same rows ArrayFieldData / VectorArrayFieldData.AppendRow(nil) store
		if isVector {
			vectorRows = append(vectorRows, emptyVectorArrayRow(int64(r.dim), r.field.GetElementType()))
		} else {
			scalarRows = append(scalarRows, nil)
		}
		validData = append(validData, false)
	}

	for _, chunk := range chunked.Chunks() {
		listArray, ok := chunk.(*array.List)
		if !ok {
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}
		if !r.field.GetNullable() && listArray.NullN() > 0 {
			return nil, nil, WrapNullRowErr(r.field)
		}
		structArray, ok := listArray.ListValues().(*array.Struct)
		if !ok {
			return nil, nil, merr.WrapErrImportFailed("expected struct in list")
		}
		for i := 0; i < listArray.Len(); i++ {
			if listArray.IsNull(i) {
				appendNullRow()
				continue
			}
			startIdx, endIdx := listArray.ValueOffsets(i)
			if err := r.checkStructSlots(structArray, startIdx, endIdx); err != nil {
				return nil, nil, err
			}
			count := int(endIdx - startIdx)
			if err := common.CheckArrayCapacity(count, maxCapacity, r.field); err != nil {
				return nil, nil, err
			}
			if isVector {
				row, err := storage.NullElementsVectorArrayRow(r.field, int64(r.dim), count)
				if err != nil {
					return nil, nil, err
				}
				vectorRows = append(vectorRows, row)
			} else {
				row, err := storage.NullElementsArrayRow(r.field, count)
				if err != nil {
					return nil, nil, err
				}
				scalarRows = append(scalarRows, row)
			}
			if r.field.GetNullable() {
				validData = append(validData, true)
			}
		}
	}

	if isVector {
		return vectorRows, validData, nil
	}
	return scalarRows, validData, nil
}
