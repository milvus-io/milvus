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

package column

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
)

// columnNestedArrayBase implements `Column` for Array<T> sub-fields of struct array. Each row is
// the list of struct elements, and each element is an inner array of leaf values of type T.
//
// Two validity layers sit on top of the row level:
//   - elementValid (inner arrays) is aligned with values: elementValid[i] holds one flag per
//     logical inner array of payload row i;
//   - leafValid is aligned with the stored inner arrays: leafValid[i][q] holds one flag per
//     logical leaf of values[i][q].
//
// In compact mode values[i] holds only the valid inner arrays and each inner array only its valid
// leaves. In sparse mode every inner array is stored (a null one is nil, with nil leaf validity)
// and leaves are dense with zero values at null positions.
type columnNestedArrayBase[T any] struct {
	*genericColumnBase[[][]T]
	leafType entity.FieldType

	elementNullable bool
	elementValid    [][]bool
	leafNullable    bool
	leafValid       [][][]bool
	elementSparse   bool
}

func newNestedArrayBase[T any](fieldName string, data [][][]T, leafType entity.FieldType) *columnNestedArrayBase[T] {
	return &columnNestedArrayBase[T]{
		genericColumnBase: &genericColumnBase[[][]T]{
			name:      fieldName,
			fieldType: entity.FieldTypeArray,
			values:    data,
		},
		leafType: leafType,
	}
}

// ElementType returns Array: the elements of the column are inner arrays.
func (c *columnNestedArrayBase[T]) ElementType() entity.FieldType {
	return entity.FieldTypeArray
}

func (c *columnNestedArrayBase[T]) LeafType() entity.FieldType {
	return c.leafType
}

// innerValid reports whether stored inner array q of payload row p is valid.
func (c *columnNestedArrayBase[T]) innerValid(p, q int) bool {
	return !c.elementNullable || !c.elementSparse || c.elementValid[p][q]
}

func (c *columnNestedArrayBase[T]) FieldData() *schemapb.FieldData {
	rows := make([]*schemapb.ScalarField, 0, len(c.values))
	for p, row := range c.values {
		inner := make([]*schemapb.ScalarField, 0, len(row))
		for q, leaves := range row {
			if !c.innerValid(p, q) {
				continue
			}
			var leafValid []bool
			if c.leafNullable {
				leafValid = c.leafValid[p][q]
				if c.elementSparse {
					leaves = compactElements(leaves, leafValid)
				}
			}
			sf := slice2Scalar(leaves, c.leafType)
			setArrayElementValidData(sf, leafValid)
			inner = append(inner, sf)
		}
		sf := &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{
					Data:        inner,
					ElementType: schemapb.DataType(c.leafType),
				},
			},
		}
		if c.elementNullable {
			setArrayElementValidData(sf, c.elementValid[p])
		}
		rows = append(rows, sf)
	}

	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: c.name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data:        rows,
						ElementType: schemapb.DataType_Array,
					},
				},
			},
		},
	}
	if c.nullable {
		setFieldDataValidData(fd, c.validData)
	}
	return fd
}

func (c *columnNestedArrayBase[T]) slice(start, end int) *columnNestedArrayBase[T] {
	_, _, valueStart, valueEnd := c.sliceRange(start, end)
	return &columnNestedArrayBase[T]{
		genericColumnBase: c.genericColumnBase.slice(start, end),
		leafType:          c.leafType,
		elementNullable:   c.elementNullable,
		elementValid:      selectElementRows(c.elementValid, valueStart, valueEnd),
		leafNullable:      c.leafNullable,
		leafValid:         selectElementRows(c.leafValid, valueStart, valueEnd),
		elementSparse:     c.elementSparse,
	}
}

// AppendValue appends one row given as `[][]T`, or `[][]*T` where a nil pointer marks a null leaf.
// A nil inner array is a null inner array when the column is element nullable, and an empty one
// otherwise.
func (c *columnNestedArrayBase[T]) AppendValue(a any) error {
	if a == nil {
		return c.AppendNull()
	}
	switch v := a.(type) {
	case [][]T:
		logical := make([][]*T, len(v))
		for j, inner := range v {
			if inner == nil {
				continue
			}
			logical[j] = make([]*T, len(inner))
			for k := range inner {
				logical[j][k] = &inner[k]
			}
		}
		return c.appendLogicalRow(logical)
	case [][]*T:
		return c.appendLogicalRow(v)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", a, c.fieldType)
	}
}

func (c *columnNestedArrayBase[T]) appendLogicalRow(logical [][]*T) error {
	values := make([][]T, 0, len(logical))
	elementValid := make([]bool, len(logical))
	leafValid := make([][]bool, 0, len(logical))
	for j, inner := range logical {
		if inner == nil && c.elementNullable {
			if c.elementSparse {
				values = append(values, nil)
				leafValid = append(leafValid, nil)
			}
			continue
		}
		elementValid[j] = true
		leaves := make([]T, 0, len(inner))
		valid := make([]bool, len(inner))
		for k, leaf := range inner {
			if leaf == nil {
				if !c.leafNullable {
					return errors.Newf("leaf %d of element %d of nested array %q is nil but the column is not leaf nullable", k, j, c.name)
				}
				if c.elementSparse {
					var zero T
					leaves = append(leaves, zero)
				}
				continue
			}
			valid[k] = true
			leaves = append(leaves, *leaf)
		}
		values = append(values, leaves)
		leafValid = append(leafValid, valid)
	}
	return c.appendRow(values, elementValid, leafValid)
}

func (c *columnNestedArrayBase[T]) appendRow(values [][]T, elementValid []bool, leafValid [][]bool) error {
	if err := c.genericColumnBase.AppendValue(values); err != nil {
		return err
	}
	if c.elementNullable {
		c.elementValid = append(c.elementValid, elementValid)
	}
	if c.leafNullable {
		c.leafValid = append(c.leafValid, leafValid)
	}
	return nil
}

func (c *columnNestedArrayBase[T]) appendNullElements(n int) error {
	if !c.elementNullable {
		return errors.Newf("append null elements to not element nullable nested array column %q", c.name)
	}
	values := [][]T{}
	var leafValid [][]bool
	if c.elementSparse {
		values = make([][]T, n)
		leafValid = make([][]bool, n)
	}
	return c.appendRow(values, make([]bool, n), leafValid)
}

// appendEmptyRow appends a valid row without inner arrays.
func (c *columnNestedArrayBase[T]) appendEmptyRow() error {
	return c.appendRow([][]T{}, nil, nil)
}

func (c *columnNestedArrayBase[T]) AppendNull() error {
	if err := c.genericColumnBase.AppendNull(); err != nil {
		return err
	}
	if c.sparseMode {
		if c.elementNullable {
			c.elementValid = append(c.elementValid, nil)
		}
		if c.leafNullable {
			c.leafValid = append(c.leafValid, nil)
		}
	}
	return nil
}

// payloadRow returns the index into values of a logical row, and false for a null row.
func (c *columnNestedArrayBase[T]) payloadRow(idx int) (int, bool, error) {
	if err := c.rangeCheck(idx); err != nil {
		return 0, false, err
	}
	if c.nullable && !c.validData[idx] {
		return 0, false, nil
	}
	return c.valueIndex(idx), true, nil
}

// storedInner maps logical inner array j of payload row p to its index in values[p], and returns
// false for a null inner array.
func (c *columnNestedArrayBase[T]) storedInner(p, j int) (int, bool, error) {
	n := len(c.values[p])
	if c.elementNullable {
		n = len(c.elementValid[p])
	}
	if err := elementRangeCheck(n, j); err != nil {
		return 0, false, err
	}
	if !c.elementNullable {
		return j, true, nil
	}
	if !c.elementValid[p][j] {
		return 0, false, nil
	}
	return storedElementIndex(c.elementValid[p], j, c.elementSparse), true, nil
}

// Get returns the inner arrays of one row: `[][]T`, or `[][]*T` when leaves are nullable, where a
// nil pointer marks a null leaf. A null inner array is nil and a null row returns nil.
func (c *columnNestedArrayBase[T]) Get(idx int) (any, error) {
	p, ok, err := c.payloadRow(idx)
	if err != nil || !ok {
		return nil, err
	}
	n, _ := c.ElementLen(idx)
	if c.leafNullable {
		result := make([][]*T, n)
		c.eachStoredInner(p, n, func(j, q int) {
			result[j] = elementPointers(c.values[p][q], c.leafValid[p][q], c.elementSparse)
		})
		return result, nil
	}
	result := make([][]T, n)
	c.eachStoredInner(p, n, func(j, q int) {
		result[j] = append([]T{}, c.values[p][q]...)
	})
	return result, nil
}

// eachStoredInner calls fn with each valid logical inner array j of payload row p and its index q in
// values[p]. It walks the row once, so a compact row is not rescanned per inner array.
func (c *columnNestedArrayBase[T]) eachStoredInner(p, n int, fn func(j, q int)) {
	if !c.elementNullable {
		for j := 0; j < n; j++ {
			fn(j, j)
		}
		return
	}
	next := 0
	for j, ok := range c.elementValid[p] {
		if !ok {
			continue
		}
		if c.elementSparse {
			fn(j, j)
		} else {
			fn(j, next)
		}
		next++
	}
}

func (c *columnNestedArrayBase[T]) ElementNullable() bool {
	return c.elementNullable
}

func (c *columnNestedArrayBase[T]) SetElementNullable(nullable bool) {
	if nullable == c.elementNullable {
		return
	}
	c.elementNullable = nullable
	if !nullable {
		c.elementValid = nil
		return
	}
	c.elementValid = make([][]bool, len(c.values))
	for i, row := range c.values {
		c.elementValid[i] = allValid(len(row))
	}
}

func (c *columnNestedArrayBase[T]) LeafNullable() bool {
	return c.leafNullable
}

func (c *columnNestedArrayBase[T]) SetLeafNullable(nullable bool) {
	if nullable == c.leafNullable {
		return
	}
	c.leafNullable = nullable
	if !nullable {
		c.leafValid = nil
		return
	}
	c.leafValid = make([][][]bool, len(c.values))
	for p, row := range c.values {
		c.leafValid[p] = make([][]bool, len(row))
		for q, leaves := range row {
			if c.innerValid(p, q) {
				c.leafValid[p][q] = allValid(len(leaves))
			}
		}
	}
}

func (c *columnNestedArrayBase[T]) ElementLen(row int) (int, error) {
	p, ok, err := c.payloadRow(row)
	if err != nil || !ok {
		return 0, err
	}
	if c.elementNullable {
		return len(c.elementValid[p]), nil
	}
	return len(c.values[p]), nil
}

func (c *columnNestedArrayBase[T]) ElementValidData(row int) ([]bool, error) {
	p, ok, err := c.payloadRow(row)
	if err != nil || !ok {
		return nil, err
	}
	if c.elementNullable {
		return append([]bool(nil), c.elementValid[p]...), nil
	}
	return allValid(len(c.values[p])), nil
}

func (c *columnNestedArrayBase[T]) ElementIsNull(row, j int) (bool, error) {
	valid, err := c.ElementValidData(row)
	if err != nil {
		return false, err
	}
	if err := elementRangeCheck(len(valid), j); err != nil {
		return false, err
	}
	return !valid[j], nil
}

func (c *columnNestedArrayBase[T]) LeafValidData(row, j int) ([]bool, error) {
	p, ok, err := c.payloadRow(row)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Newf("row %d of nested array %q is null", row, c.name)
	}
	q, valid, err := c.storedInner(p, j)
	if err != nil || !valid {
		return nil, err
	}
	if c.leafNullable {
		return append([]bool(nil), c.leafValid[p][q]...), nil
	}
	return allValid(len(c.values[p][q])), nil
}

func (c *columnNestedArrayBase[T]) LeafIsNull(row, j, k int) (bool, error) {
	valid, err := c.LeafValidData(row, j)
	if err != nil {
		return false, err
	}
	if err := elementRangeCheck(len(valid), k); err != nil {
		return false, err
	}
	return !valid[k], nil
}

func (c *columnNestedArrayBase[T]) ValidateNullable() error {
	if err := c.genericColumnBase.ValidateNullable(); err != nil {
		return err
	}
	if c.elementNullable {
		if err := validateElementRows(c.values, c.elementValid, c.elementSparse); err != nil {
			return errors.Wrapf(err, "nested array column %q", c.name)
		}
	}
	if !c.leafNullable {
		return nil
	}
	if len(c.leafValid) != len(c.values) {
		return errors.Newf("nested array column %q leaf valid data rows (%d) does not match value rows (%d)",
			c.name, len(c.leafValid), len(c.values))
	}
	for p, row := range c.values {
		if len(c.leafValid[p]) != len(row) {
			return errors.Newf("nested array column %q row %d leaf valid data has %d inner arrays, values have %d",
				c.name, p, len(c.leafValid[p]), len(row))
		}
		for q, leaves := range row {
			if !c.innerValid(p, q) {
				continue
			}
			expected := countValid(c.leafValid[p][q])
			if c.elementSparse {
				expected = len(c.leafValid[p][q])
			}
			if len(leaves) != expected {
				return errors.Newf("nested array column %q row %d inner array %d has %d leaves, but its leaf valid data expects %d",
					c.name, p, q, len(leaves), expected)
			}
		}
	}
	return nil
}

// CompactNullableValues compacts the rows, inner arrays and leaves of a sparse column.
func (c *columnNestedArrayBase[T]) CompactNullableValues() {
	if c.nullable && c.sparseMode {
		c.elementValid = filterValidRows(c.elementValid, c.validData)
		c.leafValid = filterValidRows(c.leafValid, c.validData)
	}
	c.genericColumnBase.CompactNullableValues()
	if !c.elementSparse {
		return
	}
	for p, row := range c.values {
		values := make([][]T, 0, len(row))
		var leafValid [][]bool
		if c.leafNullable {
			leafValid = make([][]bool, 0, len(row))
		}
		for q, leaves := range row {
			if !c.innerValid(p, q) {
				continue
			}
			if c.leafNullable {
				leaves = compactElements(leaves, c.leafValid[p][q])
				leafValid = append(leafValid, c.leafValid[p][q])
			}
			values = append(values, leaves)
		}
		c.values[p] = values
		if c.leafNullable {
			c.leafValid[p] = leafValid
		}
	}
	c.elementSparse = false
}

// initElementNullable sets up the validity layers of a column built by a constructor.
func (c *columnNestedArrayBase[T]) initElementNullable(elementValidData [][]bool, leafValidData [][][]bool, opts []ElementNullableOption) error {
	o := newElementNullableOptions(opts)
	applyRowOptions(c.genericColumnBase, o)
	c.elementNullable = elementValidData != nil
	c.elementValid = elementValidData
	c.leafNullable = leafValidData != nil
	c.leafValid = leafValidData
	c.elementSparse = o.sparse
	return c.ValidateNullable()
}

/* nested arrays, one type per leaf */

// The NewColumnXxxArrayArray constructors create the column of an Array<T> struct sub-field whose
// inner arrays and leaves are not nullable; data[i][j] is inner array j of row i.
//
// The NewElementNullableColumnXxxArrayArray constructors add validity layers. elementValidData[i]
// holds one flag per logical inner array of row i and makes inner arrays nullable when non-nil;
// leafValidData[i][q] holds one flag per logical leaf of stored inner array q and makes leaves
// nullable when non-nil (pass an empty, non-nil slice to declare a layer nullable without rows).
// In compact mode data[i] holds only the valid inner arrays and each only its valid leaves. Use
// WithRowValidData for null rows and WithElementSparseMode for values in sparse form.

type ColumnBoolArrayArray struct {
	*columnNestedArrayBase[bool]
}

func NewColumnBoolArrayArray(fieldName string, data [][][]bool) *ColumnBoolArrayArray {
	return &ColumnBoolArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeBool)}
}

func NewElementNullableColumnBoolArrayArray(fieldName string, data [][][]bool, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnBoolArrayArray, error) {
	col := NewColumnBoolArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnBoolArrayArray) Slice(start, end int) Column {
	return &ColumnBoolArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

type ColumnInt8ArrayArray struct {
	*columnNestedArrayBase[int8]
}

func NewColumnInt8ArrayArray(fieldName string, data [][][]int8) *ColumnInt8ArrayArray {
	return &ColumnInt8ArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeInt8)}
}

func NewElementNullableColumnInt8ArrayArray(fieldName string, data [][][]int8, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnInt8ArrayArray, error) {
	col := NewColumnInt8ArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnInt8ArrayArray) Slice(start, end int) Column {
	return &ColumnInt8ArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

type ColumnInt16ArrayArray struct {
	*columnNestedArrayBase[int16]
}

func NewColumnInt16ArrayArray(fieldName string, data [][][]int16) *ColumnInt16ArrayArray {
	return &ColumnInt16ArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeInt16)}
}

func NewElementNullableColumnInt16ArrayArray(fieldName string, data [][][]int16, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnInt16ArrayArray, error) {
	col := NewColumnInt16ArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnInt16ArrayArray) Slice(start, end int) Column {
	return &ColumnInt16ArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

type ColumnInt32ArrayArray struct {
	*columnNestedArrayBase[int32]
}

func NewColumnInt32ArrayArray(fieldName string, data [][][]int32) *ColumnInt32ArrayArray {
	return &ColumnInt32ArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeInt32)}
}

func NewElementNullableColumnInt32ArrayArray(fieldName string, data [][][]int32, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnInt32ArrayArray, error) {
	col := NewColumnInt32ArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnInt32ArrayArray) Slice(start, end int) Column {
	return &ColumnInt32ArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

type ColumnInt64ArrayArray struct {
	*columnNestedArrayBase[int64]
}

func NewColumnInt64ArrayArray(fieldName string, data [][][]int64) *ColumnInt64ArrayArray {
	return &ColumnInt64ArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeInt64)}
}

func NewElementNullableColumnInt64ArrayArray(fieldName string, data [][][]int64, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnInt64ArrayArray, error) {
	col := NewColumnInt64ArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnInt64ArrayArray) Slice(start, end int) Column {
	return &ColumnInt64ArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

type ColumnFloatArrayArray struct {
	*columnNestedArrayBase[float32]
}

func NewColumnFloatArrayArray(fieldName string, data [][][]float32) *ColumnFloatArrayArray {
	return &ColumnFloatArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeFloat)}
}

func NewElementNullableColumnFloatArrayArray(fieldName string, data [][][]float32, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnFloatArrayArray, error) {
	col := NewColumnFloatArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnFloatArrayArray) Slice(start, end int) Column {
	return &ColumnFloatArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

type ColumnDoubleArrayArray struct {
	*columnNestedArrayBase[float64]
}

func NewColumnDoubleArrayArray(fieldName string, data [][][]float64) *ColumnDoubleArrayArray {
	return &ColumnDoubleArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeDouble)}
}

func NewElementNullableColumnDoubleArrayArray(fieldName string, data [][][]float64, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnDoubleArrayArray, error) {
	col := NewColumnDoubleArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnDoubleArrayArray) Slice(start, end int) Column {
	return &ColumnDoubleArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

type ColumnVarCharArrayArray struct {
	*columnNestedArrayBase[string]
}

func NewColumnVarCharArrayArray(fieldName string, data [][][]string) *ColumnVarCharArrayArray {
	return &ColumnVarCharArrayArray{columnNestedArrayBase: newNestedArrayBase(fieldName, data, entity.FieldTypeVarChar)}
}

func NewElementNullableColumnVarCharArrayArray(fieldName string, data [][][]string, elementValidData [][]bool, leafValidData [][][]bool, opts ...ElementNullableOption) (*ColumnVarCharArrayArray, error) {
	col := NewColumnVarCharArrayArray(fieldName, data)
	return col, col.initElementNullable(elementValidData, leafValidData, opts)
}

func (c *ColumnVarCharArrayArray) Slice(start, end int) Column {
	return &ColumnVarCharArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

// columnUntypedArrayArray holds a parsed Array<T> sub-field whose rows carry no inner array, so the
// result does not tell the leaf type. ApplyStructSchema replaces it with the typed column.
type columnUntypedArrayArray struct {
	*columnNestedArrayBase[any]
}

func (c *columnUntypedArrayArray) Slice(start, end int) Column {
	return &columnUntypedArrayArray{columnNestedArrayBase: c.columnNestedArrayBase.slice(start, end)}
}

// FieldData encodes the rows without a leaf payload; none of them holds an inner array.
func (c *columnUntypedArrayArray) FieldData() *schemapb.FieldData {
	rows := make([]*schemapb.ScalarField, 0, len(c.values))
	for p := range c.values {
		sf := &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{}},
		}
		if c.elementNullable {
			setArrayElementValidData(sf, c.elementValid[p])
		}
		rows = append(rows, sf)
	}
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: c.name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data:        rows,
						ElementType: schemapb.DataType_Array,
					},
				},
			},
		},
	}
	if c.nullable {
		setFieldDataValidData(fd, c.validData)
	}
	return fd
}

// newNestedArrayColumn creates an empty Array<T> column for the given leaf type.
func newNestedArrayColumn(fieldName string, leafType entity.FieldType) (Column, error) {
	switch leafType {
	case entity.FieldTypeBool:
		return NewColumnBoolArrayArray(fieldName, nil), nil
	case entity.FieldTypeInt8:
		return NewColumnInt8ArrayArray(fieldName, nil), nil
	case entity.FieldTypeInt16:
		return NewColumnInt16ArrayArray(fieldName, nil), nil
	case entity.FieldTypeInt32:
		return NewColumnInt32ArrayArray(fieldName, nil), nil
	case entity.FieldTypeInt64:
		return NewColumnInt64ArrayArray(fieldName, nil), nil
	case entity.FieldTypeFloat:
		return NewColumnFloatArrayArray(fieldName, nil), nil
	case entity.FieldTypeDouble:
		return NewColumnDoubleArrayArray(fieldName, nil), nil
	case entity.FieldTypeVarChar, entity.FieldTypeString:
		return NewColumnVarCharArrayArray(fieldName, nil), nil
	default:
		return nil, errors.Newf("unsupported nested array leaf type %v for field %q", leafType, fieldName)
	}
}
