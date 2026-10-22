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

// columnArrayBase implement `Column` interface
// it provided specified `FieldData` behavior for Array columns.
type columnArrayBase[T any] struct {
	*genericColumnBase[[]T]
	elementType entity.FieldType

	// Element validity of an element-nullable struct sub-field. elementValid is aligned with
	// values: elementValid[i] holds one flag per logical element of payload row i. In compact
	// mode values[i] holds only the valid elements; in sparse mode it holds every element and
	// null positions carry the zero value.
	elementNullable bool
	elementValid    [][]bool
	elementSparse   bool
}

func (c *columnArrayBase[T]) FieldData() *schemapb.FieldData {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: c.name,
	}

	data := make([]*schemapb.ScalarField, 0, c.Len())
	for i, arr := range c.values {
		if !c.elementNullable {
			data = append(data, slice2Scalar(arr, c.elementType))
			continue
		}
		valid := c.elementValid[i]
		if c.elementSparse {
			arr = compactElements(arr, valid)
		}
		row := slice2Scalar(arr, c.elementType)
		setArrayElementValidData(row, valid)
		data = append(data, row)
	}

	fd.Field = &schemapb.FieldData_Scalars{
		Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{
					Data:        data,
					ElementType: schemapb.DataType(c.elementType),
				},
			},
		},
	}
	if c.nullable {
		setFieldDataValidData(fd, c.validData)
	}
	return fd
}

func (c *columnArrayBase[T]) ElementType() entity.FieldType {
	return c.elementType
}

func (c *columnArrayBase[T]) slice(start, end int) *columnArrayBase[T] {
	_, _, valueStart, valueEnd := c.sliceRange(start, end)
	result := &columnArrayBase[T]{
		genericColumnBase: c.genericColumnBase.slice(start, end),
		elementType:       c.elementType,
		elementNullable:   c.elementNullable,
		elementSparse:     c.elementSparse,
	}
	if c.elementNullable {
		result.elementValid = selectElementRows(c.elementValid, valueStart, valueEnd)
	}
	return result
}

// AppendValue appends one row. An element-nullable column accepts `[]*T`, where nil marks a null
// element, as well as `[]T` with every element valid.
func (c *columnArrayBase[T]) AppendValue(a any) error {
	if !c.elementNullable {
		return c.genericColumnBase.AppendValue(a)
	}
	if a == nil {
		return c.AppendNull()
	}
	switch v := a.(type) {
	case []T:
		return c.appendRow(v, allValid(len(v)))
	case []*T:
		values := make([]T, 0, len(v))
		valid := make([]bool, len(v))
		for j, p := range v {
			if p != nil {
				valid[j] = true
				values = append(values, *p)
			} else if c.elementSparse {
				var zero T
				values = append(values, zero)
			}
		}
		return c.appendRow(values, valid)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", a, c.fieldType)
	}
}

func (c *columnArrayBase[T]) appendRow(values []T, valid []bool) error {
	if err := c.genericColumnBase.AppendValue(values); err != nil {
		return err
	}
	if c.elementNullable {
		c.elementValid = append(c.elementValid, valid)
	}
	return nil
}

func (c *columnArrayBase[T]) appendNullElements(n int) error {
	if !c.elementNullable {
		return errors.Newf("append null elements to not element nullable column %q", c.name)
	}
	var values []T
	if c.elementSparse {
		values = make([]T, n)
	} else {
		values = []T{}
	}
	return c.appendRow(values, make([]bool, n))
}

func (c *columnArrayBase[T]) AppendNull() error {
	if err := c.genericColumnBase.AppendNull(); err != nil {
		return err
	}
	if c.elementNullable && c.sparseMode {
		c.elementValid = append(c.elementValid, nil)
	}
	return nil
}

// Get returns the elements of one row. An element-nullable column returns `[]*T`, where nil
// marks a null element; a null row returns nil.
func (c *columnArrayBase[T]) Get(idx int) (any, error) {
	if !c.elementNullable {
		return c.genericColumnBase.Get(idx)
	}
	p, ok, err := c.payloadRow(idx)
	if err != nil || !ok {
		return nil, err
	}
	return elementPointers(c.values[p], c.elementValid[p], c.elementSparse), nil
}

// payloadRow returns the index into values of a logical row, and false for a null row.
func (c *columnArrayBase[T]) payloadRow(idx int) (int, bool, error) {
	if err := c.rangeCheck(idx); err != nil {
		return 0, false, err
	}
	if c.nullable && !c.validData[idx] {
		return 0, false, nil
	}
	return c.valueIndex(idx), true, nil
}

func (c *columnArrayBase[T]) ElementNullable() bool {
	return c.elementNullable
}

func (c *columnArrayBase[T]) SetElementNullable(nullable bool) {
	if nullable == c.elementNullable {
		return
	}
	c.elementNullable = nullable
	if !nullable {
		c.elementValid = nil
		c.elementSparse = false
		return
	}
	c.elementValid = make([][]bool, len(c.values))
	for i, row := range c.values {
		c.elementValid[i] = allValid(len(row))
	}
}

func (c *columnArrayBase[T]) ElementLen(row int) (int, error) {
	p, ok, err := c.payloadRow(row)
	if err != nil || !ok {
		return 0, err
	}
	if c.elementNullable {
		return len(c.elementValid[p]), nil
	}
	return len(c.values[p]), nil
}

func (c *columnArrayBase[T]) ElementValidData(row int) ([]bool, error) {
	p, ok, err := c.payloadRow(row)
	if err != nil || !ok {
		return nil, err
	}
	if c.elementNullable {
		return append([]bool(nil), c.elementValid[p]...), nil
	}
	return allValid(len(c.values[p])), nil
}

func (c *columnArrayBase[T]) ElementIsNull(row, j int) (bool, error) {
	valid, err := c.ElementValidData(row)
	if err != nil {
		return false, err
	}
	if err := elementRangeCheck(len(valid), j); err != nil {
		return false, err
	}
	return !valid[j], nil
}

func (c *columnArrayBase[T]) ValidateNullable() error {
	if err := c.genericColumnBase.ValidateNullable(); err != nil {
		return err
	}
	if !c.elementNullable {
		return nil
	}
	if err := validateElementRows(c.values, c.elementValid, c.elementSparse); err != nil {
		return errors.Wrapf(err, "array column %q", c.name)
	}
	return nil
}

// CompactNullableValues compacts both the rows and the elements of a sparse column.
func (c *columnArrayBase[T]) CompactNullableValues() {
	if c.elementNullable && c.nullable && c.sparseMode {
		c.elementValid = filterValidRows(c.elementValid, c.validData)
	}
	c.genericColumnBase.CompactNullableValues()
	if !c.elementNullable || !c.elementSparse {
		return
	}
	for i, row := range c.values {
		c.values[i] = compactElements(row, c.elementValid[i])
	}
	c.elementSparse = false
}

// initElementNullable sets up the element layer of a column built by a constructor.
func (c *columnArrayBase[T]) initElementNullable(elementValidData [][]bool, opts []ElementNullableOption) error {
	o := newElementNullableOptions(opts)
	applyRowOptions(c.genericColumnBase, o)
	c.elementNullable = true
	c.elementValid = elementValidData
	c.elementSparse = o.sparse
	return c.ValidateNullable()
}

// setElementValidData installs element validity parsed from a result; values are in sparse form.
func (c *columnArrayBase[T]) setElementValidData(elementValid [][]bool) {
	c.elementNullable = true
	c.elementValid = elementValid
	c.elementSparse = true
}

func newArrayBase[T any](fieldName string, data [][]T, elementType entity.FieldType) *columnArrayBase[T] {
	return &columnArrayBase[T]{
		genericColumnBase: &genericColumnBase[[]T]{
			name:      fieldName,
			fieldType: entity.FieldTypeArray,
			values:    data,
		},
		elementType: elementType,
	}
}

/* bool array */

type ColumnBoolArray struct {
	*columnArrayBase[bool]
}

func NewColumnBoolArray(fieldName string, data [][]bool) *ColumnBoolArray {
	return &ColumnBoolArray{
		columnArrayBase: newArrayBase[bool](fieldName, data, entity.FieldTypeBool),
	}
}

func (c *ColumnBoolArray) Slice(start, end int) Column {
	return &ColumnBoolArray{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* int8 array */

type ColumnInt8Array struct {
	*columnArrayBase[int8]
}

func NewColumnInt8Array(fieldName string, data [][]int8) *ColumnInt8Array {
	return &ColumnInt8Array{
		columnArrayBase: newArrayBase(fieldName, data, entity.FieldTypeInt8),
	}
}

func (c *ColumnInt8Array) Slice(start, end int) Column {
	return &ColumnInt8Array{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* int16 array */

type ColumnInt16Array struct {
	*columnArrayBase[int16]
}

func NewColumnInt16Array(fieldName string, data [][]int16) *ColumnInt16Array {
	return &ColumnInt16Array{
		columnArrayBase: newArrayBase(fieldName, data, entity.FieldTypeInt16),
	}
}

func (c *ColumnInt16Array) Slice(start, end int) Column {
	return &ColumnInt16Array{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* int32 array */

type ColumnInt32Array struct {
	*columnArrayBase[int32]
}

func NewColumnInt32Array(fieldName string, data [][]int32) *ColumnInt32Array {
	return &ColumnInt32Array{
		columnArrayBase: newArrayBase(fieldName, data, entity.FieldTypeInt32),
	}
}

func (c *ColumnInt32Array) Slice(start, end int) Column {
	return &ColumnInt32Array{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* int64 array */

type ColumnInt64Array struct {
	*columnArrayBase[int64]
}

func NewColumnInt64Array(fieldName string, data [][]int64) *ColumnInt64Array {
	return &ColumnInt64Array{
		columnArrayBase: newArrayBase(fieldName, data, entity.FieldTypeInt64),
	}
}

func (c *ColumnInt64Array) Slice(start, end int) Column {
	return &ColumnInt64Array{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* float32 array */

type ColumnFloatArray struct {
	*columnArrayBase[float32]
}

func NewColumnFloatArray(fieldName string, data [][]float32) *ColumnFloatArray {
	return &ColumnFloatArray{
		columnArrayBase: newArrayBase(fieldName, data, entity.FieldTypeFloat),
	}
}

func (c *ColumnFloatArray) Slice(start, end int) Column {
	return &ColumnFloatArray{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* float64 array */

type ColumnDoubleArray struct {
	*columnArrayBase[float64]
}

func NewColumnDoubleArray(fieldName string, data [][]float64) *ColumnDoubleArray {
	return &ColumnDoubleArray{
		columnArrayBase: newArrayBase(fieldName, data, entity.FieldTypeDouble),
	}
}

func (c *ColumnDoubleArray) Slice(start, end int) Column {
	return &ColumnDoubleArray{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* varchar array */

type ColumnVarCharArray struct {
	*columnArrayBase[string]
}

func NewColumnVarCharArray(fieldName string, data [][]string) *ColumnVarCharArray {
	return &ColumnVarCharArray{
		columnArrayBase: newArrayBase(fieldName, data, entity.FieldTypeVarChar),
	}
}

func (c *ColumnVarCharArray) Slice(start, end int) Column {
	return &ColumnVarCharArray{
		columnArrayBase: c.columnArrayBase.slice(start, end),
	}
}

/* element-nullable arrays */

// The NewElementNullableColumnXxxArray constructors create the column of an element-nullable
// struct sub-field. values[i] holds the valid elements of row i (compact mode) and
// elementValidData[i] one flag per logical element. Use WithRowValidData for null rows and
// WithElementSparseMode for values in sparse form.

func NewElementNullableColumnBoolArray(fieldName string, values [][]bool, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnBoolArray, error) {
	col := NewColumnBoolArray(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnInt8Array(fieldName string, values [][]int8, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnInt8Array, error) {
	col := NewColumnInt8Array(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnInt16Array(fieldName string, values [][]int16, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnInt16Array, error) {
	col := NewColumnInt16Array(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnInt32Array(fieldName string, values [][]int32, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnInt32Array, error) {
	col := NewColumnInt32Array(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnInt64Array(fieldName string, values [][]int64, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnInt64Array, error) {
	col := NewColumnInt64Array(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnFloatArray(fieldName string, values [][]float32, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnFloatArray, error) {
	col := NewColumnFloatArray(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnDoubleArray(fieldName string, values [][]float64, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnDoubleArray, error) {
	col := NewColumnDoubleArray(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnVarCharArray(fieldName string, values [][]string, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnVarCharArray, error) {
	col := NewColumnVarCharArray(fieldName, values)
	return col, col.initElementNullable(elementValidData, opts)
}
