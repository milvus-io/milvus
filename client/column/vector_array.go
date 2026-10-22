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

// columnVectorArrayBase implements `Column` for vector-array sub-fields of struct array.
// Each row contains a variable-length list of vectors of equal `dim`.
type columnVectorArrayBase[T entity.Vector] struct {
	name        string
	fieldType   entity.FieldType // e.g. FieldTypeArray (top-level type for column matching)
	elementType entity.FieldType // underlying vector type, e.g. FieldTypeFloatVector
	dim         int
	values      [][]T // values[i] = list of vectors for row i

	// Nullable columns use compact mode for inserts and sparse mode for query results.
	nullable     bool
	validData    []bool
	sparseMode   bool
	indexMapping []int

	// Element validity of an element-nullable struct sub-field, aligned with values:
	// elementValid[i] holds one flag per logical vector of payload row i. In compact mode
	// values[i] holds only the valid vectors; in sparse mode null vectors are nil.
	elementNullable bool
	elementValid    [][]bool
	elementSparse   bool
}

func (c *columnVectorArrayBase[T]) Name() string {
	return c.name
}

func (c *columnVectorArrayBase[T]) Type() entity.FieldType {
	return c.fieldType
}

func (c *columnVectorArrayBase[T]) ElementType() entity.FieldType {
	return c.elementType
}

func (c *columnVectorArrayBase[T]) Dim() int {
	return c.dim
}

func (c *columnVectorArrayBase[T]) Len() int {
	if c.validData != nil {
		return len(c.validData)
	}
	return len(c.values)
}

// Get returns the vectors of one row; for an element-nullable column a null vector is nil.
func (c *columnVectorArrayBase[T]) Get(idx int) (any, error) {
	p, ok, err := c.payloadRow(idx)
	if err != nil || !ok {
		return nil, err
	}
	if c.elementNullable && !c.elementSparse {
		return denseElements(c.values[p], c.elementValid[p]), nil
	}
	return c.values[p], nil
}

// payloadRow returns the index into values of a logical row, and false for a null row.
func (c *columnVectorArrayBase[T]) payloadRow(idx int) (int, bool, error) {
	if err := c.rangeCheck(idx); err != nil {
		return 0, false, err
	}
	if c.nullable && !c.validData[idx] {
		return 0, false, nil
	}
	return c.valueIndex(idx), true, nil
}

func (c *columnVectorArrayBase[T]) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	v, ok := value.([]T)
	if !ok {
		return errors.Newf("unexpected append value type %T, field type %v", value, c.fieldType)
	}
	return c.appendRow(v)
}

// appendRow appends the vectors of one row; a nil vector is a null element and requires an
// element-nullable column.
func (c *columnVectorArrayBase[T]) appendRow(row []T) error {
	if !c.elementNullable {
		for j, v := range row {
			if isNilVector(v) {
				return errors.Newf("vector %d of vector array %q is nil but the column is not element nullable", j, c.name)
			}
		}
		c.values = append(c.values, row)
		c.markValueAppended()
		return nil
	}
	valid := make([]bool, len(row))
	stored := row
	if !c.elementSparse {
		stored = make([]T, 0, len(row))
	}
	for j, v := range row {
		if isNilVector(v) {
			continue
		}
		valid[j] = true
		if !c.elementSparse {
			stored = append(stored, v)
		}
	}
	c.values = append(c.values, stored)
	c.elementValid = append(c.elementValid, valid)
	c.markValueAppended()
	return nil
}

func (c *columnVectorArrayBase[T]) appendNullElements(n int) error {
	if !c.elementNullable {
		return errors.Newf("append null elements to not element nullable vector array column %q", c.name)
	}
	return c.appendRow(make([]T, n))
}

func isNilVector(v any) bool {
	switch x := v.(type) {
	case entity.FloatVector:
		return x == nil
	case entity.Float16Vector:
		return x == nil
	case entity.BFloat16Vector:
		return x == nil
	case entity.BinaryVector:
		return x == nil
	case entity.Int8Vector:
		return x == nil
	default:
		return v == nil
	}
}

func (c *columnVectorArrayBase[T]) ElementNullable() bool {
	return c.elementNullable
}

func (c *columnVectorArrayBase[T]) SetElementNullable(nullable bool) {
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

func (c *columnVectorArrayBase[T]) ElementLen(row int) (int, error) {
	p, ok, err := c.payloadRow(row)
	if err != nil || !ok {
		return 0, err
	}
	if c.elementNullable {
		return len(c.elementValid[p]), nil
	}
	return len(c.values[p]), nil
}

func (c *columnVectorArrayBase[T]) ElementValidData(row int) ([]bool, error) {
	p, ok, err := c.payloadRow(row)
	if err != nil || !ok {
		return nil, err
	}
	if c.elementNullable {
		return append([]bool(nil), c.elementValid[p]...), nil
	}
	return allValid(len(c.values[p])), nil
}

func (c *columnVectorArrayBase[T]) ElementIsNull(row, j int) (bool, error) {
	valid, err := c.ElementValidData(row)
	if err != nil {
		return false, err
	}
	if err := elementRangeCheck(len(valid), j); err != nil {
		return false, err
	}
	return !valid[j], nil
}

func (c *columnVectorArrayBase[T]) GetAsInt64(_ int) (int64, error) {
	return 0, errors.New("vector array column does not support GetAsInt64")
}

func (c *columnVectorArrayBase[T]) GetAsString(_ int) (string, error) {
	return "", errors.New("vector array column does not support GetAsString")
}

func (c *columnVectorArrayBase[T]) GetAsDouble(_ int) (float64, error) {
	return 0, errors.New("vector array column does not support GetAsDouble")
}

func (c *columnVectorArrayBase[T]) GetAsBool(_ int) (bool, error) {
	return false, errors.New("vector array column does not support GetAsBool")
}

func (c *columnVectorArrayBase[T]) IsNull(idx int) (bool, error) {
	if err := c.rangeCheck(idx); err != nil {
		return false, err
	}
	return c.nullable && !c.validData[idx], nil
}

func (c *columnVectorArrayBase[T]) AppendNull() error {
	if !c.nullable {
		return errors.New("append null to not nullable vector array column")
	}
	c.validData = append(c.validData, false)
	if c.sparseMode {
		c.values = append(c.values, nil)
		if c.elementNullable {
			c.elementValid = append(c.elementValid, nil)
		}
	} else {
		c.indexMapping = append(c.indexMapping, -1)
	}
	return nil
}

func (c *columnVectorArrayBase[T]) Nullable() bool { return c.nullable }

func (c *columnVectorArrayBase[T]) SetNullable(nullable bool) {
	c.nullable = nullable
	if nullable && c.validData == nil {
		c.validData = make([]bool, len(c.values))
		c.indexMapping = make([]int, len(c.values))
		for idx := range c.values {
			c.validData[idx] = true
			c.indexMapping[idx] = idx
		}
	}
	if !nullable {
		c.validData = nil
		c.indexMapping = nil
	}
}

func (c *columnVectorArrayBase[T]) ValidateNullable() error {
	if err := c.validateRowNullable(); err != nil {
		return err
	}
	if !c.elementNullable {
		return nil
	}
	if err := validateElementRows(c.values, c.elementValid, c.elementSparse); err != nil {
		return errors.Wrapf(err, "vector array column %q", c.name)
	}
	return nil
}

func (c *columnVectorArrayBase[T]) validateRowNullable() error {
	if !c.nullable {
		return nil
	}
	if c.sparseMode {
		if len(c.values) != len(c.validData) {
			return errors.Newf("values number (%d) does not match valid data len(%d)", len(c.values), len(c.validData))
		}
		return nil
	}

	c.rebuildIndexMapping()
	if len(c.values) != c.ValidCount() {
		return errors.Newf("values number(%d) does not match valid count(%d)", len(c.values), c.ValidCount())
	}
	return nil
}

// CompactNullableValues compacts both the rows and the vectors of a sparse column.
func (c *columnVectorArrayBase[T]) CompactNullableValues() {
	if c.nullable && c.sparseMode {
		if c.elementNullable {
			c.elementValid = filterValidRows(c.elementValid, c.validData)
		}
		values := c.values[:0]
		for idx, valid := range c.validData {
			if valid {
				values = append(values, c.values[idx])
			}
		}
		c.values = values
		c.sparseMode = false
		c.rebuildIndexMapping()
	}
	if !c.elementNullable || !c.elementSparse {
		return
	}
	for i, row := range c.values {
		c.values[i] = compactElements(row, c.elementValid[i])
	}
	c.elementSparse = false
}

func (c *columnVectorArrayBase[T]) ValidCount() int {
	if !c.nullable {
		return len(c.values)
	}
	return countValid(c.validData)
}

func (c *columnVectorArrayBase[T]) Slice(start, end int) Column {
	return c.slice(start, end)
}

func (c *columnVectorArrayBase[T]) slice(start, end int) *columnVectorArrayBase[T] {
	start, end = normalizeSliceRange(start, end, c.Len())

	valueStart, valueEnd := start, end
	if c.nullable && !c.sparseMode {
		valueStart, valueEnd = compactValueRange(c.indexMapping, start, end)
	}
	result := &columnVectorArrayBase[T]{
		name:        c.name,
		fieldType:   c.fieldType,
		elementType: c.elementType,
		dim:         c.dim,
		values:      append([][]T(nil), c.values[valueStart:valueEnd]...),
		nullable:    c.nullable,
		sparseMode:  c.sparseMode,

		elementNullable: c.elementNullable,
		elementSparse:   c.elementSparse,
	}
	if c.elementNullable {
		result.elementValid = selectElementRows(c.elementValid, valueStart, valueEnd)
	}
	if c.nullable {
		result.validData = append([]bool(nil), c.validData[start:end]...)
		result.rebuildIndexMapping()
	}
	return result
}

func (c *columnVectorArrayBase[T]) FieldData() *schemapb.FieldData {
	rows := make([]*schemapb.VectorField, 0, len(c.values))
	for i, row := range c.values {
		if !c.elementNullable {
			rows = append(rows, values2Vectors(row, c.elementType, int64(c.dim)))
			continue
		}
		valid := c.elementValid[i]
		if c.elementSparse {
			row = compactElements(row, valid)
		}
		vf := values2Vectors(row, c.elementType, int64(c.dim))
		setVectorArrayElementValidData(vf, valid)
		rows = append(rows, vf)
	}
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfVector,
		FieldName: c.name,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(c.dim),
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Dim:         int64(c.dim),
						Data:        rows,
						ElementType: schemapb.DataType(c.elementType),
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

func (c *columnVectorArrayBase[T]) rangeCheck(idx int) error {
	if idx < 0 || idx >= c.Len() {
		return errors.Newf("index %d out of range[0, %d)", idx, c.Len())
	}
	return nil
}

func (c *columnVectorArrayBase[T]) valueIndex(idx int) int {
	if !c.nullable || c.sparseMode {
		return idx
	}
	return c.indexMapping[idx]
}

func (c *columnVectorArrayBase[T]) markValueAppended() {
	if c.nullable {
		c.validData = append(c.validData, true)
		if !c.sparseMode {
			c.indexMapping = append(c.indexMapping, len(c.values)-1)
		}
	}
}

func (c *columnVectorArrayBase[T]) rebuildIndexMapping() {
	if !c.nullable || c.sparseMode {
		c.indexMapping = nil
		return
	}
	c.indexMapping = make([]int, len(c.validData))
	valueIdx := 0
	for idx, valid := range c.validData {
		if !valid {
			c.indexMapping[idx] = -1
			continue
		}
		c.indexMapping[idx] = valueIdx
		valueIdx++
	}
}

// setElementValidData installs element validity parsed from a result; values are in sparse form.
func (c *columnVectorArrayBase[T]) setElementValidData(elementValid [][]bool) {
	c.elementNullable = true
	c.elementValid = elementValid
	c.elementSparse = true
}

// initElementNullable sets up the element layer of a column built by a constructor.
func (c *columnVectorArrayBase[T]) initElementNullable(elementValidData [][]bool, opts []ElementNullableOption) error {
	o := newElementNullableOptions(opts)
	c.sparseMode = o.sparse
	if o.rowValidData != nil {
		c.nullable = true
		c.validData = o.rowValidData
	}
	c.elementNullable = true
	c.elementValid = elementValidData
	c.elementSparse = o.sparse
	return c.ValidateNullable()
}

func (c *columnVectorArrayBase[T]) setNullableData(validData []bool, sparseMode bool) error {
	c.nullable = true
	c.validData = validData
	c.sparseMode = sparseMode
	return c.ValidateNullable()
}

func countValid(validData []bool) int {
	count := 0
	for _, valid := range validData {
		if valid {
			count++
		}
	}
	return count
}

/* float vector array */

type ColumnFloatVectorArray struct {
	*columnVectorArrayBase[entity.FloatVector]
}

func NewColumnFloatVectorArray(fieldName string, dim int, data [][][]float32) *ColumnFloatVectorArray {
	values := make([][]entity.FloatVector, 0, len(data))
	for _, row := range data {
		vrow := make([]entity.FloatVector, 0, len(row))
		for _, v := range row {
			vrow = append(vrow, entity.FloatVector(v))
		}
		values = append(values, vrow)
	}
	return &ColumnFloatVectorArray{
		columnVectorArrayBase: &columnVectorArrayBase[entity.FloatVector]{
			name:        fieldName,
			fieldType:   entity.FieldTypeArray,
			elementType: entity.FieldTypeFloatVector,
			dim:         dim,
			values:      values,
		},
	}
}

func (c *ColumnFloatVectorArray) Slice(start, end int) Column {
	return &ColumnFloatVectorArray{
		columnVectorArrayBase: c.columnVectorArrayBase.slice(start, end),
	}
}

// AppendValue accepts `[]entity.FloatVector` or `[][]float32` for one row.
func (c *ColumnFloatVectorArray) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	switch v := value.(type) {
	case []entity.FloatVector:
		return c.appendRow(v)
	case [][]float32:
		row := make([]entity.FloatVector, 0, len(v))
		for _, x := range v {
			row = append(row, entity.FloatVector(x))
		}
		return c.appendRow(row)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", value, c.elementType)
	}
}

func byteVectorArrayRow[T ~[]byte](value any) ([]T, error) {
	switch v := value.(type) {
	case []T:
		return v, nil
	case [][]byte:
		row := make([]T, 0, len(v))
		for _, x := range v {
			// keep nil entries nil so they read as null vectors
			row = append(row, T(x))
		}
		return row, nil
	default:
		return nil, errors.Newf("unexpected append value type %T", value)
	}
}

/* float16 vector array */

type ColumnFloat16VectorArray struct {
	*columnVectorArrayBase[entity.Float16Vector]
}

func (c *ColumnFloat16VectorArray) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	row, err := byteVectorArrayRow[entity.Float16Vector](value)
	if err != nil {
		return err
	}
	return c.appendRow(row)
}

func NewColumnFloat16VectorArray(fieldName string, dim int, data [][][]byte) *ColumnFloat16VectorArray {
	values := make([][]entity.Float16Vector, 0, len(data))
	for _, row := range data {
		vrow := make([]entity.Float16Vector, 0, len(row))
		for _, v := range row {
			vrow = append(vrow, entity.Float16Vector(v))
		}
		values = append(values, vrow)
	}
	return &ColumnFloat16VectorArray{
		columnVectorArrayBase: &columnVectorArrayBase[entity.Float16Vector]{
			name:        fieldName,
			fieldType:   entity.FieldTypeArray,
			elementType: entity.FieldTypeFloat16Vector,
			dim:         dim,
			values:      values,
		},
	}
}

func (c *ColumnFloat16VectorArray) Slice(start, end int) Column {
	return &ColumnFloat16VectorArray{
		columnVectorArrayBase: c.columnVectorArrayBase.slice(start, end),
	}
}

/* bfloat16 vector array */

type ColumnBFloat16VectorArray struct {
	*columnVectorArrayBase[entity.BFloat16Vector]
}

func (c *ColumnBFloat16VectorArray) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	row, err := byteVectorArrayRow[entity.BFloat16Vector](value)
	if err != nil {
		return err
	}
	return c.appendRow(row)
}

func NewColumnBFloat16VectorArray(fieldName string, dim int, data [][][]byte) *ColumnBFloat16VectorArray {
	values := make([][]entity.BFloat16Vector, 0, len(data))
	for _, row := range data {
		vrow := make([]entity.BFloat16Vector, 0, len(row))
		for _, v := range row {
			vrow = append(vrow, entity.BFloat16Vector(v))
		}
		values = append(values, vrow)
	}
	return &ColumnBFloat16VectorArray{
		columnVectorArrayBase: &columnVectorArrayBase[entity.BFloat16Vector]{
			name:        fieldName,
			fieldType:   entity.FieldTypeArray,
			elementType: entity.FieldTypeBFloat16Vector,
			dim:         dim,
			values:      values,
		},
	}
}

func (c *ColumnBFloat16VectorArray) Slice(start, end int) Column {
	return &ColumnBFloat16VectorArray{
		columnVectorArrayBase: c.columnVectorArrayBase.slice(start, end),
	}
}

/* binary vector array */

type ColumnBinaryVectorArray struct {
	*columnVectorArrayBase[entity.BinaryVector]
}

func (c *ColumnBinaryVectorArray) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	row, err := byteVectorArrayRow[entity.BinaryVector](value)
	if err != nil {
		return err
	}
	return c.appendRow(row)
}

func NewColumnBinaryVectorArray(fieldName string, dim int, data [][][]byte) *ColumnBinaryVectorArray {
	values := make([][]entity.BinaryVector, 0, len(data))
	for _, row := range data {
		vrow := make([]entity.BinaryVector, 0, len(row))
		for _, v := range row {
			vrow = append(vrow, entity.BinaryVector(v))
		}
		values = append(values, vrow)
	}
	return &ColumnBinaryVectorArray{
		columnVectorArrayBase: &columnVectorArrayBase[entity.BinaryVector]{
			name:        fieldName,
			fieldType:   entity.FieldTypeArray,
			elementType: entity.FieldTypeBinaryVector,
			dim:         dim,
			values:      values,
		},
	}
}

func (c *ColumnBinaryVectorArray) Slice(start, end int) Column {
	return &ColumnBinaryVectorArray{
		columnVectorArrayBase: c.columnVectorArrayBase.slice(start, end),
	}
}

/* int8 vector array */

type ColumnInt8VectorArray struct {
	*columnVectorArrayBase[entity.Int8Vector]
}

// AppendValue accepts `[]entity.Int8Vector` or `[][]int8` for one row.
func (c *ColumnInt8VectorArray) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	switch v := value.(type) {
	case []entity.Int8Vector:
		return c.appendRow(v)
	case [][]int8:
		row := make([]entity.Int8Vector, 0, len(v))
		for _, x := range v {
			row = append(row, entity.Int8Vector(x))
		}
		return c.appendRow(row)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", value, c.elementType)
	}
}

func NewColumnInt8VectorArray(fieldName string, dim int, data [][][]int8) *ColumnInt8VectorArray {
	values := make([][]entity.Int8Vector, 0, len(data))
	for _, row := range data {
		vrow := make([]entity.Int8Vector, 0, len(row))
		for _, v := range row {
			vrow = append(vrow, entity.Int8Vector(v))
		}
		values = append(values, vrow)
	}
	return &ColumnInt8VectorArray{
		columnVectorArrayBase: &columnVectorArrayBase[entity.Int8Vector]{
			name:        fieldName,
			fieldType:   entity.FieldTypeArray,
			elementType: entity.FieldTypeInt8Vector,
			dim:         dim,
			values:      values,
		},
	}
}

func (c *ColumnInt8VectorArray) Slice(start, end int) Column {
	return &ColumnInt8VectorArray{
		columnVectorArrayBase: c.columnVectorArrayBase.slice(start, end),
	}
}

/* element-nullable vector arrays */

// The NewElementNullableColumnXxxVectorArray constructors create the column of an element-nullable
// vector struct sub-field. values[i] holds the valid vectors of row i (compact mode) and
// elementValidData[i] one flag per logical vector. Use WithRowValidData for null rows and
// WithElementSparseMode for values in sparse form, where null vectors are nil.

func NewElementNullableColumnFloatVectorArray(fieldName string, dim int, values [][][]float32, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnFloatVectorArray, error) {
	col := NewColumnFloatVectorArray(fieldName, dim, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnFloat16VectorArray(fieldName string, dim int, values [][][]byte, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnFloat16VectorArray, error) {
	col := NewColumnFloat16VectorArray(fieldName, dim, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnBFloat16VectorArray(fieldName string, dim int, values [][][]byte, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnBFloat16VectorArray, error) {
	col := NewColumnBFloat16VectorArray(fieldName, dim, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnBinaryVectorArray(fieldName string, dim int, values [][][]byte, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnBinaryVectorArray, error) {
	col := NewColumnBinaryVectorArray(fieldName, dim, values)
	return col, col.initElementNullable(elementValidData, opts)
}

func NewElementNullableColumnInt8VectorArray(fieldName string, dim int, values [][][]int8, elementValidData [][]bool, opts ...ElementNullableOption) (*ColumnInt8VectorArray, error) {
	col := NewColumnInt8VectorArray(fieldName, dim, values)
	return col, col.initElementNullable(elementValidData, opts)
}
