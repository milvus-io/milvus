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

func (c *columnVectorArrayBase[T]) Get(idx int) (any, error) {
	if err := c.rangeCheck(idx); err != nil {
		return nil, err
	}
	if c.nullable && !c.validData[idx] {
		return nil, nil
	}
	return c.values[c.valueIndex(idx)], nil
}

func (c *columnVectorArrayBase[T]) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	v, ok := value.([]T)
	if !ok {
		return errors.Newf("unexpected append value type %T, field type %v", value, c.fieldType)
	}
	c.values = append(c.values, v)
	c.markValueAppended()
	return nil
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

func (c *columnVectorArrayBase[T]) CompactNullableValues() {
	if !c.nullable || !c.sparseMode {
		return
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
	l := c.Len()
	if start < 0 {
		start = 0
	}
	if start > l {
		start = l
	}
	if end == -1 || end > l {
		end = l
	}
	if start > end {
		start = end
	}

	valueStart, valueEnd := start, end
	if c.nullable && !c.sparseMode {
		valueStart = countValid(c.validData[:start])
		valueEnd = valueStart + countValid(c.validData[start:end])
	}
	result := &columnVectorArrayBase[T]{
		name:        c.name,
		fieldType:   c.fieldType,
		elementType: c.elementType,
		dim:         c.dim,
		values:      c.values[valueStart:valueEnd],
		nullable:    c.nullable,
		sparseMode:  c.sparseMode,
	}
	if c.nullable {
		result.validData = c.validData[start:end]
		result.rebuildIndexMapping()
	}
	return result
}

func (c *columnVectorArrayBase[T]) FieldData() *schemapb.FieldData {
	rows := make([]*schemapb.VectorField, 0, len(c.values))
	for _, row := range c.values {
		rows = append(rows, values2Vectors(row, c.elementType, int64(c.dim)))
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
		fd.ValidData = c.validData
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
		c.values = append(c.values, v)
	case [][]float32:
		row := make([]entity.FloatVector, 0, len(v))
		for _, x := range v {
			row = append(row, entity.FloatVector(x))
		}
		c.values = append(c.values, row)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", value, c.elementType)
	}
	c.markValueAppended()
	return nil
}

func appendByteVectorArrayRow[T ~[]byte](values *[][]T, value any) error {
	switch v := value.(type) {
	case []T:
		*values = append(*values, v)
	case [][]byte:
		row := make([]T, 0, len(v))
		for _, x := range v {
			row = append(row, T(x))
		}
		*values = append(*values, row)
	default:
		return errors.Newf("unexpected append value type %T", value)
	}
	return nil
}

/* float16 vector array */

type ColumnFloat16VectorArray struct {
	*columnVectorArrayBase[entity.Float16Vector]
}

func (c *ColumnFloat16VectorArray) AppendValue(value any) error {
	if value == nil {
		return c.AppendNull()
	}
	if err := appendByteVectorArrayRow(&c.values, value); err != nil {
		return err
	}
	c.markValueAppended()
	return nil
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
	if err := appendByteVectorArrayRow(&c.values, value); err != nil {
		return err
	}
	c.markValueAppended()
	return nil
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
	if err := appendByteVectorArrayRow(&c.values, value); err != nil {
		return err
	}
	c.markValueAppended()
	return nil
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
		c.values = append(c.values, v)
	case [][]int8:
		row := make([]entity.Int8Vector, 0, len(v))
		for _, x := range v {
			row = append(row, entity.Int8Vector(x))
		}
		c.values = append(c.values, row)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", value, c.elementType)
	}
	c.markValueAppended()
	return nil
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
