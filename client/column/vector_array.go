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
	"github.com/milvus-io/milvus/client/v2/entity"
)

// columnVectorArrayBase implements `Column` for vector-array sub-fields of struct array.
// Each row contains a variable-length list of vectors of equal `dim`.
type columnVectorArrayBase[T entity.Vector] struct {
	name        string
	fieldType   entity.FieldType // e.g. FieldTypeArray (top-level type for column matching)
	elementType entity.FieldType // underlying vector type, e.g. FieldTypeFloatVector
	dim         int
	values      [][]T // values[i] = list of vectors for row i
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
	return len(c.values)
}

func (c *columnVectorArrayBase[T]) Get(idx int) (any, error) {
	if idx < 0 || idx >= len(c.values) {
		return nil, errors.Newf("index %d out of range[0, %d)", idx, len(c.values))
	}
	return c.values[idx], nil
}

func (c *columnVectorArrayBase[T]) AppendValue(value any) error {
	v, ok := value.([]T)
	if !ok {
		return errors.Newf("unexpected append value type %T, field type %v", value, c.fieldType)
	}
	c.values = append(c.values, v)
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

func (c *columnVectorArrayBase[T]) IsNull(_ int) (bool, error) {
	return false, nil
}

func (c *columnVectorArrayBase[T]) AppendNull() error {
	return errors.New("vector array column does not support AppendNull")
}

func (c *columnVectorArrayBase[T]) Nullable() bool { return false }

func (c *columnVectorArrayBase[T]) SetNullable(_ bool) {}

func (c *columnVectorArrayBase[T]) ValidateNullable() error { return nil }

func (c *columnVectorArrayBase[T]) CompactNullableValues() {}

func (c *columnVectorArrayBase[T]) ValidCount() int { return c.Len() }

func (c *columnVectorArrayBase[T]) Slice(start, end int) Column {
	if end == -1 || end > len(c.values) {
		end = len(c.values)
	}
	if start > end {
		start = end
	}
	return &columnVectorArrayBase[T]{
		name:        c.name,
		fieldType:   c.fieldType,
		elementType: c.elementType,
		dim:         c.dim,
		values:      c.values[start:end],
	}
}

func (c *columnVectorArrayBase[T]) FieldData() *schemapb.FieldData {
	rows := make([]*schemapb.VectorField, 0, len(c.values))
	for _, row := range c.values {
		rows = append(rows, values2Vectors(row, c.elementType, int64(c.dim)))
	}
	return &schemapb.FieldData{
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

// AppendValue accepts `[]entity.FloatVector` or `[][]float32` for one row.
func (c *ColumnFloatVectorArray) AppendValue(value any) error {
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
	return appendByteVectorArrayRow(&c.values, value)
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

/* bfloat16 vector array */

type ColumnBFloat16VectorArray struct {
	*columnVectorArrayBase[entity.BFloat16Vector]
}

func (c *ColumnBFloat16VectorArray) AppendValue(value any) error {
	return appendByteVectorArrayRow(&c.values, value)
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

/* binary vector array */

type ColumnBinaryVectorArray struct {
	*columnVectorArrayBase[entity.BinaryVector]
}

func (c *ColumnBinaryVectorArray) AppendValue(value any) error {
	return appendByteVectorArrayRow(&c.values, value)
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

/* int8 vector array */

type ColumnInt8VectorArray struct {
	*columnVectorArrayBase[entity.Int8Vector]
}

// AppendValue accepts `[]entity.Int8Vector` or `[][]int8` for one row.
func (c *ColumnInt8VectorArray) AppendValue(value any) error {
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
