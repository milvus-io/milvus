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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type vectorBase[T entity.Vector] struct {
	*genericColumnBase[T]
	dim int
}

func (b *vectorBase[T]) Dim() int {
	return b.dim
}

func (b *vectorBase[T]) FieldData() *schemapb.FieldData {
	fd := b.genericColumnBase.FieldData()
	vectors := fd.GetVectors()
	vectors.Dim = int64(b.dim)
	return fd
}

func (b *vectorBase[T]) slice(start, end int) *vectorBase[T] {
	return &vectorBase[T]{
		genericColumnBase: b.genericColumnBase.slice(start, end),
		dim:               b.dim,
	}
}

func newVectorBase[T entity.Vector](fieldName string, dim int, vectors []T, fieldType entity.FieldType) *vectorBase[T] {
	return &vectorBase[T]{
		genericColumnBase: &genericColumnBase[T]{
			name:      fieldName,
			fieldType: fieldType,
			values:    vectors,
		},
		dim: dim,
	}
}

/* float vector */

type ColumnFloatVector struct {
	*vectorBase[entity.FloatVector]
}

func NewColumnFloatVector(fieldName string, dim int, data [][]float32) *ColumnFloatVector {
	vectors := lo.Map(data, func(row []float32, _ int) entity.FloatVector { return entity.FloatVector(row) })
	return &ColumnFloatVector{
		vectorBase: newVectorBase(fieldName, dim, vectors, entity.FieldTypeFloatVector),
	}
}

// AppendValue appends vector value into values.
// override default type constrains, add `[]float32` conversion
func (c *ColumnFloatVector) AppendValue(i interface{}) error {
	switch vector := i.(type) {
	case entity.FloatVector:
		c.values = append(c.values, vector)
	case []float32:
		c.values = append(c.values, vector)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", vector, c.fieldType)
	}
	return nil
}

func (c *ColumnFloatVector) Slice(start, end int) Column {
	return &ColumnFloatVector{
		vectorBase: c.vectorBase.slice(start, end),
	}
}

/* binary vector */

type ColumnBinaryVector struct {
	*vectorBase[entity.BinaryVector]
}

func NewColumnBinaryVector(fieldName string, dim int, data [][]byte) *ColumnBinaryVector {
	vectors := lo.Map(data, func(row []byte, _ int) entity.BinaryVector { return entity.BinaryVector(row) })
	return &ColumnBinaryVector{
		vectorBase: newVectorBase(fieldName, dim, vectors, entity.FieldTypeBinaryVector),
	}
}

// AppendValue appends vector value into values.
// override default type constrains, add `[]byte` conversion
func (c *ColumnBinaryVector) AppendValue(i interface{}) error {
	switch vector := i.(type) {
	case entity.BinaryVector:
		c.values = append(c.values, vector)
	case []byte:
		c.values = append(c.values, vector)
	default:
		return errors.Newf("unexpected append value type %T, field type %v", vector, c.fieldType)
	}
	return nil
}

func (c *ColumnBinaryVector) Slice(start, end int) Column {
	return &ColumnBinaryVector{
		vectorBase: c.vectorBase.slice(start, end),
	}
}

/* fp16 vector */

type ColumnFloat16Vector struct {
	*vectorBase[entity.Float16Vector]
}

func NewColumnFloat16Vector(fieldName string, dim int, data [][]byte) *ColumnFloat16Vector {
	vectors := lo.Map(data, func(row []byte, _ int) entity.Float16Vector { return entity.Float16Vector(row) })
	return &ColumnFloat16Vector{
		vectorBase: newVectorBase(fieldName, dim, vectors, entity.FieldTypeFloat16Vector),
	}
}

func NewColumnFloat16VectorFromFp32Vector(fieldName string, dim int, data [][]float32) *ColumnFloat16Vector {
	vectors := lo.Map(data, func(row []float32, _ int) entity.Float16Vector { return entity.FloatVector(row).ToFloat16Vector() })
	return &ColumnFloat16Vector{
		vectorBase: newVectorBase(fieldName, dim, vectors, entity.FieldTypeFloat16Vector),
	}
}

// AppendValue appends vector value into values.
// Override default type constrains, add `[]byte`, `entity.FloatVector` and
// `[]float32` conversion.
func (c *ColumnFloat16Vector) AppendValue(i interface{}) error {
	switch vector := i.(type) {
	case entity.Float16Vector:
		c.values = append(c.values, vector)
	case []byte:
		c.values = append(c.values, vector)
	case entity.FloatVector:
		c.values = append(c.values, vector.ToFloat16Vector())
	case []float32:
		c.values = append(c.values, entity.FloatVector(vector).ToFloat16Vector())
	default:
		return errors.Newf("unexpected append value type %T, field type %v", vector, c.fieldType)
	}
	return nil
}

func (c *ColumnFloat16Vector) Slice(start, end int) Column {
	return &ColumnFloat16Vector{
		vectorBase: c.vectorBase.slice(start, end),
	}
}

/* bf16 vector */

type ColumnBFloat16Vector struct {
	*vectorBase[entity.BFloat16Vector]
}

func NewColumnBFloat16Vector(fieldName string, dim int, data [][]byte) *ColumnBFloat16Vector {
	vectors := lo.Map(data, func(row []byte, _ int) entity.BFloat16Vector { return entity.BFloat16Vector(row) })
	return &ColumnBFloat16Vector{
		vectorBase: newVectorBase(fieldName, dim, vectors, entity.FieldTypeBFloat16Vector),
	}
}

func NewColumnBFloat16VectorFromFp32Vector(fieldName string, dim int, data [][]float32) *ColumnBFloat16Vector {
	vectors := lo.Map(data, func(row []float32, _ int) entity.BFloat16Vector { return entity.FloatVector(row).ToBFloat16Vector() })
	return &ColumnBFloat16Vector{
		vectorBase: newVectorBase(fieldName, dim, vectors, entity.FieldTypeBFloat16Vector),
	}
}

// AppendValue appends vector value into values.
// Override default type constrains, add `[]byte`, `entity.FloatVector` and
// `[]float32` conversion.
func (c *ColumnBFloat16Vector) AppendValue(i interface{}) error {
	switch vector := i.(type) {
	case entity.BFloat16Vector:
		c.values = append(c.values, vector)
	case []byte:
		c.values = append(c.values, vector)
	case entity.FloatVector:
		c.values = append(c.values, vector.ToBFloat16Vector())
	case []float32:
		c.values = append(c.values, entity.FloatVector(vector).ToBFloat16Vector())
	default:
		return errors.Newf("unexpected append value type %T, field type %v", vector, c.fieldType)
	}
	return nil
}

func (c *ColumnBFloat16Vector) Slice(start, end int) Column {
	return &ColumnBFloat16Vector{
		vectorBase: c.vectorBase.slice(start, end),
	}
}
