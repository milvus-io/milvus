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
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/client/v2/entity"
)

var (
	// scalars
	NewNullableColumnBool                 NullableColumnCreateFunc[bool, *ColumnBool]                   = NewNullableColumnCreator(NewColumnBool).New
	NewNullableColumnInt8                 NullableColumnCreateFunc[int8, *ColumnInt8]                   = NewNullableColumnCreator(NewColumnInt8).New
	NewNullableColumnInt16                NullableColumnCreateFunc[int16, *ColumnInt16]                 = NewNullableColumnCreator(NewColumnInt16).New
	NewNullableColumnInt32                NullableColumnCreateFunc[int32, *ColumnInt32]                 = NewNullableColumnCreator(NewColumnInt32).New
	NewNullableColumnInt64                NullableColumnCreateFunc[int64, *ColumnInt64]                 = NewNullableColumnCreator(NewColumnInt64).New
	NewNullableColumnVarChar              NullableColumnCreateFunc[string, *ColumnVarChar]              = NewNullableColumnCreator(NewColumnVarChar).New
	NewNullableColumnString               NullableColumnCreateFunc[string, *ColumnString]               = NewNullableColumnCreator(NewColumnString).New
	NewNullableColumnFloat                NullableColumnCreateFunc[float32, *ColumnFloat]               = NewNullableColumnCreator(NewColumnFloat).New
	NewNullableColumnDouble               NullableColumnCreateFunc[float64, *ColumnDouble]              = NewNullableColumnCreator(NewColumnDouble).New
	NewNullableColumnTimestamptzIsoString NullableColumnCreateFunc[string, *ColumnTimestampTzIsoString] = NewNullableColumnCreator(NewColumnTimestamptzIsoString).New
	NewNullableColumnJSONBytes            NullableColumnCreateFunc[[]byte, *ColumnJSONBytes]            = NewNullableColumnCreator(NewColumnJSONBytes).New
	NewNullableColumnGeometryWKT          NullableColumnCreateFunc[string, *ColumnGeometryWKT]          = NewNullableColumnCreator(NewColumnGeometryWKT).New
	// array
	NewNullableColumnBoolArray    NullableColumnCreateFunc[[]bool, *ColumnBoolArray]      = NewNullableColumnCreator(NewColumnBoolArray).New
	NewNullableColumnInt8Array    NullableColumnCreateFunc[[]int8, *ColumnInt8Array]      = NewNullableColumnCreator(NewColumnInt8Array).New
	NewNullableColumnInt16Array   NullableColumnCreateFunc[[]int16, *ColumnInt16Array]    = NewNullableColumnCreator(NewColumnInt16Array).New
	NewNullableColumnInt32Array   NullableColumnCreateFunc[[]int32, *ColumnInt32Array]    = NewNullableColumnCreator(NewColumnInt32Array).New
	NewNullableColumnInt64Array   NullableColumnCreateFunc[[]int64, *ColumnInt64Array]    = NewNullableColumnCreator(NewColumnInt64Array).New
	NewNullableColumnVarCharArray NullableColumnCreateFunc[[]string, *ColumnVarCharArray] = NewNullableColumnCreator(NewColumnVarCharArray).New
	NewNullableColumnFloatArray   NullableColumnCreateFunc[[]float32, *ColumnFloatArray]  = NewNullableColumnCreator(NewColumnFloatArray).New
	NewNullableColumnDoubleArray  NullableColumnCreateFunc[[]float64, *ColumnDoubleArray] = NewNullableColumnCreator(NewColumnDoubleArray).New
)

func NewNullableColumnFloatVector(fieldName string, dim int, values [][]float32, validData []bool) (*ColumnFloatVector, error) {
	if len(values) != getValidCount(validData) {
		return nil, errors.Newf("values length (%d) must equal valid count (%d) in validData", len(values), getValidCount(validData))
	}
	col := NewColumnFloatVector(fieldName, dim, values)
	col.withValidData(validData)
	col.nullable = true
	return col, nil
}

func NewNullableColumnBinaryVector(fieldName string, dim int, values [][]byte, validData []bool) (*ColumnBinaryVector, error) {
	if len(values) != getValidCount(validData) {
		return nil, errors.Newf("values length (%d) must equal valid count (%d) in validData", len(values), getValidCount(validData))
	}
	col := NewColumnBinaryVector(fieldName, dim, values)
	col.withValidData(validData)
	col.nullable = true
	return col, nil
}

func NewNullableColumnFloat16Vector(fieldName string, dim int, values [][]byte, validData []bool) (*ColumnFloat16Vector, error) {
	if len(values) != getValidCount(validData) {
		return nil, errors.Newf("values length (%d) must equal valid count (%d) in validData", len(values), getValidCount(validData))
	}
	col := NewColumnFloat16Vector(fieldName, dim, values)
	col.withValidData(validData)
	col.nullable = true
	return col, nil
}

func NewNullableColumnBFloat16Vector(fieldName string, dim int, values [][]byte, validData []bool) (*ColumnBFloat16Vector, error) {
	if len(values) != getValidCount(validData) {
		return nil, errors.Newf("values length (%d) must equal valid count (%d) in validData", len(values), getValidCount(validData))
	}
	col := NewColumnBFloat16Vector(fieldName, dim, values)
	col.withValidData(validData)
	col.nullable = true
	return col, nil
}

func NewNullableColumnInt8Vector(fieldName string, dim int, values [][]int8, validData []bool) (*ColumnInt8Vector, error) {
	if len(values) != getValidCount(validData) {
		return nil, errors.Newf("values length (%d) must equal valid count (%d) in validData", len(values), getValidCount(validData))
	}
	col := NewColumnInt8Vector(fieldName, dim, values)
	col.withValidData(validData)
	col.nullable = true
	return col, nil
}

func NewNullableColumnSparseFloatVector(fieldName string, values []entity.SparseEmbedding, validData []bool) (*ColumnSparseFloatVector, error) {
	if len(values) != getValidCount(validData) {
		return nil, errors.Newf("values length (%d) must equal valid count (%d) in validData", len(values), getValidCount(validData))
	}
	col := NewColumnSparseVectors(fieldName, values)
	col.withValidData(validData)
	col.nullable = true
	return col, nil
}

func getValidCount(validData []bool) int {
	count := 0
	for _, v := range validData {
		if v {
			count++
		}
	}
	return count
}

type NullableColumnCreateFunc[T any, Col interface {
	Column
	Data() []T
}] func(name string, values []T, validData []bool, opts ...ColumnOption[T]) (Col, error)

type NullableColumnCreator[col interface {
	Column
	withValidData([]bool)
	base() *genericColumnBase[T]
}, T any] struct {
	base func(name string, values []T) col
}

func (c NullableColumnCreator[col, T]) New(name string, values []T, validData []bool, opts ...ColumnOption[T]) (col, error) {
	result := c.base(name, values)
	result.withValidData(validData)
	base := result.base()

	for _, opt := range opts {
		opt(base)
	}

	return result, result.ValidateNullable()
}

func NewNullableColumnCreator[col interface {
	Column
	withValidData([]bool)
	base() *genericColumnBase[T]
}, T any](base func(name string, values []T) col) NullableColumnCreator[col, T] {
	return NullableColumnCreator[col, T]{
		base: base,
	}
}

func NewNullableColumnTimestamptz(name string, values []time.Time, validData []bool, opts ...ColumnOption[string]) (*ColumnTimestamptz, error) {
	result := NewColumnTimestamptz(name, values)
	result.withValidData(validData)

	for _, opt := range opts {
		opt(result.genericColumnBase)
	}

	return result, result.ValidateNullable()
}
