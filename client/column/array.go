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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

// columnArrayBase implement `Column` interface
// it provided specified `FieldData` behavior for Array columns.
type columnArrayBase[T any] struct {
	*genericColumnBase[[]T]
	elementType entity.FieldType
}

func (c *columnArrayBase[T]) FieldData() *schemapb.FieldData {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: c.name,
		ValidData: c.validData,
	}

	data := make([]*schemapb.ScalarField, 0, c.Len())
	for _, arr := range c.values {
		data = append(data, slice2Scalar(arr, c.elementType))
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
	return fd
}

func (c *columnArrayBase[T]) ElementType() entity.FieldType {
	return c.elementType
}

func (c *columnArrayBase[T]) slice(start, end int) *columnArrayBase[T] {
	return &columnArrayBase[T]{
		genericColumnBase: c.genericColumnBase.slice(start, end),
		elementType:       c.elementType,
	}
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
