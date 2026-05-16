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

type GColumn[T any] interface {
	Value(idx int) T
	AppendValue(v T)
}

var _ Column = (*genericColumnBase[any])(nil)

// genericColumnBase implements `Column` interface
// it provides the basic function for each scalar params
type genericColumnBase[T any] struct {
	name      string
	fieldType entity.FieldType
	values    []T

	// nullable related fields
	// note that nullable must be set to true explicitly
	nullable  bool
	validData []bool
	// nullable column could be presented in two modes
	// - compactMode, in which all valid data are compacted into one slice
	// - sparseMode, in which valid data are located in its index position
	// while invalid one are filled with zero value.
	// for Milvus 2.5.x and before, insert request shall be in compactMode while
	// search & query results are formed in sparseMode
	// this flag indicates which form current column are in and peform validation
	// or conversion logical based on it
	sparseMode bool
	// indexMapping stores the compact-sparse mapping
	indexMapping []int
}

// Name returns column name.
func (c *genericColumnBase[T]) Name() string {
	return c.name
}

// Type returns corresponding field type.
// note that: it is not necessary to be 1-on-1 mapping
// say, `[]byte` could be lots of field type.
func (c *genericColumnBase[T]) Type() entity.FieldType {
	return c.fieldType
}

func (c *genericColumnBase[T]) Len() int {
	if c.validData != nil {
		return len(c.validData)
	}
	return len(c.values)
}

func (c *genericColumnBase[T]) AppendValue(a any) error {
	if a == nil {
		return c.AppendNull()
	}
	v, ok := a.(T)
	if !ok {
		return errors.Newf("unexpected append value type %T, field type %v", a, c.fieldType)
	}
	c.values = append(c.values, v)
	if c.nullable {
		c.validData = append(c.validData, true)
		c.indexMapping = append(c.indexMapping, len(c.values)-1)
	}
	return nil
}

func (c *genericColumnBase[T]) Slice(start, end int) Column {
	return c.slice(start, end)
}

// WARNING: this methods works only for sparse mode column
func (c *genericColumnBase[T]) slice(start, end int) *genericColumnBase[T] {
	l := c.Len()
	if start > l {
		start = l
	}
	if end == -1 || end > l {
		end = l
	}
	result := &genericColumnBase[T]{
		name:       c.name,
		fieldType:  c.fieldType,
		values:     c.values[start:end],
		nullable:   c.nullable,
		sparseMode: c.sparseMode,
	}
	if c.nullable {
		result.validData = c.validData[start:end]
	}
	return result
}

func (c *genericColumnBase[T]) FieldData() *schemapb.FieldData {
	fd := values2FieldData(c.values, c.fieldType, 0)
	fd.FieldName = c.name
	fd.Type = schemapb.DataType(c.fieldType)
	if c.nullable {
		fd.ValidData = c.validData
	}
	return fd
}

func (c *genericColumnBase[T]) rangeCheck(idx int) error {
	if idx < 0 || idx >= c.Len() {
		return errors.Newf("index %d out of range[0, %d)", idx, c.Len())
	}
	return nil
}

func (c *genericColumnBase[T]) Get(idx int) (any, error) {
	idx = c.valueIndex(idx)
	if err := c.rangeCheck(idx); err != nil {
		return nil, err
	}
	return c.values[idx], nil
}

func (c *genericColumnBase[T]) GetAsInt64(idx int) (int64, error) {
	idx = c.valueIndex(idx)
	if err := c.rangeCheck(idx); err != nil {
		return 0, err
	}
	return value2Type[T, int64](c.values[idx])
}

func (c *genericColumnBase[T]) GetAsString(idx int) (string, error) {
	idx = c.valueIndex(idx)
	if err := c.rangeCheck(idx); err != nil {
		return "", err
	}
	return value2Type[T, string](c.values[idx])
}

func (c *genericColumnBase[T]) GetAsDouble(idx int) (float64, error) {
	idx = c.valueIndex(idx)
	if err := c.rangeCheck(idx); err != nil {
		return 0, err
	}
	return value2Type[T, float64](c.values[idx])
}

func (c *genericColumnBase[T]) GetAsBool(idx int) (bool, error) {
	idx = c.valueIndex(idx)
	if err := c.rangeCheck(idx); err != nil {
		return false, err
	}
	return value2Type[T, bool](c.values[idx])
}

func (c *genericColumnBase[T]) Value(idx int) (T, error) {
	idx = c.valueIndex(idx)
	var z T
	if err := c.rangeCheck(idx); err != nil {
		return z, err
	}
	return c.values[idx], nil
}

func (c *genericColumnBase[T]) valueIndex(idx int) int {
	if !c.nullable || c.sparseMode {
		return idx
	}
	return c.indexMapping[idx]
}

func (c *genericColumnBase[T]) Data() []T {
	return c.values
}

func (c *genericColumnBase[T]) MustValue(idx int) T {
	idx = c.valueIndex(idx)
	if idx < 0 || idx > c.Len() {
		panic("index out of range")
	}
	return c.values[idx]
}

func (c *genericColumnBase[T]) AppendNull() error {
	if !c.nullable {
		return errors.New("append null to not nullable column")
	}

	c.validData = append(c.validData, false)
	if !c.sparseMode {
		c.indexMapping = append(c.indexMapping, -1)
	}
	return nil
}

func (c *genericColumnBase[T]) IsNull(idx int) (bool, error) {
	if err := c.rangeCheck(idx); err != nil {
		return false, err
	}
	if !c.nullable {
		return false, nil
	}
	return !c.validData[idx], nil
}

func (c *genericColumnBase[T]) Nullable() bool {
	return c.nullable
}

// SetNullable update the nullable flag and change the valid data array according to the flag value.
// NOTE: set nullable to false will erase all the validData previously set.
func (c *genericColumnBase[T]) SetNullable(nullable bool) {
	c.nullable = nullable
	// initialize validData only when
	if c.nullable && c.validData == nil {
		// set valid flag for all exisiting values
		c.validData = lo.RepeatBy(len(c.values), func(_ int) bool { return true })
	}

	if !c.nullable {
		c.validData = nil
	}
}

// ValidateNullable performs the sanity check for nullable column.
// it checks the length of data and the valid number indicated by validData slice,
// which shall be the same by definition
func (c *genericColumnBase[T]) ValidateNullable() error {
	// skip check if column not nullable
	if !c.nullable {
		return nil
	}

	if c.sparseMode {
		return c.validateNullableSparse()
	}
	return c.validateNullableCompact()
}

func (c *genericColumnBase[T]) validateNullableCompact() error {
	// count valid entries
	var validCnt int
	c.indexMapping = make([]int, len(c.validData))
	for idx, v := range c.validData {
		if v {
			c.indexMapping[idx] = validCnt
			validCnt++
		} else {
			c.indexMapping[idx] = -1
		}
	}
	if validCnt != len(c.values) {
		return errors.Newf("values number(%d) does not match valid count(%d)", len(c.values), validCnt)
	}
	return nil
}

func (c *genericColumnBase[T]) validateNullableSparse() error {
	if len(c.validData) != len(c.values) {
		return errors.Newf("values number (%d) does not match valid data len(%d)", len(c.values), len(c.validData))
	}
	return nil
}

func (c *genericColumnBase[T]) CompactNullableValues() {
	if !c.nullable || !c.sparseMode {
		return
	}

	c.indexMapping = make([]int, len(c.validData))
	var cnt int
	for idx, valid := range c.validData {
		if !valid {
			c.indexMapping[idx] = -1
			continue
		}
		c.values[cnt] = c.values[idx]
		c.indexMapping[idx] = cnt
		cnt++
	}
	c.values = c.values[0:cnt]
}

func (c *genericColumnBase[T]) withValidData(validData []bool) {
	if len(validData) > 0 {
		c.nullable = true
		c.validData = validData
	}
}

func (c *genericColumnBase[T]) base() *genericColumnBase[T] {
	return c
}

type ColumnOption[T any] func(*genericColumnBase[T])

// WithSparseNullableMode returns a ColumnOption that sets the sparse mode for the column.
func WithSparseNullableMode[T any](flag bool) ColumnOption[T] {
	return func(c *genericColumnBase[T]) {
		c.sparseMode = flag
	}
}
