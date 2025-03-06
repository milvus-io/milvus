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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

var _ Column = (*genericColumnBase[any])(nil)

// genericColumnBase implements `Column` interface
// it provides the basic function for each scalar params
type genericColumnBase[T any] struct {
	name      string
	fieldType entity.FieldType
	values    []T

	nullable  bool
	validData []bool
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
	}
	return nil
}

func (c *genericColumnBase[T]) Slice(start, end int) Column {
	return c.slice(start, end)
}

func (c *genericColumnBase[T]) slice(start, end int) *genericColumnBase[T] {
	l := c.Len()
	if start > l {
		start = l
	}
	if end == -1 || end > l {
		end = l
	}
	result := &genericColumnBase[T]{
		name:      c.name,
		fieldType: c.fieldType,
		values:    c.values[start:end],
		nullable:  c.nullable,
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
	if err := c.rangeCheck(idx); err != nil {
		return nil, err
	}
	return c.values[idx], nil
}

func (c *genericColumnBase[T]) GetAsInt64(idx int) (int64, error) {
	if err := c.rangeCheck(idx); err != nil {
		return 0, err
	}
	return value2Type[T, int64](c.values[idx])
}

func (c *genericColumnBase[T]) GetAsString(idx int) (string, error) {
	if err := c.rangeCheck(idx); err != nil {
		return "", err
	}
	return value2Type[T, string](c.values[idx])
}

func (c *genericColumnBase[T]) GetAsDouble(idx int) (float64, error) {
	if err := c.rangeCheck(idx); err != nil {
		return 0, err
	}
	return value2Type[T, float64](c.values[idx])
}

func (c *genericColumnBase[T]) GetAsBool(idx int) (bool, error) {
	if err := c.rangeCheck(idx); err != nil {
		return false, err
	}
	return value2Type[T, bool](c.values[idx])
}

func (c *genericColumnBase[T]) Value(idx int) (T, error) {
	var z T
	if err := c.rangeCheck(idx); err != nil {
		return z, err
	}
	return c.values[idx], nil
}

func (c *genericColumnBase[T]) Data() []T {
	return c.values
}

func (c *genericColumnBase[T]) MustValue(idx int) T {
	if idx < 0 || idx > c.Len() {
		panic("index out of range")
	}
	return c.values[idx]
}

func (c *genericColumnBase[T]) AppendNull() error {
	if !c.nullable {
		return errors.New("append null to not nullable column")
	}
	// var v T
	c.validData = append(c.validData, true)
	// c.values = append(c.values, v)
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

func (c *genericColumnBase[T]) withValidData(validData []bool) {
	if len(validData) > 0 {
		c.nullable = true
		c.validData = validData
	}
}
