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
	"github.com/milvus-io/milvus/client/v2/entity"
)

/* bool */

var _ Column = (*ColumnBool)(nil)

type ColumnBool struct {
	*genericColumnBase[bool]
}

func NewColumnBool(name string, values []bool) *ColumnBool {
	return &ColumnBool{
		genericColumnBase: &genericColumnBase[bool]{
			name:      name,
			fieldType: entity.FieldTypeBool,
			values:    values,
		},
	}
}

func (c *ColumnBool) Slice(start, end int) Column {
	return &ColumnBool{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

/* Int8 */

var _ Column = (*ColumnInt8)(nil)

type ColumnInt8 struct {
	*genericColumnBase[int8]
}

func NewColumnInt8(name string, values []int8) *ColumnInt8 {
	return &ColumnInt8{
		genericColumnBase: &genericColumnBase[int8]{
			name:      name,
			fieldType: entity.FieldTypeInt8,
			values:    values,
		},
	}
}

func (c *ColumnInt8) Slice(start, end int) Column {
	return &ColumnInt8{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

func (c *ColumnInt8) GetAsInt64(idx int) (int64, error) {
	v, err := c.Value(idx)
	return int64(v), err
}

/* Int16 */

var _ Column = (*ColumnInt16)(nil)

type ColumnInt16 struct {
	*genericColumnBase[int16]
}

func NewColumnInt16(name string, values []int16) *ColumnInt16 {
	return &ColumnInt16{
		genericColumnBase: &genericColumnBase[int16]{
			name:      name,
			fieldType: entity.FieldTypeInt16,
			values:    values,
		},
	}
}

func (c *ColumnInt16) Slice(start, end int) Column {
	return &ColumnInt16{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

func (c *ColumnInt16) GetAsInt64(idx int) (int64, error) {
	v, err := c.Value(idx)
	return int64(v), err
}

/* Int32 */

var _ Column = (*ColumnInt32)(nil)

type ColumnInt32 struct {
	*genericColumnBase[int32]
}

func NewColumnInt32(name string, values []int32) *ColumnInt32 {
	return &ColumnInt32{
		genericColumnBase: &genericColumnBase[int32]{
			name:      name,
			fieldType: entity.FieldTypeInt32,
			values:    values,
		},
	}
}

func (c *ColumnInt32) Slice(start, end int) Column {
	return &ColumnInt32{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

func (c *ColumnInt32) GetAsInt64(idx int) (int64, error) {
	v, err := c.Value(idx)
	return int64(v), err
}

/* Int64 */

var _ Column = (*ColumnInt64)(nil)

type ColumnInt64 struct {
	*genericColumnBase[int64]
}

func NewColumnInt64(name string, values []int64) *ColumnInt64 {
	return &ColumnInt64{
		genericColumnBase: &genericColumnBase[int64]{
			name:      name,
			fieldType: entity.FieldTypeInt64,
			values:    values,
		},
	}
}

func (c *ColumnInt64) Slice(start, end int) Column {
	return &ColumnInt64{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

/* Float */

var _ Column = (*ColumnFloat)(nil)

type ColumnFloat struct {
	*genericColumnBase[float32]
}

func NewColumnFloat(name string, values []float32) *ColumnFloat {
	return &ColumnFloat{
		genericColumnBase: &genericColumnBase[float32]{
			name:      name,
			fieldType: entity.FieldTypeFloat,
			values:    values,
		},
	}
}

func (c *ColumnFloat) Slice(start, end int) Column {
	return &ColumnFloat{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

func (c *ColumnFloat) GetAsDouble(idx int) (float64, error) {
	v, err := c.Value(idx)
	return float64(v), err
}

/* Double */

var _ Column = (*ColumnFloat)(nil)

type ColumnDouble struct {
	*genericColumnBase[float64]
}

func NewColumnDouble(name string, values []float64) *ColumnDouble {
	return &ColumnDouble{
		genericColumnBase: &genericColumnBase[float64]{
			name:      name,
			fieldType: entity.FieldTypeDouble,
			values:    values,
		},
	}
}

func (c *ColumnDouble) Slice(start, end int) Column {
	return &ColumnDouble{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

/* Varchar */

var _ (Column) = (*ColumnVarChar)(nil)

type ColumnVarChar struct {
	*genericColumnBase[string]
}

func NewColumnVarChar(name string, values []string) *ColumnVarChar {
	return &ColumnVarChar{
		genericColumnBase: &genericColumnBase[string]{
			name:      name,
			fieldType: entity.FieldTypeVarChar,
			values:    values,
		},
	}
}

func (c *ColumnVarChar) Slice(start, end int) Column {
	return &ColumnVarChar{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

/* String */
/* NOT USED */

var _ (Column) = (*ColumnString)(nil)

type ColumnString struct {
	*genericColumnBase[string]
}

func NewColumnString(name string, values []string) *ColumnString {
	return &ColumnString{
		genericColumnBase: &genericColumnBase[string]{
			name:      name,
			fieldType: entity.FieldTypeString,
			values:    values,
		},
	}
}

func (c *ColumnString) Slice(start, end int) Column {
	return &ColumnString{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}
