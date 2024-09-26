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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

// ColumnVarChar generated columns type for VarChar
type ColumnVarChar struct {
	ColumnBase
	name   string
	values []string
}

// Name returns column name
func (c *ColumnVarChar) Name() string {
	return c.name
}

// Type returns column entity.FieldType
func (c *ColumnVarChar) Type() entity.FieldType {
	return entity.FieldTypeVarChar
}

// Len returns column values length
func (c *ColumnVarChar) Len() int {
	return len(c.values)
}

// Get returns value at index as interface{}.
func (c *ColumnVarChar) Get(idx int) (interface{}, error) {
	if idx < 0 || idx > c.Len() {
		return "", errors.New("index out of range")
	}
	return c.values[idx], nil
}

// GetAsString returns value at idx.
func (c *ColumnVarChar) GetAsString(idx int) (string, error) {
	if idx < 0 || idx > c.Len() {
		return "", errors.New("index out of range")
	}
	return c.values[idx], nil
}

// FieldData return column data mapped to schemapb.FieldData
func (c *ColumnVarChar) FieldData() *schemapb.FieldData {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: c.name,
	}
	data := make([]string, 0, c.Len())
	for i := 0; i < c.Len(); i++ {
		data = append(data, c.values[i])
	}
	fd.Field = &schemapb.FieldData_Scalars{
		Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{
					Data: data,
				},
			},
		},
	}
	return fd
}

// ValueByIdx returns value of the provided index
// error occurs when index out of range
func (c *ColumnVarChar) ValueByIdx(idx int) (string, error) {
	var r string // use default value
	if idx < 0 || idx >= c.Len() {
		return r, errors.New("index out of range")
	}
	return c.values[idx], nil
}

// AppendValue append value into column
func (c *ColumnVarChar) AppendValue(i interface{}) error {
	v, ok := i.(string)
	if !ok {
		return fmt.Errorf("invalid type, expected string, got %T", i)
	}
	c.values = append(c.values, v)

	return nil
}

// Data returns column data
func (c *ColumnVarChar) Data() []string {
	return c.values
}

// NewColumnVarChar auto generated constructor
func NewColumnVarChar(name string, values []string) *ColumnVarChar {
	return &ColumnVarChar{
		name:   name,
		values: values,
	}
}
