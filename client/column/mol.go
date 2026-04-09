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

type ColumnMolSmiles struct {
	*genericColumnBase[string]
}

// Name returns column name.
func (c *ColumnMolSmiles) Name() string {
	return c.name
}

// Type returns column entity.FieldType.
func (c *ColumnMolSmiles) Type() entity.FieldType {
	return entity.FieldTypeMol
}

// Len returns column values length.
func (c *ColumnMolSmiles) Len() int {
	return len(c.values)
}

func (c *ColumnMolSmiles) Slice(start, end int) Column {
	l := c.Len()
	if start > l {
		start = l
	}
	if end == -1 || end > l {
		end = l
	}
	return &ColumnMolSmiles{
		genericColumnBase: c.genericColumnBase.slice(start, end),
	}
}

// Get returns value at index as interface{}.
func (c *ColumnMolSmiles) Get(idx int) (interface{}, error) {
	if idx < 0 || idx >= c.Len() {
		return nil, errors.New("index out of range")
	}
	return c.values[idx], nil
}

func (c *ColumnMolSmiles) GetAsString(idx int) (string, error) {
	return c.ValueByIdx(idx)
}

// FieldData return column data mapped to schemapb.FieldData.
func (c *ColumnMolSmiles) FieldData() *schemapb.FieldData {
	fd := c.genericColumnBase.FieldData()
	return fd
}

// ValueByIdx returns value of the provided index.
func (c *ColumnMolSmiles) ValueByIdx(idx int) (string, error) {
	if idx < 0 || idx >= c.Len() {
		return "", errors.New("index out of range")
	}
	return c.values[idx], nil
}

// AppendValue append value into column.
func (c *ColumnMolSmiles) AppendValue(i interface{}) error {
	s, ok := i.(string)
	if !ok {
		return errors.New("expect mol SMILES type(string)")
	}
	c.values = append(c.values, s)
	return nil
}

// Data returns column data.
func (c *ColumnMolSmiles) Data() []string {
	return c.values
}

func NewColumnMolSmiles(name string, values []string) *ColumnMolSmiles {
	return &ColumnMolSmiles{
		genericColumnBase: &genericColumnBase[string]{
			name:      name,
			fieldType: entity.FieldTypeMol,
			values:    values,
		},
	}
}
