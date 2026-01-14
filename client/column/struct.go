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

type columnStructArray struct {
	fields []Column
	name   string
}

func NewColumnStructArray(name string, fields []Column) Column {
	return &columnStructArray{
		fields: fields,
		name:   name,
	}
}

func (c *columnStructArray) Name() string {
	return c.name
}

func (c *columnStructArray) Type() entity.FieldType {
	return entity.FieldTypeArray
}

func (c *columnStructArray) Len() int {
	for _, field := range c.fields {
		return field.Len()
	}
	return 0
}

func (c *columnStructArray) Slice(start, end int) Column {
	fields := make([]Column, len(c.fields))
	for idx, subField := range c.fields {
		fields[idx] = subField.Slice(start, end)
	}
	c.fields = fields
	return c
}

func (c *columnStructArray) FieldData() *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: c.name,
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{
				Fields: lo.Map(c.fields, func(field Column, _ int) *schemapb.FieldData {
					return field.FieldData()
				}),
			},
		},
	}
}

func (c *columnStructArray) AppendValue(value any) error {
	return errors.New("not implemented")
}

func (c *columnStructArray) Get(idx int) (any, error) {
	m := make(map[string]any)
	for _, field := range c.fields {
		v, err := field.Get(idx)
		if err != nil {
			return nil, err
		}
		m[field.Name()] = v
	}
	return m, nil
}

func (c *columnStructArray) GetAsInt64(idx int) (int64, error) {
	return 0, errors.New("not implemented")
}

func (c *columnStructArray) GetAsString(idx int) (string, error) {
	return "", errors.New("not implemented")
}

func (c *columnStructArray) GetAsDouble(idx int) (float64, error) {
	return 0, errors.New("not implemented")
}

func (c *columnStructArray) GetAsBool(idx int) (bool, error) {
	return false, errors.New("not implemented")
}

func (c *columnStructArray) IsNull(idx int) (bool, error) {
	return false, errors.New("not implemented")
}

func (c *columnStructArray) AppendNull() error {
	return errors.New("not implemented")
}

func (c *columnStructArray) Nullable() bool {
	return false
}

func (c *columnStructArray) SetNullable(nullable bool) {
	// Shall not be set
}

func (c *columnStructArray) ValidateNullable() error {
	for _, field := range c.fields {
		if err := field.ValidateNullable(); err != nil {
			return err
		}
	}
	return nil
}

func (c *columnStructArray) CompactNullableValues() {
	for _, field := range c.fields {
		field.CompactNullableValues()
	}
}
