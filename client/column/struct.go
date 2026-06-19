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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

// columnStructArray represents a struct-array field. Each sub-column must itself be an
// "array-of-X" column (e.g. ColumnInt32Array, ColumnFloatVectorArray) so that one entry
// in the column corresponds to one row's variable-length list of struct elements.
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
	// Surface as FieldTypeArray to match the user-facing schema field, whose DataType is Array
	// and ElementType is Struct. This keeps processInsertColumns' type comparison happy.
	return entity.FieldTypeArray
}

// Len returns the row count of the struct array. All sub-columns must have identical length;
// a mismatch indicates data corruption from a failed partial append and is reported via panic
// with a descriptive message (sub-fields are fully decoupled columns, so this check is the
// earliest opportunity to surface the invariant violation).
func (c *columnStructArray) Len() int {
	if len(c.fields) == 0 {
		return 0
	}
	first := c.fields[0].Len()
	for i := 1; i < len(c.fields); i++ {
		if got := c.fields[i].Len(); got != first {
			panic(errors.Newf("struct array %q sub-field %q length %d mismatches first sub-field %q length %d",
				c.name, c.fields[i].Name(), got, c.fields[0].Name(), first).Error())
		}
	}
	return first
}

func (c *columnStructArray) Slice(start, end int) Column {
	fields := make([]Column, len(c.fields))
	for idx, subField := range c.fields {
		fields[idx] = subField.Slice(start, end)
	}
	return &columnStructArray{
		name:   c.name,
		fields: fields,
	}
}

func (c *columnStructArray) FieldData() *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
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

// AppendValue appends one row of struct elements. The row must be a map[string]any whose keys
// match the sub-field names; each value must match what the corresponding sub-column's AppendValue
// accepts (typically `[]T` for scalar arrays or `[][]float32`/`[][]byte` for vector arrays).
//
// If any sub-field append fails, previously appended sub-fields are rolled back to their pre-call
// lengths so sub-columns stay in lock-step.
func (c *columnStructArray) AppendValue(value any) error {
	row, ok := value.(map[string]any)
	if !ok {
		return errors.Newf("struct array AppendValue expects map[string]any, got %T", value)
	}
	// pre-check all keys exist before mutating any sub-column.
	for _, sub := range c.fields {
		if _, present := row[sub.Name()]; !present {
			return errors.Newf("struct array AppendValue: missing sub-field %q", sub.Name())
		}
	}
	preLens := make([]int, len(c.fields))
	for i, sub := range c.fields {
		preLens[i] = sub.Len()
	}
	for i, sub := range c.fields {
		if err := sub.AppendValue(row[sub.Name()]); err != nil {
			for j := 0; j < i; j++ {
				c.fields[j] = c.fields[j].Slice(0, preLens[j])
			}
			return errors.Wrapf(err, "struct array AppendValue: sub-field %q", sub.Name())
		}
	}
	return nil
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
	return false, nil
}

func (c *columnStructArray) AppendNull() error {
	return errors.New("struct array column does not support AppendNull")
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

func (c *columnStructArray) ValidCount() int {
	return c.Len()
}
