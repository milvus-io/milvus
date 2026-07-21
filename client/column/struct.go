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
	"github.com/milvus-io/milvus/client/v3/entity"
)

// columnStructArray represents a struct-array field. Each sub-column must itself be an
// "array-of-X" column (e.g. ColumnInt32Array, ColumnFloatVectorArray) so that one entry
// in the column corresponds to one row's variable-length list of struct elements.
type columnStructArray struct {
	fields   []Column
	name     string
	nullable bool
}

func NewColumnStructArray(name string, fields []Column) Column {
	nullable := false
	for _, field := range fields {
		if field.Nullable() {
			nullable = true
			break
		}
	}
	return &columnStructArray{
		fields:   fields,
		name:     name,
		nullable: nullable,
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
		name:     c.name,
		fields:   fields,
		nullable: c.nullable,
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
	if value == nil {
		return c.AppendNull()
	}
	row, ok := value.(map[string]any)
	if !ok {
		return errors.Newf("struct array AppendValue expects map[string]any, got %T", value)
	}
	if row == nil {
		return c.AppendNull()
	}
	// pre-check all keys exist before mutating any sub-column.
	for _, sub := range c.fields {
		value, present := row[sub.Name()]
		if !present {
			return errors.Newf("struct array AppendValue: missing sub-field %q", sub.Name())
		}
		if value == nil {
			return errors.Newf("struct array AppendValue: sub-field %q is nil; use a nil struct row to append null", sub.Name())
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
	isNull, err := c.IsNull(idx)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}

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
	if idx < 0 || idx >= c.Len() {
		return false, errors.Newf("index %d out of range[0, %d)", idx, c.Len())
	}
	if !c.nullable {
		return false, nil
	}
	return c.fields[0].IsNull(idx)
}

func (c *columnStructArray) AppendNull() error {
	if !c.nullable {
		return errors.New("append null to not nullable struct array column")
	}
	for _, field := range c.fields {
		if err := field.AppendNull(); err != nil {
			return errors.Wrapf(err, "struct array AppendNull: sub-field %q", field.Name())
		}
	}
	return nil
}

func (c *columnStructArray) Nullable() bool {
	return c.nullable
}

func (c *columnStructArray) SetNullable(nullable bool) {
	c.nullable = nullable
	for _, field := range c.fields {
		field.SetNullable(nullable)
	}
}

func (c *columnStructArray) ValidateNullable() error {
	if c.nullable && len(c.fields) == 0 {
		return errors.New("nullable struct array requires at least one sub-field")
	}
	for _, field := range c.fields {
		if err := field.ValidateNullable(); err != nil {
			return errors.Wrapf(err, "struct array sub-field %q", field.Name())
		}
		if field.Nullable() != c.nullable {
			return errors.Newf("struct array sub-field %q nullable %t does not match parent nullable %t",
				field.Name(), field.Nullable(), c.nullable)
		}
	}
	if len(c.fields) == 0 {
		return nil
	}

	rowCount := c.fields[0].Len()
	for _, field := range c.fields[1:] {
		if field.Len() != rowCount {
			return errors.Newf("struct array sub-field %q length %d does not match length %d",
				field.Name(), field.Len(), rowCount)
		}
	}
	if !c.nullable {
		return nil
	}
	for row := 0; row < rowCount; row++ {
		expected, err := c.fields[0].IsNull(row)
		if err != nil {
			return errors.Wrapf(err, "struct array sub-field %q row %d", c.fields[0].Name(), row)
		}
		for _, field := range c.fields[1:] {
			actual, err := field.IsNull(row)
			if err != nil {
				return errors.Wrapf(err, "struct array sub-field %q row %d", field.Name(), row)
			}
			if actual != expected {
				return errors.Newf("struct array sub-field %q null state at row %d does not match sub-field %q",
					field.Name(), row, c.fields[0].Name())
			}
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
	if len(c.fields) == 0 {
		return 0
	}
	return c.fields[0].ValidCount()
}
