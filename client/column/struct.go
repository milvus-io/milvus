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

// StructArrayColumn is implemented by struct array columns.
type StructArrayColumn interface {
	Column
	// Fields returns the sub-field columns.
	Fields() []Column
}

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

// NewColumnStructArrayFromSchema creates an empty StructArray column whose
// child columns match schema. Values can then be appended as map[string]any.
func NewColumnStructArrayFromSchema(name string, schema *entity.StructSchema) (Column, error) {
	if schema == nil {
		return nil, errors.New("struct schema is required")
	}
	fields := make([]Column, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if field == nil {
			return nil, errors.New("struct schema contains a nil child")
		}
		child, err := newStructArrayChildColumn(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, child)
	}
	return NewColumnStructArray(name, fields), nil
}

// newStructArrayChildColumn creates the empty column of a sub-field. The sub-field describes one
// struct element; its Nullable makes the column element nullable, and ElementNullable makes the
// leaves of an Array<T> sub-field nullable.
func newStructArrayChildColumn(field *entity.Field) (Column, error) {
	if field == nil {
		return nil, errors.New("struct child field is required")
	}
	col, err := newStructArrayChildBase(field)
	if err != nil {
		return nil, err
	}
	if field.Nullable {
		elementCol, ok := col.(ElementNullableColumn)
		if !ok {
			return nil, errors.Newf("struct sub-field %q of type %v does not support element nullable", field.Name, field.DataType)
		}
		elementCol.SetElementNullable(true)
	}
	if field.ElementNullable {
		nestedCol, ok := col.(NestedArrayColumn)
		if !ok {
			return nil, errors.Newf("struct sub-field %q of type %v does not support element nullable leaves", field.Name, field.DataType)
		}
		nestedCol.SetLeafNullable(true)
	}
	return col, nil
}

func newStructArrayChildBase(field *entity.Field) (Column, error) {
	switch field.DataType {
	case entity.FieldTypeArray:
		col, err := newNestedArrayColumn(field.Name, field.ElementType)
		if err != nil {
			return nil, errors.Wrapf(err, "sub-field %q", field.Name)
		}
		return col, nil
	case entity.FieldTypeBool:
		return NewColumnBoolArray(field.Name, nil), nil
	case entity.FieldTypeInt8:
		return NewColumnInt8Array(field.Name, nil), nil
	case entity.FieldTypeInt16:
		return NewColumnInt16Array(field.Name, nil), nil
	case entity.FieldTypeInt32:
		return NewColumnInt32Array(field.Name, nil), nil
	case entity.FieldTypeInt64:
		return NewColumnInt64Array(field.Name, nil), nil
	case entity.FieldTypeFloat:
		return NewColumnFloatArray(field.Name, nil), nil
	case entity.FieldTypeDouble:
		return NewColumnDoubleArray(field.Name, nil), nil
	case entity.FieldTypeVarChar, entity.FieldTypeString:
		return NewColumnVarCharArray(field.Name, nil), nil
	case entity.FieldTypeFloatVector:
		dim, err := field.GetDim()
		if err != nil {
			return nil, errors.Wrapf(err, "sub-field %q", field.Name)
		}
		return NewColumnFloatVectorArray(field.Name, int(dim), nil), nil
	case entity.FieldTypeFloat16Vector:
		dim, err := field.GetDim()
		if err != nil {
			return nil, errors.Wrapf(err, "sub-field %q", field.Name)
		}
		return NewColumnFloat16VectorArray(field.Name, int(dim), nil), nil
	case entity.FieldTypeBFloat16Vector:
		dim, err := field.GetDim()
		if err != nil {
			return nil, errors.Wrapf(err, "sub-field %q", field.Name)
		}
		return NewColumnBFloat16VectorArray(field.Name, int(dim), nil), nil
	case entity.FieldTypeBinaryVector:
		dim, err := field.GetDim()
		if err != nil {
			return nil, errors.Wrapf(err, "sub-field %q", field.Name)
		}
		return NewColumnBinaryVectorArray(field.Name, int(dim), nil), nil
	case entity.FieldTypeInt8Vector:
		dim, err := field.GetDim()
		if err != nil {
			return nil, errors.Wrapf(err, "sub-field %q", field.Name)
		}
		return NewColumnInt8VectorArray(field.Name, int(dim), nil), nil
	default:
		return nil, errors.Newf("unsupported struct sub-field type %v for field %q", field.DataType, field.Name)
	}
}

// Fields returns the sub-field columns.
func (c *columnStructArray) Fields() []Column {
	return c.fields
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
// accepts (typically `[]T` for scalar arrays or `[][]float32`/`[][]byte` for vector arrays, and
// `[]*T` or nil vectors for null elements of element-nullable sub-fields). An element-nullable
// sub-field may be omitted or nil: it gets one null element per struct element of the row.
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
	omitted := make([]bool, len(c.fields))
	for i, sub := range c.fields {
		value, present := row[sub.Name()]
		if present && value != nil {
			continue
		}
		if isElementNullableColumn(sub) {
			omitted[i] = true
			continue
		}
		if !present {
			return errors.Newf("struct array AppendValue: missing sub-field %q", sub.Name())
		}
		return errors.Newf("struct array AppendValue: sub-field %q is nil; use a nil struct row to append null", sub.Name())
	}
	preLens := make([]int, len(c.fields))
	for i, sub := range c.fields {
		preLens[i] = sub.Len()
	}
	rollback := func() {
		for j, field := range c.fields {
			if field.Len() > preLens[j] {
				c.fields[j] = field.Slice(0, preLens[j])
			}
		}
	}

	elementCount, countFrom := -1, ""
	for i, sub := range c.fields {
		if omitted[i] {
			continue
		}
		if err := sub.AppendValue(row[sub.Name()]); err != nil {
			rollback()
			return errors.Wrapf(err, "struct array AppendValue: sub-field %q", sub.Name())
		}
		n, err := subFieldElementLen(sub, sub.Len()-1)
		if err != nil {
			rollback()
			return errors.Wrapf(err, "struct array AppendValue: sub-field %q", sub.Name())
		}
		if elementCount < 0 {
			elementCount, countFrom = n, sub.Name()
		} else if n != elementCount {
			rollback()
			return errors.Newf("struct array AppendValue: sub-field %q has %d elements but sub-field %q has %d",
				sub.Name(), n, countFrom, elementCount)
		}
	}
	for i, sub := range c.fields {
		if !omitted[i] {
			continue
		}
		if elementCount < 0 {
			rollback()
			return errors.Newf("struct array AppendValue: cannot infer the element count of omitted sub-field %q; provide at least one sub-field", sub.Name())
		}
		if err := sub.(elementNuller).appendNullElements(elementCount); err != nil {
			rollback()
			return errors.Wrapf(err, "struct array AppendValue: sub-field %q", sub.Name())
		}
	}
	return nil
}

func isElementNullableColumn(col Column) bool {
	elementCol, ok := col.(ElementNullableColumn)
	if !ok || !elementCol.ElementNullable() {
		return false
	}
	_, ok = col.(elementNuller)
	return ok
}

// subFieldElementLen returns the logical element count of a sub-field row.
func subFieldElementLen(col Column, row int) (int, error) {
	lener, ok := col.(elementLener)
	if !ok {
		return 0, errors.Newf("column %q of type %v is not a struct sub-field column", col.Name(), col.Type())
	}
	return lener.ElementLen(row)
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
		if !field.Nullable() {
			return errors.Newf("struct array AppendNull: sub-field %q is not nullable", field.Name())
		}
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
	if err := c.validateElementCounts(rowCount); err != nil {
		return err
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

// validateElementCounts checks that every sub-field holds the same number of struct elements in
// each row.
func (c *columnStructArray) validateElementCounts(rowCount int) error {
	for row := 0; row < rowCount; row++ {
		expected, expectedFrom := -1, ""
		for _, field := range c.fields {
			lener, ok := field.(elementLener)
			if !ok {
				continue
			}
			n, err := lener.ElementLen(row)
			if err != nil {
				return errors.Wrapf(err, "struct array sub-field %q row %d", field.Name(), row)
			}
			if expected < 0 {
				expected, expectedFrom = n, field.Name()
				continue
			}
			if n != expected {
				return errors.Newf("struct array %q sub-field %q has %d elements at row %d, but sub-field %q has %d",
					c.name, field.Name(), n, row, expectedFrom, expected)
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

// ApplyStructSchema aligns the sub-columns of a struct array column with its struct schema: it
// marks the element-nullable and leaf-nullable sub-columns, and gives an Array<T> sub-column
// parsed from rows that do not tell their leaf type the type declared in the schema. Sub-columns
// absent from the schema are left as they are.
func ApplyStructSchema(col Column, schema *entity.StructSchema) error {
	structCol, ok := col.(*columnStructArray)
	if !ok {
		return errors.Newf("column %q is not a struct array column", col.Name())
	}
	if schema == nil {
		return nil
	}
	fields := make(map[string]*entity.Field, len(schema.Fields))
	for _, field := range schema.Fields {
		if field != nil {
			fields[field.Name] = field
		}
	}
	for i, sub := range structCol.fields {
		field := fields[sub.Name()]
		if field == nil {
			continue
		}
		if untyped, ok := sub.(*columnUntypedArrayArray); ok && field.DataType == entity.FieldTypeArray {
			typed, err := retypeUntypedArrayArray(untyped, field)
			if err != nil {
				return err
			}
			structCol.fields[i] = typed
			sub = typed
		}
		if elementCol, ok := sub.(ElementNullableColumn); ok && field.Nullable && !elementCol.ElementNullable() {
			elementCol.SetElementNullable(true)
		}
		if nestedCol, ok := sub.(NestedArrayColumn); ok && field.ElementNullable && !nestedCol.LeafNullable() {
			nestedCol.SetLeafNullable(true)
		}
	}
	return nil
}

// retypeUntypedArrayArray rebuilds an untyped Array<T> sub-column with the leaf type of its
// schema. None of its rows holds an inner array.
func retypeUntypedArrayArray(untyped *columnUntypedArrayArray, field *entity.Field) (Column, error) {
	typed, err := newStructArrayChildColumn(field)
	if err != nil {
		return nil, err
	}
	if untyped.Nullable() {
		typed.SetNullable(true)
	}
	for row := 0; row < untyped.Len(); row++ {
		isNull, err := untyped.IsNull(row)
		if err != nil {
			return nil, err
		}
		if isNull {
			err = typed.AppendNull()
		} else if n, _ := untyped.ElementLen(row); n > 0 {
			err = typed.(elementNuller).appendNullElements(n)
		} else {
			err = typed.(interface{ appendEmptyRow() error }).appendEmptyRow()
		}
		if err != nil {
			return nil, errors.Wrapf(err, "struct sub-field %q row %d", field.Name, row)
		}
	}
	return typed, nil
}
