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

	"github.com/milvus-io/milvus/client/v3/entity"
)

// Element validity of struct sub-field columns follows the row-level nullable model: the column
// keeps values plus a validity mask per row, one flag per logical element.
//   - compact mode (the insert form): a row's values hold only its valid elements;
//   - sparse mode (the result form): a row's values hold every element, null positions carry
//     the zero value (nil for vectors and inner arrays).
//
// Get returns the logical elements of a row: `[]*T` for scalar elements and `[]V` with nil
// entries for vector elements, nil marking a null element. AppendValue accepts the same forms.

// ElementNullableColumn is implemented by the array and vector-array columns used as struct
// sub-fields. Element accessors take the logical row and element index.
type ElementNullableColumn interface {
	Column
	// ElementNullable reports whether elements of the column may be null.
	ElementNullable() bool
	// SetElementNullable updates the element-nullable flag. Setting it to false erases the
	// element validity previously set.
	SetElementNullable(bool)
	// ElementLen returns the logical element count of a row; a null row has none.
	ElementLen(row int) (int, error)
	// ElementValidData returns the validity of each logical element of a row.
	ElementValidData(row int) ([]bool, error)
	// ElementIsNull reports whether element j of a row is null.
	ElementIsNull(row, j int) (bool, error)
}

// NestedArrayColumn is implemented by Array<T> struct sub-field columns, whose elements are
// inner arrays of leaf values. The element layer describes the inner arrays.
type NestedArrayColumn interface {
	ElementNullableColumn
	// LeafType returns the type of the leaf values.
	LeafType() entity.FieldType
	// LeafNullable reports whether leaf values may be null.
	LeafNullable() bool
	// SetLeafNullable updates the leaf-nullable flag. Setting it to false erases the leaf
	// validity previously set.
	SetLeafNullable(bool)
	// LeafValidData returns the validity of each leaf of inner array j of a row; a null inner
	// array has none.
	LeafValidData(row, j int) ([]bool, error)
	// LeafIsNull reports whether leaf k of inner array j of a row is null.
	LeafIsNull(row, j, k int) (bool, error)
}

// ElementNullableOption configures the element-nullable column constructors.
type ElementNullableOption func(*elementNullableOptions)

type elementNullableOptions struct {
	rowValidData []bool
	sparse       bool
}

// WithRowValidData makes the column row-nullable with the given row validity. Values and element
// validity then hold entries for the valid rows only, or for every row in sparse mode.
func WithRowValidData(validData []bool) ElementNullableOption {
	return func(o *elementNullableOptions) {
		o.rowValidData = validData
	}
}

// WithElementSparseMode sets the column to sparse mode: values hold an entry for every row and
// every element, and null positions carry the zero value.
func WithElementSparseMode(flag bool) ElementNullableOption {
	return func(o *elementNullableOptions) {
		o.sparse = flag
	}
}

func newElementNullableOptions(opts []ElementNullableOption) *elementNullableOptions {
	o := &elementNullableOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// applyRowOptions sets the row-level nullable state of an element-nullable column built by a
// constructor.
func applyRowOptions[T any](base *genericColumnBase[T], o *elementNullableOptions) {
	base.sparseMode = o.sparse
	if o.rowValidData != nil {
		base.nullable = true
		base.validData = o.rowValidData
	}
}

// elementNuller is implemented by columns that can append a row of null elements; struct array
// columns use it to fill an omitted element-nullable sub-field.
type elementNuller interface {
	appendNullElements(n int) error
}

// elementLener is implemented by every struct sub-field column.
type elementLener interface {
	ElementLen(row int) (int, error)
}

func allValid(n int) []bool {
	if n == 0 {
		return nil
	}
	valid := make([]bool, n)
	for i := range valid {
		valid[i] = true
	}
	return valid
}

// storedElementIndex maps logical element j to its position in the row's values. The caller
// guarantees element j is valid.
func storedElementIndex(valid []bool, j int, sparse bool) int {
	if sparse {
		return j
	}
	return countValid(valid[:j])
}

// elementPointers returns the logical elements of one row; nil marks a null element.
func elementPointers[T any](values []T, valid []bool, sparse bool) []*T {
	result := make([]*T, len(valid))
	next := 0
	for j, ok := range valid {
		idx := j
		if !sparse {
			idx = next
		}
		if ok {
			v := values[idx]
			result[j] = &v
			next++
		}
	}
	return result
}

// compactElements keeps the valid elements of one dense row.
func compactElements[T any](values []T, valid []bool) []T {
	result := make([]T, 0, countValid(valid))
	for j, ok := range valid {
		if ok {
			result = append(result, values[j])
		}
	}
	return result
}

// denseElements expands the valid elements of one compact row; null positions get the zero value.
func denseElements[T any](values []T, valid []bool) []T {
	result := make([]T, len(valid))
	next := 0
	for j, ok := range valid {
		if ok {
			result[j] = values[next]
			next++
		}
	}
	return result
}

// denseRow returns the elements of one result row in dense form together with their validity.
// A row without validity has only valid elements; a row with validity may carry dense values
// (one per element) or compact values (one per valid element).
func denseRow[T any](fieldName string, row int, values []T, valid []bool) ([]T, []bool, error) {
	if len(valid) == 0 {
		return values, allValid(len(values)), nil
	}
	if len(values) == len(valid) {
		return values, valid, nil
	}
	if len(values) == countValid(valid) {
		return denseElements(values, valid), valid, nil
	}
	return nil, nil, fmt.Errorf("field %q row %d has %d values for %d elements with %d valid",
		fieldName, row, len(values), len(valid), countValid(valid))
}

// validateElementRows checks that the stored values of each row match its element validity.
func validateElementRows[T any](values [][]T, elementValid [][]bool, sparse bool) error {
	if len(elementValid) != len(values) {
		return errors.Newf("element valid data rows (%d) does not match value rows (%d)", len(elementValid), len(values))
	}
	for i, row := range values {
		expected := countValid(elementValid[i])
		if sparse {
			expected = len(elementValid[i])
		}
		if len(row) != expected {
			return errors.Newf("row %d has %d values, but its element valid data expects %d", i, len(row), expected)
		}
	}
	return nil
}

// selectElementRows returns the entries of a per-payload-row slice that belong to the payload
// rows in [valueStart, valueEnd).
func selectElementRows[T any](rows []T, valueStart, valueEnd int) []T {
	if rows == nil {
		return nil
	}
	return append([]T(nil), rows[valueStart:valueEnd]...)
}

// filterValidRows keeps the per-payload-row entries of valid rows; used when a sparse column is
// compacted at the row level.
func filterValidRows[T any](rows []T, validData []bool) []T {
	if rows == nil {
		return nil
	}
	result := make([]T, 0, countValid(validData))
	for i, ok := range validData {
		if ok {
			result = append(result, rows[i])
		}
	}
	return result
}

func elementRangeCheck(n, j int) error {
	if j < 0 || j >= n {
		return errors.Newf("element index %d out of range[0, %d)", j, n)
	}
	return nil
}
