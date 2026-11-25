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

package agg

import (
	"cmp"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// GroupKeyOrder defines sort order along one grouping column at index ColIndex
// within groupByFieldIds.
type GroupKeyOrder struct {
	ColIndex   int
	DataType   schemapb.DataType
	Ascending  bool
	NullsFirst bool
}

type groupHeapEntry struct {
	hashVal uint64
	row     *Row
}

// groupTopKHeap is a max-heap ordered by inverse GroupKeyOrder so entries[0]
// always holds the largest (worst) retained group key.
type groupTopKHeap struct {
	entries []groupHeapEntry
	order   []GroupKeyOrder
}

func (h *groupTopKHeap) Len() int { return len(h.entries) }

func (h *groupTopKHeap) Less(i, j int) bool {
	return compareRowsByOrder(h.entries[i].row, h.entries[j].row, h.order) > 0
}

func (h *groupTopKHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *groupTopKHeap) Push(x any) {
	h.entries = append(h.entries, x.(groupHeapEntry))
}

func (h *groupTopKHeap) Pop() any {
	n := len(h.entries)
	entry := h.entries[n-1]
	h.entries[n-1] = groupHeapEntry{}
	h.entries = h.entries[:n-1]
	return entry
}

func accessorMatchesRow(hashers []FieldAccessor, rowIdx int, existing *Row) bool {
	if len(existing.fieldValues) < len(hashers) {
		return false
	}
	for col, hasher := range hashers {
		leftNull := hasher.IsNullAt(rowIdx)
		fv := existing.fieldValues[col]
		if leftNull != fv.IsNull() {
			return false
		}
		if !leftNull && !accessorEqualValue(hasher, rowIdx, fv.val) {
			return false
		}
	}
	return true
}

func accessorEqualValue(acc FieldAccessor, rowIdx int, target any) bool {
	switch a := acc.(type) {
	case *Int32FieldAccessor:
		v, ok := target.(int32)
		return ok && a.vals[rowIdx] == v
	case *Int64FieldAccessor:
		v, ok := target.(int64)
		return ok && a.vals[rowIdx] == v
	case *TimestamptzFieldAccessor:
		v, ok := target.(int64)
		return ok && a.vals[rowIdx] == v
	case *Float32FieldAccessor:
		v, ok := target.(float32)
		return ok && a.vals[rowIdx] == v
	case *Float64FieldAccessor:
		v, ok := target.(float64)
		return ok && a.vals[rowIdx] == v
	case *StringFieldAccessor:
		v, ok := target.(string)
		return ok && a.vals[rowIdx] == v
	case *BoolFieldAccessor:
		v, ok := target.(bool)
		return ok && a.vals[rowIdx] == v
	default:
		return acc.ValAt(rowIdx) == target
	}
}

func compareAccessorToRow(hashers []FieldAccessor, rowIdx int, target *Row, order []GroupKeyOrder) int {
	for _, f := range order {
		acc := hashers[f.ColIndex]
		fv := target.fieldValues[f.ColIndex]
		leftNull, rightNull := acc.IsNullAt(rowIdx), fv.IsNull()
		if leftNull || rightNull {
			if c := cmpNullOrder(leftNull, rightNull, f.NullsFirst); c != 0 {
				return c
			}
			continue
		}
		c := compareAccessorToValue(acc, rowIdx, fv.val)
		if !f.Ascending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func compareAccessorToValue(acc FieldAccessor, rowIdx int, target any) int {
	switch a := acc.(type) {
	case *Int32FieldAccessor:
		return cmpScalar(a.vals[rowIdx], target.(int32))
	case *Int64FieldAccessor:
		return cmpScalar(a.vals[rowIdx], target.(int64))
	case *TimestamptzFieldAccessor:
		return cmpScalar(a.vals[rowIdx], target.(int64))
	case *Float32FieldAccessor:
		return cmpScalar(a.vals[rowIdx], target.(float32))
	case *Float64FieldAccessor:
		return cmpScalar(a.vals[rowIdx], target.(float64))
	case *StringFieldAccessor:
		return cmpScalar(a.vals[rowIdx], target.(string))
	case *BoolFieldAccessor:
		return cmpBool(a.vals[rowIdx], target.(bool))
	default:
		return 0
	}
}

func compareRowsByOrder(a, b *Row, order []GroupKeyOrder) int {
	for _, f := range order {
		fa, fb := a.fieldValues[f.ColIndex], b.fieldValues[f.ColIndex]
		leftNull, rightNull := fa.IsNull(), fb.IsNull()
		if leftNull || rightNull {
			if c := cmpNullOrder(leftNull, rightNull, f.NullsFirst); c != 0 {
				return c
			}
			continue
		}
		c := compareAnyByType(fa.val, fb.val, f.DataType)
		if !f.Ascending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func compareAnyByType(a, b any, dataType schemapb.DataType) int {
	switch dataType {
	case schemapb.DataType_Bool:
		return cmpBool(a.(bool), b.(bool))
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return cmpScalar(a.(int32), b.(int32))
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		return cmpScalar(a.(int64), b.(int64))
	case schemapb.DataType_Float:
		return cmpScalar(a.(float32), b.(float32))
	case schemapb.DataType_Double:
		return cmpScalar(a.(float64), b.(float64))
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return cmpScalar(a.(string), b.(string))
	default:
		return 0
	}
}

func cmpNullOrder(leftNull, rightNull, nullsFirst bool) int {
	if leftNull && rightNull {
		return 0
	}
	if leftNull != nullsFirst {
		return 1
	}
	return -1
}

func cmpBool(a, b bool) int {
	if a == b {
		return 0
	}
	if !a && b {
		return -1
	}
	return 1
}

func cmpScalar[T cmp.Ordered](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
