/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"cmp"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =============================================================================
// Generic Array Helpers
// =============================================================================

// typedArray is an Arrow array that provides typed value access.
type typedArray[T any] interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}

// typedBuilder is an Arrow builder that supports typed append.
type typedBuilder[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// pickByIndices creates a new array by picking elements at the given indices.
func pickByIndices[T any, A typedArray[T], B typedBuilder[T]](arr A, builder B, indices []int) (arrow.Array, error) {
	defer builder.Release()
	arrLen := arr.Len()
	for _, idx := range indices {
		if idx < 0 || idx >= arrLen {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("index out of bounds: %d (array length: %d)", idx, arrLen))
		}
		if arr.IsNull(idx) {
			builder.AppendNull()
		} else {
			builder.Append(arr.Value(idx))
		}
	}
	return builder.NewArray(), nil
}

// compareTyped compares two values in a typed array using cmp.Ordered.
func compareTyped[T cmp.Ordered, A typedArray[T]](arr A, i, j int) int {
	return cmp.Compare(arr.Value(i), arr.Value(j))
}

// =============================================================================
// Type Dispatch Functions
// =============================================================================

// dispatchPickByIndices dispatches pickByIndices to the correct Arrow type.
func dispatchPickByIndices(pool memory.Allocator, data arrow.Array, indices []int) (arrow.Array, error) {
	switch arr := data.(type) {
	case *array.Boolean:
		return pickByIndices(arr, array.NewBooleanBuilder(pool), indices)
	case *array.Int8:
		return pickByIndices(arr, array.NewInt8Builder(pool), indices)
	case *array.Int16:
		return pickByIndices(arr, array.NewInt16Builder(pool), indices)
	case *array.Int32:
		return pickByIndices(arr, array.NewInt32Builder(pool), indices)
	case *array.Int64:
		return pickByIndices(arr, array.NewInt64Builder(pool), indices)
	case *array.Uint8:
		return pickByIndices(arr, array.NewUint8Builder(pool), indices)
	case *array.Uint16:
		return pickByIndices(arr, array.NewUint16Builder(pool), indices)
	case *array.Uint32:
		return pickByIndices(arr, array.NewUint32Builder(pool), indices)
	case *array.Uint64:
		return pickByIndices(arr, array.NewUint64Builder(pool), indices)
	case *array.Float32:
		return pickByIndices(arr, array.NewFloat32Builder(pool), indices)
	case *array.Float64:
		return pickByIndices(arr, array.NewFloat64Builder(pool), indices)
	case *array.String:
		return pickByIndices(arr, array.NewStringBuilder(pool), indices)
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported array type %T", data))
	}
}

// =============================================================================
// BaseOp
// =============================================================================

// BaseOp is the base operator with common fields.
type BaseOp struct {
	inputs  []string
	outputs []string
}

func (o *BaseOp) Inputs() []string  { return o.inputs }
func (o *BaseOp) Outputs() []string { return o.outputs }

// ReadInputColumns reads all input columns declared in o.inputs from the DataFrame.
// Returns an error if any declared input column is missing.
func (o *BaseOp) ReadInputColumns(opName string, df *DataFrame) ([]*arrow.Chunked, error) {
	cols := make([]*arrow.Chunked, len(o.inputs))
	for i, name := range o.inputs {
		col := df.Column(name)
		if col == nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("%s: column %q not found", opName, name))
		}
		cols[i] = col
	}
	return cols, nil
}

// sliceArray slices an array from start to end using zero-copy.
func sliceArray(data arrow.Array, start, end int) (arrow.Array, error) {
	return array.NewSlice(data, int64(start), int64(end)), nil
}

// getArrayValue extracts value from an array at given index.
func getArrayValue(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx)
	case *array.Int8:
		return a.Value(idx)
	case *array.Int16:
		return a.Value(idx)
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Uint8:
		return a.Value(idx)
	case *array.Uint16:
		return a.Value(idx)
	case *array.Uint32:
		return a.Value(idx)
	case *array.Uint64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	default:
		return nil
	}
}
