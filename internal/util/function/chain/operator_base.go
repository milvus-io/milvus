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
	"context"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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
			return nil, merr.WrapErrServiceInternalMsg("index out of bounds: %d (array length: %d)", idx, arrLen)
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
		return takeArrayWithNulls(pool, data, indices)
	}
}

// takeArrayWithNulls supports nested List/Struct and fixed-size vector columns.
// A -1 index creates a typed null, used by outer explode and nullable structs.
func takeArrayWithNulls(pool memory.Allocator, data arrow.Array, indices []int) (arrow.Array, error) {
	if data == nil {
		return nil, merr.WrapErrServiceInternal("take input array is nil")
	}
	builder := array.NewInt64Builder(pool)
	defer builder.Release()
	for _, index := range indices {
		if index == -1 {
			builder.AppendNull()
		} else if index < 0 || index >= data.Len() {
			return nil, merr.WrapErrServiceInternalMsg("take index %d exceeds array length %d", index, data.Len())
		} else {
			builder.Append(int64(index))
		}
	}
	selection := builder.NewArray()
	defer selection.Release()
	result, err := compute.TakeArray(compute.WithAllocator(context.Background(), pool), data, selection)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "Arrow take failed")
	}
	return result, nil
}

// =============================================================================
// BaseOp
// =============================================================================

// BaseOp is the base operator with common fields.
type BaseOp struct {
	inputs         []string
	outputs        []string
	inputDataTypes []schemapb.DataType
}

func (o *BaseOp) Inputs() []string  { return o.inputs }
func (o *BaseOp) Outputs() []string { return o.outputs }

func (o *BaseOp) SetInputDataTypes(types []schemapb.DataType) {
	o.inputDataTypes = append([]schemapb.DataType(nil), types...)
}

func (o *BaseOp) InputDataTypes() []schemapb.DataType { return o.inputDataTypes }

// ReadInputColumns reads all input columns declared in o.inputs from the DataFrame.
// It uses the same path resolution as all native operators. Returned columns
// are owned by the caller and must be released, including top-level columns.
func (o *BaseOp) ReadInputColumns(opName string, df *DataFrame) ([]*arrow.Chunked, error) {
	return o.readResolvedColumns(opName, df, memory.DefaultAllocator)
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
